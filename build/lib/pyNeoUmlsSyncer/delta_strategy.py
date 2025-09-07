import csv
from pathlib import Path
from neo4j import Driver
from rich.console import Console
from .config import settings

console = Console()

class DeltaStrategy:
    """
    Implements the "Snapshot Diff" incremental update strategy using APOC.
    """

    def __init__(self, driver: Driver, new_version: str, csv_dir: Path):
        self.driver = driver
        self.new_version = new_version
        self.csv_dir = csv_dir

    def _run_query(self, query: str, params: dict = None, db=None):
        """Helper to run a query within a session."""
        db = db or settings.neo4j_database
        self.driver.execute_query(query, parameters_=params, database_=db)

    def ensure_constraints(self):
        """Creates unique constraints for Concept and Code nodes."""
        console.log("Ensuring database constraints exist...")
        self._run_query("CREATE CONSTRAINT IF NOT EXISTS FOR (c:Concept) REQUIRE c.cui IS UNIQUE")
        self._run_query("CREATE CONSTRAINT IF NOT EXISTS FOR (c:Code) REQUIRE c.code_id IS UNIQUE")
        self._run_query("CREATE CONSTRAINT IF NOT EXISTS FOR (m:UMLS_Meta) REQUIRE m.version IS UNIQUE")
        console.log("[green]Constraints are in place.[/green]")

    def update_meta_node(self):
        """Updates the UMLS metadata node to the new version."""
        console.log(f"Updating metadata version to {self.new_version}...")
        self._run_query(
            "MERGE (m:UMLS_Meta {id: 'singleton'}) SET m.version = $version",
            params={"version": self.new_version}
        )
        console.log("[green]Metadata version updated.[/green]")

    def process_deleted_cuis(self, deleted_cui_file: Path):
        """Processes DELETEDCUI.RRF to remove deleted concepts."""
        if not deleted_cui_file.exists():
            console.log("[yellow]DELETEDCUI.RRF not found. Skipping deletion.[/yellow]")
            return

        console.log("Processing deleted CUIs...")
        cuis_to_delete = [row[0] for row in csv.reader(deleted_cui_file.open('r'), delimiter='|')]

        query = """
        CALL apoc.periodic.iterate(
          'UNWIND $cuis AS cui MATCH (c:Concept {cui: cui}) RETURN c',
          'DETACH DELETE c',
          {batchSize: $batchSize, parallel: false, params: {cuis: $cuis}}
        )
        """
        self._run_query(query, params={"cuis": cuis_to_delete, "batchSize": settings.apoc_batch_size})
        console.log(f"Submitted deletion task for {len(cuis_to_delete)} CUIs.")

    def process_merged_cuis(self, merged_cui_file: Path):
        """Processes MERGEDCUI.RRF to merge concepts."""
        if not merged_cui_file.exists():
            console.log("[yellow]MERGEDCUI.RRF not found. Skipping merges.[/yellow]")
            return

        console.log("Processing merged CUIs...")
        # MERGEDCUI.RRF contains CUI_OLD|CUI_NEW
        merges = [row for row in csv.reader(merged_cui_file.open('r'), delimiter='|')]

        # Using apoc.refactor.mergeNodes is the most robust way to handle this.
        # It correctly merges properties and migrates relationships.
        query = """
        CALL apoc.periodic.iterate(
          'UNWIND $merges AS merge_op
           MATCH (old:Concept {cui: merge_op[0]}), (new:Concept {cui: merge_op[1]})
           RETURN old, new',
          'CALL apoc.refactor.mergeNodes([old], new, {properties: "combine", mergeRels: true}) YIELD node RETURN count(*)',
          {batchSize: 100, parallel: false, params: {merges: $merges}}
        )
        """
        self._run_query(query, params={"merges": merges})
        console.log(f"Submitted merge task for {len(merges)} CUI pairs.")

    def apply_additions_and_updates(self):
        """Applies all additions and updates from the new snapshot using MERGE."""
        console.log("Applying additions and updates from new snapshot CSVs...")

        # Use apoc.load.csv to stream from the files.
        # Set last_seen_version on all nodes and rels.

        # 1. Concepts
        concepts_query = """
        CALL apoc.load.csv($url, {header:true}) YIELD map AS row
        MERGE (c:Concept {cui: row['cui:ID(Concept-ID)']})
        ON CREATE SET
            c.preferred_name = row['preferred_name:string'],
            c.last_seen_version = $version
        ON MATCH SET
            c.preferred_name = row['preferred_name:string'],
            c.last_seen_version = $version
        // Set labels using apoc.create.addLabels
        WITH c, row[':LABEL'] as labels
        CALL apoc.create.addLabels(c, apoc.text.split(labels, ';')) YIELD node
        RETURN count(*)
        """
        self._run_query(concepts_query, params={"url": (self.csv_dir / "nodes_concepts.csv").as_uri(), "version": self.new_version})

        # 2. Codes
        codes_query = """
        CALL apoc.load.csv($url, {header:true}) YIELD map AS row
        MERGE (c:Code {code_id: row['code_id:ID(Code-ID)']})
        ON CREATE SET c.sab = row['sab:string'], c.name = row['name:string'], c.last_seen_version = $version
        ON MATCH SET c.sab = row['sab:string'], c.name = row['name:string'], c.last_seen_version = $version
        RETURN count(*)
        """
        self._run_query(codes_query, params={"url": (self.csv_dir / "nodes_codes.csv").as_uri(), "version": self.new_version})

        # 3. HAS_CODE Relationships
        has_code_rel_query = """
        CALL apoc.load.csv($url, {header:true}) YIELD map AS row
        MATCH (start:Concept {cui: row[':START_ID(Concept-ID)']})
        MATCH (end:Code {code_id: row[':END_ID(Code-ID)']})
        MERGE (start)-[r:HAS_CODE]->(end)
        ON CREATE SET r.last_seen_version = $version
        ON MATCH SET r.last_seen_version = $version
        RETURN count(*)
        """
        self._run_query(has_code_rel_query, params={"url": (self.csv_dir / "rels_has_code.csv").as_uri(), "version": self.new_version})

        # 4. Inter-Concept Relationships
        # This query correctly uses apoc.merge.relationship to create relationships
        # with dynamic types idempotently, fulfilling the FRD requirements.
        inter_concept_rel_query = """
        CALL apoc.periodic.iterate(
        'CALL apoc.load.csv($url, {header:true}) YIELD map AS row RETURN row',
        '
            MATCH (start:Concept {cui: row[":START_ID(Concept-ID)"]})
            MATCH (end:Concept {cui: row[":END_ID(Concept-ID)"]})

            // Use apoc.merge.relationship to create a dynamic relationship type
            // The relationship is uniquely identified by its source_rela property.
            CALL apoc.merge.relationship(
                start,
                row[":TYPE"],
                { source_rela: row["source_rela:string"] }, // Identity properties
                { // Properties to set on create or match
                    last_seen_version: $version,
                    asserted_by_sabs: apoc.text.split(row["asserted_by_sabs:string[]"], ";")
                },
                end
            ) YIELD rel
            RETURN count(*)
        ', {batchSize: $batchSize, parallel: false, params: {url: $url, version: $version, batchSize: $apocBatchSize}})
        """
        self._run_query(inter_concept_rel_query, params={
            "url": (self.csv_dir / "rels_inter_concept.csv").as_uri(),
            "version": self.new_version,
            "apocBatchSize": settings.apoc_batch_size,
            "batchSize": 1000  # Outer batch size for iterate
        })

        console.log("[green]Finished applying additions and updates.[/green]")

    def remove_stale_entities(self):
        """Removes all entities not seen in the latest snapshot."""
        console.log(f"Removing stale entities (not seen in version {self.new_version})...")

        # 1. Remove stale relationships
        rel_cleanup_query = """
        CALL apoc.periodic.iterate(
          'MATCH ()-[r]-() WHERE r.last_seen_version <> $version OR r.last_seen_version IS NULL RETURN id(r) AS rel_id',
          'MATCH ()-[r]-() WHERE id(r) = rel_id DELETE r',
          {batchSize: $batchSize, parallel: false, params: {version: $version}}
        )
        """
        self._run_query(rel_cleanup_query, params={"version": self.new_version, "batchSize": settings.apoc_batch_size})
        console.log("Stale relationship cleanup task submitted.")

        # 2. Remove stale Code nodes
        code_cleanup_query = """
        CALL apoc.periodic.iterate(
          'MATCH (c:Code) WHERE c.last_seen_version <> $version OR c.last_seen_version IS NULL RETURN c',
          'DETACH DELETE c',
          {batchSize: $batchSize, parallel: false, params: {version: $version}}
        )
        """
        self._run_query(code_cleanup_query, params={"version": self.new_version, "batchSize": settings.apoc_batch_size})
        console.log("Stale Code node cleanup task submitted.")

        # Note: We do not remove stale :Concept nodes here. Their lifecycle is managed
        # exclusively by the DELETEDCUI and MERGEDCUI files to prevent accidental data loss.
        console.log("[green]Stale entity cleanup complete.[/green]")
