# Jules was here
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

    def __init__(self, driver: Driver, new_version: str, import_dir: Path):
        self.driver = driver
        self.new_version = new_version
        self.import_dir = import_dir

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

        # This process is broken down into sequential queries to avoid transactional
        # complexities and ensure each step is handled atomically.
        for old_cui, new_cui in merges:
            # Check if the target node exists before attempting a merge
            with self.driver.session(database=settings.neo4j_database) as session:
                result = session.run("MATCH (c:Concept {cui: $cui}) RETURN c", cui=new_cui)
                if not result.single():
                    console.log(f"[bold yellow]Skipping merge for {old_cui} -> {new_cui}: Target CUI not found.[/bold yellow]")
                    continue

            params = {"old_cui": old_cui, "new_cui": new_cui, "version": self.new_version}

            # Step 1: Migrate outgoing relationships
            outgoing_rels_query = """
            MATCH (old:Concept {cui: $old_cui}), (new:Concept {cui: $new_cui})
            MATCH (old)-[r]->(target)
            WHERE elementId(target) <> elementId(new) AND type(r) <> 'HAS_CODE'
            WITH new, r, target
            CALL apoc.merge.relationship(new, type(r), {source_rela: r.source_rela}, {}, target) YIELD rel
            SET rel.asserted_by_sabs = apoc.coll.union(coalesce(rel.asserted_by_sabs, []), r.asserted_by_sabs),
                rel.last_seen_version = $version
            """
            self._run_query(outgoing_rels_query, params)

            # Step 2: Migrate incoming relationships
            incoming_rels_query = """
            MATCH (old:Concept {cui: $old_cui}), (new:Concept {cui: $new_cui})
            MATCH (source)-[r]->(old)
            WHERE elementId(source) <> elementId(new) AND type(r) <> 'HAS_CODE'
            WITH source, new, r
            CALL apoc.merge.relationship(source, type(r), {source_rela: r.source_rela}, {}, new) YIELD rel
            SET rel.asserted_by_sabs = apoc.coll.union(coalesce(rel.asserted_by_sabs, []), r.asserted_by_sabs),
                rel.last_seen_version = $version
            """
            self._run_query(incoming_rels_query, params)

            # Step 3: Migrate codes
            codes_query = """
            MATCH (old:Concept {cui: $old_cui}), (new:Concept {cui: $new_cui})
            MATCH (old)-[r:HAS_CODE]->(c:Code)
            MERGE (new)-[new_r:HAS_CODE]->(c)
            SET new_r.last_seen_version = $version
            """
            self._run_query(codes_query, params)

            # Step 4: Delete the old concept
            delete_query = "MATCH (c:Concept {cui: $old_cui}) DETACH DELETE c"
            self._run_query(delete_query, params)

        console.log(f"Processed {len(merges)} CUI merge operations.")

    def _read_csv_to_list(self, filename: str) -> list[dict]:
        """Reads a CSV file from the import directory into a list of dicts."""
        file_path = self.import_dir / filename
        if not file_path.exists():
            return []
        with file_path.open('r', encoding='utf-8') as f:
            # The transformer uses csv.DictWriter with standard quoting, so we need to handle the quotes here
            reader = csv.DictReader(f)
            return list(reader)

    def apply_additions_and_updates(self):
        """Applies all additions and updates from the new snapshot using a single transaction per file."""
        console.log("Applying additions and updates from new snapshot CSVs...")

        # NOTE: We are abandoning apoc.periodic.iterate for this method as it proves
        # unreliable with complex inner queries involving MERGE and procedure calls.
        # A simple UNWIND is transactionally safer for this logic, although it may
        # be less performant on extremely large files. Given the context of incremental
        # updates, this is a reasonable trade-off for correctness and reliability.

        # 1. Concepts
        concepts_data = self._read_csv_to_list("nodes_concepts.csv")
        if concepts_data:
            concepts_query = """
            UNWIND $rows AS row
            MERGE (c:Concept {cui: row["cui:ID(Concept-ID)"]})
            SET c += {preferred_name: row["preferred_name:string"], last_seen_version: $version}
            WITH c, row[":LABEL"] as labels
            CALL apoc.create.setLabels(c, apoc.text.split(labels, ";")) YIELD node
            RETURN count(node)
            """
            self._run_query(concepts_query, params={"rows": concepts_data, "version": self.new_version})

        # 2. Codes
        codes_data = self._read_csv_to_list("nodes_codes.csv")
        if codes_data:
            codes_query = """
            UNWIND $rows AS row
            MERGE (c:Code {code_id: row["code_id:ID(Code-ID)"]})
            SET c += {sab: row["sab:string"], name: row["name:string"], last_seen_version: $version}
            RETURN count(c)
            """
            self._run_query(codes_query, params={"rows": codes_data, "version": self.new_version})

        # 3. HAS_CODE Relationships
        has_code_data = self._read_csv_to_list("rels_has_code.csv")
        if has_code_data:
            has_code_rel_query = """
            UNWIND $rows AS row
            MATCH (start:Concept {cui: row[":START_ID(Concept-ID)"]})
            MATCH (end:Code {code_id: row[":END_ID(Code-ID)"]})
            MERGE (start)-[r:HAS_CODE]->(end)
            SET r.last_seen_version = $version
            RETURN count(r)
            """
            self._run_query(has_code_rel_query, params={"rows": has_code_data, "version": self.new_version})

        # 4. Inter-Concept Relationships
        inter_concept_data = self._read_csv_to_list("rels_inter_concept.csv")
        if inter_concept_data:
            inter_concept_rel_query = """
            UNWIND $rows AS row
            MATCH (start:Concept {cui: row[":START_ID(Concept-ID)"]})
            MATCH (end:Concept {cui: row[":END_ID(Concept-ID)"]})
            CALL apoc.merge.relationship(
                start,
                row[":TYPE"],
                { source_rela: row["source_rela:string"] },
                {},
                end
            ) YIELD rel
            SET rel.last_seen_version = $version,
                rel.asserted_by_sabs = apoc.coll.union(
                    coalesce(rel.asserted_by_sabs, []),
                    apoc.text.split(row["asserted_by_sabs:string[]"], ";")
                )
            RETURN count(rel)
            """
            self._run_query(inter_concept_rel_query, params={"rows": inter_concept_data, "version": self.new_version})

        console.log("[green]Finished applying additions and updates.[/green]")

    def remove_stale_entities(self):
        """Removes all entities not seen in the latest snapshot."""
        console.log(f"Removing stale entities (not seen in version {self.new_version})...")

        # 1. Remove stale relationships
        rel_cleanup_query = """
        CALL apoc.periodic.iterate(
          'MATCH ()-[r]-() WHERE r.last_seen_version <> $version OR r.last_seen_version IS NULL RETURN elementId(r) AS rel_id',
          'MATCH ()-[r]-() WHERE elementId(r) = rel_id DELETE r',
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
