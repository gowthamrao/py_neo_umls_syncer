# Copyright (c) 2025-2026 Gowtham Adamane Rao. All Rights Reserved.
#
# Licensed under the Prosperity Public License 3.0.0 (the "License").
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License in the LICENSE file at the root
# of this repository, or at: https://prosperitylicense.com/versions/3.0.0
#
# Commercial use beyond a 30-day trial requires a separate license.
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
        """Processes MERGEDCUI.RRF to merge concepts in a batched, idempotent manner."""
        if not merged_cui_file.exists():
            console.log("[yellow]MERGEDCUI.RRF not found. Skipping merges.[/yellow]")
            return

        console.log("Processing merged CUIs using a batched approach...")
        merges = [{"old_cui": row[0], "new_cui": row[1]} for row in csv.reader(merged_cui_file.open('r'), delimiter='|') if row and len(row) == 2]

        if not merges:
            console.log("No valid merge operations found in MERGEDCUI.RRF.")
            return

        inner_query = """
            MATCH (old:Concept {cui: merge_op.old_cui})
            MERGE (new:Concept {cui: merge_op.new_cui})
                ON CREATE SET new.last_seen_version = $version
            WITH old, new
            // Collect relationships before modification to avoid concurrent modification issues
            WITH old, new,
                 [(old)-[r:HAS_CODE]->(c:Code) | r] as has_code_rels,
                 [(old)-[r]->(t:Concept) WHERE NOT type(r) = 'HAS_CODE' | r] as outgoing_rels,
                 [(s:Concept)-[r]->(old) WHERE NOT type(r) = 'HAS_CODE' | r] as incoming_rels
            // Process collected relationships
            FOREACH (r IN has_code_rels |
                MERGE (new)-[new_r:HAS_CODE]->(endNode(r))
                SET new_r.last_seen_version = $version
            )
            FOREACH (r IN outgoing_rels |
                CALL apoc.merge.relationship(
                    new, apoc.relation.type(r), {source_rela: r.source_rela},
                    {asserted_by_sabs: r.asserted_by_sabs, last_seen_version: $version},
                    endNode(r),
                    {last_seen_version: $version, asserted_by_sabs: apoc.coll.union(coalesce(r.asserted_by_sabs, []), r.asserted_by_sabs)}
                ) YIELD rel
                SET rel.asserted_by_sabs = apoc.coll.union(coalesce(rel.asserted_by_sabs, []), r.asserted_by_sabs)
            )
            FOREACH (r IN incoming_rels |
                CALL apoc.merge.relationship(
                    startNode(r), apoc.relation.type(r), {source_rela: r.source_rela},
                    {asserted_by_sabs: r.asserted_by_sabs, last_seen_version: $version},
                    new,
                    {last_seen_version: $version, asserted_by_sabs: apoc.coll.union(coalesce(r.asserted_by_sabs, []), r.asserted_by_sabs)}
                ) YIELD rel
                SET rel.asserted_by_sabs = apoc.coll.union(coalesce(rel.asserted_by_sabs, []), r.asserted_by_sabs)
            )
            // Finally, delete the old concept
            DETACH DELETE old
        """

        outer_query = f"""
        CALL apoc.periodic.iterate(
          "UNWIND $merges AS merge_op RETURN merge_op",
          "{inner_query.replace('"', '\\"')}",
          {{
            batchSize: 100,
            parallel: false,
            params: {{ merges: $merges, version: $version }}
          }}
        )
        """
        self._run_query(outer_query, params={"merges": merges, "version": self.new_version})
        console.log(f"Submitted batch merge task for {len(merges)} CUIs.")

    def apply_additions_and_updates(self):
        """
        Applies all additions and updates from the new snapshot using `apoc.load.csv`
        for a scalable, low-memory, batched import process.
        """
        console.log("Applying additions and updates from new snapshot CSVs...")
        base_params = {"version": self.new_version, "batchSize": settings.apoc_batch_size}

        # 1. Concepts
        concepts_csv_path = "nodes_concepts.csv"
        if (self.import_dir / concepts_csv_path).exists():
            console.log(f"Loading {concepts_csv_path}...")
            query = f"""
            CALL apoc.periodic.iterate(
              'CALL apoc.load.csv("file:///{concepts_csv_path}", {{header:true}}) YIELD map AS row RETURN row',
              '
                MERGE (c:Concept {{cui: row["cui:ID(Concept-ID)"]}})
                SET c += {{preferred_name: row["preferred_name:string"], last_seen_version: $version}}
                WITH c, row[":LABEL"] as labels
                CALL apoc.create.setLabels(c, apoc.text.split(labels, ";")) YIELD node
              ',
              {{batchSize: $batchSize, parallel: false, params: {{version: $version}}}}
            )
            """
            self._run_query(query, params=base_params)

        # 2. Codes
        codes_csv_path = "nodes_codes.csv"
        if (self.import_dir / codes_csv_path).exists():
            console.log(f"Loading {codes_csv_path}...")
            query = f"""
            CALL apoc.periodic.iterate(
              'CALL apoc.load.csv("file:///{codes_csv_path}", {{header:true}}) YIELD map AS row RETURN row',
              '
                MERGE (c:Code {{code_id: row["code_id:ID(Code-ID)"]}})
                SET c += {{sab: row["sab:string"], name: row["name:string"], last_seen_version: $version}}
              ',
              {{batchSize: $batchSize, parallel: false, params: {{version: $version}}}}
            )
            """
            self._run_query(query, params=base_params)

        # 3. HAS_CODE Relationships
        has_code_csv_path = "rels_has_code.csv"
        if (self.import_dir / has_code_csv_path).exists():
            console.log(f"Loading {has_code_csv_path}...")
            query = f"""
            CALL apoc.periodic.iterate(
              'CALL apoc.load.csv("file:///{has_code_csv_path}", {{header:true}}) YIELD map AS row RETURN row',
              '
                MATCH (start:Concept {{cui: row[":START_ID(Concept-ID)"]}})
                MATCH (end:Code {{code_id: row[":END_ID(Code-ID)"]}})
                MERGE (start)-[r:HAS_CODE]->(end)
                SET r.last_seen_version = $version
              ',
              {{batchSize: $batchSize, parallel: false, params: {{version: $version}}}}
            )
            """
            self._run_query(query, params=base_params)

        # 4. Inter-Concept Relationships
        inter_concept_csv_path = "rels_inter_concept.csv"
        if (self.import_dir / inter_concept_csv_path).exists():
            console.log(f"Loading {inter_concept_csv_path}...")
            query = f"""
            CALL apoc.periodic.iterate(
              'CALL apoc.load.csv("file:///{inter_concept_csv_path}", {{header:true}}) YIELD map AS row RETURN row',
              '
                MATCH (start:Concept {{cui: row[":START_ID(Concept-ID)"]}})
                MATCH (end:Concept {{cui: row[":END_ID(Concept-ID)"]}})
                CALL apoc.merge.relationship(
                    start,
                    row[":TYPE"],
                    {{ source_rela: row["source_rela:string"] }},
                    {{}},
                    end
                ) YIELD rel
                SET rel.last_seen_version = $version,
                    rel.asserted_by_sabs = apoc.coll.union(
                        coalesce(rel.asserted_by_sabs, []),
                        apoc.text.split(row["asserted_by_sabs:string[]"], ";")
                    )
              ',
              {{batchSize: $batchSize, parallel: false, params: {{version: $version}}}}
            )
            """
            self._run_query(query, params=base_params)

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
