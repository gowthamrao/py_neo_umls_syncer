"""
Orchestration of the Incremental "Snapshot Diff" Update Strategy.

This module contains the core logic for performing an incremental update
of the Neo4j database to a new UMLS version.
"""
import logging
from pathlib import Path
from typing import List, Dict
from collections import defaultdict

from .loader import Neo4jLoader
from .config import settings
from .models import Concept, Code, HasCodeRelationship, ConceptRelationship

logger = logging.getLogger(__name__)

class DeltaStrategy:
    """
    Implements the full incremental update workflow.
    """
    def __init__(self, loader: Neo4jLoader, rrf_path: Path, new_version: str):
        self.loader = loader
        self.rrf_path = rrf_path
        self.new_version = new_version

    def run_incremental_update(
        self,
        concepts: List[Concept],
        codes: List[Code],
        has_code_rels: List[HasCodeRelationship],
        concept_rels: List[ConceptRelationship]
    ):
        """
        Executes the full incremental update process.
        """
        logger.info(f"Starting incremental update to version {self.new_version}...")

        # Step 1: Handle CUI identity changes from UMLS change files.
        self._process_deleted_cuis()
        self._process_merged_cuis()

        # Step 2: Apply all additions and updates from the new snapshot.
        self._apply_snapshot_updates(concepts, codes, has_code_rels, concept_rels)

        # Step 3: Remove all entities not present in the new snapshot.
        self._remove_stale_entities()

        # Step 4: Update the database metadata to the new version.
        self.loader.update_umls_version(self.new_version)

        logger.info(f"Incremental update to version {self.new_version} has completed successfully.")

    def _read_change_file(self, file_name: str) -> List[List[str]]:
        """Helper to read simple pipe-delimited change files like DELETEDCUI.RRF."""
        file_path = self.rrf_path / file_name
        if not file_path.exists():
            logger.warning(f"Change file not found, skipping processing for: {file_path}")
            return []
        with open(file_path, 'r', encoding='utf-8') as f:
            return [line.strip().split('|') for line in f if line.strip()]

    def _process_deleted_cuis(self):
        """Processes DELETEDCUI.RRF to remove concepts."""
        deleted_rows = self._read_change_file("DELETEDCUI.RRF")
        if not deleted_rows:
            return

        cuis_to_delete = [{'cui': row[0]} for row in deleted_rows if row]
        logger.info(f"Processing {len(cuis_to_delete)} CUI deletions from DELETEDCUI.RRF.")

        inner_query = """
        MATCH (c:Concept {cui: row.cui})
        // Find all codes connected ONLY to this concept
        OPTIONAL MATCH (c)-[:HAS_CODE]->(code:Code)
        WHERE size((code)--()) = 1
        DETACH DELETE c, code
        """
        self.loader.execute_apoc_iterate(inner_query, cuis_to_delete, "Deleting CUIs")

    def _process_merged_cuis(self):
        """Processes MERGEDCUI.RRF to merge concepts."""
        merged_rows = self._read_change_file("MERGEDCUI.RRF")
        if not merged_rows:
            return

        # row[0] is old CUI, row[1] is new CUI
        merges = [{'old_cui': row[0], 'new_cui': row[1]} for row in merged_rows if len(row) > 1]
        logger.info(f"Processing {len(merges)} CUI merges from MERGEDCUI.RRF.")

        # Use apoc.refactor.mergeNodes for robust merging of nodes and relationships.
        # It combines properties and moves relationships before deleting the old node.
        inner_query = """
        MATCH (old:Concept {cui: row.old_cui})
        MATCH (new:Concept {cui: row.new_cui})
        CALL apoc.refactor.mergeNodes([old, new], {
            properties: 'combine',
            mergeRels: true
        }) YIELD node
        RETURN count(*)
        """
        self.loader.execute_apoc_iterate(inner_query, merges, "Merging CUIs")

    def _apply_snapshot_updates(
        self,
        concepts: List[Concept],
        codes: List[Code],
        has_code_rels: List[HasCodeRelationship],
        concept_rels: List[ConceptRelationship]
    ):
        """Merges all nodes and relationships from the new UMLS snapshot into the DB."""
        logger.info("Applying snapshot updates: merging concepts, codes, and relationships.")

        # 1. Merge Concept nodes
        concept_rows = [c.dict() for c in concepts]
        concept_query = """
        MERGE (c:Concept {cui: row.cui})
        ON CREATE SET c.preferred_name = row.preferred_name,
                      c.last_seen_version = row.last_seen_version
        ON MATCH SET c.preferred_name = row.preferred_name,
                     c.last_seen_version = row.last_seen_version
        // Remove all old labels before setting new ones to handle category changes
        CALL apoc.create.removeLabels(c, [l IN labels(c) WHERE l <> 'Concept']) YIELD node
        CALL apoc.create.addLabels(node, row.biolink_categories) YIELD node as final_node
        """
        self.loader.execute_apoc_iterate(concept_query, concept_rows, "Merging Concepts")

        # 2. Merge Code nodes
        code_rows = [c.dict() for c in codes]
        code_query = """
        MERGE (c:Code {code_id: row.code_id})
        ON CREATE SET c.sab = row.sab, c.name = row.name, c.last_seen_version = row.last_seen_version
        ON MATCH SET c.name = row.name, c.last_seen_version = row.last_seen_version
        """
        self.loader.execute_apoc_iterate(code_query, code_rows, "Merging Codes")

        # 3. Merge HAS_CODE relationships
        has_code_rows = [r.dict() for r in has_code_rels]
        has_code_query = """
        MATCH (con:Concept {cui: row.cui})
        MATCH (cod:Code {code_id: row.code_id})
        MERGE (con)-[:HAS_CODE]->(cod)
        """
        self.loader.execute_apoc_iterate(has_code_query, has_code_rows, "Merging HAS_CODE relationships")

        # 4. Merge Concept-Concept relationships, grouped by type
        grouped_rels = defaultdict(list)
        for rel in concept_rels:
            grouped_rels[rel.rel_type].append(rel.dict())

        for rel_type, rows in grouped_rels.items():
            escaped_rel_type = f"`{rel_type}`"
            rel_query = f"""
            MATCH (src:Concept {{cui: row.source_cui}})
            MATCH (tgt:Concept {{cui: row.target_cui}})
            MERGE (src)-[r:{escaped_rel_type}]->(tgt)
            ON CREATE SET r.source_rela = row.source_rela,
                          r.asserted_by_sabs = row.asserted_by_sabs,
                          r.last_seen_version = row.last_seen_version
            ON MATCH SET r.asserted_by_sabs = row.asserted_by_sabs,
                         r.last_seen_version = row.last_seen_version
            """
            self.loader.execute_apoc_iterate(rel_query, rows, f"Merging {rel_type} relationships")

    def _remove_stale_entities(self):
        """Removes all entities with a last_seen_version older than the new version."""
        logger.info(f"Removing stale entities (version < {self.new_version})...")

        # Using a single APOC query for each phase is more efficient
        params = {'new_version': self.new_version, 'batchSize': settings.apoc_batch_size * 5}

        # Remove stale relationships first
        logger.info("Removing stale relationships...")
        rel_cleanup_query = """
        CALL apoc.periodic.iterate(
            "MATCH ()-[r]-() WHERE r.last_seen_version < $new_version RETURN r",
            "DELETE r",
            {batchSize: $batchSize, parallel: false}
        )
        """
        self.loader.driver.execute_query(rel_cleanup_query, **params, database_=self.loader.database)

        # Remove stale Code nodes (now disconnected)
        logger.info("Removing stale Code nodes...")
        code_cleanup_query = """
        CALL apoc.periodic.iterate(
            "MATCH (c:Code) WHERE c.last_seen_version < $new_version AND size((c)--()) = 0 RETURN c",
            "DELETE c",
            {batchSize: $batchSize, parallel: false}
        )
        """
        self.loader.driver.execute_query(code_cleanup_query, **params, database_=self.loader.database)

        # Remove stale Concept nodes (now disconnected)
        logger.info("Removing stale Concept nodes...")
        concept_cleanup_query = """
        CALL apoc.periodic.iterate(
            "MATCH (c:Concept) WHERE c.last_seen_version < $new_version AND size((c)--()) = 0 RETURN c",
            "DELETE c",
            {batchSize: $batchSize, parallel: false}
        )
        """
        self.loader.driver.execute_query(concept_cleanup_query, **params, database_=self.loader.database)
        logger.info("Stale entity removal complete.")
