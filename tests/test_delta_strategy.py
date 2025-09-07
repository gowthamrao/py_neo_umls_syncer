import pytest
from neo4j import Driver
from pathlib import Path
import csv

from pyNeoUmlsSyncer.delta_strategy import DeltaStrategy

def _create_file(path: Path, content: list[list[str]]):
    """Helper to create a pipe-delimited file for tests."""
    with open(path, 'w', newline='') as f:
        writer = csv.writer(f, delimiter='|')
        writer.writerows(content)

def test_merged_cui_logic(neo4j_driver: Driver, tmp_path: Path):
    """
    Tests the MERGEDCUI logic thoroughly, verifying relationship migration
    and provenance merging as per the FRD.
    """
    # 1. SETUP: Create the initial graph state in the test Neo4j instance.
    with neo4j_driver.session() as session:
        session.run("""
        // Old Concept (to be merged)
        CREATE (old:Concept {cui: 'C001', preferred_name: 'Old Name'})
        CREATE (old)-[:HAS_CODE]->(:Code {code_id: 'SAB1:123', sab: 'SAB1', name: 'Old Code Name'})

        // New Concept (target of merge)
        CREATE (new:Concept {cui: 'C002', preferred_name: 'New Name'})
        CREATE (new)-[:HAS_CODE]->(:Code {code_id: 'SAB2:456', sab: 'SAB2', name: 'New Code Name'})

        // Related concepts for relationship testing
        CREATE (rel_target:Concept {cui: 'C003', preferred_name: 'Target Concept'})
        CREATE (rel_source:Concept {cui: 'C004', preferred_name: 'Source Concept'})

        // Relationships to be migrated and merged
        // Outgoing relationship from 'old' that conflicts with 'new'
        CREATE (old)-[:RELATED {source_rela: 'treats', asserted_by_sabs: ['SAB1']}]->(rel_target)
        // Outgoing relationship from 'new' that conflicts with 'old'
        CREATE (new)-[:RELATED {source_rela: 'treats', asserted_by_sabs: ['SAB2']}]->(rel_target)

        // Unique incoming relationship to 'old'
        CREATE (rel_source)-[:RELATED {source_rela: 'associated_with', asserted_by_sabs: ['SAB3']}]->(old)
        """)

    # Create a mock MERGEDCUI.RRF file
    merged_cui_file = tmp_path / "MERGEDCUI.RRF"
    _create_file(merged_cui_file, [['C001', 'C002']]) # Old CUI | New CUI

    # 2. EXECUTE: Run the delta strategy for merging CUIs.
    # We pass a dummy csv_dir as it's not used by this specific method.
    strategy = DeltaStrategy(driver=neo4j_driver, new_version="test", csv_dir=tmp_path)
    strategy.process_merged_cuis(merged_cui_file)

    # 3. ASSERT: Verify the graph is in the correct final state.
    with neo4j_driver.session() as session:
        # Assert Old concept is deleted
        old_concept_result = session.run("MATCH (c:Concept {cui: 'C001'}) RETURN c").single()
        assert old_concept_result is None, "Old concept C001 should be deleted."

        # Assert codes are migrated
        new_concept_codes = session.run("""
            MATCH (c:Concept {cui: 'C002'})-[:HAS_CODE]->(code:Code)
            RETURN collect(code.code_id) as code_ids
        """).single()
        assert new_concept_codes is not None
        # Use a set to ignore order
        assert set(new_concept_codes['code_ids']) == {'SAB1:123', 'SAB2:456'}

        # Assert relationship migration and provenance merge
        merged_rel_result = session.run("""
            MATCH (c:Concept {cui: 'C002'})-[r:RELATED]->(t:Concept {cui: 'C003'})
            WHERE r.source_rela = 'treats'
            RETURN r.asserted_by_sabs as sabs
        """).single()
        assert merged_rel_result is not None, "Merged relationship should exist."
        # apoc.refactor.mergeNodes combines lists, so we check for both SABs
        assert 'SAB1' in merged_rel_result['sabs']
        assert 'SAB2' in merged_rel_result['sabs']

        # Assert unique incoming relationship was migrated
        migrated_incoming_rel = session.run("""
            MATCH (s:Concept {cui: 'C004'})-[r:RELATED]->(c:Concept {cui: 'C002'})
            WHERE r.source_rela = 'associated_with'
            RETURN r
        """).single()
        assert migrated_incoming_rel is not None, "Incoming relationship should be migrated to new concept."


def test_stale_entity_deletion(neo4j_driver: Driver, tmp_path: Path):
    """
    Tests the Snapshot Diff strategy to ensure stale entities are correctly deleted.
    """
    # 1. SETUP: Create entities with different `last_seen_version` tags.
    with neo4j_driver.session() as session:
        session.run("""
        // Stale entities (should be deleted)
        CREATE (stale_concept:Concept {cui: 'C001', last_seen_version: 'v1'})
        CREATE (stale_code:Code {code_id: 'SAB1:123', last_seen_version: 'v1'})
        CREATE (stale_concept)-[r1:HAS_CODE {last_seen_version: 'v1'}]->(stale_code)

        // Kept entities (should remain)
        CREATE (kept_concept:Concept {cui: 'C002', last_seen_version: 'v2'})
        CREATE (kept_code:Code {code_id: 'SAB2:456', last_seen_version: 'v2'})
        CREATE (kept_concept)-[r2:HAS_CODE {last_seen_version: 'v2'}]->(kept_code)

        // A stale relationship between two kept concepts
        CREATE (kept_concept)-[r3:RELATED {last_seen_version: 'v1'}]->(stale_concept)
        """)

    # 2. EXECUTE: Run the stale entity removal process for 'v2'.
    strategy = DeltaStrategy(driver=neo4j_driver, new_version="v2", csv_dir=tmp_path)
    strategy.remove_stale_entities()

    # 3. ASSERT: Verify the correct entities were deleted.
    with neo4j_driver.session() as session:
        # Assert stale nodes are gone
        stale_code_count = session.run("MATCH (c:Code {code_id: 'SAB1:123'}) RETURN count(c) as count").single()['count']
        assert stale_code_count == 0, "Stale Code node should be deleted."

        # Note: Per our strategy, Concepts are only deleted via DELETEDCUI, not the stale cleanup.
        # So, C001 should *remain* but be detached.
        stale_concept_exists = session.run("MATCH (c:Concept {cui: 'C001'}) RETURN c").single() is not None
        assert stale_concept_exists, "Stale Concept node should NOT be deleted by this process."

        # Assert stale relationships are gone
        stale_rel_count = session.run("MATCH ()-[r]-() WHERE r.last_seen_version = 'v1' RETURN count(r) as count").single()['count']
        assert stale_rel_count == 0, "All relationships with last_seen_version 'v1' should be deleted."

        # Assert kept nodes and rels still exist
        kept_code_count = session.run("MATCH (c:Code {code_id: 'SAB2:456'}) RETURN count(c) as count").single()['count']
        assert kept_code_count == 1, "Kept Code node should still exist."

        kept_rel_count = session.run("MATCH ()-[r]-() WHERE r.last_seen_version = 'v2' RETURN count(r) as count").single()['count']
        assert kept_rel_count == 1, "Relationship with last_seen_version 'v2' should still exist."
