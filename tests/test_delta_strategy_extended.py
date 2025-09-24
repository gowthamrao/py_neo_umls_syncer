# Copyright (c) 2025-2026 Gowtham Adamane Rao. All Rights Reserved.
#
# Licensed under the Prosperity Public License 3.0.0 (the "License").
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License in the LICENSE file at the root
# of this repository, or at: https://prosperitylicense.com/versions/3.0.0
#
# Commercial use beyond a 30-day trial requires a separate license.
# tests/test_delta_strategy_extended.py

import pytest
from neo4j import Driver
from pathlib import Path
import csv

from py_neo_umls_syncer.delta_strategy import DeltaStrategy

def _create_pipe_delimited_file(path: Path, content: list[list[str]]):
    """Helper to create a pipe-delimited file for tests."""
    with open(path, 'w', newline='') as f:
        writer = csv.writer(f, delimiter='|')
        writer.writerows(content)

def test_chain_merge_logic(neo4j_driver: Driver, test_csv_dir: Path):
    """
    Tests a multi-step merge (CUI1 -> CUI2, then CUI2 -> CUI3) to ensure
    relationships are correctly migrated through the chain.
    """
    # 1. SETUP: Create the initial graph state.
    with neo4j_driver.session() as session:
        session.run("CREATE (:Concept {cui: 'CUI1', preferred_name: 'Concept 1'})-[:REL_A {source_rela: 'rel_a'}]->(:Target {id: 'T1'})")
        session.run("CREATE (:Concept {cui: 'CUI2', preferred_name: 'Concept 2'})-[:REL_B {source_rela: 'rel_b'}]->(:Target {id: 'T2'})")
        session.run("CREATE (:Concept {cui: 'CUI3', preferred_name: 'Concept 3'})")

    strategy = DeltaStrategy(driver=neo4j_driver, new_version="test", import_dir=test_csv_dir)

    # 2. ACTION 1: Merge CUI1 into CUI2
    merged_cui_file_1 = test_csv_dir / "MERGEDCUI_1.RRF"
    _create_pipe_delimited_file(merged_cui_file_1, [['CUI1', 'CUI2']])
    strategy.process_merged_cuis(merged_cui_file_1)

    # 3. ASSERT 1: Check intermediate state
    with neo4j_driver.session() as session:
        assert session.run("MATCH (c:Concept {cui: 'CUI1'}) RETURN c").single() is None
        # CUI2 should now have both REL_A and REL_B
        rels_on_cui2 = session.run("""
            MATCH (c:Concept {cui: 'CUI2'})-[r]->(t)
            RETURN type(r) as rel_type
        """).data()
        assert {'REL_A', 'REL_B'} == {r['rel_type'] for r in rels_on_cui2}

    # 4. ACTION 2: Merge CUI2 into CUI3
    merged_cui_file_2 = test_csv_dir / "MERGEDCUI_2.RRF"
    _create_pipe_delimited_file(merged_cui_file_2, [['CUI2', 'CUI3']])
    strategy.process_merged_cuis(merged_cui_file_2)

    # 5. ASSERT 2: Check final state
    with neo4j_driver.session() as session:
        assert session.run("MATCH (c:Concept {cui: 'CUI2'}) RETURN c").single() is None
        # CUI3 should now have both REL_A and REL_B
        rels_on_cui3 = session.run("""
            MATCH (c:Concept {cui: 'CUI3'})-[r]->(t)
            RETURN type(r) as rel_type
        """).data()
        assert {'REL_A', 'REL_B'} == {r['rel_type'] for r in rels_on_cui3}


def test_merge_to_non_existent_node(neo4j_driver: Driver, test_csv_dir: Path):
    """
    Tests that a merge operation fails gracefully if the target CUI does not exist.
    The source node should NOT be deleted.
    """
    # 1. SETUP: Create the source concept but not the target.
    with neo4j_driver.session() as session:
        session.run("CREATE (:Concept {cui: 'CUI1', preferred_name: 'Concept 1'})-[:REL_A {source_rela: 'rel_a'}]->(:Target {id: 'T1'})")
        initial_node_count = session.run("MATCH (n) RETURN count(n) as count").single()['count']

    strategy = DeltaStrategy(driver=neo4j_driver, new_version="test", import_dir=test_csv_dir)

    # 2. ACTION: Attempt to merge CUI1 into a non-existent CUI2
    merged_cui_file = test_csv_dir / "MERGEDCUI.RRF"
    _create_pipe_delimited_file(merged_cui_file, [['CUI1', 'CUI2']]) # CUI2 does not exist

    # The query should find no matching nodes and complete gracefully without error.
    strategy.process_merged_cuis(merged_cui_file)


    # 3. ASSERT: Verify that the original node was NOT deleted.
    with neo4j_driver.session() as session:
        assert session.run("MATCH (c:Concept {cui: 'CUI1'}) RETURN c").single() is not None
        final_node_count = session.run("MATCH (n) RETURN count(n) as count").single()['count']
        assert final_node_count == initial_node_count


def test_complex_provenance_merge(neo4j_driver: Driver, test_csv_dir: Path):
    """
    Tests that relationship properties (especially provenance) are merged correctly
    when a relationship of the same type exists on both the source and target nodes.
    """
    # 1. SETUP: Create a complex scenario
    with neo4j_driver.session() as session:
        session.run("CREATE (c1:Concept {cui: 'CUI1'})")
        session.run("CREATE (c2:Concept {cui: 'CUI2'})")
        session.run("CREATE (t1:Concept {cui: 'TARGET1'})")
        session.run("CREATE (t2:Concept {cui: 'TARGET2'})")
        session.run("CREATE (c1)-[:TREATS {source_rela: 'treats', asserted_by_sabs: ['SAB_A']}]->(t1)")
        session.run("CREATE (c2)-[:TREATS {source_rela: 'treats', asserted_by_sabs: ['SAB_B']}]->(t1)")
        session.run("CREATE (c1)-[:AFFECTS {source_rela: 'affects', asserted_by_sabs: ['SAB_C']}]->(t2)")

    strategy = DeltaStrategy(driver=neo4j_driver, new_version="test", import_dir=test_csv_dir)

    # 2. ACTION: Merge CUI1 into CUI2
    merged_cui_file = test_csv_dir / "MERGEDCUI.RRF"
    _create_pipe_delimited_file(merged_cui_file, [['CUI1', 'CUI2']])
    strategy.process_merged_cuis(merged_cui_file)

    # 3. ASSERT: Check the final state of CUI2's relationships
    with neo4j_driver.session() as session:
        assert session.run("MATCH (c:Concept {cui: 'CUI1'}) RETURN c").single() is None

        # Check the merged :TREATS relationship
        treats_rel = session.run("""
            MATCH (:Concept {cui: 'CUI2'})-[r:TREATS]->(:Concept {cui: 'TARGET1'})
            RETURN r.asserted_by_sabs as sabs
        """).data()
        assert len(treats_rel) == 1 # Should be one merged relationship
        assert set(treats_rel[0]['sabs']) == {'SAB_A', 'SAB_B'}

        # Check the migrated :AFFECTS relationship
        affects_rel = session.run("""
            MATCH (:Concept {cui: 'CUI2'})-[r:AFFECTS]->(:Concept {cui: 'TARGET2'})
            RETURN r.asserted_by_sabs as sabs
        """).data()
        assert len(affects_rel) == 1
        assert set(affects_rel[0]['sabs']) == {'SAB_C'}


def test_snapshot_diff_logic(neo4j_driver: Driver, test_csv_dir: Path):
    """
    Tests the stale entity removal logic of the snapshot diff strategy.
    - Stale relationships should be removed.
    - Stale :Code nodes should be removed.
    - Stale :Concept nodes should NOT be removed, as their lifecycle is
      managed by DELETEDCUI/MERGEDCUI files.
    """
    # 1. SETUP: Create a graph representing the old state (v1)
    with neo4j_driver.session() as session:
        session.run("CREATE (c1:Concept {cui: 'CUI1', last_seen_version: 'v1'})")
        session.run("CREATE (code1:Code {code_id: 'CODE1', last_seen_version: 'v1'})")
        session.run("CREATE (c1)-[:HAS_CODE {last_seen_version: 'v1'}]->(code1)")
        session.run("CREATE (c2:Concept {cui: 'CUI2', last_seen_version: 'v1'})")
        session.run("CREATE (code2:Code {code_id: 'CODE2', last_seen_version: 'v1'})")
        session.run("CREATE (c2)-[:HAS_CODE {last_seen_version: 'v1'}]->(code2)")
        session.run("CREATE (c1)-[:RELATED_TO {source_rela: 'RELATED_TO', last_seen_version: 'v1'}]->(c2)")
        session.run("CREATE (c3_stale:Concept {cui: 'CUI3', last_seen_version: 'v1'})")


    # 2. SIMULATE V2 UPDATE: Manually update the `last_seen_version` for entities
    # that are supposed to exist in the new snapshot.
    with neo4j_driver.session() as session:
        session.run("MATCH (c:Concept {cui: 'CUI1'}) SET c.last_seen_version = 'v2'")
        session.run("MATCH (c:Code {code_id: 'CODE1'}) SET c.last_seen_version = 'v2'")
        session.run("MATCH (:Concept {cui: 'CUI1'})-[r:HAS_CODE]->(:Code {code_id: 'CODE1'}) SET r.last_seen_version = 'v2'")
        session.run("MATCH (c:Concept {cui: 'CUI2'}) SET c.last_seen_version = 'v2'")

    # 3. EXECUTE: Run the stale entity removal process.
    strategy = DeltaStrategy(driver=neo4j_driver, new_version="v2", import_dir=test_csv_dir)
    strategy.remove_stale_entities()

    # 4. ASSERT:
    with neo4j_driver.session() as session:
        # Assert stale relationship is deleted
        stale_rel_count = session.run("MATCH ()-[r:RELATED_TO]->() RETURN count(r) as count").single()['count']
        assert stale_rel_count == 0, "Stale RELATED_TO relationship should be deleted."

        # Assert stale Code node is deleted
        stale_code_count = session.run("MATCH (c:Code {code_id: 'CODE2'}) RETURN count(c) as count").single()['count']
        assert stale_code_count == 0, "Stale Code node should be deleted."

        # Assert stale HAS_CODE relationship is also gone (implicitly by DETACH DELETE)
        stale_has_code_count = session.run("MATCH (:Concept {cui:'CUI2'})-[r:HAS_CODE]->() RETURN count(r) as count").single()['count']
        assert stale_has_code_count == 0, "Stale HAS_CODE relationship should be deleted."

        # Assert stale Concept node is NOT deleted
        stale_concept_count = session.run("MATCH (c:Concept {cui: 'CUI3'}) RETURN count(c) as count").single()['count']
        assert stale_concept_count == 1, "Stale Concept node CUI3 should NOT be deleted."

        # Assert kept nodes and rels still exist
        kept_concept_count = session.run("MATCH (c:Concept) WHERE c.last_seen_version = 'v2' RETURN count(c) as count").single()['count']
        assert kept_concept_count == 2, "Should be 2 concepts marked with v2 (CUI1, CUI2)."

        kept_code_count = session.run("MATCH (c:Code) WHERE c.last_seen_version = 'v2' RETURN count(c) as count").single()['count']
        assert kept_code_count == 1, "Should be 1 code marked with v2."

        kept_rel_count = session.run("MATCH ()-[r]-() WHERE r.last_seen_version = 'v2' RETURN count(r) as count").single()['count']
        assert kept_rel_count == 1, "Should be 1 relationship marked with v2."
