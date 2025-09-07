"""
Integration tests for the delta_strategy module.
"""
import pytest
from neo4j import Driver
from pathlib import Path
from pyNeoUmlsSyncer import delta_strategy

def test_snapshot_diff_cleanup(neo4j_driver: Driver):
    """
    Tests that snapshot_diff_cleanup correctly removes stale nodes and relationships.
    """
    new_version = "v2"
    old_version = "v1"

    # Setup: Create nodes and rels with different versions
    with neo4j_driver.session() as session:
        session.run("""
        CREATE (c1:Concept {cui: 'C1', last_seen_version: $old})
        CREATE (c2:Concept {cui: 'C2', last_seen_version: $new})
        CREATE (c3:Concept {cui: 'C3', last_seen_version: $old})
        CREATE (c4:Concept {cui: 'C4', last_seen_version: $new})
        CREATE (c1)-[:REL {last_seen_version: $old}]->(c3)
        CREATE (c2)-[:REL {last_seen_version: $new}]->(c4)
        """, old=old_version, new=new_version)

    # Execute the cleanup function
    delta_strategy.snapshot_diff_cleanup(neo4j_driver, new_version, 100)

    # Verify: Check that old data is gone and new data remains
    with neo4j_driver.session() as session:
        nodes = session.run("MATCH (n) RETURN n.cui AS cui").data()
        cuis = {n['cui'] for n in nodes}

        assert cuis == {'C2', 'C4'}

        rels = session.run("MATCH ()-[r]-() RETURN r").data()
        assert len(rels) == 1


def test_merged_cui_logic(neo4j_driver: Driver, tmp_path: Path):
    """
    Tests the MERGEDCUI logic, including relationship and provenance migration.
    """
    old_cui = "C_OLD"
    new_cui = "C_NEW"

    # Setup: Create a scenario for merging
    with neo4j_driver.session() as session:
        session.run("""
        // Create nodes
        CREATE (old:Concept {cui: $old_cui})
        CREATE (new:Concept {cui: $new_cui})
        CREATE (source:Concept {cui: 'C_SOURCE'})
        CREATE (downstream:Concept {cui: 'C_DOWNSTREAM'})
        CREATE (code:Code {code_id: 'SAB:123'})

        // Create relationships to be migrated
        CREATE (old)-[:HAS_CODE]->(code)
        CREATE (source)-[r1:TREATS {asserted_by_sabs: ['SAB1']}]->(old)
        CREATE (old)-[r2:CAUSES {asserted_by_sabs: ['SAB2']}]->(downstream)

        // Create a pre-existing relationship on the new CUI to test provenance merge
        CREATE (source)-[r3:TREATS {asserted_by_sabs: ['SAB3']}]->(new)
        """, old_cui=old_cui, new_cui=new_cui)

    # Create a dummy MERGEDCUI.RRF file
    merged_cui_file = tmp_path / "MERGEDCUI.RRF"
    merged_cui_file.write_text(f"{old_cui}|{new_cui}")

    # Execute the merge process
    delta_strategy.process_merged_cuis(neo4j_driver, merged_cui_file, 100)

    # Verify the results
    with neo4j_driver.session() as session:
        # 1. Old CUI should be gone
        old_node = session.run("MATCH (c:Concept {cui: $old_cui}) RETURN c", old_cui=old_cui).single()
        assert old_node is None

        # 2. Migrated relationships should exist on the new CUI
        has_code_rel = session.run("MATCH (c:Concept {cui: $new_cui})-[:HAS_CODE]->(:Code {code_id: 'SAB:123'}) RETURN count(*) as ct", new_cui=new_cui).single()['ct']
        assert has_code_rel == 1

        causes_rel = session.run("MATCH (:Concept {cui: $new_cui})-[:CAUSES]->(:Concept {cui: 'C_DOWNSTREAM'}) RETURN count(*) as ct", new_cui=new_cui).single()['ct']
        assert causes_rel == 1

        # 3. Provenance should be merged correctly
        treats_rel = session.run("MATCH (:Concept {cui: 'C_SOURCE'})-[r:TREATS]->(:Concept {cui: $new_cui}) RETURN r.asserted_by_sabs AS sabs", new_cui=new_cui).single()
        assert treats_rel is not None
        # The set comparison handles order differences
        assert set(treats_rel['sabs']) == {'SAB1', 'SAB3'}
