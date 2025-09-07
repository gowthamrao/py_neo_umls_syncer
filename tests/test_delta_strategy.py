import pytest
from neo4j import Driver
from pyNeoUmlsSyncer.config import Settings
from pyNeoUmlsSyncer.delta_strategy import UmlsDeltaStrategy

@pytest.mark.integration
def test_cui_merge_strategy(neo4j_driver: Driver, test_settings: Settings):
    """
    Tests the CUI merge logic by:
    1. Creating a small graph with concepts C1, C2, C3.
    2. Defining relationships C1->C3 and C2->C3.
    3. Executing the merge query to merge C1 into C2.
    4. Asserting that C1 is deleted and its relationships are moved to C2.
    """
    # 1. Setup: Clear the database and create the initial state
    with neo4j_driver.session(database=test_settings.neo4j_database) as session:
        session.run("MATCH (n) DETACH DELETE n")
        session.run("""
            CREATE (c1:Concept {cui: 'C0000001', preferred_name: 'Concept One'})
            CREATE (c2:Concept {cui: 'C0000002', preferred_name: 'Concept Two'})
            CREATE (c3:Concept {cui: 'C0000003', preferred_name: 'Concept Three'})
            MERGE (c1)-[:part_of {source_rela: 'part_of', asserted_by_sabs: ['SAB1']}]->(c3)
            MERGE (c2)-[:isa {source_rela: 'isa', asserted_by_sabs: ['SAB1', 'SAB2']}]->(c3)
        """)

    # 2. Instantiate the delta strategy
    delta_strategy = UmlsDeltaStrategy(test_settings)
    merge_query = delta_strategy.generate_merged_cui_query()
    merge_data = [{"old_cui": "C0000001", "new_cui": "C0000002"}]

    # 3. Execute the merge query
    with neo4j_driver.session(database=test_settings.neo4j_database) as session:
        session.run(
            merge_query,
            rows=merge_data,
            version=test_settings.umls_version
        )

    # 4. Assert the final state of the graph
    with neo4j_driver.session(database=test_settings.neo4j_database) as session:
        # Assert C1 is deleted
        c1_result = session.run("MATCH (c:Concept {cui: 'C0000001'}) RETURN c")
        assert c1_result.single() is None, "Old concept C0000001 should be deleted"

        # Assert C2 has the migrated relationship
        c2_rel_result = session.run("""
            MATCH (c2:Concept {cui: 'C0000002'})-[r:part_of]->(c3:Concept {cui: 'C0000003'})
            RETURN r
        """)
        assert c2_rel_result.single() is not None, "C0000002 should have the migrated 'part_of' relationship"

        # Assert C2 still has its original relationship
        c2_original_rel_result = session.run("""
            MATCH (c2:Concept {cui: 'C0000002'})-[r:isa]->(c3:Concept {cui: 'C0000003'})
            RETURN r
        """)
        assert c2_original_rel_result.single() is not None, "C0000002 should still have its original 'isa' relationship"

        # Assert C2 and C3 still exist
        c2_c3_count = session.run("MATCH (c) WHERE c.cui IN ['C0000002', 'C0000003'] RETURN count(c) as count")
        assert c2_c3_count.single()["count"] == 2, "Concepts C2 and C3 should still exist"
