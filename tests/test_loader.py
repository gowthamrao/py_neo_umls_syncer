import pytest
from unittest.mock import patch
from neo4j import Driver
from pyNeoUmlsSyncer.config import Settings
from pyNeoUmlsSyncer.loader import UmlsLoader
from pathlib import Path

@pytest.mark.integration
@patch('pyNeoUmlsSyncer.loader.UmlsLoader.run_full_import')
@patch('pyNeoUmlsSyncer.loader.UmlsLoader.run_incremental_sync')
def test_loader_orchestration(
    mock_incremental_sync,
    mock_full_import,
    neo4j_driver: Driver,
    test_settings: Settings
):
    """
    Tests that the UmlsLoader correctly calls the full import method for an
    empty database and the incremental sync method for an initialized one.
    """
    loader = UmlsLoader(test_settings)

    # --- Scenario 1: Empty Database ---
    # Ensure the database is clean before the test
    with neo4j_driver.session(database=test_settings.neo4j_database) as session:
        session.run("MATCH (n) DETACH DELETE n")

    assert loader.is_database_empty() is True, "Database should be detected as empty"

    # Run the loader
    loader.run()

    # Assert that the correct method was called
    mock_full_import.assert_called_once()
    mock_incremental_sync.assert_not_called()

    # Reset mocks
    mock_full_import.reset_mock()

    # --- Scenario 2: Initialized Database ---
    # Manually create the meta node to simulate a previous run
    with neo4j_driver.session(database=test_settings.neo4j_database) as session:
        session.run("CREATE (:UMLS_Meta {version: '2023AA'})")

    assert loader.is_database_empty() is False, "Database should be detected as initialized"

    # Run the loader again
    loader.run()

    # Assert that the correct method was called this time
    mock_full_import.assert_not_called()
    mock_incremental_sync.assert_called_once()

    loader.close()

@pytest.mark.integration
def test_incremental_sync_end_to_end(neo4j_driver: Driver, test_settings: Settings):
    """
    Performs a full end-to-end test of the incremental sync logic, including
    initial load, MERGEDCUI handling, idempotency, and stale data removal.
    """
    # Helper function to query the graph state
    def get_graph_state(tx):
        nodes = tx.run("MATCH (n) RETURN count(n) as count").single()['count']
        rels = tx.run("MATCH ()-[r]->() RETURN count(r) as count").single()['count']
        c4_rels = tx.run("MATCH (c:Concept {cui:'C0000004'})-[r]->() RETURN count(r) as count").single()['count']
        c4_has_location = tx.run("MATCH (:Concept {cui:'C0000004'})-[:biolink:has_location]->(:Concept {cui:'C0000002'}) RETURN count(*) > 0 as exists").single()['exists']
        return {"nodes": nodes, "rels": rels, "c4_rels": c4_rels, "c4_has_location": c4_has_location}

    # --- 1. Initial Incremental Load ---
    loader = UmlsLoader(test_settings)
    with neo4j_driver.session(database=test_settings.neo4j_database) as session:
        session.run("MATCH (n) DETACH DELETE n")

    loader.run_incremental_sync()

    with neo4j_driver.session(database=test_settings.neo4j_database) as session:
        initial_state = session.read_transaction(get_graph_state)
        # Check that C0000005 was merged into C0000004
        c5_exists = session.run("MATCH (c:Concept {cui: 'C0000005'}) RETURN count(c) > 0 as exists").single()['exists']
        assert not c5_exists, "C0000005 should have been merged and deleted"
        # C0000004 should have its own relationship ('location_of') and the one from C0000005 ('isa')
        assert initial_state["c4_rels"] == 2, "C0000004 should have 2 outgoing relationships after merge"

    # --- 2. Idempotency Check ---
    # Rerunning the sync should result in the exact same graph state
    loader.run_incremental_sync()
    with neo4j_driver.session(database=test_settings.neo4j_database) as session:
        idempotent_state = session.read_transaction(get_graph_state)
        assert idempotent_state == initial_state, "Graph state changed after idempotent run"

    # --- 3. Snapshot Diff Test (Stale Relationship Removal) ---
    # Simulate a new data version by removing a relationship from the source file
    version_dir = Path(test_settings.data_dir) / test_settings.umls_version
    mrrel_path = version_dir / "MRREL.RRF"
    with open(mrrel_path, 'r') as f:
        lines = f.readlines()
    # Remove the 'location_of' relationship between C0000004 and C0000002
    with open(mrrel_path, 'w') as f:
        for line in lines:
            if "C0000004|A0000004|SCUI|location_of|C0000002" not in line:
                f.write(line)

    # Create a new loader with an updated version to trigger the diff logic
    new_version_settings = test_settings.copy(update={"umls_version": "2024AB"})
    diff_loader = UmlsLoader(new_version_settings)
    diff_loader.run_incremental_sync()

    with neo4j_driver.session(database=test_settings.neo4j_database) as session:
        final_state = session.read_transaction(get_graph_state)
        assert not final_state["c4_has_location"], "The 'has_location' relationship should have been deleted"
        assert final_state["c4_rels"] == 1, "C0000004 should only have 1 relationship left"
        meta_version = session.run("MATCH (m:UMLS_Meta) RETURN m.version as v").single()['v']
        assert meta_version == "2024AB", "Meta node version was not updated"

    loader.close()
    diff_loader.close()
