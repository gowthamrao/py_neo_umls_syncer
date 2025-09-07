import pytest
from unittest.mock import patch
from neo4j import Driver
from pyNeoUmlsSyncer.config import Settings
from pyNeoUmlsSyncer.loader import UmlsLoader

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
