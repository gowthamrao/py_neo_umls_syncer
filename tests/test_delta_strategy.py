import pytest
from unittest.mock import MagicMock, call
from pathlib import Path

from .mocks import mock_settings
mock_settings()

from pyNeoUmlsSyncer.delta_strategy import DeltaStrategy

@pytest.fixture
def mock_loader():
    """Provides a mocked instance of Neo4jLoader."""
    return MagicMock()

@pytest.fixture
def rrf_path():
    """Provides the path to the test RRF data."""
    return Path(__file__).parent / "data" / "test_version" / "META"

@pytest.fixture
def delta_strategy(mock_loader, rrf_path):
    """Provides an instance of DeltaStrategy with a mocked loader."""
    return DeltaStrategy(loader=mock_loader, rrf_path=rrf_path, new_version="test_version")

def test_process_deleted_cuis(delta_strategy, mock_loader):
    """
    Tests that DELETEDCUI.RRF is read and the correct Cypher is executed.
    """
    delta_strategy._process_deleted_cuis()

    # Assert that the loader's APOC method was called
    mock_loader.execute_apoc_iterate.assert_called_once()

    # Get the arguments passed to the mock
    args, kwargs = mock_loader.execute_apoc_iterate.call_args

    # Check the data passed (it's a positional argument)
    assert args[1] == [{'cui': 'C004'}]
    assert "Deleting CUIs" in args[2]

    # Check that the Cypher query contains the key logic
    cypher_query = args[0]
    assert "MATCH (c:Concept {cui: row.cui})" in cypher_query
    assert "DETACH DELETE c, code" in cypher_query
    assert "size((code)--()) = 1" in cypher_query # Ensures we don't delete shared codes

def test_process_merged_cuis(delta_strategy, mock_loader):
    """
    Tests that MERGEDCUI.RRF is read and the correct APOC procedure is called.
    """
    delta_strategy._process_merged_cuis()

    mock_loader.execute_apoc_iterate.assert_called_once()
    args, kwargs = mock_loader.execute_apoc_iterate.call_args

    # Check data (it's a positional argument)
    assert args[1] == [{'old_cui': 'C005', 'new_cui': 'C006'}]
    assert "Merging CUIs" in args[2]

    # Check that we are using the robust apoc.refactor.mergeNodes procedure
    cypher_query = args[0]
    assert "apoc.refactor.mergeNodes" in cypher_query
    assert "mergeRels: true" in cypher_query
    assert "properties: 'combine'" in cypher_query

def test_remove_stale_entities(delta_strategy, mock_loader):
    """
    Tests that the correct queries are executed to clean up stale data.
    """
    delta_strategy._remove_stale_entities()

    # We expect three separate calls to the driver's execute_query method
    assert mock_loader.driver.execute_query.call_count == 3

    # Get all the calls
    all_calls = mock_loader.driver.execute_query.call_args_list

    # Extract the Cypher from each call
    executed_cypher = [c.args[0] for c in all_calls]

    # Check for stale relationship deletion query
    assert any("MATCH ()-[r]-() WHERE r.last_seen_version < $new_version" in q for q in executed_cypher)
    assert any("DELETE r" in q for q in executed_cypher)

    # Check for stale Code node deletion query
    assert any("MATCH (c:Code) WHERE c.last_seen_version < $new_version" in q for q in executed_cypher)
    assert any("size((c)--()) = 0" in q for q in executed_cypher) # Must be disconnected
    assert any("DELETE c" in q for q in executed_cypher)

    # Check for stale Concept node deletion query
    assert any("MATCH (c:Concept) WHERE c.last_seen_version < $new_version" in q for q in executed_cypher)

    # Check that new_version was passed as a parameter in all calls
    for c in all_calls:
        assert c.kwargs['new_version'] == 'test_version'

def test_full_run_orchestration(delta_strategy, mock_loader):
    """
    Tests that the main `run_incremental_update` method calls its sub-methods
    in the correct order.
    """
    # We can use a single mock to track the order of calls
    mock_manager = MagicMock()
    delta_strategy._process_deleted_cuis = mock_manager.delete
    delta_strategy._process_merged_cuis = mock_manager.merge
    delta_strategy._apply_snapshot_updates = mock_manager.apply
    delta_strategy._remove_stale_entities = mock_manager.remove
    mock_loader.update_umls_version = mock_manager.update_meta

    # Dummy data for the snapshot
    concepts, codes, has_code_rels, concept_rels = [], [], [], []

    # Run the main method
    delta_strategy.run_incremental_update(concepts, codes, has_code_rels, concept_rels)

    # Check the call order
    expected_calls = [
        call.delete(),
        call.merge(),
        call.apply(concepts, codes, has_code_rels, concept_rels),
        call.remove(),
        call.update_meta("test_version")
    ]
    assert mock_manager.mock_calls == expected_calls
