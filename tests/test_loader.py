# Copyright (c) 2025-2026 Gowtham Adamane Rao. All Rights Reserved.
#
# Licensed under the Prosperity Public License 3.0.0 (the "License").
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License in the LICENSE file at the root
# of this repository, or at: https://prosperitylicense.com/versions/3.0.0
#
# Commercial use beyond a 30-day trial requires a separate license.
import pytest
from unittest.mock import patch, MagicMock
from pathlib import Path

from py_neo_umls_syncer.loader import Neo4jLoader
from py_neo_umls_syncer.config import settings

@patch('py_neo_umls_syncer.loader.RRFParser')
@patch('py_neo_umls_syncer.loader.CSVTransformer')
def test_run_bulk_import(mock_csv_transformer, mock_rrf_parser, tmp_path):
    """
    Test that run_bulk_import correctly orchestrates parsing, transforming,
    and generating the neo4j-admin command.
    """
    # Arrange
    loader = Neo4jLoader(driver=None) # run_bulk_import does not need a driver
    meta_dir = tmp_path / "META"
    meta_dir.mkdir()
    version = "2023AA"

    # Mock the parser and transformer instances
    mock_parser_instance = mock_rrf_parser.return_value
    mock_parser_instance.parse_files.return_value = ([], [], [], [], {})

    mock_transformer_instance = mock_csv_transformer.return_value

    # Act
    loader.run_bulk_import(meta_dir, version)

    # Assert
    mock_rrf_parser.assert_called_once_with(meta_dir)
    mock_parser_instance.parse_files.assert_called_once()

    mock_csv_transformer.assert_called_once_with(Path(settings.neo4j_import_dir))
    mock_transformer_instance.transform_to_csvs.assert_called_once_with(
        [], [], [], [], {}, version
    )

@patch('py_neo_umls_syncer.loader.DeltaStrategy')
def test_update_meta_node_after_bulk(mock_delta_strategy):
    """
    Test that update_meta_node_after_bulk correctly calls the DeltaStrategy.
    """
    # Arrange
    mock_driver = MagicMock()
    loader = Neo4jLoader(driver=mock_driver)
    version = "2023AA"
    mock_strategy_instance = mock_delta_strategy.return_value

    # Act
    loader.update_meta_node_after_bulk(version)

    # Assert
    mock_delta_strategy.assert_called_once_with(mock_driver, version, Path(settings.neo4j_import_dir))
    mock_strategy_instance.ensure_constraints.assert_called_once()
    mock_strategy_instance.update_meta_node.assert_called_once()


@patch('py_neo_umls_syncer.loader.RRFParser')
@patch('py_neo_umls_syncer.loader.CSVTransformer')
@patch('py_neo_umls_syncer.loader.DeltaStrategy')
def test_run_incremental_sync(mock_delta_strategy, mock_csv_transformer, mock_rrf_parser, tmp_path):
    """
    Test that run_incremental_sync correctly orchestrates the sync process.
    """
    # Arrange
    mock_driver = MagicMock()
    loader = Neo4jLoader(driver=mock_driver)
    meta_dir = tmp_path / "META"
    meta_dir.mkdir()
    version = "2023AB"

    mock_parser_instance = mock_rrf_parser.return_value
    mock_parser_instance.parse_files.return_value = ([], [], [], [], {})

    mock_strategy_instance = mock_delta_strategy.return_value
    (meta_dir / "DELETEDCUI.RRF").touch()
    (meta_dir / "MERGEDCUI.RRF").touch()

    # Act
    loader.run_incremental_sync(meta_dir, version)

    # Assert
    mock_delta_strategy.assert_called_once()

    mock_strategy_instance.ensure_constraints.assert_called_once()
    mock_strategy_instance.process_deleted_cuis.assert_called_once()
    mock_strategy_instance.process_merged_cuis.assert_called_once()
    mock_strategy_instance.apply_additions_and_updates.assert_called_once()
    mock_strategy_instance.remove_stale_entities.assert_called_once()
    mock_strategy_instance.update_meta_node.assert_called_once()
