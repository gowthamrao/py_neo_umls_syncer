import pytest
from typer.testing import CliRunner
import os
from neo4j import Driver
from pathlib import Path
import requests_mock
import io
import zipfile
import hashlib
import json

from py_neo_umls_syncer.cli import app
from py_neo_umls_syncer.config import settings
from py_neo_umls_syncer.delta_strategy import DeltaStrategy
from py_neo_umls_syncer.loader import Neo4jLoader
from py_neo_umls_syncer.downloader import UMLSDownloader

# Use the same mock data as the pipeline test for consistency
from .test_pipeline import (
    V1_MRCONSO, V1_MRREL, V1_MRSTY,
    V2_MRCONSO, V2_MRREL, V2_MRSTY,
    DELETEDCUI, MERGEDCUI
)

runner = CliRunner()

@pytest.fixture
def mock_v1_zip_content() -> bytes:
    """Creates a zip file in memory with V1 data."""
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
        zip_file.writestr("META/MRCONSO.RRF", V1_MRCONSO)
        zip_file.writestr("META/MRREL.RRF", V1_MRREL)
        zip_file.writestr("META/MRSTY.RRF", V1_MRSTY)
    return zip_buffer.getvalue()

@pytest.fixture
def mock_v2_zip_content() -> bytes:
    """Creates a zip file in memory with V2 data."""
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
        zip_file.writestr("META/MRCONSO.RRF", V2_MRCONSO)
        zip_file.writestr("META/MRREL.RRF", V2_MRREL)
        zip_file.writestr("META/MRSTY.RRF", V2_MRSTY)
        zip_file.writestr("META/DELETEDCUI.RRF", DELETEDCUI)
        zip_file.writestr("META/MERGEDCUI.RRF", MERGEDCUI)
    return zip_buffer.getvalue()

@pytest.fixture(scope="function")
def setup_v1_database(
    neo4j_driver: Driver,
    test_csv_dir: Path,
    tmp_path: Path,
    monkeypatch,
    requests_mock,
    mock_v1_zip_content: bytes,
):
    """
    Sets up the database with V1 data by running the full import logic.
    This prepares the ground for testing the incremental sync.
    """
    VERSION = "2025AA"
    import_dir = test_csv_dir # Use the mounted directory for imports
    download_dir = tmp_path / "download" # Use a regular temp dir for downloads
    download_dir.mkdir()
    monkeypatch.setattr(settings, "neo4j_import_dir", str(import_dir))
    monkeypatch.setattr(settings, "download_dir", str(download_dir))

    zip_content = mock_v1_zip_content
    md5_checksum = hashlib.md5(zip_content).hexdigest()
    release_info = {
        "result": [{
            "name": VERSION,
            "downloadUrl": f"https://download.nlm.nih.gov/umls/kss/{VERSION}/2025AA-full.zip",
            "md5": md5_checksum
        }]
    }
    requests_mock.get(UMLSDownloader.RELEASE_API_URL, json=release_info)
    requests_mock.get(UMLSDownloader.DOWNLOAD_API_URL, content=zip_content)

    # We don't use the CLI runner here, we call the loader directly to setup the state
    loader = Neo4jLoader(driver=neo4j_driver)
    meta_dir = loader.downloader.download_and_extract_release(version=VERSION)
    loader.run_bulk_import(meta_dir=meta_dir, version=VERSION)

    # Simulate the neo4j-admin import by loading the CSVs directly
    strategy = DeltaStrategy(neo4j_driver, VERSION, import_dir)
    strategy.apply_additions_and_updates()
    loader.update_meta_node_after_bulk(VERSION)

    return {"import_dir": import_dir, "download_dir": download_dir}


def test_full_import_cli(
    neo4j_driver: Driver,
    test_csv_dir: Path,
    tmp_path: Path,
    monkeypatch,
    requests_mock,
    mock_v1_zip_content: bytes,
):
    """
    Tests the full-import CLI command end-to-end, mocking the download.
    """
    VERSION = "2025AA"
    # 1. Setup environment variables
    import_dir = test_csv_dir
    download_dir = tmp_path / "download"
    download_dir.mkdir()
    monkeypatch.setattr(settings, "neo4j_import_dir", str(import_dir))
    monkeypatch.setattr(settings, "download_dir", str(download_dir))

    # 2. Mock UMLS API responses
    zip_content = mock_v1_zip_content
    md5_checksum = hashlib.md5(zip_content).hexdigest()
    release_info = {
        "result": [{
            "name": VERSION,
            "downloadUrl": f"https://download.nlm.nih.gov/umls/kss/{VERSION}/2025AA-full.zip",
            "md5": md5_checksum
        }]
    }
    requests_mock.get(UMLSDownloader.RELEASE_API_URL, json=release_info)
    requests_mock.get(UMLSDownloader.DOWNLOAD_API_URL, content=zip_content)

    # 3. Run the CLI command
    result = runner.invoke(app, ["full-import", "--version", VERSION])

    # 4. Assert the output
    assert result.exit_code == 0
    assert "UMLS release 2025AA already downloaded and extracted" not in result.stdout
    assert "Download complete" in result.stdout
    assert "Checksum verified successfully" in result.stdout
    assert "Extraction complete" in result.stdout
    assert "Bulk import files and command generated successfully." in result.stdout

    # Check for the neo4j-admin command in the output
    assert 'neo4j-admin database import full' in result.stdout
    assert f'--nodes=Concept:Concept-ID="nodes_concepts.csv"' in result.stdout

    # 5. Verify CSVs were created
    assert (import_dir / "nodes_concepts.csv").exists()

    # 6. To fully validate, we load the data and check the DB state
    strategy = DeltaStrategy(neo4j_driver, VERSION, import_dir)
    strategy.apply_additions_and_updates()
    Neo4jLoader(driver=neo4j_driver).update_meta_node_after_bulk(VERSION)

    with neo4j_driver.session() as session:
        meta_version = session.run("MATCH (m:UMLS_Meta) RETURN m.version AS version").single()["version"]
        assert meta_version == VERSION
        node_count = session.run("MATCH (n) RETURN count(n) AS count").single()["count"]
        assert node_count == 11  # 5 concepts + 5 codes + 1 meta
        rel_count = session.run("MATCH ()-[r]->() RETURN count(r) AS count").single()["count"]
        assert rel_count == 5  # 5 has_code, 0 inter_concept in mock data


from neo4j import GraphDatabase

def test_incremental_sync_cli(
    neo4j_driver: Driver,
    monkeypatch,
    requests_mock,
    setup_v1_database, # This fixture sets up the DB with V1 data
    mock_v2_zip_content: bytes
):
    # This is the key to fixing the CLI tests. We need to ensure that when the
    # CLI code calls GraphDatabase.driver(), it receives the test driver from
    # the container, not a new one pointing to a default (and wrong) address.
    monkeypatch.setattr(GraphDatabase, "driver", lambda *args, **kwargs: neo4j_driver)
    """
    Tests the incremental-sync CLI command end-to-end.
    """
    VERSION = "2025AB"
    # Env is already patched by the setup_v1_database fixture

    # 1. Mock V2 UMLS API responses
    zip_content = mock_v2_zip_content
    md5_checksum = hashlib.md5(zip_content).hexdigest()
    release_info = {
        "result": [{
            "name": VERSION,
            "downloadUrl": f"https://download.nlm.nih.gov/umls/kss/{VERSION}/2025AB-full.zip",
            "md5": md5_checksum
        }]
    }
    # Important: Make sure the mock for releases is updated for the new version
    requests_mock.get(UMLSDownloader.RELEASE_API_URL, json=release_info)
    requests_mock.get(UMLSDownloader.DOWNLOAD_API_URL, content=zip_content)

    # 2. Run the CLI command
    result = runner.invoke(app, ["incremental-sync", "--version", VERSION])
    print(result.stdout)
    # 3. Assert the output
    assert result.exit_code == 0
    assert "Starting Incremental Sync to Version: 2025AB" in result.stdout
    assert "Processing deleted CUIs..." in result.stdout
    assert "Processing merged CUIs..." in result.stdout
    assert "Applying additions and updates" in result.stdout
    assert "Removing stale entities" in result.stdout
    assert "Updating metadata version" in result.stdout
    assert "Incremental sync to version 2025AB completed successfully!" in result.stdout

    # 4. Verify the database state
    with neo4j_driver.session() as session:
        # Meta node updated
        meta_version = session.run("MATCH (m:UMLS_Meta) RETURN m.version AS version").single()["version"]
        assert meta_version == VERSION

        # Deleted CUI is gone
        deleted_node = session.run("MATCH (c:Concept {cui: 'C0000005'}) RETURN c").single()
        assert deleted_node is None

        # Merged CUI is gone
        merged_node = session.run("MATCH (c:Concept {cui: 'C0000004'}) RETURN c").single()
        assert merged_node is None

        # Merged CUI's relationships migrated
        res = session.run("""
            MATCH (c:Concept {cui: 'C0000002'})-[r:biolink:subclass_of]->(c)
            RETURN r.source_rela AS rela
        """).single()
        assert res is not None and res["rela"] == "isa"

        # Stale relationship is gone (C0000001->C0000003)
        stale_rel = session.run("MATCH (:Concept {cui:'C0000001'})-[r]->(:Concept {cui:'C0000003'}) RETURN r").single()
        assert stale_rel is None

        # New concept and rel are present
        new_node = session.run("MATCH (c:Concept {cui: 'C0000006'}) RETURN c").single()
        assert new_node is not None
        new_rel = session.run("MATCH (:Concept {cui:'C0000006'})-[r]->(:Concept {cui:'C0000001'}) RETURN r").single()
        assert new_rel is not None

def test_incremental_sync_no_meta_node(
    neo4j_driver: Driver, test_csv_dir: Path, tmp_path: Path, monkeypatch, requests_mock, mock_v2_zip_content: bytes
):
    monkeypatch.setattr(GraphDatabase, "driver", lambda *args, **kwargs: neo4j_driver)
    """
    Tests that incremental-sync fails gracefully if the meta node is not present.
    """
    VERSION = "2025AB"
    # 1. Setup environment to mock download, but on an empty DB
    download_dir = tmp_path / "download"
    download_dir.mkdir()
    monkeypatch.setattr(settings, "download_dir", str(download_dir))
    monkeypatch.setattr(settings, "neo4j_import_dir", str(test_csv_dir))


    # 2. Mock V2 UMLS API responses so the command doesn't fail on download
    zip_content = mock_v2_zip_content
    md5_checksum = hashlib.md5(zip_content).hexdigest()
    release_info = {
        "result": [{
            "name": VERSION,
            "downloadUrl": f"https://download.nlm.nih.gov/umls/kss/{VERSION}/2025AB-full.zip",
            "md5": md5_checksum
        }]
    }
    requests_mock.get(UMLSDownloader.RELEASE_API_URL, json=release_info)
    requests_mock.get(UMLSDownloader.DOWNLOAD_API_URL, content=zip_content)

    # 3. Run the CLI command against an empty DB
    result = runner.invoke(app, ["incremental-sync", "--version", VERSION])

    # 4. Assert the result
    assert result.exit_code != 0
    assert isinstance(result.exception, SystemExit)
    assert "UMLS_Meta node not found" in str(result.exception)

    # 5. Verify no meta node was created
    with neo4j_driver.session() as session:
        meta_node = session.run("MATCH (m:UMLS_Meta) RETURN m").single()
        assert meta_node is None
