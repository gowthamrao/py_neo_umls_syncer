import pytest
from neo4j import GraphDatabase, Driver
from testcontainers.neo4j import Neo4jContainer
from pathlib import Path
import shutil

@pytest.fixture(scope="session")
def session_tmp_path(tmpdir_factory):
    """A session-scoped temporary directory."""
    return tmpdir_factory.mktemp("data")

@pytest.fixture(scope="session")
def neo4j_container(session_tmp_path):
    """
    A pytest fixture that starts and stops a Neo4j container for the test session.
    The container is configured with APOC plugins and a mounted import volume.
    """
    # Use the fluent API (`.with_...` methods) to avoid constructor conflicts.
    container = Neo4jContainer(image="neo4j:5.18")
    container.with_env("NEO4J_PLUGINS", '["apoc"]')
    container.with_env("NEO4J_apoc_export_file_enabled", "true")
    container.with_env("NEO4J_apoc_import_file_enabled", "true")
    container.with_env("NEO4J_apoc_import_file_use__neo4j__config", "true")
    container.with_env("NEO4J_dbms_security_procedures_unrestricted", "apoc.*")
    container.with_env("NEO4J_AUTH", "neo4j/password")
    # Mount the session-scoped temp dir to the container's import directory
    container.with_volume_mapping(str(session_tmp_path), "/var/lib/neo4j/import")

    with container as c:
        c.driver = c.get_driver()
        yield c
        c.driver.close()

@pytest.fixture
def neo4j_driver(neo4j_container: Neo4jContainer):
    """
    Provides a driver to the test Neo4j container and cleans the database
    before each test function.
    """
    driver = neo4j_container.driver
    # Clean database before each test
    with driver.session() as session:
        session.run("MATCH (n) DETACH DELETE n")
    yield driver

@pytest.fixture
def test_csv_dir(session_tmp_path: Path) -> Path:
    """
    Provides the session-scoped temporary directory for CSV files.
    This directory is mounted into the Neo4j container.
    """
    # The directory is already created by the session_tmp_path fixture.
    # We just return the path. Cleanup is handled by the tmpdir_factory.
    yield session_tmp_path
