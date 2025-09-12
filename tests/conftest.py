# Jules was here
import pytest
from neo4j import GraphDatabase, Driver
from testcontainers.neo4j import Neo4jContainer
from pathlib import Path
import shutil
import time
import subprocess
import os
import requests
from testcontainers.core.waiting_utils import wait_for_logs

@pytest.fixture(scope="session")
def test_import_dir() -> Path:
    """
    Provides a session-scoped, static temporary directory for CSV files.
    This directory is mounted into the Neo4j container.
    """
    import_dir = Path("/tmp/pyneo_test_import")
    # Clean up the directory from previous runs, handling potential permission errors
    if import_dir.exists():
        try:
            shutil.rmtree(import_dir)
        except PermissionError:
            subprocess.run(["sudo", "chown", "-R", f"{os.getuid()}:{os.getgid()}", str(import_dir)], check=True)
            shutil.rmtree(import_dir)
    import_dir.mkdir(parents=True, exist_ok=True)
    yield import_dir
    # Final cleanup after the session
    try:
        shutil.rmtree(import_dir)
    except PermissionError:
        subprocess.run(["sudo", "chown", "-R", f"{os.getuid()}:{os.getgid()}", str(import_dir)], check=True)
        shutil.rmtree(import_dir)

@pytest.fixture(scope="session")
def neo4j_container(test_import_dir: Path):
    """
    A pytest fixture that starts and stops a Neo4j container for the test session.
    The container is configured with APOC plugins and a mounted import volume.
    """
    # We use a specific version for reproducibility
    NEO4J_VERSION = "5.22.0"

    container = Neo4jContainer(image=f"neo4j:{NEO4J_VERSION}")
    # Let the official Neo4j image handle APOC download via this environment variable
    container.with_env("NEO4J_PLUGINS", '["apoc"]')

    # Standard configuration for APOC to allow file imports and exports
    container.with_env("NEO4J_apoc_export_file_enabled", "true")
    container.with_env("NEO4J_apoc_import_file_enabled", "true")
    container.with_env("NEO4J_apoc_import_file_use__neo4j__config", "true")
    container.with_env("NEO4J_dbms_security_procedures_unrestricted", "apoc.*,algo.*")

    # Set authentication and map the import directory
    container.with_env("NEO4J_AUTH", "neo4j/password")
    container.with_env("NEO4J_dbms_directories_import", "/import")
    container.with_volume_mapping(str(test_import_dir), "/import")

    with container as c:
        # The original waiting logic, which is simple and less prone to race conditions.
        wait_for_logs(c, "Started.", 60)
        time.sleep(20) # Give APOC time to initialize fully after startup
        c.driver = c.get_driver()
        yield c
        c.driver.close()

@pytest.fixture
def neo4j_driver(neo4j_container: Neo4jContainer):
    """
    Provides a driver to the test Neo4j container and cleans the database
    after each test function. This is a more robust approach to ensuring
    test isolation than cleaning before.
    """
    driver = neo4j_container.driver
    # Clean before the test runs to be safe
    with driver.session() as session:
        session.run("MATCH (n) DETACH DELETE n")
        # Also drop constraints to ensure a clean slate
        constraints = session.run("SHOW CONSTRAINTS YIELD name").data()
        for constraint in constraints:
            session.run(f"DROP CONSTRAINT {constraint['name']}")

    yield driver

    # Clean up after the test has run
    with driver.session() as session:
        session.run("MATCH (n) DETACH DELETE n")
        # Also drop constraints to ensure a clean slate
        constraints = session.run("SHOW CONSTRAINTS YIELD name").data()
        for constraint in constraints:
            session.run(f"DROP CONSTRAINT {constraint['name']}")

@pytest.fixture(scope="function")
def test_csv_dir(test_import_dir: Path) -> Path:
    """
    Provides a function-scoped temporary directory for CSV files.
    This directory is mounted into the Neo4j container.
    It is cleaned before each test.
    """
    for item in test_import_dir.iterdir():
        if item.is_file():
            item.unlink()
        elif item.is_dir():
            shutil.rmtree(item)
    yield test_import_dir
