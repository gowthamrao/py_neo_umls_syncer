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
    # Manually download the APOC jar to ensure it's available
    apoc_version = "5.22.0"
    jar_url = f"https://github.com/neo4j/apoc/releases/download/{apoc_version}/apoc-{apoc_version}-core.jar"
    jar_path = test_import_dir / f"apoc-{apoc_version}-core.jar"

    if not jar_path.exists():
        with requests.get(jar_url, stream=True) as r:
            r.raise_for_status()
            with open(jar_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)

    container = Neo4jContainer(image=f"neo4j:{apoc_version}")
    # container.with_env("NEO4J_PLUGINS", '["apoc"]')
    container.with_env("NEO4J_apoc_export_file_enabled", "true")
    container.with_env("NEO4J_apoc_import_file_enabled", "true")
    container.with_env("NEO4J_apoc_import_file_use__neo4j__config", "true")
    container.with_env("NEO4J_dbms_security_procedures_unrestricted", "apoc.*")
    container.with_env("NEO4J_AUTH", "neo4j/password")
    container.with_env("NEO4J_dbms_directories_import", "/import")
    # Mount the static temp dir to the container's import directory
    container.with_volume_mapping(str(test_import_dir), "/import")
    # Mount the downloaded APOC jar into the plugins directory
    container.with_volume_mapping(str(jar_path), "/plugins/apoc.jar")

    with container as c:
        wait_for_logs(c, "Started.", 60)
        time.sleep(10) # Crude way to wait for APOC to be fully ready
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
        # Also drop constraints to ensure a clean slate
        constraints = session.run("SHOW CONSTRAINTS YIELD name").data()
        for constraint in constraints:
            session.run(f"DROP CONSTRAINT {constraint['name']}")
    yield driver

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
