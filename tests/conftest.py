import pytest
from neo4j import GraphDatabase, Driver, exceptions
from testcontainers.neo4j import Neo4jContainer
from pathlib import Path
import shutil
import time
import subprocess
import os
import requests
from testcontainers.core.waiting_utils import wait_for_logs
from testcontainers.core.wait_strategies import LogMessageWaitStrategy
from rich.console import Console

console = Console()

@pytest.fixture(scope="session")
def test_import_dir() -> Path:
    """
    Provides a session-scoped, static temporary directory for CSV files.
    This directory is mounted into the Neo4j container.
    """
    import_dir = Path("/tmp/pyneo_test_import")
    if import_dir.exists():
        try:
            shutil.rmtree(import_dir)
        except PermissionError:
            subprocess.run(["sudo", "chown", "-R", f"{os.getuid()}:{os.getgid()}", str(import_dir)], check=True)
            shutil.rmtree(import_dir)
    import_dir.mkdir(parents=True, exist_ok=True)
    yield import_dir
    try:
        shutil.rmtree(import_dir)
    except PermissionError:
        subprocess.run(["sudo", "chown", "-R", f"{os.getuid()}:{os.getgid()}", str(import_dir)], check=True)
        shutil.rmtree(import_dir)

@pytest.fixture(scope="session")
def neo4j_container(test_import_dir: Path):
    """
    A pytest fixture that starts and stops a Neo4j container for the test session.
    It attempts to install APOC using the standard environment variable.
    """
    NEO4J_VERSION = "5.18-enterprise" # Use a version that is known to work
    container = Neo4jContainer(image=f"neo4j:{NEO4J_VERSION}")

    # For Neo4j 5, we must explicitly enable APOC Core procedures
    # The .with_apoc() helper does not exist in this version of the library
    container.with_env("NEO4J_PLUGINS", '["apoc"]')
    container.with_env("NEO4J_dbms_security_procedures_unrestricted", "apoc.*")
    container.with_env("NEO4J_apoc_import_file_enabled", "true")
    container.with_env("NEO4J_ACCEPT_LICENSE_AGREEMENT", "yes")
    container.with_env("NEO4J_AUTH", "neo4j/password")
    container.with_env("NEO4J_dbms_directories_import", "/import")
    container.with_volume_mapping(str(test_import_dir), "/import")

    # Use a more robust wait strategy
    container.waiting_for(LogMessageWaitStrategy("Remote interface available at"))
    with container as c:
        c.driver = c.get_driver()
        yield c
        c.driver.close()

@pytest.fixture
def neo4j_driver(neo4j_container: Neo4jContainer):
    """
    Provides a driver to the test Neo4j container and cleans the database
    after each test function.
    """
    driver = neo4j_container.driver
    with driver.session() as session:
        session.run("MATCH (n) DETACH DELETE n")
        constraints = session.run("SHOW CONSTRAINTS YIELD name").data()
        for constraint in constraints:
            session.run(f"DROP CONSTRAINT {constraint['name']}")
    yield driver
    with driver.session() as session:
        session.run("MATCH (n) DETACH DELETE n")
        constraints = session.run("SHOW CONSTRAINTS YIELD name").data()
        for constraint in constraints:
            session.run(f"DROP CONSTRAINT {constraint['name']}")

@pytest.fixture(scope="function")
def test_csv_dir(test_import_dir: Path) -> Path:
    """
    Provides a function-scoped temporary directory for CSV files.
    """
    for item in test_import_dir.iterdir():
        if item.is_file():
            item.unlink()
        elif item.is_dir():
            shutil.rmtree(item)
    yield test_import_dir
