import pytest
from neo4j import GraphDatabase, Driver, exceptions
from testcontainers.neo4j import Neo4jContainer
from pathlib import Path
import shutil
import time
import subprocess
import os
import requests
from testcontainers.core.wait_strategies import LogMessageWaitStrategy
from rich.console import Console

# Local import from the project
from src.py_neo_umls_syncer.config import settings
from src.py_neo_umls_syncer.loader import Neo4jLoader
from src.py_neo_umls_syncer.downloader import UMLSDownloader
from src.py_neo_umls_syncer.delta_strategy import DeltaStrategy
from typer.testing import CliRunner
from src.py_neo_umls_syncer.cli import app
import hashlib

console = Console()
runner = CliRunner()

def _create_file(filepath: Path, rows: list[list[str]]):
    """Helper to create a pipe-delimited file."""
    filepath.parent.mkdir(parents=True, exist_ok=True)
    with open(filepath, "w") as f:
        for row in rows:
            f.write("|".join(row) + "|\n")

def _create_csv_file(filepath: Path, header: list[str], rows: list[list[str]]):
    """Helper to create a CSV file."""
    filepath.parent.mkdir(parents=True, exist_ok=True)
    with open(filepath, "w") as f:
        writer = subprocess.run(["csv", "writer", "-H"] + header, input="\n".join([",".join(row) for row in rows]), text=True, check=True)

def _create_pipe_delimited_file(filepath: Path, rows: list[list[str]]):
    """Helper to create a pipe-delimited file."""
    _create_file(filepath, rows)


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
    It attempts to install APOC using the standard environment variable and mounts
    a directory to persist the downloaded plugin.
    """
    NEO4J_VERSION = "5.18-enterprise"
    plugins_dir = Path("/tmp/pyneo_test_plugins")
    plugins_dir.mkdir(parents=True, exist_ok=True)

    container = Neo4jContainer(image=f"neo4j:{NEO4J_VERSION}")
    container.with_env("NEO4J_PLUGINS", '["apoc-extended", "graph-data-science"]')
    container.with_env("NEO4J_dbms_security_procedures_unrestricted", "apoc.*,gds.*")
    container.with_env("NEO4J_apoc_import_file_enabled", "true")
    container.with_env("NEO4J_dbms_security_allow__csv__import__from__file__urls", "true")
    container.with_env("NEO4J_apoc_export_file_enabled", "true")
    container.with_env("NEO4J_ACCEPT_LICENSE_AGREEMENT", "yes")
    container.with_env("NEO4J_AUTH", "neo4j/password")
    container.with_env("NEO4J_dbms_directories_import", "/import")
    container.with_volume_mapping(str(test_import_dir), "/import")
    container.with_volume_mapping(str(plugins_dir), "/plugins")

    container.waiting_for(LogMessageWaitStrategy("Remote interface available at"))
    with container as c:
        driver = c.get_driver()
        start_time = time.time()
        while time.time() - start_time < 180:  # 3-minute timeout
            try:
                with driver.session() as session:
                    session.run("RETURN apoc.version(), gds.version()")
                    console.log("[green]APOC and GDS plugins confirmed available.[/green]")
                    break
            except (exceptions.ServiceUnavailable, exceptions.ClientError) as e:
                console.log(f"[yellow]Waiting for plugins to be available... ({e})[/yellow]")
                time.sleep(3)
        else:
            raise RuntimeError("Plugins did not become available in time.")

        c.driver = driver
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
            try:
                session.run(f"DROP CONSTRAINT {constraint['name']}")
            except exceptions.ClientError:
                pass # Ignore if constraint not found, might have been dropped by cascade
    yield driver
    with driver.session() as session:
        session.run("MATCH (n) DETACH DELETE n")
        constraints = session.run("SHOW CONSTRAINTS YIELD name").data()
        for constraint in constraints:
            try:
                session.run(f"DROP CONSTRAINT {constraint['name']}")
            except exceptions.ClientError:
                pass

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
