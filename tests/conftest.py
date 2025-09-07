import pytest
from neo4j import GraphDatabase, Driver
from testcontainers.neo4j import Neo4jContainer
from pathlib import Path
import shutil

@pytest.fixture(scope="session")
def neo4j_container():
    """
    A pytest fixture that starts and stops a Neo4j container for the test session.
    The container is configured with APOC plugins.
    """
    with Neo4jContainer(
        image="neo4j:5.18",  # Use a recent version
        port=7687,
        http_port=7474,
        environ={
            "NEO4J_PLUGINS": '["apoc"]',
            "NEO4J_apoc_export_file_enabled": "true",
            "NEO4J_apoc_import_file_enabled": "true",
            "NEO4J_apoc_import_file_use__neo4j__config": "true",
            "NEO4J_dbms_security_procedures_unrestricted": "apoc.*"
        }
    ) as container:
        container.driver = container.get_driver()
        yield container
        container.driver.close()


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
def test_csv_dir() -> Path:
    """
    Creates a temporary directory for test CSV files.
    """
    test_dir = Path("./test_csv_output")
    test_dir.mkdir(exist_ok=True)
    yield test_dir
    # Teardown: remove the directory after tests are done
    shutil.rmtree(test_dir)
