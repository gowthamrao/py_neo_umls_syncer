import pytest
from neo4j import GraphDatabase, Driver
from testcontainers.neo4j import Neo4jContainer

from pyNeoUmlsSyncer.config import Settings

@pytest.fixture(scope="session")
def neo4j_container():
    """
    A pytest fixture that starts a Neo4j container for the test session.
    """
    # Use the official Neo4j container with APOC plugins
    with Neo4jContainer(image="neo4j:5.17", apoc=True) as container:
        yield container

@pytest.fixture(scope="session")
def neo4j_driver(neo4j_container: Neo4jContainer) -> Driver:
    """
    A fixture that provides a configured Neo4j driver for the container.
    """
    uri = neo4j_container.get_connection_url()
    user = neo4j_container.NEO4J_USER
    password = neo4j_container.NEO4J_PASSWORD
    driver = GraphDatabase.driver(uri, auth=(user, password))
    yield driver
    driver.close()

@pytest.fixture
def test_settings(neo4j_container: Neo4jContainer, tmp_path) -> Settings:
    """
    A fixture that provides a Settings object configured for the test environment.
    It points to the test data directory and the test container.
    """
    # Override settings to use the test container and test data
    test_data_dir = tmp_path / "data"
    test_data_dir.mkdir()

    # This is a bit of a hack to make the settings work with the test data structure
    # The parser expects the versioned folder to be inside the data_dir
    versioned_data_dir = test_data_dir
    (versioned_data_dir / "2024AA").mkdir(parents=True, exist_ok=True)

    # Copy sample data into the temporary test data directory
    import shutil
    shutil.copytree("tests/data/2024AA", str(versioned_data_dir / "2024AA"), dirs_exist_ok=True)

    return Settings(
        umls_api_key="fake-key",
        umls_version="2024AA",
        neo4j_uri=neo4j_container.get_connection_url(),
        neo4j_user=neo4j_container.NEO4J_USER,
        neo4j_password=neo4j_container.NEO4J_PASSWORD,
        data_dir=str(test_data_dir),
        sab_filter=[], # No filter for tests
    )
