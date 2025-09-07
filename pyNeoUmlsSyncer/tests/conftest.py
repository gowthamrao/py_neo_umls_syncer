"""
Pytest configuration and fixtures for the test suite.
"""
import pytest
from neo4j import GraphDatabase, Driver
from testcontainers.neo4j import Neo4jContainer

@pytest.fixture(scope="session")
def neo4j_container():
    """
    Starts a Neo4j container for the test session.
    """
    with Neo4jContainer("neo4j:5-enterprise") as container:
        container.with_env("NEO4J_ACCEPT_LICENSE_AGREEMENT", "yes")
        container.with_apoc()
        yield container

@pytest.fixture(scope="function")
def neo4j_driver(neo4j_container: Neo4jContainer) -> Driver:
    """
    Provides a driver to the test Neo4j container and cleans the DB for each test.
    """
    uri = neo4j_container.get_connection_url()
    auth = (neo4j_container.username, neo4j_container.password)
    driver = GraphDatabase.driver(uri, auth=auth)

    # Clean database before each test
    with driver.session() as session:
        session.run("MATCH (n) DETACH DELETE n")

    yield driver

    driver.close()
