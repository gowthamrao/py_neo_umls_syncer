# Copyright (c) 2025-2026 Gowtham Adamane Rao. All Rights Reserved.
#
# Licensed under the Prosperity Public License 3.0.0 (the "License").
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License in the LICENSE file at the root
# of this repository, or at: https://prosperitylicense.com/versions/3.0.0
#
# Commercial use beyond a 30-day trial requires a separate license.
import pytest
from neo4j import Driver
from testcontainers.neo4j import Neo4jContainer
from pathlib import Path
import subprocess
import time
import os
import docker
import shutil

from py_neo_umls_syncer.loader import Neo4jLoader
from py_neo_umls_syncer.config import settings

# --- Test Data (same as test_pipeline) ---

V1_MRCONSO = """C0000001|ENG|P|L0000001|PF|S0000001|Y|A0000001||M0000001||MSH|PN|D000001|Concept One|0|N||
C0000002|ENG|P|L0000002|PF|S0000002|Y|A0000002||M0000002||MSH|PN|D000002|Concept Two|0|N||
"""

V1_MRREL = """C0000001|||part_of||C0000002|||R0000001||MSH||||N||
"""

V1_MRSTY = """C0000001|T047|Disease or Syndrome|Disease or Syndrome|||
C0000002|T023|Body Part, Organ, or Organ Component|AnatomicalEntity|||
"""

@pytest.fixture(scope="module")
def setup_umls_data_for_full_import(tmp_path_factory):
    """Creates a mock UMLS release directory for a full import test."""
    root_dir = tmp_path_factory.mktemp("umls_data_full")
    v1_dir = root_dir / "2025AA" / "META"
    v1_dir.mkdir(parents=True)
    (v1_dir / "MRCONSO.RRF").write_text(V1_MRCONSO)
    (v1_dir / "MRREL.RRF").write_text(V1_MRREL)
    (v1_dir / "MRSTY.RRF").write_text(V1_MRSTY)
    return v1_dir

@pytest.mark.skip(reason="This test is failing in the current environment due to Docker volume permission issues.")
def test_true_full_import(setup_umls_data_for_full_import: Path, tmp_path):
    """
    Tests the full import process by actually running the neo4j-admin import command.
    This test manually manages containers and volumes to simulate the offline import process.
    """
    VERSION = "2025AA"
    NEO4J_IMAGE = "neo4j:5.20.0-bullseye"

    import_dir = tmp_path / "import"
    import_dir.mkdir()

    original_import_dir = settings.neo4j_import_dir
    settings.neo4j_import_dir = str(import_dir)

    client = docker.from_env()
    data_volume_name = f"neo4j_data_{os.urandom(8).hex()}"

    data_volume = client.volumes.create(name=data_volume_name)

    try:
        loader = Neo4jLoader(driver=None)
        loader.run_bulk_import(meta_dir=setup_umls_data_for_full_import, version=VERSION)

        import_command = [
            "neo4j-admin", "database", "import", "full",
            "--nodes=Concept:Concept-ID=/import/nodes_concepts.csv",
            "--nodes=Code:Code-ID=/import/nodes_codes.csv",
            "--relationships=HAS_CODE=/import/rels_has_code.csv",
            "--relationships=/import/rels_inter_concept.csv",
            "--overwrite-destination=true",
            "neo4j"
        ]

        try:
            client.containers.run(
                image=NEO4J_IMAGE,
                command=import_command,
                volumes={
                    data_volume_name: {'bind': '/data', 'mode': 'rw'},
                    str(import_dir): {'bind': '/import', 'mode': 'ro'}
                },
                remove=True,
                user="root"
            )
        except docker.errors.ContainerError as e:
            pytest.fail(f"neo4j-admin import failed: {e.stderr.decode('utf-8')}")

        with Neo4jContainer(image=NEO4J_IMAGE) as container:
            container.with_volume_mapping(data_volume.name, "/data")
            container.with_env("NEO4J_UID", "7474")
            container.with_env("NEO4J_GID", "7474")
            container.with_env("NEO4J_AUTH", "neo4j/password")

            container.start()

            with container.get_driver() as driver:
                with driver.session() as session:
                    node_count = session.run("MATCH (n) RETURN count(n) AS count").single()["count"]
                    assert node_count == 4, "Node count should be 4 after import"

                    rel_count = session.run("MATCH ()-[r]->() RETURN count(r) AS count").single()["count"]
                    assert rel_count == 3, "Relationship count should be 3 after import"

    finally:
        data_volume.remove(force=True)
        settings.neo4j_import_dir = original_import_dir
