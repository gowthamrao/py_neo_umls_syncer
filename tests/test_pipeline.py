# Copyright (c) 2025-2026 Gowtham Adamane Rao. All Rights Reserved.
#
# Licensed under the Prosperity Public License 3.0.0 (the "License").
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License in the LICENSE file at the root
# of this repository, or at: https://prosperitylicense.com/versions/3.0.0
#
# Commercial use beyond a 30-day trial requires a separate license.
# Jules was here
import pytest
from neo4j import Driver
from testcontainers.neo4j import Neo4jContainer
from pathlib import Path
import subprocess
import time
import os

from py_neo_umls_syncer.loader import Neo4jLoader
from py_neo_umls_syncer.config import settings
from py_neo_umls_syncer.delta_strategy import DeltaStrategy

# --- Test Data ---

V1_MRCONSO = """C0000001|ENG|P|L0000001|PF|S0000001|Y|A0000001||M0000001||MSH|PN|D000001|Concept One|0|N||
C0000002|ENG|P|L0000002|PF|S0000002|Y|A0000002||M0000002||MSH|PN|D000002|Concept Two|0|N||
C0000003|ENG|P|L0000003|PF|S0000003|Y|A0000003||M0000003||MSH|PN|D000003|Concept Three (stale)|0|N||
C0000004|ENG|P|L0000004|PF|S0000004|Y|A0000004||M0000004||MSH|PN|D000004|Concept Four (to be merged)|0|N||
C0000005|ENG|P|L0000005|PF|S0000005|Y|A0000005||M0000005||MSH|PN|D000005|Concept Five (to be deleted)|0|N||
"""

V1_MRREL = """C0000001|||part_of||C0000002|||R0000001||MSH||||N||
C0000001|||part_of||C0000003|||R0000002||MSH||||N||
C0000004|||isa||C0000002|||R0000003||MSH||||N||
"""

V1_MRSTY = """C0000001|T047|Disease or Syndrome|Disease or Syndrome|||
C0000002|T023|Body Part, Organ, or Organ Component|AnatomicalEntity|||
C0000003|T023|Body Part, Organ, or Organ Component|AnatomicalEntity|||
C0000004|T047|Disease or Syndrome|Disease or Syndrome|||
C0000005|T047|Disease or Syndrome|Disease or Syndrome|||
"""

V2_MRCONSO = """C0000001|ENG|P|L0000001|PF|S0000001|Y|A0000001||M0000001||MSH|PN|D000001|Concept One|0|N||
C0000002|ENG|P|L0000002|PF|S0000002|Y|A0000002||M0000002||MSH|PN|D000002|Concept Two Updated|0|N||
C0000006|ENG|P|L0000006|PF|S0000006|Y|A0000006||M0000006||MSH|PN|D000006|Concept Six (New)|0|N||
"""

V2_MRREL = """C0000001|||part_of||C0000002|||R0000001||MSH||||N||
C0000006|||isa||C0000001|||R0000004||MSH||||N||
"""

V2_MRSTY = """C0000001|T047|Disease or Syndrome|Disease or Syndrome|||
C0000002|T023|Body Part, Organ, or Organ Component|AnatomicalEntity|||
C0000006|T047|Disease or Syndrome|Disease or Syndrome|||
"""

DELETEDCUI = "C0000005|Concept Five (to be deleted)\n"
MERGEDCUI = "C0000004|C0000002\n"


@pytest.fixture(scope="module")
def setup_umls_data(tmp_path_factory):
    """Creates mock UMLS release directories and files."""
    root_dir = tmp_path_factory.mktemp("umls_data")

    # V1 release
    v1_dir = root_dir / "2025AA" / "META"
    v1_dir.mkdir(parents=True)
    (v1_dir / "MRCONSO.RRF").write_text(V1_MRCONSO)
    (v1_dir / "MRREL.RRF").write_text(V1_MRREL)
    (v1_dir / "MRSTY.RRF").write_text(V1_MRSTY)

    # V2 release
    v2_dir = root_dir / "2025AB" / "META"
    v2_dir.mkdir(parents=True)
    (v2_dir / "MRCONSO.RRF").write_text(V2_MRCONSO)
    (v2_dir / "MRREL.RRF").write_text(V2_MRREL)
    (v2_dir / "MRSTY.RRF").write_text(V2_MRSTY)
    (v2_dir / "DELETEDCUI.RRF").write_text(DELETEDCUI)
    (v2_dir / "MERGEDCUI.RRF").write_text(MERGEDCUI)

    return {"v1": v1_dir, "v2": v2_dir}

# We use a class to share state between dependent tests
class TestPipeline:
    @pytest.mark.dependency()
    def test_full_import_and_init(self, neo4j_driver: Driver, setup_umls_data: dict):
        """
        Tests the initial bulk import CSV generation and subsequent metadata initialization.
        """
        VERSION = "2025AA"
        loader = Neo4jLoader(driver=neo4j_driver)
        loader.run_bulk_import(meta_dir=setup_umls_data["v1"], version=VERSION)

        # Instead of running neo4j-admin, we simulate the load using APOC for simplicity
        # This still validates the CSVs are correct.
        # The import_dir is already configured in the neo4j_container fixture
        strategy = DeltaStrategy(neo4j_driver, VERSION, Path(settings.neo4j_import_dir))
        strategy.apply_additions_and_updates()

        # Now run the metadata initialization
        loader.update_meta_node_after_bulk(VERSION)

        # Verification
        with neo4j_driver.session() as session:
            meta_version = session.run("MATCH (m:UMLS_Meta) RETURN m.version AS version").single()["version"]
            assert meta_version == VERSION

            node_count = session.run("MATCH (n) RETURN count(n) AS count").single()["count"]
            assert node_count == 11 # 5 concepts + 5 codes + 1 meta

            rel_count = session.run("MATCH ()-[r]->() RETURN count(r) AS count").single()["count"]
            # The mock data for V1 only has 3 relationships in MRREL.RRF
            assert rel_count == 8 # 5 has_code + 3 inter_concept

    @pytest.mark.dependency(depends=["TestPipeline::test_full_import_and_init"])
    def test_incremental_sync(self, neo4j_driver: Driver, setup_umls_data: dict):
        """
        Tests the incremental synchronization process, including deletions, merges, and stale data removal.
        """
        VERSION = "2025AB"
        loader = Neo4jLoader(driver=neo4j_driver)
        loader.run_incremental_sync(meta_dir=setup_umls_data["v2"], version=VERSION)

        # Verification
        with neo4j_driver.session() as session:
            # 1. Meta node updated
            meta_version = session.run("MATCH (m:UMLS_Meta) RETURN m.version AS version").single()["version"]
            assert meta_version == VERSION

            # 2. Deleted CUI is gone
            deleted_node = session.run("MATCH (c:Concept {cui: 'C0000005'}) RETURN c").single()
            assert deleted_node is None

            # 3. Merged CUI is gone
            merged_node = session.run("MATCH (c:Concept {cui: 'C0000004'}) RETURN c").single()
            assert merged_node is None

            # 4. Merged CUI's relationships migrated to new CUI
            # C0000004->C0000002 became C0000002->C0000002 (self-ref), which is weird but we'll check the isa rel
            res = session.run("""
                MATCH (c:Concept {cui: 'C0000002'})-[r:biolink:subclass_of]->(c)
                RETURN r.source_rela AS rela
            """).single()
            assert res is not None
            assert res["rela"] == "isa"

            # 5. Stale relationship is gone (C0000001->C0000003)
            stale_rel = session.run("MATCH (:Concept {cui:'C0000001'})-[r]->(:Concept {cui:'C0000003'}) RETURN r").single()
            assert stale_rel is None

            # 6. New concept and rel are present
            new_node = session.run("MATCH (c:Concept {cui: 'C0000006'}) RETURN c").single()
            assert new_node is not None
            new_rel = session.run("MATCH (:Concept {cui:'C0000006'})-[r]->(:Concept {cui:'C0000001'}) RETURN r").single()
            assert new_rel is not None

            # 7. Concept property is updated
            updated_node = session.run("MATCH (c:Concept {cui: 'C0000002'}) RETURN c.preferred_name AS name").single()
            assert updated_node is not None
            assert updated_node["name"] == "Concept Two Updated"

    @pytest.mark.dependency(depends=["TestPipeline::test_incremental_sync"])
    def test_idempotency(self, neo4j_driver: Driver, setup_umls_data: dict):
        """
        Tests that running the same incremental sync again does not change the database state.
        """
        VERSION = "2025AB"
        # Get a snapshot of the current state (node and rel counts)
        with neo4j_driver.session() as session:
            initial_node_count = session.run("MATCH (n) RETURN count(n) AS count").single()["count"]
            initial_rel_count = session.run("MATCH ()-[r]->() RETURN count(r) AS count").single()["count"]

        # Run the sync again
        loader = Neo4jLoader(driver=neo4j_driver)
        loader.run_incremental_sync(meta_dir=setup_umls_data["v2"], version=VERSION)

        # Verify counts are identical
        with neo4j_driver.session() as session:
            final_node_count = session.run("MATCH (n) RETURN count(n) AS count").single()["count"]
            final_rel_count = session.run("MATCH ()-[r]->() RETURN count(r) AS count").single()["count"]
            assert initial_node_count == final_node_count
            assert initial_rel_count == final_rel_count
