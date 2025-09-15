import pytest
from neo4j import Driver
from pathlib import Path

from py_neo_umls_syncer.loader import Neo4jLoader
from py_neo_umls_syncer.delta_strategy import DeltaStrategy
from py_neo_umls_syncer.config import settings

# --- Test Data (V1 is a subset of test_pipeline's V1) ---

V1_MRCONSO = """C0000001|ENG|P|L0000001|PF|S0000001|Y|A0000001||M0000001||MSH|PN|D000001|Concept One|0|N||
C0000002|ENG|P|L0000002|PF|S0000002|Y|A0000002||M0000002||MSH|PN|D000002|Concept Two|0|N||
"""
V1_MRREL = "C0000001|||part_of||C0000002|||R0000001||MSH||||N||\n"
V1_MRSTY = """C0000001|T047|Disease or Syndrome|Disease or Syndrome|||
C0000002|T023|Body Part, Organ, or Organ Component|AnatomicalEntity|||
"""

# V2 is a new version, but without any deletes or merges
V2_MRCONSO = V1_MRCONSO + "C0000006|ENG|P|L0000006|PF|S0000006|Y|A0000006||M0000006||MSH|PN|D000006|Concept Six (New)|0|N||\n"
V2_MRREL = V1_MRREL + "C0000006|||isa||C0000001|||R0000004||MSH||||N||\n"
V2_MRSTY = V1_MRSTY + "C0000006|T047|Disease or Syndrome|Disease or Syndrome|||\n"


@pytest.fixture
def setup_missing_change_files_data(tmp_path_factory):
    """Creates mock UMLS releases where V2 is missing change files."""
    root_dir = tmp_path_factory.mktemp("umls_data_missing_changes")

    # V1 release
    v1_dir = root_dir / "2026AA" / "META"
    v1_dir.mkdir(parents=True)
    (v1_dir / "MRCONSO.RRF").write_text(V1_MRCONSO)
    (v1_dir / "MRREL.RRF").write_text(V1_MRREL)
    (v1_dir / "MRSTY.RRF").write_text(V1_MRSTY)

    # V2 release (no DELETEDCUI.RRF or MERGEDCUI.RRF)
    v2_dir = root_dir / "2026AB" / "META"
    v2_dir.mkdir(parents=True)
    (v2_dir / "MRCONSO.RRF").write_text(V2_MRCONSO)
    (v2_dir / "MRREL.RRF").write_text(V2_MRREL)
    (v2_dir / "MRSTY.RRF").write_text(V2_MRSTY)

    return {"v1": v1_dir, "v2": v2_dir}


def test_incremental_sync_missing_change_files(neo4j_driver: Driver, setup_missing_change_files_data: dict):
    """
    Tests that incremental sync runs successfully when DELETEDCUI and MERGEDCUI are not present in the release.
    """
    # Step 1: Full import of V1
    v1_version = "2026AA"
    loader_v1 = Neo4jLoader(driver=neo4j_driver)
    # Simulate neo4j-admin import by directly running the Cypher
    strategy_v1 = DeltaStrategy(neo4j_driver, v1_version, Path(settings.neo4j_import_dir))
    loader_v1.run_bulk_import(meta_dir=setup_missing_change_files_data["v1"], version=v1_version)
    strategy_v1.apply_additions_and_updates()
    loader_v1.update_meta_node_after_bulk(v1_version)

    # Verify initial state
    with neo4j_driver.session() as session:
        node_count = session.run("MATCH (n) WHERE not n:UMLS_Meta RETURN count(n) AS count").single()["count"]
        assert node_count == 4 # 2 concepts, 2 codes

    # Step 2: Incremental sync of V2
    v2_version = "2026AB"
    loader_v2 = Neo4jLoader(driver=neo4j_driver)
    loader_v2.run_incremental_sync(meta_dir=setup_missing_change_files_data["v2"], version=v2_version)

    # Step 3: Verification
    with neo4j_driver.session() as session:
        # Meta node updated
        meta_version = session.run("MATCH (m:UMLS_Meta) RETURN m.version AS version").single()["version"]
        assert meta_version == v2_version

        # New concept is present
        new_node = session.run("MATCH (c:Concept {cui: 'C0000006'}) RETURN c").single()
        assert new_node is not None

        # Total nodes = 4 (from V1) + 2 (new concept and code)
        node_count = session.run("MATCH (n) WHERE not n:UMLS_Meta RETURN count(n) AS count").single()["count"]
        assert node_count == 6
