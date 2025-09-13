import pytest
from pathlib import Path
import shutil

from py_neo_umls_syncer.parser import RRFParser

# --- Malformed Test Data ---

# A correct line has 18 fields and a trailing pipe, resulting in 19 elements from csv.reader
VALID_MRCONSO_LINE = "C0000001|ENG|P|L0000001|PF|S0000001|Y|A0000001||M0000001||MSH|PN|D000001|Concept One|0|N||\n"
# A malformed line with an extra column (19 fields), resulting in 20 elements
MALFORMED_MRCONSO_LINE = "C0000002|ENG|P|L0000002|PF|S0000002|Y|A0000002||M0000002||MSH|PN|D000002|Concept Two|0|N|EXTRA_COLUMN||\n"

MALFORMED_MRCONSO = VALID_MRCONSO_LINE + MALFORMED_MRCONSO_LINE

# A correct MRREL line has 16 fields and a trailing pipe -> 17 elements
VALID_MRREL_LINE = "C0000001|AUI1|STYPE1|REL|C0000002|AUI2|STYPE2|RELA|RUI|SRUI|MSH|SL|RG|DIR|N||\n"
# A malformed MRREL line with one fewer column -> 16 elements
MALFORMED_MRREL_LINE = "C0000001|AUI1|STYPE1|REL|C0000002|AUI2|STYPE2|RELA|RUI|SRUI|MSH|SL|RG|DIR|N\n"

MALFORMED_MRREL = VALID_MRREL_LINE + MALFORMED_MRREL_LINE


@pytest.fixture
def setup_malformed_mrconso(tmp_path):
    """Creates a mock UMLS release with a malformed MRCONSO.RRF."""
    meta_dir = tmp_path / "MALFORMED_CONSO" / "META"
    meta_dir.mkdir(parents=True)
    (meta_dir / "MRCONSO.RRF").write_text(MALFORMED_MRCONSO)
    (meta_dir / "MRREL.RRF").write_text("")
    (meta_dir / "MRSTY.RRF").write_text("")
    return meta_dir

@pytest.fixture
def setup_malformed_mrrel(tmp_path):
    """Creates a mock UMLS release with a malformed MRREL.RRF."""
    meta_dir = tmp_path / "MALFORMED_REL" / "META"
    meta_dir.mkdir(parents=True)
    # We need valid concepts for the relationships to be processed
    valid_mrconso_for_rel_test = (
        VALID_MRCONSO_LINE +
        "C0000002|ENG|P|L0000002|PF|S0000002|Y|A0000002||M0000002||MSH|PN|D000002|Concept Two|0|N||\n"
    )
    (meta_dir / "MRCONSO.RRF").write_text(valid_mrconso_for_rel_test)
    (meta_dir / "MRREL.RRF").write_text(MALFORMED_MRREL)
    (meta_dir / "MRSTY.RRF").write_text("")
    return meta_dir

def test_malformed_mrconso_is_skipped(setup_malformed_mrconso: Path):
    """
    Tests that the parser skips a malformed row in MRCONSO.RRF and continues without error.
    """
    parser = RRFParser(setup_malformed_mrconso)
    concepts, codes, _, _, _ = parser.parse_files()

    # The malformed row for C0000002 should be skipped, but C0000001 should be processed.
    assert len(concepts) == 1
    assert "C0000001" in concepts
    assert "C0000002" not in concepts
    assert len(codes) == 1
    assert codes[0].code_id == "MSH:D000001"

def test_malformed_mrrel_is_skipped(setup_malformed_mrrel: Path):
    """
    Tests that the parser skips a malformed row in MRREL.RRF and continues without error.
    """
    parser = RRFParser(setup_malformed_mrrel)
    _, _, _, rels, _ = parser.parse_files()

    # The malformed row should be skipped, but the valid one should be processed.
    assert len(rels) == 1
    assert rels[0].source_cui == "C0000001"
