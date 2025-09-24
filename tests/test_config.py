import pytest
from pathlib import Path

from py_neo_umls_syncer.parser import RRFParser
from py_neo_umls_syncer.config import settings

# --- Test Data with multiple Source Vocabularies (SABs) ---

MRCONSO_SAB_TEST = """C0000001|ENG|P|L1|PF|S1|Y|A1||M1||SAB1|PN|D1|Concept SAB1|0|N||
C0000002|ENG|P|L2|PF|S2|Y|A2||M2||SAB2|PN|D2|Concept SAB2|0|N||
C0000003|ENG|P|L3|PF|S3|Y|A3||M3||SAB1|PN|D3|Concept SAB1-2|0|N||
"""

@pytest.fixture
def setup_sab_filter_data(tmp_path):
    """Creates a mock UMLS release with multiple SABs in MRCONSO."""
    meta_dir = tmp_path / "SAB_TEST" / "META"
    meta_dir.mkdir(parents=True)
    (meta_dir / "MRCONSO.RRF").write_text(MRCONSO_SAB_TEST)
    # Create empty files for other parsers
    (meta_dir / "MRREL.RRF").write_text("")
    (meta_dir / "MRSTY.RRF").write_text("")
    return meta_dir

def test_sab_filter(monkeypatch, setup_sab_filter_data: Path):
    """
    Tests that the SAB_FILTER setting correctly filters concepts during parsing.
    """
    # Patch the settings object directly for this test. It expects a list of strings.
    monkeypatch.setattr(settings, "sab_filter", ["SAB1"])

    parser = RRFParser(setup_sab_filter_data)
    concepts, codes, _, _, _ = parser.parse_files()

    # Verification
    # Only concepts from SAB1 should be present.
    assert len(concepts) == 2
    assert "C0000001" in concepts
    assert "C0000003" in concepts
    assert "C0000002" not in concepts

    # Check that the corresponding codes are also filtered
    assert len(codes) == 2
    code_sabs = {c.sab for c in codes}
    assert "SAB1" in code_sabs
    assert "SAB2" not in code_sabs
