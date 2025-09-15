from pathlib import Path
import pytest

from py_neo_umls_syncer.parser import stream_mrconso, stream_mrsty, stream_mrrel
from py_neo_umls_syncer.config import Settings

@pytest.fixture
def test_settings():
    """Fixture to provide a Settings object for tests."""
    # The test files are in tests/data relative to the project root
    # The tests are run from the project root (py_neo_umls_syncer/)
    settings = Settings(input_dir="tests/data")
    return settings

def test_stream_mrconso(test_settings):
    """Test streaming of the MRCONSO.RRF file."""
    records = list(stream_mrconso(test_settings))
    assert len(records) == 6

    first_record = records[0]
    assert first_record["CUI"] == "C0000001"
    assert first_record["SAB"] == "RXNORM"
    assert first_record["STR"] == "Aspirin"
    assert first_record["SUPPRESS"] == "N"

def test_stream_mrsty(test_settings):
    """Test streaming of the MRSTY.RRF file."""
    records = list(stream_mrsty(test_settings))
    assert len(records) == 4

    first_record = records[0]
    assert first_record["CUI"] == "C0000001"
    assert first_record["TUI"] == "T047"
    assert first_record["STY"] == "Disease or Syndrome"

def test_stream_mrrel(test_settings):
    """Test streaming of the MRREL.RRF file."""
    records = list(stream_mrrel(test_settings))
    assert len(records) == 3

    first_record = records[0]
    assert first_record["CUI1"] == "C0000002"
    assert first_record["CUI2"] == "C0000001"
    assert first_record["RELA"] == "TREATS"
    assert first_record["SAB"] == "SEMMEDDB"

def test_parser_file_not_found(test_settings):
    """Test that the parsers handle non-existent files gracefully."""
    # Point to a non-existent directory
    test_settings.input_dir = "non_existent_dir"

    mrconso_records = list(stream_mrconso(test_settings))
    assert len(mrconso_records) == 0

    mrsty_records = list(stream_mrsty(test_settings))
    assert len(mrsty_records) == 0

    mrrel_records = list(stream_mrrel(test_settings))
    assert len(mrrel_records) == 0
