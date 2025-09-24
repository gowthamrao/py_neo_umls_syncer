# Copyright (c) 2025-2026 Gowtham Adamane Rao. All Rights Reserved.
#
# Licensed under the Prosperity Public License 3.0.0 (the "License").
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License in the LICENSE file at the root
# of this repository, or at: https://prosperitylicense.com/versions/3.0.0
#
# Commercial use beyond a 30-day trial requires a separate license.
import pytest
from pathlib import Path

from py_neo_umls_syncer.parser import (
    parse_mrsty,
    parse_mrrel,
    parse_mrconso,
)
from py_neo_umls_syncer.config import get_settings

# Define the path to the test data directory
TEST_DATA_DIR = Path(__file__).parent / "data"

@pytest.fixture(autouse=True)
def setup_test_environment(monkeypatch):
    """
    A pytest fixture to set up the environment variables needed for tests.
    `autouse=True` ensures it runs before every test in this module.
    """
    monkeypatch.setenv("UMLS_API_KEY", "test-key-for-testing")
    # Since we are using a factory, we must clear the cache to ensure
    # the new environment variables are picked up by the settings object.
    get_settings.cache_clear()
    yield
    # Teardown: clear the cache again after the test
    get_settings.cache_clear()


def test_parse_mrsty():
    """
    Tests the MRSTY parser with a sample file.
    """
    mrsty_file = TEST_DATA_DIR / "MRSTY.RRF"
    result = parse_mrsty(mrsty_file)

    assert isinstance(result, dict)
    assert len(result) == 3
    assert result["C0000001"] == "T047"
    assert result["C0000002"] == "T121"
    assert result["C0000003"] == "T028"

def test_parse_mrrel():
    """
    Tests the MRREL parser with a sample file, checking filtering.
    """
    mrrel_file = TEST_DATA_DIR / "MRREL.RRF"
    results = list(parse_mrrel(mrrel_file))

    assert len(results) == 2
    assert results[0]["source_cui"] == "C0000001"
    assert results[0]["target_cui"] == "C0000002"
    assert results[0]["rela"] == "treats"
    assert results[0]["sab"] == "RXNORM"

    assert results[1]["source_cui"] == "C0000003"
    assert results[1]["rel"] == "RB"
    assert results[1]["rela"] == ""

def test_parse_mrconso_preferred_name_logic():
    """
    Tests the MRCONSO parser, focusing on the preferred name selection logic
    and filtering.
    """
    mrconso_file = TEST_DATA_DIR / "MRCONSO.RRF"
    results = list(parse_mrconso(mrconso_file))

    assert len(results) == 2

    cui1_res = next((res for res in results if res[0] == "C0000001"), None)
    assert cui1_res is not None
    cui, preferred_name, codes = cui1_res
    assert preferred_name == "Aspirin"
    assert len(codes) == 2
    assert any(c.code_id == "RXNORM:CODE1" for c in codes)
    assert any(c.code_id == "SNOMEDCT_US:CODE2" for c in codes)

    cui2_res = next((res for res in results if res[0] == "C0000002"), None)
    assert cui2_res is not None
    cui, preferred_name, codes = cui2_res
    assert preferred_name == "Preferred Name"
    assert len(codes) == 2
    assert any(c.code_id == "MTH:CODE4" for c in codes)
