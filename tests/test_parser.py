# Jules was here
"""
Tests for the RRFParser, focusing on complex logic like preferred name selection.
"""
import pytest
from pathlib import Path

from py_neo_umls_syncer.parser import RRFParser
from py_neo_umls_syncer.config import settings

def test_preferred_name_selection_logic():
    """
    Validates that the parser's reduction step correctly selects the preferred name
    for a concept based on the two-tiered priority system:
    1. The SAB_PRIORITY list from the configuration.
    2. The standard UMLS term ranking (TS, STT, ISPREF) as a fallback.
    """
    # ARRANGE
    # Override the default SAB priority for a predictable test environment
    settings.sab_priority = ["HIGHEST_PRIORITY_SAB", "MID_PRIORITY_SAB"]

    cui1 = "C0001"  # Test case for SAB priority
    cui2 = "C0002"  # Test case for UMLS rank fallback

    # Mock data representing parsed terms from MRCONSO.RRF
    # This list contains multiple terms for each CUI with carefully chosen attributes
    # to test the sorting and selection logic.
    mock_parsed_terms = [
        # Terms for CUI1: The top priority SAB should win, even with worse UMLS rank
        (cui1, {"sab": "LOW_PRIORITY_SAB", "name": "CUI1 Best UMLS Rank", "ts": "P", "stt": "PF", "ispref": "Y", "code": "1", "tty": "PT"}),
        (cui1, {"sab": "MID_PRIORITY_SAB", "name": "CUI1 Mid Priority SAB", "ts": "S", "stt": "VO", "ispref": "N", "code": "2", "tty": "AB"}),
        (cui1, {"sab": "HIGHEST_PRIORITY_SAB", "name": "CUI1 Top Priority SAB", "ts": "S", "stt": "VO", "ispref": "N", "code": "3", "tty": "SY"}),

        # Terms for CUI2: All have same/low SAB priority, so UMLS rank should decide
        (cui2, {"sab": "OTHER_SAB", "name": "CUI2 Worst Rank", "ts": "S", "stt": "VO", "ispref": "N", "code": "4", "tty": "PT"}),
        (cui2, {"sab": "OTHER_SAB", "name": "CUI2 Best Rank (TS=P)", "ts": "P", "stt": "VO", "ispref": "N", "code": "5", "tty": "PT"}),
        (cui2, {"sab": "OTHER_SAB", "name": "CUI2 Second Best Rank (STT=PF)", "ts": "S", "stt": "PF", "ispref": "N", "code": "6", "tty": "PT"}),
        (cui2, {"sab": "OTHER_SAB", "name": "CUI2 Third Best Rank (ISPREF=Y)", "ts": "S", "stt": "VO", "ispref": "Y", "code": "7", "tty": "PT"}),
    ]

    # Instantiate a dummy parser; we don't need a real `meta_dir` for this unit test,
    # but the __init__ method requires a Path-like object.
    parser = RRFParser(meta_dir=Path("/tmp"))

    # ACT
    # Call the private method that contains the logic we want to test
    concepts, codes, concept_to_code_rels = parser._reduce_mrconso_results(mock_parsed_terms)

    # ASSERT
    # Verify that the correct preferred name was chosen for each concept
    assert len(concepts) == 2
    assert concepts[cui1].preferred_name == "CUI1 Top Priority SAB", \
        "The term from the highest priority SAB should always be chosen."

    assert concepts[cui2].preferred_name == "CUI2 Best Rank (TS=P)", \
        "When SAB priority is equal, the term with TS='P' should be chosen."

    # Verify that all corresponding Code nodes and relationships were still created
    assert len(codes) == 7
    assert len(concept_to_code_rels) == 7
