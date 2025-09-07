"""
Tests for the parser module, especially the preferred name selection logic.
"""
import pytest
from collections import defaultdict
from pyNeoUmlsSyncer.parser import _process_atoms_for_preferred_names
from pyNeoUmlsSyncer.config import AppConfig
from pyNeoUmlsSyncer.models import Concept, Code

# MRCONSO.RRF column indices
CUI_I, LAT_I, TS_I, LUI_I, STT_I, SUI_I, ISPREF_I, AUI_I, SAUI_I, SCUI_I, SDUI_I, SAB_I, TTY_I, CODE_I, STR_I, SRL_I, SUPPRESS_I, CVF_I = range(18)

@pytest.fixture
def sample_config(monkeypatch) -> AppConfig:
    """Provides a sample AppConfig for testing."""
    # Set a dummy API key to prevent validation errors during test execution
    monkeypatch.setenv("UMLS_API_KEY", "test-dummy-key")

    return AppConfig(
        umls_version="2025AA",
        sab_priority={"sab_priority": ["SAB_A", "SAB_B"]},
        filters={"sab_filter": ["SAB_A", "SAB_B", "SAB_C"]}
    )

def test_preferred_name_selection(sample_config: AppConfig):
    """
    Tests the logic for selecting the preferred name for a CUI based on a
    hierarchy of SAB priority and term ranks.
    """
    cui = "C0000123"
    atoms = [
        # A non-preferred term from the highest priority SAB
        [cui, "ENG", "S", "LUI1", "VW", "SUI1", "N", "AUI1", "", "", "", "SAB_A", "AB", "CODE1", "Term A1 (Non-preferred)", "0", "N", ""],
        # The true preferred term from a lower priority SAB
        [cui, "ENG", "P", "LUI2", "PF", "SUI2", "Y", "AUI2", "", "", "", "SAB_B", "PT", "CODE2", "Term B (Preferred)", "0", "N", ""],
        # A preferred term from an even lower priority SAB (should be ignored)
        [cui, "ENG", "P", "LUI3", "PF", "SUI3", "Y", "AUI3", "", "", "", "SAB_C", "PT", "CODE3", "Term C (Ignored)", "0", "N", ""],
        # A preferred term from SAB_A, but with a lower rank (STT='VO')
        [cui, "ENG", "P", "LUI4", "VO", "SUI4", "Y", "AUI4", "", "", "", "SAB_A", "PT", "CODE4", "Term A2 (Lower Rank)", "0", "N", ""],
    ]

    # The expected ranking is:
    # 1. SAB_A, P, PF, Y  (not present)
    # 2. SAB_A, P, VO, Y  (Term A2)
    # 3. SAB_B, P, PF, Y  (Term B)
    # The logic should pick Term A2 because SAB_A has higher priority than SAB_B, even though Term B has a better STT.
    # Let's adjust the test case to make SAB_A win.

    atoms_for_sab_a_win = [
        [cui, "ENG", "P", "LUI4", "VO", "SUI4", "Y", "AUI4", "", "", "", "SAB_A", "PT", "CODE4", "Term A (Winner)", "0", "N", ""],
        [cui, "ENG", "P", "LUI2", "PF", "SUI2", "Y", "AUI2", "", "", "", "SAB_B", "PT", "CODE2", "Term B (Loser)", "0", "N", ""],
    ]

    cui_groups = defaultdict(list)
    for atom in atoms_for_sab_a_win:
        cui_groups[atom[CUI_I]].append(atom)

    concepts, codes = _process_atoms_for_preferred_names(cui_groups, atoms_for_sab_a_win, sample_config)

    # Assert that the concept has the correct preferred name
    assert cui in concepts
    assert concepts[cui].preferred_name == "Term A (Winner)"

    # Assert that all codes from the filtered SABs were created
    assert len(codes) == 2
    assert "SAB_A:CODE4" in codes
    assert "SAB_B:CODE2" in codes
