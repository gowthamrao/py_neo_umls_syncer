# Copyright (c) 2025-2026 Gowtham Adamane Rao. All Rights Reserved.
#
# Licensed under the Prosperity Public License 3.0.0 (the "License").
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License in the LICENSE file at the root
# of this repository, or at: https://prosperitylicense.com/versions/3.0.0
#
# Commercial use beyond a 30-day trial requires a separate license.
from py_neo_umls_syncer.biolink_mapper import (
    TUI_TO_BIOLINK_CATEGORY,
    RELA_TO_BIOLINK_PREDICATE,
)

def test_tui_to_biolink_category_mapping():
    """
    Tests that key TUIs are correctly mapped to Biolink categories.
    """
    assert TUI_TO_BIOLINK_CATEGORY["T047"] == "biolink:Disease"
    assert TUI_TO_BIOLINK_CATEGORY["T121"] == "biolink:Drug"
    assert TUI_TO_BIOLINK_CATEGORY["T028"] == "biolink:Gene"
    assert TUI_TO_BIOLINK_CATEGORY["T184"] == "biolink:PhenotypicFeature"
    assert TUI_TO_BIOLINK_CATEGORY["T025"] == "biolink:Cell"

def test_rela_to_biolink_predicate_mapping():
    """
    Tests that key RELAs and RELs are correctly mapped to Biolink predicates.
    """
    assert RELA_TO_BIOLINK_PREDICATE["treats"] == "biolink:treats"
    assert RELA_TO_BIOLINK_PREDICATE["isa"] == "biolink:subclass_of"

    # Test a fallback REL value
    assert RELA_TO_BIOLINK_PREDICATE["RB"] == "biolink:broad_match"
    assert RELA_TO_BIOLINK_PREDICATE["CHD"] == "biolink:subclass_of"
