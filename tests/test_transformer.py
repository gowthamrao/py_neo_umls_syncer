import pytest
from pathlib import Path

# Mock settings before other imports
from .mocks import mock_settings
mock_settings()

from pyNeoUmlsSyncer.parser import UmlsParser
from pyNeoUmlsSyncer.transformer import UmlsTransformer
from pyNeoUmlsSyncer.config import settings

@pytest.fixture(scope="module")
def transformed_data():
    """
    A fixture that runs the parser and transformer on test data once.
    This is efficient as the parsing/transforming only happens once per test module run.
    """
    # Using a manual patch to avoid issues with multiprocessing context
    original_filter = settings.sab_filter
    settings.sab_filter = ['SAB1', 'SAB2']

    try:
        rrf_path = Path(__file__).parent / "data" / "test_version" / "META"
        parser = UmlsParser(rrf_path)

        # We need to limit the workers in a test environment
        parser.max_workers = 1

        cui_terms = parser.get_cui_terms()
        cui_stys = parser.get_cui_semantic_types()
        # Only get relationships for the SABs we are testing
        all_rels = parser.get_cui_relationships()
        cui_rels = [r for r in all_rels if r[3] in settings.sab_filter]

        transformer = UmlsTransformer(version="test_version")
        concepts, codes, has_code_rels, concept_rels = transformer.transform_data(
            cui_terms, cui_stys, cui_rels
        )

        return {
            "concepts": {c.cui: c for c in concepts},
            "codes": {c.code_id: c for c in codes},
            "has_code_rels": has_code_rels,
            "concept_rels": concept_rels,
        }
    finally:
        # Ensure we clean up the patch
        settings.sab_filter = original_filter


def test_concept_creation(transformed_data):
    """Tests that Concept nodes are created correctly."""
    concepts = transformed_data["concepts"]
    assert "C001" in concepts
    assert "C002" in concepts

    # Test preferred name selection (SAB1 is higher prio than SAB2)
    assert concepts["C001"].preferred_name == "Term 1 Sab1 Pref"

    # Test Biolink category mapping
    assert "biolink:Disease" in concepts["C001"].biolink_categories
    assert "biolink:PharmacologicSubstance" in concepts["C002"].biolink_categories
    assert "biolink:OrganicChemical" in concepts["C002"].biolink_categories

def test_code_creation(transformed_data):
    """Tests that Code nodes and HAS_CODE relationships are created."""
    codes = transformed_data["codes"]
    has_code_rels = transformed_data["has_code_rels"]

    assert "SAB1:CODE1" in codes
    assert "SAB2:CODE2" in codes

    # Check that a code was created for the non-preferred term as well
    assert codes["SAB2:CODE2"].name == "Term 1 Sab2"

    # Check that HAS_CODE relationships were created for both
    cui1_codes = {r.code_id for r in has_code_rels if r.cui == 'C001'}
    assert {"SAB1:CODE1", "SAB2:CODE2"} == cui1_codes

def test_relationship_aggregation(transformed_data):
    """
    Tests the crucial logic that relationships asserted by multiple sources
    are aggregated into a single relationship with combined provenance.
    """
    concept_rels = transformed_data["concept_rels"]

    # The C001-treats-C002 relationship is in the test data twice (SAB1, SAB2)
    # It should only appear once in the output.

    treats_rels = [r for r in concept_rels if r.rel_type == 'biolink:treats']
    assert len(treats_rels) == 1

    the_treats_rel = treats_rels[0]
    assert the_treats_rel.source_cui == "C001"
    assert the_treats_rel.target_cui == "C002"

    # Check that the asserted_by_sabs property contains both sources
    assert the_treats_rel.asserted_by_sabs == {"SAB1", "SAB2"}

    # Check that the other relationship is also present
    assoc_rels = [r for r in concept_rels if r.source_rela == 'associated_with']
    assert len(assoc_rels) == 1
    assert assoc_rels[0].rel_type == 'biolink:related_to'
    assert assoc_rels[0].asserted_by_sabs == {"SAB1"}
