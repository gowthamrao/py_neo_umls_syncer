"""
Tests for the transformer module.
"""
import pytest
import csv
from pathlib import Path
from pyNeoUmlsSyncer import transformer
from pyNeoUmlsSyncer.models import Concept, Code, ConceptRelationship

@pytest.fixture
def sample_parsed_data():
    """Provides a sample of parsed data for testing the transformer."""
    concepts = {
        "C001": Concept(cui="C001", preferred_name="Concept 1", last_seen_version="v1"),
        "C002": Concept(cui="C002", preferred_name="Concept 2", last_seen_version="v1"),
    }
    codes = {
        "SAB_A:1": Code(cui="C001", code_id="SAB_A:1", sab="SAB_A", name="Code 1A", last_seen_version="v1"),
        "SAB_B:2": Code(cui="C001", code_id="SAB_B:2", sab="SAB_B", name="Code 1B", last_seen_version="v1"),
        "SAB_A:3": Code(cui="C002", code_id="SAB_A:3", sab="SAB_A", name="Code 2A", last_seen_version="v1"),
    }
    relationships = [
        ConceptRelationship(
            source_cui="C001",
            target_cui="C002",
            biolink_predicate="biolink:related_to",
            source_rela="RO",
            asserted_by_sabs={"SAB_A"},
            last_seen_version="v1"
        )
    ]
    cui_to_tuis = {
        "C001": {"T047"},  # Disease
        "C002": {"T121"},  # Chemical
    }
    return (concepts, codes, relationships, cui_to_tuis)


def test_transform_to_csv_has_code(sample_parsed_data, tmp_path: Path):
    """
    Tests that the HAS_CODE relationship CSV is generated correctly and reliably.
    """
    transformer.transform_to_csv(sample_parsed_data, tmp_path)

    has_code_file = tmp_path / "has_code_rels.csv"
    assert has_code_file.exists()

    with open(has_code_file, 'r') as f:
        reader = csv.reader(f)
        header = next(reader)
        assert header == [":START_ID(Concept)", ":END_ID(Code)", "last_seen_version:string", ":TYPE"]

        rows = list(reader)
        assert len(rows) == 3

        # Convert rows to a set of tuples for easy comparison, ignoring order
        row_set = {tuple(row) for row in rows}

        expected_rows = {
            ("C001", "SAB_A:1", "v1", "HAS_CODE"),
            ("C001", "SAB_B:2", "v1", "HAS_CODE"),
            ("C002", "SAB_A:3", "v1", "HAS_CODE"),
        }

        assert row_set == expected_rows
