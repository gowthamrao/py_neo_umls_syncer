# Jules was here
"""
Tests for the CSVTransformer, focusing on complex logic like provenance aggregation.
"""
import pytest
import csv
from pathlib import Path

from py_neo_umls_syncer.transformer import CSVTransformer
from py_neo_umls_syncer.models import InterConceptRelationship

def test_inter_concept_relationship_provenance_aggregation(test_csv_dir: Path):
    """
    Validates that the CSVTransformer correctly aggregates multiple relationship
    assertions from different source vocabularies (SABs) into a single
    relationship row with combined provenance.
    """
    # ARRANGE
    # Instantiate the transformer, pointing it to the test-specific temp directory
    # provided by the pytest fixture.
    transformer = CSVTransformer(import_dir=test_csv_dir)

    # Mock data representing parsed relationships from MRREL.RRF.
    # We have three distinct conceptual relationships to test:
    # 1. C001 -> C002 ('treats'): Asserted by two different SABs. This should be aggregated.
    # 2. C001 -> C003 ('associated_with'): Asserted by one SAB. This is a baseline case.
    # 3. C002 -> C001 ('treated_by'): A different relationship (direction/type). Should not be merged.
    mock_relationships = [
        InterConceptRelationship(source_cui="C001", target_cui="C002", source_rela="treats", sab="SAB_A"),
        InterConceptRelationship(source_cui="C001", target_cui="C002", source_rela="treats", sab="SAB_C"), # Same rel, different SAB
        InterConceptRelationship(source_cui="C001", target_cui="C003", source_rela="associated_with", sab="SAB_B"),
        InterConceptRelationship(source_cui="C002", target_cui="C001", source_rela="treated_by", sab="SAB_A"),
    ]

    # ACT
    # Call the private method that contains the aggregation logic.
    # Testing private methods is acceptable for focused unit tests like this.
    transformer._write_inter_concept_rels_csv(mock_relationships, version="v_test")

    # ASSERT
    # Read the generated CSV file and verify its contents.
    output_file = test_csv_dir / "rels_inter_concept.csv"
    assert output_file.exists(), "The output CSV file was not created."

    with open(output_file, 'r', newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    assert len(rows) == 3, f"Expected 3 aggregated relationship rows, but found {len(rows)}."

    # 1. Check the aggregated row (C001 -> C002)
    agg_row = next((r for r in rows if r[":START_ID(Concept-ID)"] == "C001" and r[":END_ID(Concept-ID)"] == "C002"), None)
    assert agg_row is not None, "Could not find the aggregated relationship for C001->C002."

    # The list of SABs should be sorted alphabetically and joined by a semicolon.
    assert agg_row["asserted_by_sabs:string[]"] == "SAB_A;SAB_C"
    assert agg_row["source_rela:string"] == "treats"
    assert agg_row["last_seen_version:string"] == "v_test"
    assert agg_row[":TYPE"] == "biolink:treats"  # Mapped from 'treats'

    # 2. Check the single-assertion row (C001 -> C003)
    single_row = next((r for r in rows if r[":START_ID(Concept-ID)"] == "C001" and r[":END_ID(Concept-ID)"] == "C003"), None)
    assert single_row is not None, "Could not find the single-assertion relationship for C001->C003."
    assert single_row["asserted_by_sabs:string[]"] == "SAB_B"
    assert single_row["source_rela:string"] == "associated_with"
    assert single_row[":TYPE"] == "biolink:related_to" # Mapped from 'associated_with'

    # 3. Check the reverse-direction row (C002 -> C001)
    reverse_row = next((r for r in rows if r[":START_ID(Concept-ID)"] == "C002" and r[":END_ID(Concept-ID)"] == "C001"), None)
    assert reverse_row is not None, "Could not find the reverse-direction relationship for C002->C001."
    assert reverse_row["asserted_by_sabs:string[]"] == "SAB_A"
    assert reverse_row["source_rela:string"] == "treated_by"
    assert reverse_row[":TYPE"] == "biolink:treated_by" # Mapped from 'treated_by'
