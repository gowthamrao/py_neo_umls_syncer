import pytest
from pathlib import Path
import pandas as pd
import shutil

from py_neo_umls_syncer.transformer import Transformer
from py_neo_umls_syncer.config import Settings

@pytest.fixture
def transformer_test_environment(tmp_path):
    """
    Fixture to set up a test environment with sample data and a temporary output directory.
    """
    # Create a temporary output directory
    output_dir = tmp_path / "output"
    output_dir.mkdir()

    # Settings for the test
    settings = Settings(
        input_dir="tests/data",
        output_dir=str(output_dir),
        sab_filter=["RXNORM", "MSH", "HGNC", "GO", "SEMMEDDB"], # Exclude 'SRC'
        sab_priority=["RXNORM", "MSH", "HGNC"]
    )

    yield settings, output_dir

    # Teardown: shutil.rmtree(output_dir) is handled by tmp_path fixture

def test_transformer_full_run(transformer_test_environment):
    """
    Integration test for the Transformer class.
    It runs the full transformation process and verifies the output CSVs.
    """
    settings, output_dir = transformer_test_environment

    # Run the transformation
    transformer = Transformer(settings)
    transformer.transform_for_bulk_import()

    # --- Verify Headers ---
    assert (output_dir / "nodes_concepts_header.csv").exists()
    assert (output_dir / "nodes_codes_header.csv").exists()
    assert (output_dir / "rels_has_code_header.csv").exists()
    assert (output_dir / "rels_concept_biolink_treats_header.csv").exists()
    assert (output_dir / "rels_concept_biolink_subclass_of_header.csv").exists()

    # --- Verify Nodes Concepts CSV ---
    concepts_df = pd.read_csv(output_dir / "nodes_concepts.csv")
    assert len(concepts_df) == 3

    # C0000001 (Aspirin)
    concept1 = concepts_df[concepts_df['cui'] == 'C0000001'].iloc[0]
    assert concept1['preferred_name'] == 'Aspirin' # From RXNORM (high priority)
    assert concept1['LABEL'] == 'Concept;biolink:Disease'

    # C0000002 (BRCA1)
    concept2 = concepts_df[concepts_df['cui'] == 'C0000002'].iloc[0]
    assert concept2['preferred_name'] == 'BRCA1'
    assert 'biolink:Gene' in concept2['LABEL']

    # --- Verify Nodes Codes CSV ---
    codes_df = pd.read_csv(output_dir / "nodes_codes.csv")
    # 2 for C0000001, 1 for C0000002 (one suppressed), 1 for C0000003
    # The SRC record for C0000001 is filtered out by sab_filter
    assert len(codes_df) == 4
    assert "RXNORM:12345" in codes_df['code_id'].values
    assert "MSH:D12345" in codes_df['code_id'].values
    assert "HGNC:987" in codes_df['code_id'].values

    # --- Verify HAS_CODE Rels CSV ---
    has_code_df = pd.read_csv(output_dir / "rels_has_code.csv")
    assert len(has_code_df) == 4

    # --- Verify Concept-Concept Rels CSVs ---
    # treats
    treats_df = pd.read_csv(output_dir / "rels_concept_biolink_treats.csv")
    assert len(treats_df) == 1
    rel1 = treats_df.iloc[0]
    assert rel1['START_ID'] == 'C0000002'
    assert rel1['END_ID'] == 'C0000001'
    assert rel1['source_rela'] == 'TREATS'
    assert rel1['asserted_by_sabs'] == 'SEMMEDDB'

    # subclass_of
    isa_df = pd.read_csv(output_dir / "rels_concept_biolink_subclass_of.csv")
    assert len(isa_df) == 1
    rel2 = isa_df.iloc[0]
    assert rel2['START_ID'] == 'C0000003'
    assert rel2['END_ID'] == 'C0000001'
    assert rel2['source_rela'] == 'isa'
    assert rel2['asserted_by_sabs'] == 'GO'

    # Verify that the filtered-out relationship was not processed
    assert not (output_dir / "rels_concept_biolink_related_to.csv").exists()
