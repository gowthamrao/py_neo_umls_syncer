"""
Transforms parsed UMLS data into CSV files for Neo4j bulk import.

- Takes the output from the parser module.
- Enriches the data with Biolink mappings.
- Generates CSV files for nodes and relationships, formatted for `neo4j-admin`.
"""
import csv
import os
from pathlib import Path
from typing import Dict, List, Set

from .models import Concept, Code, ConceptRelationship
from .biolink_mapper import get_biolink_categories, get_biolink_predicate

def _write_csv(filepath: Path, header: List[str], rows: List[List[str]]):
    """Utility function to write data to a CSV file."""
    with open(filepath, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(rows)

def transform_to_csv(
    parsed_data: tuple,
    output_dir: Path
):
    """
    Transforms parsed UMLS data and writes it to CSV files for bulk import.

    Args:
        parsed_data: A tuple containing concepts, codes, relationships, and TUI mappings.
        output_dir: The directory to write the CSV files to.
    """
    concepts, codes, relationships, cui_to_tuis = parsed_data
    output_dir.mkdir(exist_ok=True)

    # --- 1. Prepare Concept Nodes CSV ---
    concept_header = ["cui:ID", "preferred_name:string", "last_seen_version:string", ":LABEL"]
    concept_rows = []
    for cui, concept in concepts.items():
        tuis = cui_to_tuis.get(cui, set())
        biolink_labels = get_biolink_categories(tuis)
        labels = "Concept;" + ";".join(sorted(list(biolink_labels)))
        concept_rows.append([
            concept.cui,
            concept.preferred_name,
            concept.last_seen_version,
            labels
        ])
    _write_csv(output_dir / "concepts.csv", concept_header, concept_rows)

    # --- 2. Prepare Code Nodes CSV ---
    code_header = ["code_id:ID", "sab:string", "name:string", "last_seen_version:string", ":LABEL"]
    code_rows = []
    for code_id, code in codes.items():
        code_rows.append([
            code.code_id,
            code.sab,
            code.name,
            code.last_seen_version,
            "Code"
        ])
    _write_csv(output_dir / "codes.csv", code_header, code_rows)

    # --- 3. Prepare HAS_CODE Relationship CSV ---
    has_code_header = [":START_ID(Concept)", ":END_ID(Code)", "last_seen_version:string", ":TYPE"]
    has_code_rows = []
    for code_id, code in codes.items():
        # The CUI is now reliably attached to the code object from the parser
        has_code_rows.append([code.cui, code.code_id, code.last_seen_version, "HAS_CODE"])
    _write_csv(output_dir / "has_code_rels.csv", has_code_header, has_code_rows)


    # --- 4. Prepare Concept-Concept Relationship CSV ---
    concept_rel_header = [
        ":START_ID(Concept)",
        ":END_ID(Concept)",
        "source_rela:string",
        "asserted_by_sabs:string[]",
        "last_seen_version:string",
        ":TYPE"
    ]
    concept_rel_rows = []
    # Aggregate relationships
    agg_rels = {}
    for rel in relationships:
        key = (rel.source_cui, rel.target_cui, get_biolink_predicate(rel.source_rela))
        if key not in agg_rels:
            rel.biolink_predicate = key[2]
            agg_rels[key] = rel
        else:
            agg_rels[key].asserted_by_sabs.update(rel.asserted_by_sabs)

    for rel in agg_rels.values():
         # Ensure both source and target concepts exist before creating a relationship
        if rel.source_cui in concepts and rel.target_cui in concepts:
            concept_rel_rows.append([
                rel.source_cui,
                rel.target_cui,
                rel.source_rela,
                ";".join(sorted(list(rel.asserted_by_sabs))),
                rel.last_seen_version,
                rel.biolink_predicate
            ])
    _write_csv(output_dir / "concept_rels.csv", concept_rel_header, concept_rel_rows)

    print(f"CSV files for bulk import have been generated in {output_dir}")
