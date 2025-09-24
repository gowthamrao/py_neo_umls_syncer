# Copyright (c) 2025-2026 Gowtham Adamane Rao. All Rights Reserved.
#
# Licensed under the Prosperity Public License 3.0.0 (the "License").
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License in the LICENSE file at the root
# of this repository, or at: https://prosperitylicense.com/versions/3.0.0
#
# Commercial use beyond a 30-day trial requires a separate license.
# Jules was here
"""
This module provides mappings from UMLS to the Biolink Model.

NOTE: These mappings are representative and not exhaustive. A production-grade
system would require a more comprehensive and curated mapping, potentially
leveraging external resources or services from the NCATS Translator community.
The Biolink Model is the ultimate source of truth: https://github.com/biolink/biolink-model
"""

# Default mappings for unclassified entities
DEFAULT_BIOLINK_CATEGORY = "biolink:NamedThing"
DEFAULT_BIOLINK_PREDICATE = "biolink:related_to"

# UMLS Semantic Type (TUI) to Biolink Category Mapping
# A selection of common mappings.
UMLS_TUI_TO_BIOLINK_CATEGORY = {
    # Disorders
    "T019": "biolink:Disease",  # Congenital Abnormality
    "T020": "biolink:Disease",  # Acquired Abnormality
    "T037": "biolink:Disease",  # Injury or Poisoning
    "T047": "biolink:Disease",  # Disease or Syndrome
    "T048": "biolink:Disease",  # Mental or Behavioral Dysfunction
    "T049": "biolink:Disease",  # Cell or Molecular Dysfunction
    "T190": "biolink:Disease",  # Anatomical Abnormality
    "T191": "biolink:Disease",  # Neoplastic Process
    # Chemicals & Drugs
    "T109": "biolink:ChemicalEntity", # Organic Chemical
    "T116": "biolink:AminoAcidSequence", # Amino Acid, Peptide, or Protein
    "T121": "biolink:Drug",  # Pharmacologic Substance
    "T123": "biolink:ChemicalEntity", # Biologically Active Substance
    "T197": "biolink:ChemicalEntity", # Inorganic Chemical
    "T200": "biolink:Drug",  # Clinical Drug
    # Genes & Molecular
    "T028": "biolink:Gene",  # Gene or Genome
    "T114": "biolink:NucleicAcidSequence", # Nucleotide Sequence
    # Anatomy
    "T017": "biolink:AnatomicalEntity",  # Anatomical Structure
    "T023": "biolink:AnatomicalEntity",  # Body Part, Organ, or Organ Component
    "T024": "biolink:Tissue",  # Tissue
    "T025": "biolink:Cell",  # Cell
    "T026": "biolink:CellularComponent",  # Cell Component
    # Phenotypes & Findings
    "T033": "biolink:PhenotypicFeature",  # Finding
    "T034": "biolink:LaboratoryFinding", # Laboratory or Test Result
    "T184": "biolink:SignOrSymptom",  # Sign or Symptom
    # Procedures
    "T061": "biolink:Procedure",  # Therapeutic or Preventive Procedure
    # Biological Processes
    "T039": "biolink:PhysiologicalProcess", # Physiologic Function
    "T040": "biolink:OrganismalProcess", # Organism Function
    "T041": "biolink:PathologicalProcess", # Pathologic Function
    "T043": "biolink:BiologicalProcess",  # Cell Function
}

# UMLS Relationship Attribute (RELA) to Biolink Predicate Mapping
# This mapping is highly context-dependent. The REL value (the semantic relationship)
# is often more important than the RELA (the attribute). This is a simplified mapping.
# See https://www.ncbi.nlm.nih.gov/books/NBK9684/table/ch03.T.relationship_attributes_in_mrrelrr/?report=objectonly
UMLS_RELA_TO_BIOLINK_PREDICATE = {
    "treats": "biolink:treats",
    "treated_by": "biolink:treated_by",
    "isa": "biolink:subclass_of",
    "part_of": "biolink:part_of",
    "has_part": "biolink:has_part",
    "associated_with": "biolink:related_to",
    "causes": "biolink:causes",
    "caused_by": "biolink:caused_by",
    "location_of": "biolink:location_of",
    "has_location": "biolink:located_in", # Note inversion
    "diagnoses": "biolink:diagnoses",
    "diagnosed_by": "biolink:biomarker_for", # Approximation
    "prevents": "biolink:prevents",
    "prevented_by": "biolink:prevented_by",
    "produces": "biolink:produces",
    "produced_by": "biolink:produced_by",
    "contraindicated_with": "biolink:contraindicated_in",
}


def get_biolink_category(tui: str) -> str:
    """Maps a UMLS TUI to a Biolink Category."""
    return UMLS_TUI_TO_BIOLINK_CATEGORY.get(tui, DEFAULT_BIOLINK_CATEGORY)

def get_biolink_predicate(rela: str) -> str:
    """Maps a UMLS RELA to a Biolink Predicate."""
    # RELA values are often descriptive phrases, so we look for keywords.
    # This is a simplistic approach; a more robust solution would be needed for production.
    rela_lower = rela.lower()

    # Direct mapping for common cases
    if rela_lower in UMLS_RELA_TO_BIOLINK_PREDICATE:
        return UMLS_RELA_TO_BIOLINK_PREDICATE[rela_lower]

    # Keyword-based mapping for other cases
    for keyword, predicate in UMLS_RELA_TO_BIOLINK_PREDICATE.items():
        if keyword in rela_lower:
            return predicate

    return DEFAULT_BIOLINK_PREDICATE
