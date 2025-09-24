"""
Contains mappings from UMLS to the Biolink Model.

This module provides dictionaries to translate UMLS Semantic Types (TUIs)
and Relationship Attributes (RELAs) into their corresponding Biolink Model
categories and predicates. This is a critical step for standardizing the
LPG according to the Biolink Model.

The mappings provided here are not exhaustive and are based on a
combination of the project's FRD and expert knowledge. They can be
expanded as needed.
"""

# Mapping from UMLS Semantic Type Identifier (TUI) to Biolink Model Category
# Based on research from https://cthoyt.com/umlsst/ and the Biolink Model YAML.
# Default category is biolink:NamedThing for any TUI not in this map.
TUI_TO_BIOLINK_CATEGORY = {
    # Diseases and Phenotypes
    "T047": "biolink:Disease",  # Disease or Syndrome
    "T191": "biolink:Disease",  # Neoplastic Process
    "T184": "biolink:PhenotypicFeature",  # Sign or Symptom
    "T033": "biolink:PhenotypicFeature",  # Finding
    "T048": "biolink:Disease",  # Mental or Behavioral Dysfunction
    "T019": "biolink:Disease",  # Congenital Abnormality
    "T049": "biolink:PathologicalProcess",  # Cell or Molecular Dysfunction
    "T037": "biolink:Disease",  # Injury or Poisoning

    # Chemicals and Drugs
    "T121": "biolink:Drug",  # Pharmacologic Substance
    "T109": "biolink:SmallMolecule",  # Organic Chemical
    "T103": "biolink:ChemicalEntity",  # Chemical
    "T127": "biolink:SmallMolecule",  # Vitamin
    "T195": "biolink:Drug", # Antibiotic

    # Genes and Molecular Biology
    "T028": "biolink:Gene",  # Gene or Genome
    "T114": "biolink:NucleicAcidEntity",  # Nucleic Acid, Nucleoside, or Nucleotide
    "T116": "biolink:Protein",  # Amino Acid, Peptide, or Protein
    "T126": "biolink:Protein",  # Enzyme
    "T192": "biolink:Protein",  # Receptor

    # Anatomy and Physiology
    "T017": "biolink:AnatomicalEntity",  # Anatomical Structure
    "T023": "biolink:GrossAnatomicalStructure",  # Body Part, Organ, or Organ Component
    "T024": "biolink:Tissue",  # Tissue
    "T025": "biolink:Cell",  # Cell
    "T026": "biolink:CellularComponent",  # Cell Component
    "T039": "biolink:PhysiologicalProcess",  # Physiologic Function
    "T046": "biolink:PathologicalProcess",  # Pathologic Function

    # Organisms
    "T007": "biolink:Bacterium", # Bacterium
    "T005": "biolink:Virus", # Virus
}

# Mapping from UMLS RELA (Relationship Attribute) and REL (Relationship)
# to Biolink Model Predicates. The RELA values are more specific and take
# precedence over the more general REL values.
# Based on research from https://www.nlm.nih.gov/research/umls/knowledge_sources/metathesaurus/release/abbreviations.html
RELA_TO_BIOLINK_PREDICATE = {
    # Causal relationships
    "treats": "biolink:treats",
    "treated_by": "biolink:treated_by",
    "cause_of": "biolink:causes",
    "diagnoses": "biolink:diagnoses",
    "diagnosed_by": "biolink:is_diagnosed_by",
    "produces": "biolink:produces",
    "produced_by": "biolink:produced_by",
    "affects": "biolink:affects",

    # Hierarchical and partitive relationships
    "isa": "biolink:subclass_of",
    "part_of": "biolink:part_of",
    "has_part": "biolink:has_part",
    "location_of": "biolink:location_of",
    "has_location": "biolink:located_in",
    "branch_of": "biolink:part_of",
    "has_branch": "biolink:has_part",

    # Associative relationships
    "associated_with": "biolink:associated_with",
    "co-occurs_with": "biolink:coexists_with",
    "physically_interacts_with": "biolink:physically_interacts_with",
    "interacts_with": "biolink:interacts_with",
    "connected_to": "biolink:related_to",

    # Mappings from REL values (fallback)
    "RB": "biolink:broad_match",  # Broader relationship
    "RN": "biolink:narrow_match",  # Narrower relationship
    "RO": "biolink:related_to",  # Other relationship
    "SY": "biolink:same_as",  # Synonym
    "CHD": "biolink:subclass_of",  # Child of
    "PAR": "biolink:superclass_of",  # Parent of
}
