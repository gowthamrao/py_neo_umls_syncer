"""
Handles the mapping from UMLS semantics to the Biolink Model.

- Maps UMLS Type Unique Identifiers (TUIs) to Biolink categories.
- Maps UMLS relationship attributes (RELA) to Biolink predicates.
"""
from typing import List, Dict, Set

# A simplified mapping of UMLS TUIs to Biolink categories.
# A comprehensive mapping would be required for a full implementation.
TUI_TO_BIOLINK_CATEGORY: Dict[str, str] = {
    # Diseases and Disorders
    "T047": "biolink:Disease",
    "T191": "biolink:NeoplasticProcess",
    "T048": "biolink:MentalOrBehavioralDysfunction",
    "T020": "biolink:AcquiredAbnormality",
    "T190": "biolink:AnatomicalAbnormality",
    "T049": "biolink:CellOrMolecularDysfunction",
    "T019": "biolink:CongenitalAbnormality",
    "T037": "biolink:InjuryOrPoisoning",
    "T046": "biolink:PathologicFunction",
    # Chemicals and Drugs
    "T121": "biolink:ChemicalEntity",
    "T109": "biolink:OrganicChemical",
    "T121": "biolink:Pharmaceutical",
    "T122": "biolink:Receptor",
    # Anatomy
    "T017": "biolink:AnatomicalEntity",
    "T023": "biolink:BodyPart",
    "T029": "biolink:BodyLocationOrRegion",
    "T030": "biolink:BodySpaceOrJunction",
    "T031": "biolink:BodySubstance",
    "T022": "biolink:BodySystem",
    "T025": "biolink:Cell",
    "T024": "biolink:Tissue",
    # Genes and Proteins
    "T028": "biolink:GeneOrGeneProduct",
    "T114": "biolink:NucleicAcidNucleosideOrNucleotide",
    "T116": "biolink:AminoAcidPeptideOrProtein",
    # Procedures
    "T061": "biolink:Procedure",
    "T060": "biolink:DiagnosticProcedure",
    "T063": "biolink:MolecularActivity",
    # Phenotypes
    "T033": "biolink:Finding",
    "T042": "biolink:OrganOrTissuePathology",
    "T050": "biolink:ExperimentalModelOfDisease",
    # Default
    "DEFAULT": "biolink:NamedThing"
}

# A simplified mapping of UMLS RELA to Biolink predicates.
RELA_TO_BIOLINK_PREDICATE: Dict[str, str] = {
    "treats": "biolink:treats",
    "may_treat": "biolink:treats",
    "is_a": "biolink:subclass_of",
    "part_of": "biolink:part_of",
    "location_of": "biolink:located_in",
    "causes": "biolink:causes",
    "may_cause": "biolink:causes",
    "produces": "biolink:produces",
    "contraindicates": "biolink:contraindicated_in",
    "may_prevent": "biolink:prevents",
    "prevents": "biolink:prevents",
    "disrupts": "biolink:disrupts",
    "associated_with": "biolink:related_to",
    "occurs_in": "biolink:occurs_in",
    # Default
    "DEFAULT": "biolink:related_to"
}


def get_biolink_categories(tuis: Set[str]) -> Set[str]:
    """
    Maps a set of TUIs to a set of Biolink categories.
    """
    categories = {
        TUI_TO_BIOLINK_CATEGORY.get(tui, TUI_TO_BIOLINK_CATEGORY["DEFAULT"])
        for tui in tuis
    }
    # If the only mapping is the default, just return that.
    if len(categories) > 1 and TUI_TO_BIOLINK_CATEGORY["DEFAULT"] in categories:
        categories.remove(TUI_TO_BIOLINK_CATEGORY["DEFAULT"])

    if not categories:
        return {TUI_TO_BIOLINK_CATEGORY["DEFAULT"]}

    return categories


def get_biolink_predicate(rela: str) -> str:
    """
    Maps a UMLS RELA to a Biolink predicate.
    """
    # Normalize rela to lower case and replace spaces with underscores
    normalized_rela = rela.lower().replace(' ', '_')
    return RELA_TO_BIOLINK_PREDICATE.get(normalized_rela, RELA_TO_BIOLINK_PREDICATE["DEFAULT"])
