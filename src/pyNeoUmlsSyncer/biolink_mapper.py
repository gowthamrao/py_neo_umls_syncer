"""
biolink_mapper.py

This module provides the logic for mapping UMLS entities to the Biolink Model.
It is essential for standardizing the graph schema and ensuring interoperability.

The mappings provided here are representative and not exhaustive. A production
system would require a more comprehensive and regularly updated mapping source.
"""

from typing import Dict, List

class BiolinkMapper:
    """
    Handles the mapping of UMLS semantic types (TUIs) and relationship
    attributes (RELAs) to their corresponding Biolink Model identifiers.
    """

    # A mapping from UMLS Semantic Type Unique Identifiers (TUIs) to Biolink Categories.
    # This is a curated subset. A complete mapping is a significant undertaking.
    # Source: Combination of manual curation and inspection of existing resources.
    TUI_TO_BIOLINK_CATEGORY: Dict[str, str] = {
        "T001": "biolink:Organism",
        "T005": "biolink:Virus",
        "T007": "biolink:Bacterium",
        "T017": "biolink:AnatomicalEntity",
        "T022": "biolink:GrossAnatomicalStructure",
        "T023": "biolink:BodyPart",
        "T024": "biolink:Tissue",
        "T025": "biolink:Cell",
        "T026": "biolink:CellComponent",
        "T028": "biolink:Gene",
        "T033": "biolink:Finding",
        "T034": "biolink:LaboratoryOrTestResult",
        "T037": "biolink:InjuryOrPoisoning",
        "T046": "biolink:Pathology",
        "T047": "biolink:Disease",
        "T048": "biolink:MentalOrBehavioralDysfunction",
        "T058": "biolink:HealthCareActivity",
        "T060": "biolink:DiagnosticProcedure",
        "T061": "biolink:TherapeuticOrPreventiveProcedure",
        "T062": "biolink:ResearchActivity",
        "T064": "biolink:MedicalDevice",
        "T082": "biolink:SpatialConcept",
        "T101": "biolink:PatientOrPopulation",
        "T109": "biolink:OrganicChemical",
        "T114": "biolink:NucleicAcid",
        "T116": "biolink:AminoAcidSequence",
        "T121": "biolink:PharmacologicSubstance",
        "T123": "biolink:BiologicallyActiveSubstance",
        "T129": "biolink:ImmunologicFactor",
        "T131": "biolink:HazardousOrPoisonousSubstance",
        "T167": "biolink:Substance",
        "T184": "biolink:SignOrSymptom",
        "T191": "biolink:NeoplasticProcess",
        "T200": "biolink:ClinicalDrug",
        "T201": "biolink:ClinicalAttribute",
    }

    # A mapping from UMLS Relationship Attributes (RELAs) to Biolink Predicates.
    # This is also a curated subset. It focuses on common, high-value relationships.
    RELA_TO_BIOLINK_PREDICATE: Dict[str, str] = {
        "treats": "biolink:treats",
        "treated_by": "biolink:treated_by",
        "causes": "biolink:causes",
        "caused_by": "biolink:caused_by",
        "diagnoses": "biolink:diagnoses",
        "diagnosed_by": "biolink:diagnosed_by",
        "prevents": "biolink:prevents",
        "prevented_by": "biolink:prevented_by",
        "disrupts": "biolink:disrupts",
        "disrupted_by": "biolink:disrupted_by",
        "co-occurs_with": "biolink:co-occurs_with",
        "location_of": "biolink:location_of",
        "has_location": "biolink:has_location",
        "part_of": "biolink:part_of",
        "has_part": "biolink:has_part",
        "isa": "biolink:subclass_of", # 'isa' is a common RELA for hierarchical relationships
        "has_ingredient": "biolink:has_ingredient",
        "has_active_ingredient": "biolink:has_active_ingredient",
        "contraindicated_with": "biolink:contraindicated_with",
        "may_treat": "biolink:may_treat",
        "may_prevent": "biolink:may_prevent",
    }

    DEFAULT_BIOLINK_CATEGORY = "biolink:NamedThing"
    DEFAULT_BIOLINK_PREDICATE = "biolink:related_to"

    def get_biolink_categories(self, tuis: List[str]) -> List[str]:
        """
        Maps a list of TUIs to a list of Biolink categories.

        Args:
            tuis: A list of Semantic Type Identifiers (TUI) from MRSTY.RRF.

        Returns:
            A list of corresponding Biolink category labels. Defaults to
            'biolink:NamedThing' if no specific mapping is found.
        """
        categories = {
            self.TUI_TO_BIOLINK_CATEGORY.get(tui, self.DEFAULT_BIOLINK_CATEGORY)
            for tui in tuis
        }
        # If the only mapping is the default, just return that.
        # Otherwise, remove the default if more specific categories were found.
        if len(categories) > 1 and self.DEFAULT_BIOLINK_CATEGORY in categories:
            categories.remove(self.DEFAULT_BIOLINK_CATEGORY)

        return sorted(list(categories))

    def get_biolink_predicate(self, rela: str) -> str:
        """
        Maps a UMLS RELA to a Biolink predicate.

        Args:
            rela: The relationship attribute from MRREL.RRF.

        Returns:
            The corresponding Biolink predicate label. Defaults to
            'biolink:related_to' if no specific mapping is found.
        """
        if rela is None:
            return self.DEFAULT_BIOLINK_PREDICATE
        return self.RELA_TO_BIOLINK_PREDICATE.get(rela.lower(), self.DEFAULT_BIOLINK_PREDICATE)

# Instantiate a global mapper to be used by other modules
biolink_mapper = BiolinkMapper()
