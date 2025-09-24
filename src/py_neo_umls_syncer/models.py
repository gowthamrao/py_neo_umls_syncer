# Jules was here
from pydantic import BaseModel
from typing import List, Optional

class Concept(BaseModel):
    """
    Represents a single UMLS Concept (CUI).
    This is a node in the graph with the :Concept label.
    """
    cui: str
    preferred_name: str
    # The last_seen_version will be handled by the loader, not in this model

class Code(BaseModel):
    """
    Represents a source-level code (e.g., from RXNORM, SNOMEDCT).
    This is a node in the graph with the :Code label.
    """
    code_id: str  # Format: SAB:CODE
    sab: str
    name: str

class ConceptToCodeRelationship(BaseModel):
    """
    Represents the relationship from a Concept to a Code.
    """
    cui: str
    code_id: str

class InterConceptRelationship(BaseModel):
    """
    Represents a relationship between two Concepts, derived from MRREL.
    This is a single assertion of a relationship from a given source.
    """
    source_cui: str
    target_cui: str
    source_rela: str  # The original UMLS RELA/REL
    sab: str # The source vocabulary of the relationship assertion

class SemanticType(BaseModel):
    """
    Represents a semantic type assignment for a CUI from MRSTY.
    This will be used to add Biolink category labels to :Concept nodes.
    """
    cui: str
    tui: str
    sty: str # Semantic Type Name

class ParsedData(BaseModel):
    """
    A container for all parsed data from the RRF files,
    ready for the transformation stage.
    """
    concepts: List[Concept]
    codes: List[Code]
    concept_to_code_rels: List[ConceptToCodeRelationship]
    inter_concept_rels: List[InterConceptRelationship]
    semantic_types: List[SemanticType]
