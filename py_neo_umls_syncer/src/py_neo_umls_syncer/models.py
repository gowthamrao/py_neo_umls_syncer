from pydantic import BaseModel
from typing import List, Set

class Concept(BaseModel):
    """
    Represents a :Concept node in the Neo4j LPG.
    This corresponds to a single UMLS CUI.
    """
    cui: str
    preferred_name: str
    last_seen_version: str
    biolink_categories: Set[str]

class Code(BaseModel):
    """
    Represents a :Code node in the Neo4j LPG.
    This corresponds to a code from a specific source vocabulary (SAB).
    """
    code_id: str  # Format: SAB:CODE
    sab: str
    name: str
    last_seen_version: str

class HasCodeRelationship(BaseModel):
    """
    Represents a [:HAS_CODE] relationship between a Concept and a Code.
    """
    source_cui: str
    target_code_id: str

class ConceptRelationship(BaseModel):
    """
    Represents a relationship between two :Concept nodes.
    This is derived from MRREL.RRF.
    """
    source_cui: str
    target_cui: str
    type: str  # Mapped Biolink Predicate
    source_rela: str  # Original UMLS RELA
    asserted_by_sabs: Set[str]
    last_seen_version: str
