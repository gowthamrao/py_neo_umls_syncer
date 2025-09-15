from pydantic import BaseModel, Field
from typing import List, Set

class Concept(BaseModel):
    """
    Represents a :Concept node in the Neo4j LPG.
    This corresponds to a UMLS CUI.
    """
    cui: str = Field(..., description="The Concept Unique Identifier (CUI).")
    preferred_name: str = Field(..., description="The preferred name for the concept, determined by SAB_PRIORITY.")
    biolink_categories: Set[str] = Field(..., description="A set of Biolink categories mapped from UMLS Semantic Types (TUIs).")
    last_seen_version: str = Field(..., description="The UMLS release version this concept was last seen in.")


class Code(BaseModel):
    """
    Represents a :Code node in the Neo4j LPG.
    This corresponds to a specific code in a source vocabulary (SAB).
    """
    code_id: str = Field(..., description="Unique identifier for the code, in the format 'SAB:CODE'.")
    sab: str = Field(..., description="The source vocabulary (SAB).")
    name: str = Field(..., description="The string (STR) associated with this code from the source.")
    last_seen_version: str = Field(..., description="The UMLS release version this code was last seen in.")


class HasCode(BaseModel):
    """
    Represents a [:HAS_CODE] relationship between a Concept and a Code.
    """
    from_cui: str = Field(..., description="The CUI of the source Concept node.")
    to_code_id: str = Field(..., description="The code_id of the target Code node.")


class ConceptRelationship(BaseModel):
    """
    Represents a relationship between two :Concept nodes.
    """
    from_cui: str = Field(..., description="The CUI of the source Concept node.")
    to_cui: str = Field(..., description="The CUI of the target Concept node.")
    biolink_predicate: str = Field(..., description="The Biolink predicate mapped from the UMLS relationship attribute (RELA).")
    source_rela: str = Field(..., description="The original UMLS RELA or REL attribute.")
    asserted_by_sabs: Set[str] = Field(..., description="Set of SABs that assert this relationship.")
    last_seen_version: str = Field(..., description="The UMLS release version this relationship was last seen in.")
