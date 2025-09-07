"""
Defines the Pydantic models for the UMLS LPG schema.

These models represent the structure of nodes and relationships in the Neo4j graph,
ensuring data consistency and alignment with the Biolink Model.
"""
from typing import List, Set
from pydantic import BaseModel, Field


class Concept(BaseModel):
    """
    Represents a :Concept node at the CUI level.
    This is the central node for a medical concept.
    """
    cui: str = Field(..., description="The unique Concept Unique Identifier (CUI).")
    preferred_name: str = Field(..., description="The preferred name for the concept.")
    last_seen_version: str = Field(..., description="The UMLS version this entity was last seen in.")
    biolink_categories: Set[str] = Field(
        default_factory=set,
        description="A set of Biolink category labels (e.g., 'biolink:Disease')."
    )

    @property
    def labels(self) -> List[str]:
        """Returns the list of labels for this node, including the base :Concept label."""
        return [":Concept"] + sorted(list(self.biolink_categories))


class Code(BaseModel):
    """
    Represents a :Code node, a specific code from a source vocabulary (SAB).
    """
    cui: str = Field(..., description="The CUI this code belongs to. Used for relationship creation.")
    code_id: str = Field(..., description="A unique identifier for the code, formatted as 'SAB:CODE'.")
    sab: str = Field(..., description="The source vocabulary (SAB).")
    name: str = Field(..., description="The string/name associated with the code in its source.")
    last_seen_version: str = Field(..., description="The UMLS version this entity was last seen in.")

    @property
    def labels(self) -> List[str]:
        """Returns the list of labels for this node."""
        return [":Code"]


class ConceptRelationship(BaseModel):
    """
    Represents a relationship between two :Concept nodes.
    """
    source_cui: str
    target_cui: str
    biolink_predicate: str = Field(..., description="The Biolink-mapped relationship type (e.g., 'biolink:treats').")
    source_rela: str = Field(..., description="The original UMLS RELA/REL attribute.")
    asserted_by_sabs: Set[str] = Field(default_factory=set, description="List of SABs asserting this relationship.")
    last_seen_version: str = Field(..., description="The UMLS version this entity was last seen in.")


class HasCodeRelationship(BaseModel):
    """
    Represents a [:HAS_CODE] relationship from a :Concept to a :Code node.
    """
    cui: str
    code_id: str
    last_seen_version: str


class UmlsMetadata(BaseModel):
    """
    Represents the :UMLS_Meta node for tracking database version.
    """
    version: str
    last_updated: str
