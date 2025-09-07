"""
models.py

This module defines the Pydantic models for the Labeled Property Graph (LPG) schema.
These models serve as the data structures used throughout the ETL process before
serialization into CSVs for bulk loading or into parameters for Cypher queries
during incremental updates.
"""
from typing import List, Set

from pydantic import BaseModel, Field


class Concept(BaseModel):
    """
    Represents a UMLS Concept (CUI). This node will also carry a Biolink Model
    category label (e.g., :biolink:Disease).
    """
    cui: str = Field(..., description="The unique Concept Unique Identifier (CUI).")
    preferred_name: str = Field(..., description="The preferred name for the concept, determined by SAB priority.")
    biolink_categories: Set[str] = Field(default_factory=set, description="A set of Biolink categories mapped from TUIs.")
    last_seen_version: str = Field(..., description="The UMLS release version this entity was last seen in.")


class Code(BaseModel):
    """
    Represents a source-specific code that maps to a Concept.
    """
    code_id: str = Field(..., description="A unique, source-prefixed identifier (e.g., 'RXNORM:198440').")
    sab: str = Field(..., description="The source abbreviation (SAB) of the vocabulary.")
    name: str = Field(..., description="The string representation (STR) of the code in its source.")
    last_seen_version: str = Field(..., description="The UMLS release version this entity was last seen in.")


class ConceptRelationship(BaseModel):
    """
    Represents a relationship between two Concepts, derived from MRREL.
    """
    source_cui: str = Field(..., description="The CUI of the source node.")
    target_cui: str = Field(..., description="The CUI of the target node.")
    biolink_predicate: str = Field(..., description="The Biolink predicate mapped from the UMLS relationship attribute (RELA).")
    source_rela: str = Field(..., description="The original UMLS relationship attribute (RELA) or relation (REL).")
    asserted_by_sabs: Set[str] = Field(default_factory=set, description="A list of SABs that assert this relationship.")
    last_seen_version: str = Field(..., description="The UMLS release version this entity was last seen in.")


class HasCodeRelationship(BaseModel):
    """
    Represents the structural relationship between a Concept and a Code.
    """
    cui: str = Field(..., description="The CUI of the parent Concept.")
    code_id: str = Field(..., description="The unique ID of the child Code.")
    last_seen_version: str = Field(..., description="The UMLS release version this entity was last seen in.")
