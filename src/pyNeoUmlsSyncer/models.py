"""
Pydantic Models for the UMLS Labeled Property Graph (LPG) Schema.

These models define the structure of the data as it is processed through the
ETL pipeline, ensuring type safety and a consistent data representation before
being loaded into Neo4j. They also include helpers for CSV serialization.
"""
from pydantic import BaseModel, Field
from typing import Set, List

class Concept(BaseModel):
    """Represents a :Concept node, corresponding to a single CUI."""
    cui: str = Field(..., description="The Concept Unique Identifier (CUI).")
    preferred_name: str = Field(..., description="The selected preferred name for the concept.")
    biolink_categories: Set[str] = Field(default_factory=set)
    last_seen_version: str = Field(..., description="The UMLS version this entity was last observed in.")

    def get_csv_header(self) -> List[str]:
        return ["cui:ID(Concept)", "preferred_name", "last_seen_version", ":LABEL"]

    def to_csv_row(self) -> List[str]:
        labels = ";".join(["Concept"] + sorted(list(self.biolink_categories)))
        return [self.cui, self.preferred_name, self.last_seen_version, labels]

class Code(BaseModel):
    """Represents a :Code node, corresponding to an atom in a source vocabulary."""
    code_id: str = Field(..., description="Unique identifier, 'SAB:CODE'.")
    sab: str = Field(..., description="The source vocabulary (SAB) of the code.")
    name: str = Field(..., description="The string/name associated with the code.")
    last_seen_version: str = Field(..., description="The UMLS version this entity was last observed in.")

    def get_csv_header(self) -> List[str]:
        return ["code_id:ID(Code)", "sab", "name", "last_seen_version", ":LABEL"]

    def to_csv_row(self) -> List[str]:
        return [self.code_id, self.sab, self.name, self.last_seen_version, "Code"]

class HasCodeRelationship(BaseModel):
    """Represents a [:HAS_CODE] relationship from a :Concept to a :Code."""
    cui: str = Field(..., description="The CUI of the source :Concept node (:START_ID).")
    code_id: str = Field(..., description="The code_id of the target :Code node (:END_ID).")

    def get_csv_header(self) -> List[str]:
        return [":START_ID(Concept)", ":END_ID(Code)"]

    def to_csv_row(self) -> List[str]:
        return [self.cui, self.code_id]

class ConceptRelationship(BaseModel):
    """Represents a relationship between two :Concept nodes, derived from MRREL."""
    source_cui: str = Field(..., description="CUI of the source concept (:START_ID).")
    target_cui: str = Field(..., description="CUI of the target concept (:END_ID).")
    rel_type: str = Field(..., description="The Biolink-mapped relationship type.")
    source_rela: str = Field(..., description="The original UMLS RELA.")
    asserted_by_sabs: Set[str] = Field(default_factory=set)
    last_seen_version: str = Field(..., description="The UMLS version this entity was last observed in.")

    def get_csv_header(self) -> List[str]:
        return [":START_ID(Concept)", ":END_ID(Concept)", "source_rela", "asserted_by_sabs:string[]", "last_seen_version"]

    def to_csv_row(self) -> List[str]:
        return [
            self.source_cui,
            self.target_cui,
            self.source_rela,
            ";".join(sorted(list(self.asserted_by_sabs))),
            self.last_seen_version,
        ]
