from pydantic import BaseModel, Field
from typing import List, Optional
import multiprocessing

class Settings(BaseModel):
    """
    Configuration settings for the py_neo_umls_syncer package.
    It is recommended to manage sensitive data like API keys via environment
    variables or a secret management system.
    """
    # --- Data Acquisition ---
    umls_api_key: Optional[str] = Field(
        default=None,
        description="UMLS UTS API key. Required for downloading UMLS data."
    )

    # --- File Paths ---
    input_dir: str = Field(
        default="data/input",
        description="Base directory for input files."
    )
    output_dir: str = Field(
        default="data/output",
        description="Base directory for output files, including CSVs for bulk import."
    )

    # --- Filtering and Processing ---
    sab_filter: List[str] = Field(
        default_factory=lambda: ["RXNORM", "SNOMEDCT_US", "MSH", "MTH", "HPO", "HGNC", "GO", "SEMMEDDB"],
        description="List of Source Vocabularies (SABs) to include in the graph."
    )
    sab_priority: List[str] = Field(
        default_factory=lambda: ["RXNORM", "SNOMEDCT_US", "MSH", "MTH", "HPO", "HGNC"],
        description="Ordered list of SABs to determine the preferred_name for a CUI."
    )
    suppress_flags: List[str] = Field(
        default_factory=lambda: ["O", "Y", "E"],
        description="List of SUPPRESS column flags from MRCONSO to exclude."
    )

    # --- Optimization ---
    max_parallel_processes: int = Field(
        default=max(1, multiprocessing.cpu_count() - 1),
        description="Maximum number of parallel processes for parsing."
    )
    apoc_batch_size: int = Field(
        default=10000,
        description="Batch size for APOC periodic iterate operations."
    )

    # --- Versioning ---
    release_version: str = Field(
        default="2025AA", # Example, should be updated for each release
        description="The version of the UMLS release being processed (e.g., '2025AA')."
    )

    class Config:
        validate_assignment = True
