"""
Configuration Management for pyNeoUmlsSyncer.

This module uses Pydantic's BaseSettings to define and manage configuration.
Settings can be loaded from environment variables or a .env file.
"""
from typing import List, Literal, Optional
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    """
    Holds all configuration for the pyNeoUmlsSyncer application.
    """
    # --- UTS API Credentials ---
    # It is strongly recommended to set this via an environment variable or .env file.
    umls_api_key: str = Field(..., description="UMLS UTS API key for downloading distributions.")

    # --- Data Source and Versioning ---
    umls_version: str = Field(
        "2024AA",
        description="The target UMLS release version (e.g., '2024AA')."
    )
    data_dir: str = Field(
        "./data",
        description="Root directory for downloading and extracting UMLS data."
    )

    # --- ETL Filtering and Logic ---
    sab_filter: List[str] = Field(
        default_factory=lambda: ["SNOMEDCT_US", "RXNORM", "HGNC", "HPO", "MSH", "NCI"],
        description="List of source vocabularies (SABs) to include. Processing will be limited to these."
    )
    suppression_handling: List[Literal["O", "Y", "E"]] = Field(
        default_factory=lambda: ["O", "Y"],
        description="List of suppression flags from MRCONSO.RRF to exclude associated atoms."
    )
    sab_priority: List[str] = Field(
        default_factory=lambda: [
            "SNOMEDCT_US", "RXNORM", "MSH", "HGNC", "HPO", "NCI",
            "ICD10CM", "LNC", "GO", "MED-RT", "VANDF"
        ],
        description="Ordered list of SABs used to select the CUI's preferred_name."
    )

    # --- Performance and Optimization ---
    max_parallel_processes: Optional[int] = Field(
        default=None,
        description="Number of parallel processes for parsing. If None, uses os.cpu_count()."
    )

    # --- Neo4j Connection and Loader Settings ---
    neo4j_uri: str = Field("bolt://localhost:7687", description="Neo4j bolt URI.")
    neo4j_user: str = Field("neo4j", description="Neo4j username.")
    neo4j_password: str = Field("password", description="Neo4j password.")
    neo4j_database: str = Field("neo4j", description="Neo4j target database name.")
    apoc_batch_size: int = Field(
        default=20000,
        description="Batch size for apoc.periodic.iterate operations during incremental loads."
    )
    # This path must be configured in neo4j.conf (dbms.directories.import)
    neo4j_import_dir: str = Field(
        "import",
        description="The name of Neo4j's import directory for bulk loading."
    )

    model_config = SettingsConfigDict(
        env_prefix="PYNEOUMLSSYNCER_", # e.g. PYNEOUMLSSYNCER_UMLS_API_KEY
        env_file='.env',
        env_file_encoding='utf-8',
        extra='ignore'
    )

# Single, importable instance of the settings
settings = Settings()
