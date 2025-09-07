"""
config.py

This module defines the configuration for the pyNeoUmlsSyncer application using
Pydantic's Settings management. It allows for configuration via environment
variables or a .env file.
"""
import multiprocessing
from typing import List, Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """
    Manages application-wide settings.
    """
    # UMLS Unified Medical Language System (UTS) Credentials
    # It is recommended to set this via an environment variable or .env file.
    umls_api_key: str = Field(..., validation_alias="UMLS_API_KEY")

    # Neo4j Database Credentials
    neo4j_uri: str = Field("bolt://localhost:7687", validation_alias="NEO4J_URI")
    neo4j_user: str = Field("neo4j", validation_alias="NEO4J_USER")
    neo4j_password: str = Field("password", validation_alias="NEO4J_PASSWORD")
    neo4j_database: str = Field("neo4j", validation_alias="NEO4J_DATABASE")

    # ETL and Data Filtering Settings
    # Define a list of Source Abbreviations (SABs) to include in the import.
    # An empty list means all SABs will be included.
    sab_filter: List[str] = Field(default_factory=list, validation_alias="SAB_FILTER")

    # Define the priority of SABs for selecting the preferred_name of a Concept.
    # The first SAB in the list with a term for a given CUI will be used.
    # Example: ["RXNORM", "SNOMEDCT_US", "MSH"]
    sab_priority: List[str] = Field(
        default_factory=lambda: ["RXNORM", "SNOMEDCT_US", "MSH", "MTH", "NCI"],
        validation_alias="SAB_PRIORITY"
    )

    # Define which MRCONSO.RRF TTYs (Term Types) to use for preferred names.
    # This list is based on UMLS standard practice for finding the best name.
    preferred_name_tty_ranking: List[str] = Field(
        default_factory=lambda: ["PN", "PCE", "PT", "FN", "SY"],
        validation_alias="PREFERRED_NAME_TTY_RANKING"
    )

    # Define which MRCONSO.RRF suppression flags to exclude.
    # 'O': Obsolete content
    # 'Y': Suppressible content not routinely suppressed
    # 'E': Editor-suppressed content
    suppression_handling: List[Literal["O", "Y", "E"]] = Field(
        default_factory=lambda: ["O", "Y", "E"],
        validation_alias="SUPPRESSION_HANDLING"
    )

    # Optimization and Performance Settings
    # Max number of parallel processes for parsing RRF files.
    # Defaults to the number of CPU cores.
    max_parallel_processes: int = Field(
        default_factory=multiprocessing.cpu_count,
        validation_alias="MAX_PARALLEL_PROCESSES"
    )

    # Batch size for APOC periodic iterate operations during incremental loads.
    apoc_batch_size: int = Field(10000, validation_alias="APOC_BATCH_SIZE")

    # File paths
    data_dir: str = Field("./data", validation_alias="DATA_DIR")

    # UMLS Release Version
    # e.g. "2024AA"
    umls_version: str = Field(..., validation_alias="UMLS_VERSION")

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding='utf-8',
        extra='ignore' # Ignore extra fields from .env file
    )

# The settings object should be instantiated where needed (e.g., in the CLI)
# to allow for easier testing and configuration management.
