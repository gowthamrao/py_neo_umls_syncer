# Copyright (c) 2025-2026 Gowtham Adamane Rao. All Rights Reserved.
#
# Licensed under the Prosperity Public License 3.0.0 (the "License").
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License in the LICENSE file at the root
# of this repository, or at: https://prosperitylicense.com/versions/3.0.0
#
# Commercial use beyond a 30-day trial requires a separate license.
# Jules was here
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import List, Literal

class Settings(BaseSettings):
    """
    Manages the application's configuration settings.
    Utilizes Pydantic's BaseSettings to allow for environment variable overrides.
    """
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        env_prefix="PYNEOUMLSSYNCER_"
    )

    # --- Credentials ---
    umls_api_key: str = Field(
        ...,
        description="UMLS API key for authenticating with the UTS API."
    )

    # --- Neo4j Database ---
    neo4j_uri: str = Field("neo4j://localhost:7687", description="Neo4j instance URI.")
    neo4j_user: str = Field("neo4j", description="Neo4j username.")
    neo4j_password: str = Field("password", description="Neo4j password.")
    neo4j_database: str = Field("neo4j", description="Neo4j target database name.")

    # --- ETL Filters & Behavior ---
    sab_filter: List[str] = Field(
        default=["RXNORM", "SNOMEDCT_US", "MTH", "MSH", "LNC"],
        description="List of UMLS Source Vocabularies (SABs) to include in the import."
    )
    suppression_handling: List[Literal["O", "Y", "E"]] = Field(
        default=["O", "E"],
        description="List of suppression flags to exclude from MRCONSO."
    )
    sab_priority: List[str] = Field(
        default=[
            "RXNORM", "SNOMEDCT_US", "MTH", "MSH", "LNC", "GO", "HGNC",
            "NCBI", "OMIM", "ICD10CM", "CPT"
        ],
        description="Ordered list of SABs for selecting the preferred name of a concept."
    )

    # --- Optimization Settings ---
    max_parallel_processes: int = Field(
        default=4,
        description="Maximum number of parallel processes for RRF parsing."
    )
    apoc_batch_size: int = Field(
        default=10000,
        description="Batch size for apoc.periodic.iterate operations during incremental loads."
    )

    # --- File Paths ---
    neo4j_import_dir: str = Field(
        ...,
        description="Absolute path to the Neo4j import directory. The user running the script must have write permissions to this directory."
    )
    download_dir: str = Field("./umls_download", description="Directory to store downloaded UMLS files.")
    # The csv_dir is now dynamically set based on the neo4j_import_dir, so it's removed from here.


# Instantiate a global settings object to be used throughout the application
settings = Settings()
