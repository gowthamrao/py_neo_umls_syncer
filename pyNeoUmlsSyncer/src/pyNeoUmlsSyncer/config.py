"""
Configuration module for pyNeoUmlsSyncer.

Uses Pydantic for robust and type-hinted settings management.
Handles credentials, filtering, optimization, and source priority settings.
"""
from typing import List, Literal, Optional
from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class UmlsCredentials(BaseSettings):
    """
    UMLS credentials for UTS API authentication.
    """
    api_key: SecretStr = Field(
        ...,
        description="UMLS API key for authenticating with the UTS API.",
        alias="UMLS_API_KEY"
    )

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding='utf-8',
        extra='ignore'
    )


class FilterConfig(BaseSettings):
    """
    Configuration for filtering UMLS data.
    """
    sab_filter: List[str] = Field(
        default=["RXNORM", "SNOMEDCT_US", "MTH", "MSH", "LNC", "GO", "HGNC", "HPO"],
        description="List of source vocabularies (SABs) to include."
    )
    suppression_handling: List[Literal['O', 'E', 'Y']] = Field(
        default=['O', 'Y', 'E'],
        description="List of suppression flags to exclude ('O'bscure, 'E'xternal, 'Y'es)."
    )


class OptimizationConfig(BaseSettings):
    """
    Settings for optimizing the ETL process.
    """
    max_parallel_processes: Optional[int] = Field(
        default=None,
        description="Maximum number of parallel processes for parsing. If None, defaults to os.cpu_count()."
    )
    apoc_batch_size: int = Field(
        default=10000,
        description="Batch size for APOC periodic iterate operations."
    )


class SabPriorityConfig(BaseSettings):
    """
    Configuration for prioritizing SABs for preferred name selection.
    """
    sab_priority: List[str] = Field(
        default=[
            "SNOMEDCT_US",
            "RXNORM",
            "MTH",
            "MSH",
            "LNC",
            "GO",
            "HGNC",
            "HPO",
        ],
        description="Ordered list of SABs to determine the CUI's preferred name."
    )


class AppConfig(BaseSettings):
    """
    Main application configuration.
    """
    credentials: UmlsCredentials = Field(default_factory=UmlsCredentials)
    filters: FilterConfig = Field(default_factory=FilterConfig)
    optimization: OptimizationConfig = Field(default_factory=OptimizationConfig)
    sab_priority: SabPriorityConfig = Field(default_factory=SabPriorityConfig)
    neo4j_uri: str = Field("bolt://localhost:7687", description="Neo4j URI")
    neo4j_user: str = Field("neo4j", description="Neo4j username")
    neo4j_password: SecretStr = Field("password", description="Neo4j password")
    umls_version: str = Field(..., description="Target UMLS version, e.g., '2025AA'")


# The AppConfig object should be instantiated by the application's entry point (e.g., the CLI)
# to avoid import-time side effects and facilitate testing.
