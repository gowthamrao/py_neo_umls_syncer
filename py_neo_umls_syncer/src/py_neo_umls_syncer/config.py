from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import List, Set
from functools import lru_cache

class Settings(BaseSettings):
    """
    Application settings.
    Values are loaded from environment variables.
    Using a factory function `get_settings` with lru_cache allows for
    lazy loading and easy testing.
    """
    # UMLS UTS API configuration
    UMLS_API_KEY: str

    # Filtering configuration
    SAB_FILTER: Set[str] = {
        "RXNORM", "SNOMEDCT_US", "MTH", "MSH", "CHV", "NCI"
    }
    SUPPRESSION_HANDLING: Set[str] = {"O", "Y", "E"}

    # Optimization configuration
    MAX_PARALLEL_PROCESSES: int = 4
    APOC_BATCH_SIZE: int = 10000

    # Preferred name logic configuration
    SAB_PRIORITY: List[str] = [
        "RXNORM",
        "SNOMEDCT_US",
        "MTH",
        "MSH",
        "CHV",
        "NCI",
    ]

    # Neo4j configuration
    NEO4J_URI: str = "bolt://localhost:7687"
    NEO4J_USER: str = "neo4j"
    NEO4J_PASSWORD: str = "password"
    NEO4J_DATABASE: str = "neo4j"

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )

@lru_cache()
def get_settings() -> Settings:
    """
    Returns a cached instance of the Settings object.
    This function is used to provide settings via dependency injection,
    which makes testing easier.
    """
    return Settings()
