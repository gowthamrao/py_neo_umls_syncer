# pyNeoUmlsSyncer

**pyNeoUmlsSyncer** is a production-ready Python package for creating and maintaining a UMLS (Unified Medical Language System) Labeled Property Graph (LPG) in a Neo4j (5.x) database. It is designed for high performance, reliability, and standardization, ensuring your graph is always up-to-date with the latest UMLS release.

The entire package was architected and implemented by Jules, a Principal Bioinformatics Data Architect and Senior Software Engineer.

## Key Features

-   **High-Performance Initial Load**: Utilizes `neo4j-admin database import` for the fastest possible initial ingestion of the entire UMLS dataset.
-   **Idempotent Incremental Updates**: Implements a sophisticated **"Snapshot Diff"** strategy using Neo4j's APOC library. You can re-run the synchronization process at any time, and it will reliably bring the database to the correct state.
-   **Robust and Parallelized ETL**: The parsing of massive UMLS RRF files (`MRCONSO`, `MRREL`, etc.) is heavily optimized using Python's multiprocessing.
-   **Biolink Model Standardization**: Maps UMLS Semantic Types (TUIs) and Relationship Attributes (RELAs) to the [Biolink Model](https://biolink.github.io/biolink-model/), creating a standardized, interoperable graph.
-   **Intelligent Configuration**: Uses Pydantic for clean configuration management via environment variables or a `.env` file.
-   **Modern and Maintainable**: Built with Python 3.10+, full type hinting, a Typer-based CLI, and a structured, modular architecture.

## Graph Schema

The package builds a hybrid LPG schema in Neo4j that balances query performance with data provenance.

-   **`:Concept` Nodes**: Represent a UMLS Concept (CUI).
    -   Properties: `cui` (UNIQUE), `preferred_name`, `last_seen_version`.
    -   Additional Labels: Mapped Biolink categories (e.g., `:biolink:Disease`).
-   **`:Code` Nodes**: Represent a code from a source vocabulary (e.g., an RXNORM code).
    -   Properties: `code_id` (UNIQUE, e.g., "RXNORM:198440"), `sab`, `name`, `last_seen_version`.
-   **Relationships**:
    -   `(:Concept)-[:HAS_CODE]->(:Code)`: Connects a concept to its source codes.
    -   `(:Concept)-[:biolink_predicate]->(:Concept)`: Represents relationships from `MRREL.RRF`.
        -   The edge label is a mapped Biolink Predicate (e.g., `:biolink:treats`).
        -   Properties: `source_rela` (the original UMLS RELA), `asserted_by_sabs` (list of sources), `last_seen_version`.

## Installation

1.  **Clone the repository:**
    ```bash
    git clone <repository_url>
    cd pyNeoUmlsSyncer
    ```

2.  **Install dependencies using Poetry:**
    This project uses [Poetry](https://python-poetry.org/) for dependency management.
    ```bash
    poetry install
    ```

## Configuration

Configuration is managed via environment variables. Create a `.env` file in the project root directory and add the following settings:

```dotenv
# --- Required Settings ---
# Your UMLS API Key from the UTS website
UMLS_API_KEY="your-api-key-here"
# The UMLS release version you want to sync (e.g., "2024AA")
UMLS_VERSION="2024AA"

# --- Neo4j Database Settings ---
NEO4J_URI="bolt://localhost:7687"
NEO4J_USER="neo4j"
NEO4J_PASSWORD="your_neo4j_password"
NEO4J_DATABASE="neo4j" # Or "umls" or another database name

# --- Optional ETL & Filter Settings ---
# Comma-separated list of SABs to include (e.g., "RXNORM,SNOMEDCT_US,MSH").
# If empty, all SABs are included.
SAB_FILTER=

# Ordered, comma-separated list of SABs for selecting the preferred name.
SAB_PRIORITY="RXNORM,SNOMEDCT_US,MSH,MTH,NCI"

# --- Optional Performance Settings ---
# Number of parallel processes for parsing. Defaults to number of CPU cores.
# MAX_PARALLEL_PROCESSES=8
# Batch size for APOC incremental updates.
# APOC_BATCH_SIZE=10000
```

## Usage

The application is run via the `py-neo-umls-syncer` command-line interface.

### Main Synchronization Command

This is the only command you need to run for routine operation. It automatically detects if the database is empty and chooses the correct loading strategy.

```bash
poetry run py-neo-umls-syncer sync
```

-   **On the first run**: The tool will detect an empty database and guide you to perform the initial bulk import. This requires stopping the Neo4j instance and running the `neo4j-admin` command. The loader will provide instructions.
-   **On subsequent runs**: The tool will detect an existing installation and perform a fast, idempotent incremental update based on the new UMLS snapshot and change files.

### Other Commands

-   `py-neo-umls-syncer download`: Only downloads and extracts the UMLS files without processing them.

## Development and Testing

To run the integration tests, you will need Docker installed, as the test suite uses `testcontainers` to spin up a live Neo4j database.

```bash
# Run the test suite
poetry run pytest
```
