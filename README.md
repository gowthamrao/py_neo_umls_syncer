# pyNeoUmlsSyncer

`pyNeoUmlsSyncer` is a production-ready Python package for creating and maintaining a UMLS (Unified Medical Language System) Labeled Property Graph (LPG) in a Neo4j (v5.x) database. It is designed for high performance, reliability, and standards compliance.

## Key Features

- **High-Performance ETL**: Utilizes Python's multiprocessing for efficient parsing of large UMLS RRF files.
- **Optimized Loading Strategies**:
  - **Initial Load**: Generates CSVs for the ultra-fast `neo4j-admin database import` command.
  - **Incremental Load**: Uses `apoc.periodic.iterate` for resilient, batched transactional updates to an existing database.
- **Reliable Synchronization**: Implements a sophisticated **"Snapshot Diff"** strategy. It processes UMLS change files (`DELETEDCUI`, `MERGEDCUI`) and compares the new release snapshot against the database state to ensure the graph is a perfect reflection of the new version.
- **Idempotent**: All loading processes are designed to be safely restartable without corrupting data.
- **Biolink Model Compliant**: The graph schema is mapped to the Biolink Model for standardization and interoperability.
- **Modern Tech Stack**: Built with Python 3.10+, Pydantic v2, Typer, and the official Neo4j driver.

## Graph Schema

The LPG schema is designed to capture both conceptual relationships and source-level provenance.

- **:Concept (`CUI`)**: The central node for a unique medical idea.
  - Properties: `cui` (unique), `preferred_name`, `last_seen_version`.
  - Additional Labels: Mapped Biolink categories (e.g., `:biolink:Disease`).

- **:Code (`SAB:CODE`)**: Represents an atom from a source vocabulary.
  - Properties: `code_id` (unique, e.g., "RXNORM:198440"), `sab`, `name`, `last_seen_version`.

- **Relationships**:
  - `(:Concept)-[:HAS_CODE]->(:Code)`: Connects a concept to its source codes.
  - `(:Concept)-[:biolink:predicate]->(:Concept)`: Represents a relationship from `MRREL.RRF` (e.g., `:biolink:treats`). Provenance from multiple sources is aggregated into the `asserted_by_sabs` property.

## Installation

It is recommended to install the package in a virtual environment.

```bash
# Clone the repository
git clone <repository_url>
cd pyNeoUmlsSyncer

# Install using pip
pip install .

# Or, if you use Poetry
poetry install
```

## Configuration

Configuration is managed via environment variables or a `.env` file in the project root.

1.  **Create a `.env` file:**
    ```
    cp .env.example .env
    ```

2.  **Edit the `.env` file:** The most critical setting is your UMLS API key.

    ```dotenv
    # .env file
    # Required: Your UMLS API key from the UTS.
    PYNEOUMLSSYNCER_UMLS_API_KEY="your-long-api-key-here"

    # --- Optional: Database Connection ---
    PYNEOUMLSSYNCER_NEO4J_URI="bolt://localhost:7687"
    PYNEOUMLSSYNCER_NEO4J_USER="neo4j"
    PYNEOUMLSSYNCER_NEO4J_PASSWORD="your_neo4j_password"
    PYNEOUMLSSYNCER_NEO4J_DATABASE="neo4j"

    # --- Optional: ETL and Filter Settings ---
    # A comma-separated list of source vocabularies (SABs) to include.
    # Example:
    # PYNEOUMLSSYNCER_SAB_FILTER="SNOMEDCT_US,RXNORM,HGNC,HPO,MSH"
    ```

## Usage

The package provides a command-line interface, `py-neo-umls-syncer`.

### 1. Initial Bulk Load

This is a one-time operation to populate an empty database. It is extremely fast as it uses `neo4j-admin`.

```bash
py-neo-umls-syncer initial-load --version 2024AA --output-dir ./import_data
```

This command will:
1.  Download and extract the specified UMLS version.
2.  Parse and transform the data.
3.  Generate CSV files and an `import.sh` script in the `./import_data` directory.

**To complete the import:**
1.  **STOP** your Neo4j database service: `neo4j stop`
2.  From the project root, run the generated script: `./import_data/import.sh`
3.  **START** your Neo4j database service: `neo4j start`

### 2. Incremental Update

This command updates an existing database to a newer UMLS version.

```bash
py-neo-umls-syncer incremental-update --version 2024AB
```

This command connects directly to the database and performs the full "Snapshot Diff" synchronization:
1.  Connects to Neo4j to verify the current version.
2.  Downloads and processes the new UMLS release.
3.  Processes `DELETEDCUI.RRF` and `MERGEDCUI.RRF` to handle identity changes.
4.  Merges the new snapshot data, updating the `last_seen_version` on all touched entities.
5.  Deletes any entities from the database whose `last_seen_version` does not match the new version.
6.  Updates the database's metadata to the new version number.

## Development & Testing

To set up a development environment:

```bash
# Install with dev dependencies
poetry install

# Run tests
pytest
```
