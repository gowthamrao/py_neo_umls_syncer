<!-- Jules was here -->
# py_neo_umls_syncer

**A production-ready Python package for creating and maintaining a UMLS (Unified Medical Language System) Labeled Property Graph (LPG) in Neo4j (v5.x).**

This package is designed to be highly optimized, robust, and standardized, implementing a sophisticated, idempotent incremental update strategy to keep your biomedical knowledge graph in sync with the latest UMLS releases.

## Core Features

- **Automated Data Acquisition**: Authenticates with the UMLS UTS API, downloads the latest UMLS Rich Release Format (RRF) distribution, and verifies checksums.
- **Optimized ETL**:
    - **Parallel Parsing**: Utilizes Python's `multiprocessing` to rapidly parse large RRF files (`MRCONSO`, `MRREL`, `MRSTY`).
    - **Bulk Loading**: Generates optimized CSVs and the precise command for `neo4j-admin database import`, the fastest way to ingest data into Neo4j.
    - **Resilient Incremental Updates**: Uses `apoc.periodic.iterate` for batched, transactional updates, ensuring that synchronizations are efficient and can be safely restarted.
- **Sophisticated "Snapshot Diff" Synchronization**: Implements an intelligent delta strategy that goes beyond simple additions. It correctly processes UMLS change files (`DELETEDCUI`, `MERGEDCUI`) and removes any stale nodes or relationships that were not present in the new release, ensuring the graph is a true reflection of the source.
- **Biolink Model Standardization**: Maps UMLS semantic types (TUIs) and relationship attributes (RELAs) to Biolink-compliant categories and predicates, promoting data interoperability.
- **Highly Maintainable**: Built with modern Python (3.10+), full type hinting, Pydantic v2+ for configuration, and Typer for a clean CLI.

## Installation

```bash
pip install .
```
For development, install in editable mode with testing dependencies:
```bash
pip install -e ".[test]"
```

## Configuration

The package is configured via environment variables. You can set them in your shell or place them in a `.env` file in the root of your project directory.

**Crucially, you must provide your UMLS API key.**

```bash
# .env file
# Required: Your UMLS API Key from the UTS website
PYNEOUMLSSYNCER_UMLS_API_KEY="your-api-key-here"

# Required: The absolute path to your Neo4j instance's import directory.
# This tool needs to write CSVs here, and Neo4j needs to read them.
# Example for Docker: "/path/on/host/mapped/to/import"
# Example for local install: "/var/lib/neo4j/import"
PYNEOUMLSSYNCER_NEO4J_IMPORT_DIR="/your/neo4j/import/dir"

# --- Optional Overrides ---
# Neo4j connection details
PYNEOUMLSSYNCER_NEO4J_URI="neo4j://localhost:7687"
PYNEOUMLSSYNCER_NEO4J_USER="neo4j"
PYNEOUMLSSYNCER_NEO4J_PASSWORD="password"
PYNEOUMLSSYNCER_NEO4J_DATABASE="neo4j"

# Comma-separated list of source vocabularies to include
PYNEOUMLSSYNCER_SAB_FILTER="RXNORM,SNOMEDCT_US,MTH,MSH,LNC"
```

## Usage

The primary entry point is the `py-neo-umls-syncer` command-line interface.

### Step 1: Initial Bulk Import

The first time you populate your database, you must use the `full-import` command. This will download the latest UMLS release, process it, and generate the necessary command for Neo4j's offline bulk importer.

You must provide the version of the release you are importing. This is critical for enabling future incremental updates.

```bash
# Example for the May 2025 release
py-neo-umls-syncer full-import --version "2025AA"
```

This command will produce output that includes a shell command block like this:

```bash
# IMPORTANT: The target Neo4j database must be stopped before running this command.
# Example: `neo4j stop -d neo4j`

neo4j-admin database import full \
    --nodes=Concept:Concept-ID="nodes_concepts.csv" \
    --nodes=Code:Code-ID="nodes_codes.csv" \
    --relationships=HAS_CODE="rels_has_code.csv" \
    --relationships="rels_inter_concept.csv" \
    --overwrite-destination=true \
    neo4j
```

You must **stop your Neo4j database** and then copy, paste, and execute this command in your terminal to perform the high-speed data import.

### Step 2: Incremental Synchronization

For all subsequent UMLS releases, you will use the `incremental-sync` command. This connects to a live Neo4j database and applies the "Snapshot Diff" update.

You must provide the version of the new release you are synchronizing.

```bash
# Example for the May 2025 release
py-neo-umls-syncer incremental-sync --version "2025AB"
```

This command will:
1.  Download the specified UMLS release (if not already present).
2.  Process UMLS change files to delete and merge concepts.
3.  Load all new and updated information, tagging it with the new version.
4.  Delete any data from the graph that was not tagged with the new version.
5.  Update a metadata node in the graph to lock in the new version number.

## Graph Schema

The generated Labeled Property Graph (LPG) has the following structure:

-   **Nodes**:
    -   `:Concept`: Represents a single UMLS Concept (CUI).
        - Properties: `cui` (unique), `preferred_name`, `last_seen_version`.
        - Additional Labels: Mapped Biolink Categories (e.g., `:biolink:Disease`).
    -   `:Code`: Represents a source-level identifier (e.g., an RxNorm code).
        - Properties: `code_id` (unique, e.g., "RXNORM:198440"), `sab`, `name`, `last_seen_version`.

-   **Relationships**:
    -   `(:Concept)-[:HAS_CODE]->(:Code)`: Connects a concept to its source codes.
    -   `(:Concept)-[:biolink:treats]->(:Concept)`: Inter-concept relationships use the mapped Biolink Predicate as the relationship **type**.
        - Properties:
            - `source_rela`: The original UMLS RELA/REL value.
            - `asserted_by_sabs`: A list of source vocabularies asserting the relationship.
            - `last_seen_version`: The UMLS release this relationship was last seen in.

## Development

To set up a development environment:

```bash
# Clone the repository
git clone https://github.com/your-org/py_neo_umls_syncer.git
cd py_neo_umls_syncer

# Create and activate a virtual environment
python -m venv .venv
source .venv/bin/activate

# Install in editable mode with test dependencies
pip install -e ".[test]"

# Set up your .env file with your UMLS API key
echo 'PYNEOUMLSSYNCER_UMLS_API_KEY="your-key-here"' > .env
```

### Running Tests

The project includes an integration test suite using `pytest` and `testcontainers`. The tests will automatically spin up a Neo4j instance with the APOC plugin.

```bash
pytest
```
