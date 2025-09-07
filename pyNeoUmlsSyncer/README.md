# pyNeoUmlsSyncer

A production-ready Python package for creating and maintaining a UMLS (Unified Medical Language System) Labeled Property Graph (LPG) in Neo4j (version 5.x).

This package is designed to be highly optimized, robust, standardized (Biolink compliant), and implement a sophisticated, idempotent incremental update strategy.

## Core Features

- **UMLS Data Acquisition**: Authenticates with the UMLS UTS API, downloads the RRF distribution, and verifies checksums.
- **Optimized ETL**:
    - **Initial Load**: Utilizes `neo4j-admin` for bulk importing, ensuring maximum speed.
    - **Incremental Load**: Employs Neo4j's APOC library for resilient, batched transaction management.
    - **Parallel Processing**: Leverages Python's `multiprocessing` for CPU-intensive parsing tasks.
- **Reliable Synchronization**: Implements a "Snapshot Diff" strategy combined with UMLS change files (`DELETEDCUI`, `MERGEDCUI`) to keep the database perfectly in sync with new UMLS releases.
- **Biolink Model Compliance**: The graph schema is aligned with the Biolink Model for interoperability.
- **Modern & Maintainable**: Built with Python 3.10+, full type hinting, Pydantic for configuration, Typer for a clean CLI, and comprehensive Pytest integration tests.

## Installation

```bash
# TBD
```

## Usage

```bash
# TBD
```
