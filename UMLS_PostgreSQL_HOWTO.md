# How-To: End-to-End UMLS Loading into PostgreSQL

This document provides a comprehensive, step-by-step guide for loading and maintaining data from the Unified Medical Language System (UMLS) in a PostgreSQL database. It covers the initial full import and a sophisticated, idempotent delta-loading strategy for subsequent UMLS releases.

The methodology described here is inspired by production-grade data loading systems and is designed to be robust, efficient, and maintainable.

## 1. Introduction to UMLS

The [Unified Medical Language System (UMLS)](https://www.nlm.nih.gov/research/umls/index.html) is a comprehensive collection of biomedical vocabularies and standards produced by the U.S. National Library of Medicine (NLM). It integrates millions of concepts from various sources (e.g., SNOMED-CT, RxNorm, MeSH) into a single, unified structure. Its primary components include:

*   **Metathesaurus**: The core, containing concepts (CUIs), terms, and the relationships between them.
*   **Semantic Network**: A system of broad categories (Semantic Types) and relationships used to categorize the concepts.

Loading UMLS into a relational database like PostgreSQL enables powerful querying capabilities for applications in clinical informatics, biomedical research, and natural language processing.

## 2. Prerequisites

Before you begin, ensure you have the following:

1.  **UMLS License**: You must have a valid UMLS Metathesaurus License from the NLM. You can [register for one here](https://uts.nlm.nih.gov/uts/signup-login).
2.  **UMLS API Key**: Once registered, you will need your API key from your UTS (UMLS Terminology Services) profile page. This is required for downloading the UMLS release files.
3.  **PostgreSQL Server**: A running instance of PostgreSQL (version 12 or higher is recommended). You need superuser or database owner privileges to create tables, run `COPY` commands, and create extensions.
4.  **`psql` CLI**: The PostgreSQL command-line interface must be installed and available in your system's PATH.
5.  **Python Environment**: A Python 3.8+ environment is recommended for running the data parsing and downloading scripts. The examples will assume you have the `requests` library installed (`pip install requests`).
6.  **Disk Space**: A full UMLS release is large. Ensure you have at least 50 GB of free disk space for the downloaded archive, the extracted files, and the database itself.

## 3. Data Acquisition

The first step is to download the UMLS release files. We will use the official NLM API to find the correct download URL for a specific version and then download the archive.

The following Python script automates this process. It is idempotent, meaning it will skip the download if the extracted directory already exists.

**`download_umls.py`**
```python
import hashlib
import zipfile
from pathlib import Path
import requests
import os
import sys

# --- Configuration ---
# Your UMLS API key. Consider loading this from an environment variable for better security.
API_KEY = os.environ.get("UMLS_API_KEY", "YOUR_API_KEY_HERE")
# The UMLS version to download (e.g., "2025AA").
UMLS_VERSION = "2025AA"
# Directory to store downloads.
DOWNLOAD_DIR = Path("./umls_downloads")
# ---------------------

RELEASE_API_URL = "https://uts-ws.nlm.nih.gov/releases"
DOWNLOAD_API_URL = "https://uts-ws.nlm.nih.gov/download"

def get_release_info(version: str) -> dict:
    """Fetches metadata for a specific UMLS full release version."""
    print(f"Fetching UMLS release information for version: {version}...")
    params = {"releaseType": "umls-full-release"}
    response = requests.get(RELEASE_API_URL, params=params)
    response.raise_for_status()
    data = response.json()

    if not data["result"]:
        raise ValueError("No UMLS full releases found in API response.")

    for release in data["result"]:
        if release.get("name") == version:
            print(f"Found matching release: {release['name']}")
            return release

    available = [r.get('name') for r in data['result']]
    raise ValueError(f"UMLS release version '{version}' not found. Available versions: {available}")

def calculate_md5(filepath: Path) -> str:
    """Calculates the MD5 checksum of a file."""
    hash_md5 = hashlib.md5()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()

def download_and_extract_release():
    """
    Orchestrates the download, verification, and extraction of a specific UMLS release.
    Returns the path to the extracted META directory.
    """
    if not API_KEY or API_KEY == "YOUR_API_KEY_HERE":
        print("Error: UMLS_API_KEY is not set. Please edit the script or set the environment variable.")
        sys.exit(1)

    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
    release_info = get_release_info(UMLS_VERSION)
    download_url = release_info["downloadUrl"]
    expected_checksum = release_info.get("md5")

    release_version_dir = DOWNLOAD_DIR / UMLS_VERSION
    zip_filename = Path(download_url).name
    zip_filepath = DOWNLOAD_DIR / zip_filename
    # The RRF files are in a directory named 'META' inside a version-specific folder
    extracted_meta_path = release_version_dir / release_info['name'] / "META"

    if extracted_meta_path.exists() and extracted_meta_path.is_dir():
        print(f"UMLS release {UMLS_VERSION} already downloaded and extracted. Skipping.")
        return extracted_meta_path

    print(f"Downloading {zip_filename}...")
    download_params = {"url": download_url, "apiKey": API_KEY}
    with requests.get(DOWNLOAD_API_URL, params=download_params, stream=True) as r:
        r.raise_for_status()
        total_size = int(r.headers.get('content-length', 0))
        with open(zip_filepath, 'wb') as f:
            for i, chunk in enumerate(r.iter_content(chunk_size=8192)):
                f.write(chunk)
                if i % 100 == 0: # Print progress update
                    print(f"Downloaded {f.tell() / (1024*1024):.2f} / {total_size / (1024*1024):.2f} MB", end='\\r')

    print(f"\\nDownload complete: {zip_filepath}")

    if expected_checksum:
        print("Verifying checksum...")
        actual_checksum = calculate_md5(zip_filepath)
        if actual_checksum.lower() != expected_checksum.lower():
            raise RuntimeError(f"Checksum mismatch! Expected: {expected_checksum}, Got: {actual_checksum}")
        print("Checksum verified successfully.")
    else:
        print("MD5 checksum not provided in release metadata. Skipping verification.")

    print(f"Extracting {zip_filename} to {release_version_dir}...")
    with zipfile.ZipFile(zip_filepath, 'r') as zip_ref:
        zip_ref.extractall(release_version_dir)
    print("Extraction complete.")

    zip_filepath.unlink()
    print(f"Removed zip file: {zip_filepath}")

    if not extracted_meta_path.exists():
         raise FileNotFoundError(f"Extracted META directory not found at {extracted_meta_path}")

    return extracted_meta_path

if __name__ == "__main__":
    meta_path = download_and_extract_release()
    print(f"\\nSuccessfully acquired UMLS data. RRF files are in: {meta_path.resolve()}")

### How to Run

1.  Save the code above as `download_umls.py`.
2.  **Set your API Key**:
    *   **Option A (Recommended)**: Set an environment variable.
        ```bash
        export UMLS_API_KEY="your-long-api-key-here"
        ```
    *   **Option B**: Edit the `API_KEY` variable directly in the script.
3.  Run the script from your terminal:
    ```bash
    python download_umls.py
    ```

After the script finishes, you will have a `umls_downloads/<version>/<version>/META` directory containing all the Rich Release Format (`.RRF`) files needed for the next steps.

## 4. PostgreSQL Schema Definition

Next, we need to create the tables in our PostgreSQL database. The following schema is designed to efficiently store the core UMLS data from the `MRCONSO`, `MRREL`, and `MRSTY` files.

**Important Note**: We will create the tables without primary keys, foreign keys, or indexes initially. These constraints will be added *after* the initial bulk data load to maximize ingestion speed.

Connect to your PostgreSQL database using `psql` or your favorite SQL client and run the following DDL statements.

```sql
-- Table to hold the core UMLS Concepts (CUIs)
CREATE TABLE concepts (
    cui VARCHAR(10) NOT NULL,
    preferred_name TEXT,
    last_seen_version VARCHAR(10) -- Tracks the UMLS release this concept was last seen in
);

-- Table to hold source-specific codes (e.g., from SNOMED-CT, RxNorm)
CREATE TABLE codes (
    code_id VARCHAR(100) NOT NULL, -- A composite ID, e.g., 'SNOMEDCT_US:73211009'
    cui VARCHAR(10) NOT NULL,      -- The concept this code maps to
    sab VARCHAR(40) NOT NULL,      -- Source Vocabulary (e.g., 'SNOMEDCT_US')
    code VARCHAR(100) NOT NULL,    -- The code in its native source
    name TEXT,
    last_seen_version VARCHAR(10)
);

-- Table for inter-concept relationships from MRREL.RRF
CREATE TABLE relationships (
    source_cui VARCHAR(10) NOT NULL,
    target_cui VARCHAR(10) NOT NULL,
    -- RELA is the relationship attribute, REL is a broader category. We prioritize RELA.
    source_rela VARCHAR(100),
    -- List of sources that assert this specific relationship
    asserted_by_sabs VARCHAR(40)[], -- Using a text array for multiple sources
    last_seen_version VARCHAR(10)
);

-- Table for semantic type assignments from MRSTY.RRF
CREATE TABLE semantic_types (
    cui VARCHAR(10) NOT NULL,
    tui VARCHAR(10) NOT NULL, -- The unique ID for the semantic type
    sty_name TEXT             -- The name of the semantic type (e.g., 'Disease or Syndrome')
);

-- A simple metadata table to track the current UMLS version loaded in the DB
CREATE TABLE umls_meta (
    meta_key VARCHAR(50) PRIMARY KEY,
    meta_value VARCHAR(50)
);

-- Insert an initial value for the version. It will be updated after each load.
INSERT INTO umls_meta (meta_key, meta_value) VALUES ('version', 'none');

```

## 5. Full Load Process (Initial Import)

The full load process is performed once to populate your database with an entire UMLS release. The strategy is to first parse the raw, pipe-delimited RRF files into clean, structured CSV files. Then, we use PostgreSQL's highly efficient `COPY` command to bulk-load the data.

### Step 5.1: Parse RRF Files into CSVs

The following Python script reads the main RRF files (`MRCONSO`, `MRREL`, `MRSTY`), processes the data, and writes it to CSV files suitable for our schema. It includes the crucial logic for selecting a preferred name for each concept.

**Key Logic Implemented**:
*   **Parallel Processing**: Uses `multiprocessing` to speed up parsing of the large `MRCONSO.RRF` file.
*   **Preferred Name Selection**: Implements a scoring system to choose the best name for a concept based on Term Status (TS), State of Term (STT), and Preference (ISPREF) fields, prioritizing sources like 'RXNORM' and 'SNOMEDCT_US'.
*   **Filtering**: Ignores suppressed terms and can be configured to include only specific source vocabularies (SABs).
*   **Relationship Aggregation**: For `MRREL.RRF`, it aggregates different source assertions for the same conceptual relationship.

**`parse_rrf_to_csv.py`**
```python
import csv
from pathlib import Path
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor
import os

# --- Configuration ---
# Path to the extracted META directory from Step 3
META_PATH = Path("./umls_downloads/2025AA/2025AA/META")
# Directory to save the processed CSV files
OUTPUT_DIR = Path("./processed_csvs")
# UMLS release version
UMLS_VERSION = "2025AA"
# Filter to include only these source vocabularies. Leave empty to include all.
# Example: SAB_FILTER = {"RXNORM", "SNOMEDCT_US", "MTH", "MSH", "LNC"}
SAB_FILTER = set()
# Prioritized list of SABs for selecting the preferred name
SAB_PRIORITY = ["RXNORM", "SNOMEDCT_US", "MTH", "MSH", "LNC"]
# --- End Configuration ---

# Column indices for MRCONSO.RRF
CUI_I, LAT_I, TS_I, LUI_I, STT_I, SUI_I, ISPREF_I, AUI_I, SAUI_I, SCUI_I, SDUI_I, SAB_I, TTY_I, CODE_I, STR_I, SRL_I, SUPPRESS_I, CVF_I = range(18)
# Column indices for MRREL.RRF
CUI1_I, AUI1_I, STYPE1_I, REL_I, CUI2_I, AUI2_I, STYPE2_I, RELA_I, RUI_I, SRUI_I, SAB_REL_I, SL_I, RG_I, DIR_I, SUPPRESS_REL_I, CVF_REL_I = range(16)
# Column indices for MRSTY.RRF
CUI_STY_I, TUI_I, STN_I, STY_I, ATUI_I, CVF_STY_I = range(6)

def process_mrconso_chunk(chunk_info):
    """Worker function to parse a chunk of MRCONSO.RRF."""
    filepath, start, end = chunk_info
    results = defaultdict(list)
    with open(filepath, 'r', encoding='utf-8') as f:
        f.seek(start)
        reader = csv.reader(f, delimiter='|', quotechar='\\x00')
        while f.tell() < end:
            try:
                row = next(reader)
                if SAB_FILTER and row[SAB_I] not in SAB_FILTER:
                    continue
                if row[SUPPRESS_I] in ('O', 'Y'): # Skip obsolete or suppressed content
                    continue

                term_info = {
                    "cui": row[CUI_I], "sab": row[SAB_I], "code": row[CODE_I],
                    "name": row[STR_I], "tty": row[TTY_I], "ispref": row[ISPREF_I],
                    "ts": row[TS_I], "stt": row[STT_I]
                }
                results[row[CUI_I]].append(term_info)
            except (StopIteration, IndexError):
                break
    return results

def get_file_chunks(filepath: str, num_chunks: int):
    """Splits a file into byte-offset chunks."""
    file_size = os.path.getsize(filepath)
    chunk_size = file_size // num_chunks
    chunks = []
    start = 0
    with open(filepath, 'rb') as f:
        while start < file_size:
            end = min(start + chunk_size, file_size)
            if end < file_size:
                f.seek(end)
                f.readline()
                end = f.tell()
            chunks.append((filepath, start, end))
            start = end
    return chunks

def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    mrconso_path = META_PATH / "MRCONSO.RRF"
    mrrel_path = META_PATH / "MRREL.RRF"
    mrsty_path = META_PATH / "MRSTY.RRF"

    # 1. Parse MRCONSO in parallel
    print("Parsing MRCONSO.RRF...")
    num_workers = os.cpu_count() or 1
    chunks = get_file_chunks(str(mrconso_path), num_workers * 4)
    all_terms = defaultdict(list)
    with ProcessPoolExecutor(max_workers=num_workers) as executor:
        for chunk_result in executor.map(process_mrconso_chunk, chunks):
            for cui, terms in chunk_result.items():
                all_terms[cui].extend(terms)

    # 2. Process parsed terms to generate concepts.csv and codes.csv
    print("Generating concepts.csv and codes.csv...")
    concepts = []
    codes = []
    sab_priority_map = {sab: i for i, sab in enumerate(SAB_PRIORITY)}

    for cui, terms in all_terms.items():
        # Generate all code entries
        for term in terms:
            code_id = f"{term['sab']}:{term['code']}"
            codes.append([code_id, cui, term['sab'], term['code'], term['name'], UMLS_VERSION])

        # Select preferred name
        terms.sort(key=lambda t: (
            sab_priority_map.get(t['sab'], 999),
            t['ts'] != 'P', t['stt'] != 'PF', t['ispref'] != 'Y'
        ))
        preferred_term = terms[0]
        concepts.append([cui, preferred_term['name'], UMLS_VERSION])

    with open(OUTPUT_DIR / "concepts.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["cui", "preferred_name", "last_seen_version"])
        writer.writerows(concepts)

    with open(OUTPUT_DIR / "codes.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["code_id", "cui", "sab", "code", "name", "last_seen_version"])
        writer.writerows(codes)

    # 3. Parse MRREL
    print("Parsing MRREL.RRF and generating relationships.csv...")
    agg_rels = defaultdict(set)
    with open(mrrel_path, 'r', encoding='utf-8') as f:
        reader = csv.reader(f, delimiter='|', quotechar='\\x00')
        for row in reader:
            if SAB_FILTER and row[SAB_REL_I] not in SAB_FILTER:
                continue
            # Ensure the relationship is between concepts we are actually loading
            if row[CUI1_I] in all_terms and row[CUI2_I] in all_terms:
                key = (row[CUI1_I], row[CUI2_I], row[RELA_I] or row[REL_I])
                agg_rels[key].add(row[SAB_REL_I])

    with open(OUTPUT_DIR / "relationships.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["source_cui", "target_cui", "source_rela", "asserted_by_sabs", "last_seen_version"])
        for (cui1, cui2, rela), sabs in agg_rels.items():
            # Format array for PostgreSQL COPY: {val1,val2}
            sabs_str = "{" + ",".join(sorted(list(sabs))) + "}"
            writer.writerow([cui1, cui2, rela, sabs_str, UMLS_VERSION])

    # 4. Parse MRSTY
    print("Parsing MRSTY.RRF and generating semantic_types.csv...")
    with open(mrsty_path, 'r', encoding='utf-8') as f_in, \
         open(OUTPUT_DIR / "semantic_types.csv", "w", newline="", encoding="utf-8") as f_out:
        reader = csv.reader(f_in, delimiter='|', quotechar='\\x00')
        writer = csv.writer(f_out)
        writer.writerow(["cui", "tui", "sty_name"])
        for row in reader:
            if row[CUI_STY_I] in all_terms:
                writer.writerow([row[CUI_STY_I], row[TUI_I], row[STY_I]])

    print("CSV generation complete.")

if __name__ == '__main__':
    main()
```

### Step 5.2: Run the Bulk Load with `psql`

Once the CSV files are generated in your `processed_csvs` directory, you can load them into PostgreSQL. Run these commands from your terminal, ensuring `psql` is connected to the correct database.

```bash
# Path to your CSV files
CSV_DIR="processed_csvs"

# Use psql's \copy command for efficient, client-side bulk loading
psql -c "\\copy concepts FROM '${CSV_DIR}/concepts.csv' WITH (FORMAT csv, HEADER true)"
psql -c "\\copy codes FROM '${CSV_DIR}/codes.csv' WITH (FORMAT csv, HEADER true)"
psql -c "\\copy relationships FROM '${CSV_DIR}/relationships.csv' WITH (FORMAT csv, HEADER true)"
psql -c "\\copy semantic_types FROM '${CSV_DIR}/semantic_types.csv' WITH (FORMAT csv, HEADER true)"

# Finally, update the metadata table to reflect the loaded version
psql -c "UPDATE umls_meta SET meta_value = '${UMLS_VERSION}' WHERE meta_key = 'version';"
```

### Step 5.3: Create Constraints and Indexes

After the data is loaded, it's time to create the primary keys, foreign keys, and indexes. This improves query performance and enforces data integrity.

```sql
-- Add Primary Keys
ALTER TABLE concepts ADD PRIMARY KEY (cui);
ALTER TABLE codes ADD PRIMARY KEY (code_id);

-- Add Foreign Keys
-- Note: You may discover some codes or relationships reference CUIs not in MRCONSO.
-- To handle this, you might need a clean-up step or load all CUIs, even those without terms.
-- For simplicity here, we assume all referenced CUIs exist.
ALTER TABLE codes ADD CONSTRAINT fk_codes_cui FOREIGN KEY (cui) REFERENCES concepts(cui);
ALTER TABLE relationships ADD CONSTRAINT fk_rels_source_cui FOREIGN KEY (source_cui) REFERENCES concepts(cui);
ALTER TABLE relationships ADD CONSTRAINT fk_rels_target_cui FOREIGN KEY (target_cui) REFERENCES concepts(cui);
ALTER TABLE semantic_types ADD CONSTRAINT fk_sty_cui FOREIGN KEY (cui) REFERENCES concepts(cui);

-- Create Indexes for common query patterns
CREATE INDEX idx_codes_cui ON codes (cui);
CREATE INDEX idx_codes_sab ON codes (sab);
CREATE INDEX idx_codes_code ON codes (code);
CREATE INDEX idx_rels_source_cui ON relationships (source_cui);
CREATE INDEX idx_rels_target_cui ON relationships (target_cui);
CREATE INDEX idx_sty_cui ON semantic_types (cui);
CREATE INDEX idx_sty_tui ON semantic_types (tui);
```

Your database is now fully loaded with the UMLS release.

## 6. Delta Load Process (Incremental Synchronization)

Once you have a fully loaded database, you don't need to repeat the entire process for new UMLS releases. Instead, you can perform a delta load (or incremental synchronization). This guide describes a robust "Snapshot Diff" strategy.

**The core idea is**:
1.  Process explicit change files provided by UMLS (`DELETEDCUI`, `MERGEDCUI`).
2.  Load the entire new UMLS release into temporary staging tables.
3.  Use this "snapshot" to update your main tables, "tagging" every record with the new version number.
4.  Remove any data from your main tables that was not "tagged" with the new version.
5.  Update the database's metadata to lock in the new version.

This approach is idempotent and resilient, ensuring your database is a perfect reflection of the new UMLS release.

### Preparation: The New Snapshot

Before starting the delta load, you must:
1.  Use the `download_umls.py` script from Step 3 to download the **new** UMLS release version (e.g., `2025AB`).
2.  Use the `parse_rrf_to_csv.py` script from Step 5.1 to parse the new RRF files into a new set of CSVs. Make sure to update the `UMLS_VERSION` variable in the script.
3.  Create staging tables to hold the new snapshot. These are temporary copies of your main tables.

```sql
-- Create staging tables for the new release data
CREATE TABLE concepts_stage (LIKE concepts);
CREATE TABLE codes_stage (LIKE codes);
CREATE TABLE relationships_stage (LIKE relationships);
CREATE TABLE semantic_types_stage (LIKE semantic_types);
```

4.  Load the new CSVs into these staging tables using the `\copy` commands from Step 5.2, targeting the `_stage` tables.

### Step 6.1: Process Deletions (`DELETEDCUI.RRF`)

This file lists CUIs that have been completely removed from UMLS.

```sql
-- First, create a temporary table to hold the CUIs to delete.
CREATE TEMP TABLE deleted_cuis (cui VARCHAR(10) PRIMARY KEY);

-- Load the DELETEDCUI.RRF file into it. The file has one column.
-- Make sure to provide the correct path to your new release's META directory.
\copy deleted_cuis FROM 'path/to/your/new/META/DELETEDCUI.RRF' WITH (FORMAT csv, DELIMITER '|');

-- Now, delete these concepts and all related data.
-- Using a transaction ensures this happens atomically.
BEGIN;

-- Delete relationships pointing to or from the deleted CUIs
DELETE FROM relationships WHERE source_cui IN (SELECT cui FROM deleted_cuis);
DELETE FROM relationships WHERE target_cui IN (SELECT cui FROM deleted_cuis);

-- Delete codes and semantic types associated with the CUIs
DELETE FROM codes WHERE cui IN (SELECT cui FROM deleted_cuis);
DELETE FROM semantic_types WHERE cui IN (SELECT cui FROM deleted_cuis);

-- Finally, delete the concepts themselves
DELETE FROM concepts WHERE cui IN (SELECT cui FROM deleted_cuis);

COMMIT;

-- Clean up
DROP TABLE deleted_cuis;
```

### Step 6.2: Process Merges (`MERGEDCUI.RRF`)

This file lists CUIs that have been merged into other CUIs. For each row (`old_cui|new_cui`), we must migrate all data from `old_cui` to `new_cui`.

```sql
-- Create a temporary table to hold the merge operations.
CREATE TEMP TABLE merged_cuis (old_cui VARCHAR(10), new_cui VARCHAR(10));
\copy merged_cuis FROM 'path/to/your/new/META/MERGEDCUI.RRF' WITH (FORMAT csv, DELIMITER '|');

-- It's crucial to process these merges one by one within a transaction.
-- A plpgsql loop is a good way to handle this robustly.
DO $$
DECLARE
    merge_rec RECORD;
BEGIN
    FOR merge_rec IN SELECT old_cui, new_cui FROM merged_cuis LOOP
        -- Re-assign codes from the old CUI to the new CUI
        UPDATE codes SET cui = merge_rec.new_cui WHERE cui = merge_rec.old_cui;

        -- Update relationships where the old CUI was the source
        UPDATE relationships SET source_cui = merge_rec.new_cui WHERE source_cui = merge_rec.old_cui;

        -- Update relationships where the old CUI was the target
        UPDATE relationships SET target_cui = merge_rec.new_cui WHERE target_cui = merge_rec.old_cui;

        -- Re-assign semantic types
        UPDATE semantic_types SET cui = merge_rec.new_cui WHERE cui = merge_rec.old_cui;

        -- Finally, delete the old concept, as it has been fully merged.
        DELETE FROM concepts WHERE cui = merge_rec.old_cui;
    END LOOP;
END$$;

-- Clean up
DROP TABLE merged_cuis;
```

### Step 6.3: Apply Additions and Updates

Now we use the staging tables to update our main tables. The `INSERT ... ON CONFLICT` statement is perfect for this. It will insert new records and update existing ones. For every record we touch, we "tag" it with the new version number.

```sql
-- Set the new version for easy reference
-- In psql: \set new_version '2025AB'
BEGIN;

-- Upsert Concepts
INSERT INTO concepts (cui, preferred_name, last_seen_version)
SELECT cui, preferred_name, last_seen_version FROM concepts_stage
ON CONFLICT (cui) DO UPDATE SET
    preferred_name = EXCLUDED.preferred_name,
    last_seen_version = EXCLUDED.last_seen_version;

-- Upsert Codes
INSERT INTO codes (code_id, cui, sab, code, name, last_seen_version)
SELECT code_id, cui, sab, code, name, last_seen_version FROM codes_stage
ON CONFLICT (code_id) DO UPDATE SET
    cui = EXCLUDED.cui,
    sab = EXCLUDED.sab,
    code = EXCLUDED.code,
    name = EXCLUDED.name,
    last_seen_version = EXCLUDED.last_seen_version;

-- For relationships, we need a unique key to handle conflicts.
-- Let's add one temporarily if it doesn't exist.
ALTER TABLE relationships ADD CONSTRAINT uq_relationships UNIQUE (source_cui, target_cui, source_rela);

-- Upsert Relationships
INSERT INTO relationships (source_cui, target_cui, source_rela, asserted_by_sabs, last_seen_version)
SELECT source_cui, target_cui, source_rela, asserted_by_sabs, last_seen_version FROM relationships_stage
ON CONFLICT (source_cui, target_cui, source_rela) DO UPDATE SET
    asserted_by_sabs = EXCLUDED.asserted_by_sabs,
    last_seen_version = EXCLUDED.last_seen_version;

ALTER TABLE relationships DROP CONSTRAINT uq_relationships;

-- Semantic types can be cleared and re-inserted, as they don't have a version.
-- This is simpler than an upsert.
TRUNCATE semantic_types;
INSERT INTO semantic_types (cui, tui, sty_name)
SELECT cui, tui, sty_name FROM semantic_types_stage;

COMMIT;
```

### Step 6.4: Remove Stale Data

This is the "diff" step. Any record that was not "tagged" with the new version is now considered stale and can be removed.

**Important**: We only remove codes and relationships this way. Concepts are only ever removed via the explicit `DELETEDCUI.RRF` file to prevent accidental data loss.

```sql
-- In psql: \set new_version '2025AB'
BEGIN;

DELETE FROM codes WHERE last_seen_version IS NULL OR last_seen_version != :'new_version';
DELETE FROM relationships WHERE last_seen_version IS NULL OR last_seen_version != :'new_version';

COMMIT;
```

### Step 6.5: Finalize and Clean Up

The last steps are to update the metadata version and drop the staging tables.

```sql
-- In psql: \set new_version '2025AB'

-- Lock in the new version
UPDATE umls_meta SET meta_value = :'new_version' WHERE meta_key = 'version';

-- Drop the staging tables
DROP TABLE concepts_stage;
DROP TABLE codes_stage;
DROP TABLE relationships_stage;
DROP TABLE semantic_types_stage;
```

Your database is now synchronized with the new UMLS release.

## 7. Indexing and Maintenance

### Indexing Strategy

The indexes created in Step 5.3 provide a solid baseline for query performance. Here's a summary of why they are important:

*   `PRIMARY KEY` on `concepts(cui)` and `codes(code_id)`: Ensures uniqueness and provides the fastest possible lookup for a single concept or code.
*   `INDEX` on `codes(cui)`: Quickly find all codes associated with a given concept.
*   `INDEX` on `codes(sab)` and `codes(code)`: Efficiently search for codes from a specific source vocabulary, or by the source-native code itself.
*   `INDEX` on `relationships(source_cui)` and `relationships(target_cui)`: Speeds up finding all outgoing or incoming relationships for a concept, which is essential for graph-like queries.
*   `INDEX` on `semantic_types(cui)` and `semantic_types(tui)`: Allows for fast lookup of a concept's semantic types or finding all concepts with a specific semantic type.

Depending on your specific use case, you may need to add more advanced indexes, such as multi-column indexes or GIN indexes for full-text search on `preferred_name` or `name` columns.

### Database Maintenance

After large data loading or update operations, it's crucial to perform routine maintenance to ensure PostgreSQL's query planner has up-to-date statistics about your tables.

1.  **ANALYZE**: This command collects statistics about the data distribution in your tables. Accurate statistics are critical for the query planner to choose efficient execution plans.
2.  **VACUUM**: This command reclaims storage occupied by dead tuples (e.g., rows that were deleted or updated). While PostgreSQL's autovacuum daemon handles this automatically, running it manually after a large batch update can be beneficial.

It's good practice to run `VACUUM ANALYZE` on all affected tables after a delta load completes.

```sql
-- Run after a full or delta load
VACUUM ANALYZE concepts;
VACUUM ANALYZE codes;
VACUUM ANALYZE relationships;
VACUUM ANALYZE semantic_types;
```

This concludes the end-to-end guide for loading and maintaining UMLS data in PostgreSQL.
