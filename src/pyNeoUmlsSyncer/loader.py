"""
Neo4j Database Loader.

This module provides functionalities to load transformed UMLS data into a Neo4j
database. It supports two strategies:
1.  A high-speed bulk load using `neo4j-admin database import`.
2.  An incremental, idempotent load using the Neo4j driver and APOC procedures.
"""
import logging
import csv
from pathlib import Path
from typing import List, Dict, Any, Iterable, Optional
from collections import defaultdict
from neo4j import GraphDatabase, Driver, exceptions
from tqdm import tqdm

from .config import settings
from .models import Concept, Code, HasCodeRelationship, ConceptRelationship

logger = logging.getLogger(__name__)

class Neo4jLoader:
    """Manages connection and loading operations into a Neo4j database."""
    def __init__(self):
        self._driver: Optional[Driver] = None
        self.database = settings.neo4j_database
        # This path is relative to the directory specified in neo4j.conf (dbms.directories.import)
        self.import_dir = Path(settings.neo4j_import_dir)

    @property
    def driver(self) -> Driver:
        """Initializes the Neo4j driver on first access."""
        if self._driver is None:
            logger.info(f"Initializing Neo4j driver for {settings.neo4j_uri}...")
            try:
                self._driver = GraphDatabase.driver(
                    settings.neo4j_uri,
                    auth=(settings.neo4j_user, settings.neo4j_password)
                )
                self._driver.verify_connectivity()
                logger.info("Neo4j driver initialized successfully.")
            except exceptions.AuthError as e:
                logger.error(f"Neo4j authentication failed: {e}")
                raise
            except Exception as e:
                logger.error(f"Failed to connect to Neo4j at {settings.neo4j_uri}: {e}")
                raise
        return self._driver

    def close(self):
        """Closes the Neo4j driver connection."""
        if self._driver is not None:
            self._driver.close()
            self._driver = None
            logger.info("Neo4j driver closed.")

    def _write_csv(self, file_path: Path, header: List[str], rows: Iterable[List[str]]):
        """Utility to write data to a CSV file with a progress bar."""
        with open(file_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(header)
            # Since rows can be a large list, we wrap it with tqdm for progress
            for row in tqdm(rows, desc=f"Writing {file_path.name}"):
                writer.writerow(row)

    def generate_bulk_import_files(
        self,
        output_dir: Path,
        concepts: List[Concept],
        codes: List[Code],
        has_code_rels: List[HasCodeRelationship],
        concept_rels: List[ConceptRelationship]
    ):
        """Generates all necessary CSV files and the import script for a bulk load."""
        logger.info(f"Generating neo4j-admin import files in: {output_dir}")
        output_dir.mkdir(parents=True, exist_ok=True)

        # --- Write Node Files ---
        if concepts:
            self._write_csv(
                output_dir / "concepts.csv",
                concepts[0].get_csv_header(),
                (c.to_csv_row() for c in concepts)
            )
        if codes:
            self._write_csv(
                output_dir / "codes.csv",
                codes[0].get_csv_header(),
                (c.to_csv_row() for c in codes)
            )

        # --- Write Relationship Files ---
        if has_code_rels:
            self._write_csv(
                output_dir / "rels-has_code.csv",
                has_code_rels[0].get_csv_header(),
                (r.to_csv_row() for r in has_code_rels)
            )

        # Group concept relationships by type and write separate files
        grouped_rels = defaultdict(list)
        for rel in concept_rels:
            # Sanitize rel_type for use in filenames
            sanitized_type = rel.rel_type.replace(":", "_")
            grouped_rels[sanitized_type].append(rel)

        for rel_type, rels in grouped_rels.items():
            if rels:
                self._write_csv(
                    output_dir / f"rels-concepts-{rel_type}.csv",
                    rels[0].get_csv_header(),
                    (r.to_csv_row() for r in rels)
                )

        self._generate_import_script(output_dir)
        logger.info("Bulk import file generation complete.")

    def _generate_import_script(self, output_dir: Path):
        """Creates a shell script to run the neo4j-admin import command."""
        script_path = output_dir / "import.sh"
        db_name = settings.neo4j_database

        # Paths used inside the script are relative to the Neo4j import directory
        relative_output_dir = output_dir.relative_to(Path.cwd()) # Assumes CWD is project root

        cmd_parts = [
            f"neo4j-admin database import full {db_name}",
            "--overwrite-destination=true",
            "--multiline-fields=true",
        ]

        for f in sorted(output_dir.glob("*.csv")):
            if "concepts" in f.name:
                cmd_parts.append(f"--nodes '{self.import_dir / relative_output_dir / f.name}'")
            elif "codes" in f.name:
                cmd_parts.append(f"--nodes '{self.import_dir / relative_output_dir / f.name}'")
            elif "rels-has_code" in f.name:
                cmd_parts.append(f"--relationships '{self.import_dir / relative_output_dir / f.name}'")
            elif "rels-concepts" in f.name:
                # Relationship type needs to be specified for concept rels
                rel_type = f.stem.split("rels-concepts-")[1].replace("_", ":")
                cmd_parts.append(f"--relationships '{rel_type}','{self.import_dir / relative_output_dir / f.name}'")

        full_cmd = " \\\n  ".join(cmd_parts)

        with open(script_path, 'w') as f:
            f.write("#!/bin/bash\n\n")
            f.write("# This script runs the neo4j-admin bulk import.\n")
            f.write("# IMPORTANT: \n")
            f.write(f"# 1. Stop your Neo4j database instance before running: `neo4j stop`\n")
            f.write(f"# 2. The paths in this script assume your neo4j import directory is '{self.import_dir}'.\n")
            f.write(f"#    Verify this path in your neo4j.conf (dbms.directories.import).\n")
            f.write(f"# 3. Run this script from the project root directory.\n")
            f.write(f"# 4. After completion, start your database: `neo4j start`\n\n")
            f.write(full_cmd + "\n")

        # Make the script executable
        script_path.chmod(0o755)
        logger.info(f"Generated import script at: {script_path}")

    # --- Incremental Load Methods ---

    def get_current_umls_version(self) -> Optional[str]:
        """Queries the database for the version in the :UMLS_Meta node."""
        query = "MATCH (m:UMLS_Meta) RETURN m.version AS version"
        try:
            records, _, _ = self.driver.execute_query(query, database_=self.database)
            return records[0]["version"] if records else None
        except Exception as e:
            logger.error(f"Failed to query UMLS meta version: {e}")
            return None

    def update_umls_version(self, version: str):
        """Sets the version in the :UMLS_Meta node."""
        query = "MERGE (m:UMLS_Meta) SET m.version = $version"
        try:
            self.driver.execute_query(query, version=version, database_=self.database)
            logger.info(f"Updated :UMLS_Meta version to {version}")
        except Exception as e:
            logger.error(f"Failed to update UMLS meta version: {e}")
            raise

    def execute_apoc_iterate(self, inner_query: str, rows: List[Dict[str, Any]], desc: str):
        """
        Executes a batched update using apoc.periodic.iterate.

        Args:
            inner_query: The Cypher query to execute for each batch item.
            rows: A list of dictionaries, where each dict is a row to be processed.
            desc: A description for the progress bar.
        """
        if not rows:
            logger.info(f"No rows to process for '{desc}', skipping.")
            return

        outer_query = """
        CALL apoc.periodic.iterate(
            "UNWIND $rows AS row RETURN row",
            $inner_query,
            {batchSize: $batchSize, parallel: false, params: {rows: $rows}}
        )
        YIELD batches, total, errorMessages
        RETURN batches, total, errorMessages
        """
        logger.info(f"Executing APOC iterate for '{desc}' ({len(rows)} rows)...")
        try:
            # This is a long-running query, so no timeout
            records, _, _ = self.driver.execute_query(
                outer_query,
                rows=rows,
                inner_query=inner_query,
                batchSize=settings.apoc_batch_size,
                database_=self.database
            )
            summary = records[0]
            if summary["errorMessages"] and summary["errorMessages"]:
                logger.error(f"APOC iterate for '{desc}' encountered errors: {summary['errorMessages']}")
            else:
                logger.info(f"APOC iterate for '{desc}' completed. Batches: {summary['batches']}, Total: {summary['total']}.")
        except Exception as e:
            logger.error(f"An exception occurred during APOC iterate for '{desc}': {e}", exc_info=True)
            raise
