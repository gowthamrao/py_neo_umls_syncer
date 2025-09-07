"""
loader.py

This module orchestrates the entire data loading process into Neo4j.
It determines whether to perform an initial bulk import or an incremental update
and executes the corresponding workflow.
"""
import csv
import logging
import subprocess
from pathlib import Path
from typing import List, Tuple

from neo4j import GraphDatabase, Driver

from .config import Settings
from .delta_strategy import UmlsDeltaStrategy
from .parser import UmlsParser
from .transformer import UmlsTransformer

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class UmlsLoader:
    """
    Manages the connection to Neo4j and orchestrates the loading process.
    """

    def __init__(self, settings: Settings):
        self.settings = settings
        self.driver = self._get_driver()
        self.delta_strategy = UmlsDeltaStrategy(settings)
        self.parser = UmlsParser(settings)
        self.transformer = UmlsTransformer(settings, self.parser)
        self.data_dir = Path(self.settings.data_dir)
        self.version = self.settings.umls_version
        self.transformed_csv_dir = self.data_dir / "transformed" / self.version

    def _get_driver(self) -> Driver:
        """Initializes and returns the Neo4j driver."""
        logger.info(f"Connecting to Neo4j at {self.settings.neo4j_uri}...")
        try:
            driver = GraphDatabase.driver(
                self.settings.neo4j_uri,
                auth=(self.settings.neo4j_user, self.settings.neo4j_password)
            )
            driver.verify_connectivity()
            logger.info("Neo4j connection successful.")
            return driver
        except Exception as e:
            logger.error(f"Failed to connect to Neo4j: {e}")
            raise

    def is_database_empty(self) -> bool:
        """Checks if the database has been previously initialized."""
        logger.info("Checking if database is initialized...")
        with self.driver.session(database=self.settings.neo4j_database) as session:
            result = session.run("MATCH (m:UMLS_Meta) RETURN count(m) > 0 as exists")
            is_initialized = result.single()["exists"]
            logger.info(f"Database initialized: {is_initialized}")
            return not is_initialized

    def _run_command(self, command: List[str]):
        """Executes a shell command."""
        logger.info(f"Executing command: {' '.join(command)}")
        subprocess.run(command, check=True)

    def run_full_import(self):
        """
        Performs a full bulk import using neo4j-admin.
        This assumes the Neo4j database is stopped.
        """
        logger.info("Starting full bulk import process...")
        # 1. Generate CSVs
        self.transformer.transform_for_bulk_import()

        # 2. Build and run neo4j-admin import command
        # This requires the user to stop the database first.
        # We will print the command and instructions.

        # 2. Build the neo4j-admin import command dynamically
        command = [
            "neo4j-admin", "database", "import", "full", self.settings.neo4j_database,
            "--overwrite-destination=true",
        ]

        # Find node files
        node_files = list(self.transformed_csv_dir.glob("*_nodes_header.csv"))
        for header_file in node_files:
            # e.g., concepts_nodes_header.csv -> "Concept"
            label = header_file.name.split('_')[0].capitalize()
            data_file = header_file.name.replace('_header.csv', '_data.csv')
            command.append(f"--nodes={label}={header_file.resolve()},{self.transformed_csv_dir.joinpath(data_file).resolve()}")

        # Find relationship files
        rel_files = list(self.transformed_csv_dir.glob("*_rels_header.csv"))
        for header_file in rel_files:
            data_file = header_file.name.replace('_header.csv', '_data.csv')
            command.append(f"--relationships={header_file.resolve()},{self.transformed_csv_dir.joinpath(data_file).resolve()}")

        logger.info("The following command will be executed:")
        logger.info(" ".join(command))
        logger.warning("This requires the Neo4j database to be stopped. Please ensure it is stopped before proceeding.")
        # We can add a confirmation prompt here in a real CLI

        try:
            self._run_command(command)
            logger.info("Bulk import completed successfully.")
            logger.info("Please start the Neo4j database to continue.")
            # 3. Create schema constraints and meta node after import
            self.create_schema_and_meta_node()
        except subprocess.CalledProcessError as e:
            logger.error(f"Neo4j-admin import failed: {e}")
            logger.error("Please check the output from neo4j-admin for more details.")
            raise

    def create_schema_and_meta_node(self):
        """Creates DB constraints and the metadata node after a bulk import."""
        logger.info("Creating database constraints and metadata node...")
        with self.driver.session(database=self.settings.neo4j_database) as session:
            session.run("CREATE CONSTRAINT IF NOT EXISTS FOR (c:Concept) REQUIRE c.cui IS UNIQUE")
            session.run("CREATE CONSTRAINT IF NOT EXISTS FOR (c:Code) REQUIRE c.code_id IS UNIQUE")
            session.run(
                self.delta_strategy.generate_meta_node_update_query(),
                version=self.version
            )
        logger.info("Schema and metadata created successfully.")

    def _execute_apoc_iterate(self, query: str, rows: List[dict]):
        """Executes a given APOC iterate query with a list of data rows."""
        if not rows:
            logger.info("No rows to process, skipping query execution.")
            return

        logger.info(f"Executing APOC query on {len(rows)} rows...")
        with self.driver.session(database=self.settings.neo4j_database) as session:
            try:
                # We don't need to unpack the results, just run it.
                session.run(query, rows=rows, version=self.version)
            except Exception as e:
                logger.error(f"An error occurred during APOC execution: {e}")
                raise

    def _parse_change_file(self, filename: str) -> List[List[str]]:
        """A simple parser for DELETEDCUI.RRF and MERGEDCUI.RRF."""
        filepath = self.data_dir / self.version / filename
        if not filepath.exists():
            logger.warning(f"Change file not found: {filepath}. Skipping.")
            return []
        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.reader(f, delimiter='|')
            # The files have a trailing delimiter, so the last column is empty
            return [row[:-1] for row in reader]

    def run_incremental_sync(self):
        """
        Performs an incremental update using the "Snapshot Diff" strategy.
        This is a placeholder for the full logic which is highly complex.
        """
        logger.info("Starting incremental synchronization...")

        # 1. Process DELETEDCUI.RRF
        deleted_cuis = [{"cui": row[0]} for row in self._parse_change_file("DELETEDCUI.RRF")]
        if deleted_cuis:
            logger.info(f"Processing {len(deleted_cuis)} deleted CUIs...")
            query = self.delta_strategy.generate_deleted_cui_query()
            self._execute_apoc_iterate(query, deleted_cuis)

        # 2. Process MERGEDCUI.RRF
        merged_cuis = [{"old_cui": row[0], "new_cui": row[1]} for row in self._parse_change_file("MERGEDCUI.RRF")]
        if merged_cuis:
            logger.info(f"Processing {len(merged_cuis)} merged CUIs...")
            query = self.delta_strategy.generate_merged_cui_query()
            self._execute_apoc_iterate(query, merged_cuis)

        # 3. Apply new snapshot (this is the most complex part)
        # 3. Apply new snapshot by streaming from the transformer
        logger.info("Applying new snapshot data...")
        for entity_type, batch in self.transformer.stream_transformed_data():
            logger.info(f"Merging {len(batch)} entities of type '{entity_type}'...")
            query = ""
            if entity_type == "concepts":
                query = self.delta_strategy.generate_node_merge_query("Concept", "cui")
            elif entity_type == "codes":
                query = self.delta_strategy.generate_node_merge_query("Code", "code_id")
            elif entity_type == "concept_has_code_rels":
                query = self.delta_strategy.generate_relationship_merge_query(
                    "Concept", "cui", "Code", "code_id", "HAS_CODE", ""
                )
            elif entity_type == "concept_concept_rels":
                query = self.delta_strategy.generate_relationship_merge_query(
                    "Concept", "cui", "Concept", "cui", "type", "key" # `type` and `key` are properties in the streamed dict
                )

            if query:
                self._execute_apoc_iterate(query, batch)

        # 4. Cleanup stale entities
        logger.info("Cleaning up stale relationships...")
        stale_rel_query = self.delta_strategy.generate_stale_relationship_cleanup_query()
        self._execute_apoc_iterate(stale_rel_query, [])

        logger.info("Cleaning up stale nodes...")
        stale_node_query = self.delta_strategy.generate_stale_node_cleanup_query()
        self._execute_apoc_iterate(stale_node_query, [])

        # 5. Update metadata
        logger.info("Updating metadata node...")
        with self.driver.session(database=self.settings.neo4j_database) as session:
            session.run(
                self.delta_strategy.generate_meta_node_update_query(),
                version=self.version
            )

        logger.info("Incremental synchronization complete.")

    def run(self):
        """
        Main entry point to start the synchronization process.
        """
        if self.is_database_empty():
            self.run_full_import()
        else:
            self.run_incremental_sync()

    def close(self):
        """Closes the Neo4j driver connection."""
        if self.driver:
            self.driver.close()
            logger.info("Neo4j connection closed.")

if __name__ == '__main__':
    # settings = Settings()
    # loader = UmlsLoader(settings)
    # loader.run()
    # loader.close()
    pass
