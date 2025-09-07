from pathlib import Path
import subprocess
from neo4j import GraphDatabase, Driver
from rich.console import Console
from rich.panel import Panel
from rich.syntax import Syntax

from .config import settings
from .parser import RRFParser
from .transformer import CSVTransformer
from .delta_strategy import DeltaStrategy

console = Console()

class Neo4jLoader:
    """
    Orchestrates the loading of UMLS data into Neo4j, supporting both
    initial bulk import and incremental synchronization.
    """

    def __init__(self):
        self._driver = None

    def _get_driver(self) -> Driver:
        """Initializes and returns a Neo4j driver instance."""
        if self._driver is None or not self._driver.closed():
             self._driver = GraphDatabase.driver(
                settings.neo4j_uri,
                auth=(settings.neo4j_user, settings.neo4j_password)
            )
        return self._driver

    def close(self):
        """Closes the Neo4j driver connection."""
        if self._driver and not self._driver.closed():
            self._driver.close()
            console.log("Neo4j driver connection closed.")

    def run_bulk_import(self, meta_dir: Path, version: str):
        """
        Parses and transforms UMLS data into CSVs and generates the
        neo4j-admin import command for the user to execute.
        """
        console.log("Starting bulk import process...")
        import_dir = Path(settings.neo4j_import_dir)

        # Step 1: Parse RRF files
        parser = RRFParser(meta_dir)
        concepts, codes, concept_to_code_rels, inter_concept_rels, sty_map = parser.parse_files()

        # Step 2: Transform parsed data into CSVs in the Neo4j import directory
        transformer = CSVTransformer(import_dir)
        transformer.transform_to_csvs(
            concepts, codes, concept_to_code_rels, inter_concept_rels, sty_map, version
        )

        # Step 3: Generate the neo4j-admin command
        # The command uses relative paths (just filenames) because neo4j-admin
        # automatically looks inside the configured import directory.
        command = f"""
neo4j-admin database import full \\
    --nodes=Concept:Concept-ID="nodes_concepts.csv" \\
    --nodes=Code:Code-ID="nodes_codes.csv" \\
    --relationships=HAS_CODE="rels_has_code.csv" \\
    --relationships="rels_inter_concept.csv" \\
    --overwrite-destination=true \\
    {settings.neo4j_database}
        """

        console.print(Panel.fit(
            Syntax(command, "bash", theme="monokai", line_numbers=True),
            title="[bold yellow]neo4j-admin Bulk Import Command[/bold yellow]",
            border_style="yellow",
            padding=(1, 2)
        ))

        console.print("\n[bold red]IMPORTANT:[/] The target Neo4j database must be stopped before running this command.")
        console.print(f"Example: `neo4j stop -d {settings.neo4j_database}`")
        console.print("After starting the database, the metadata node will be created.")
        console.print("[green]Bulk import command generated successfully.[/green]")

    def update_meta_node_after_bulk(self, version: str):
        """
        Connects to the database and creates the UMLS_Meta node.
        This is intended to be called after a bulk import is complete and the DB is running.
        """
        console.log("Attempting to connect to the database to set the metadata version...")
        driver = self._get_driver()
        try:
            strategy = DeltaStrategy(driver, version, Path(settings.neo4j_import_dir))
            strategy.ensure_constraints()
            strategy.update_meta_node()
        finally:
            self.close()

    def run_incremental_sync(self, meta_dir: Path, version: str):
        """
        Orchestrates the 'Snapshot Diff' synchronization with a new UMLS version.
        """
        console.log(f"Starting incremental sync for version: [bold cyan]{version}[/bold cyan]")
        driver = self._get_driver()
        import_dir = Path(settings.neo4j_import_dir)

        # It's more efficient to regenerate CSVs for the new version than to hold it all in memory.
        console.log("Generating new snapshot from RRF files...")
        parser = RRFParser(meta_dir)
        concepts, codes, concept_to_code_rels, inter_concept_rels, sty_map = parser.parse_files()
        transformer = CSVTransformer(import_dir)
        transformer.transform_to_csvs(
            concepts, codes, concept_to_code_rels, inter_concept_rels, sty_map, version
        )
        console.log("[green]New snapshot generated successfully.[/green]")

        strategy = DeltaStrategy(driver, version, import_dir)

        try:
            # 1. Ensure constraints are in place
            strategy.ensure_constraints()

            # 2. Process change files
            # Note: These files are typically in the main META directory
            strategy.process_deleted_cuis(meta_dir / "DELETEDCUI.RRF")
            strategy.process_merged_cuis(meta_dir / "MERGEDCUI.RRF")

            # 3. Apply additions and updates from the new snapshot
            strategy.apply_additions_and_updates()

            # 4. Remove stale entities not seen in this version
            strategy.remove_stale_entities()

            # 5. Update the metadata node to lock in the new version
            strategy.update_meta_node()

            console.print(Panel(
                f"[bold green]Incremental sync to version {version} completed successfully![/bold green]",
                title="[bold green]Sync Complete[/bold green]"
            ))

        except Exception as e:
            console.print_exception()
            console.print(Panel(
                f"[bold red]An error occurred during the incremental sync: {e}[/bold red]",
                title="[bold red]Sync Failed[/bold red]"
            ))
        finally:
            self.close()
