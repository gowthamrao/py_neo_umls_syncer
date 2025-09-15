# Jules was here
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

from typing import Optional

class Neo4jLoader:
    """
    Orchestrates the loading of UMLS data into Neo4j, supporting both
    initial bulk import and incremental synchronization.
    """

    def __init__(self, driver: Optional[Driver] = None):
        from .downloader import UMLSDownloader
        self._driver = driver
        self.downloader = UMLSDownloader(
            api_key=settings.umls_api_key,
            download_dir=settings.download_dir
        )

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
            title="[bold yellow]Step 1: Run the neo4j-admin Bulk Import Command[/bold yellow]",
            border_style="yellow",
            padding=(1, 2)
        ))

        console.print("\n[bold red]IMPORTANT:[/] The target Neo4j database must be [bold]stopped[/bold] before running this command.", highlight=False)
        console.print(f"  Example: `neo4j stop -d {settings.neo4j_database}`")
        console.print("\n[bold yellow]After the import is complete, restart your database.[/bold yellow]")
        console.print(f"  Example: `neo4j start -d {settings.neo4j_database}`")
        console.print("\nThen, run the following command to set up constraints and metadata:")
        console.print(f"  [bold cyan]py-neo-umls-syncer init-meta --version {version}[/bold cyan]")
        console.print("\n[green]Bulk import files and command generated successfully.[/green]")

    def update_meta_node_after_bulk(self, version: str):
        """
        Connects to the database and creates the UMLS_Meta node.
        This is intended to be called after a bulk import is complete and the DB is running.
        """
        if not self._driver:
            raise ValueError("A Neo4j driver is required for this operation.")
        console.log("Attempting to connect to the database to set the metadata version...")
        strategy = DeltaStrategy(self._driver, version, Path(settings.neo4j_import_dir))
        strategy.ensure_constraints()
        strategy.update_meta_node()

    def run_incremental_sync(self, meta_dir: Path, version: str):
        """
        Orchestrates the 'Snapshot Diff' synchronization with a new UMLS version.
        """
        if not self._driver:
            raise ValueError("A Neo4j driver is required for this operation.")
        console.log(f"Starting incremental sync for version: [bold cyan]{version}[/bold cyan]")
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

        strategy = DeltaStrategy(self._driver, version, import_dir)

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
