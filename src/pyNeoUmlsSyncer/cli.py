from pathlib import Path
import typer
from rich.console import Console
from rich.panel import Panel

# We wrap the settings import in a try-except block to provide a nicer
# error message if the required environment variables are not set.
try:
    from .config import settings
except Exception as e:
    console = Console()
    console.print(Panel(
        f"[bold red]Configuration Error:[/bold red]\n{e}\n\nPlease ensure you have a .env file or have set the required environment variables, such as [bold cyan]PYNEOUMLSSYNCER_UMLS_API_KEY[/bold cyan].",
        title="[bold red]Initialization Failed[/bold red]",
        border_style="red"
    ))
    exit(1)

from .downloader import download_umls_if_needed
from .loader import Neo4jLoader


app = typer.Typer(
    name="py-neo-umls-syncer",
    help="A production-ready Python package for creating and maintaining a UMLS Labeled Property Graph in Neo4j."
)
console = Console()

@app.command(name="full-import", help="Perform a one-time bulk import to create a new Neo4j database from UMLS.")
def full_import(
    version: str = typer.Option(
        ...,
        "--version",
        "-v",
        help="The version of the UMLS release being imported (e.g., '2025AA'). This is mandatory for future incremental updates."
    )
):
    """
    Orchestrates the entire initial bulk import process:
    1. Downloads the latest UMLS release (if not already present).
    2. Parses the RRF files in parallel.
    3. Transforms the data into CSVs, tagging with the version.
    4. Generates the `neo4j-admin` command for the user to run.
    """
    console.print(Panel(f"[bold cyan]Starting Full UMLS Bulk Import Process for Version: {version}[/bold cyan]", border_style="cyan"))

    try:
        # Step 1: Download UMLS data
        meta_dir = download_umls_if_needed()

        # Step 2: Orchestrate the import command generation
        loader = Neo4jLoader()
        loader.run_bulk_import(meta_dir, version)
        loader.update_meta_node_after_bulk(version)

    except Exception as e:
        console.print_exception()
        console.print(Panel(f"[bold red]An error occurred during the bulk import process: {e}", title="[bold red]Error[/bold red]"))
        raise typer.Exit(code=1)

@app.command(name="incremental-sync", help="Synchronize an existing DB with a new UMLS version.")
def incremental_sync(
    version: str = typer.Option(
        ...,
        "--version",
        "-v",
        help="The version of the new UMLS release (e.g., '2025AA'). This is crucial for tagging data."
    )
):
    """
    Orchestrates the 'Snapshot Diff' synchronization process:
    1. Downloads the latest UMLS release.
    2. Generates a new snapshot (CSVs).
    3. Applies deletions and merges from UMLS change files.
    4. Merges the new data into the graph.
    5. Removes any data not present in the new snapshot.
    6. Updates the graph version.
    """
    console.print(Panel(f"[bold cyan]Starting Incremental Sync to Version: {version}[/bold cyan]", border_style="cyan"))

    try:
        # Step 1: Download UMLS data
        meta_dir = download_umls_if_needed()

        # Step 2: Orchestrate the incremental sync
        loader = Neo4jLoader()
        loader.run_incremental_sync(meta_dir, version)

    except Exception as e:
        console.print_exception()
        console.print(Panel(f"[bold red]An error occurred during the incremental sync process: {e}", title="[bold red]Error[/bold red]"))
        raise typer.Exit(code=1)

if __name__ == "__main__":
    app()
