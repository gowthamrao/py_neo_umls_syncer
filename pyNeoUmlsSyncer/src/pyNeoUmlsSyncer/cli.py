"""
Command-line interface for pyNeoUmlsSyncer using Typer.
"""
import os
from pathlib import Path
import typer
from rich.console import Console

from . import config
from . import downloader
from . import parser
from . import transformer
from . import loader

app = typer.Typer(help="A tool to synchronize UMLS data with a Neo4j graph.")
console = Console()

@app.command()
def sync(
    version: str = typer.Option(..., "--version", "-v", help="The UMLS version to sync (e.g., '2025AA')."),
    neo4j_home: Path = typer.Option(
        ...,
        "--neo4j-home",
        help="Path to the Neo4j installation directory (NEO4J_HOME).",
        exists=True,
        file_okay=False,
        dir_okay=True,
        readable=True
    ),
    force_bulk: bool = typer.Option(
        False,
        "--force-bulk",
        help="Force a full bulk import, wiping existing data."
    ),
    download_dir: Path = typer.Option(
        Path("./umls_download"),
        help="Directory to store downloaded UMLS files."
    ),
    csv_dir: Path = typer.Option(
        Path("./neo4j_import"),
        help="Directory to store CSV files for bulk import."
    )
):
    """
    Download, parse, and load a UMLS release into a Neo4j database.
    """
    console.print(f"[bold green]Starting pyNeoUmlsSyncer for version {version}...[/bold green]")

    # --- Configuration ---
    # Load config and override version from CLI
    try:
        app_config = config.AppConfig(umls_version=version)
        api_key = app_config.credentials.api_key.get_secret_value()
        if not api_key:
            raise ValueError("UMLS_API_KEY not found in environment.")
    except Exception as e:
        console.print(f"[bold red]Configuration Error:[/bold red] {e}")
        raise typer.Exit(code=1)

    # --- 1. Download ---
    console.print("\n[bold]Step 1: Downloading UMLS Data...[/bold]")
    try:
        umls_downloader = downloader.UmlsDownloader(version, api_key, download_dir)
        umls_files_dir = umls_downloader.execute()
    except Exception as e:
        console.print(f"[bold red]Download failed:[/bold red] {e}")
        raise typer.Exit(code=1)

    # --- 2. Parse ---
    console.print("\n[bold]Step 2: Parsing RRF files...[/bold]")
    try:
        max_workers = app_config.optimization.max_parallel_processes
        parsed_data = parser.parse_umls_files(umls_files_dir, app_config, max_workers)
    except Exception as e:
        console.print(f"[bold red]Parsing failed:[/bold red] {e}")
        raise typer.Exit(code=1)

    # --- 3. Transform & Load ---
    console.print("\n[bold]Step 3: Loading data into Neo4j...[/bold]")
    driver = loader._get_driver(app_config)
    current_version = loader.get_current_db_version(driver)
    driver.close()

    is_initial_load = force_bulk or not current_version

    try:
        if is_initial_load:
            console.print("Performing initial bulk load.")
            console.print("Transforming data to CSV...")
            transformer.transform_to_csv(parsed_data, csv_dir)
            loader.bulk_load(app_config, csv_dir, neo4j_home)
        else:
            console.print(f"Performing incremental load from {current_version} to {version}.")
            inc_driver = loader._get_driver(app_config)
            loader.incremental_load(inc_driver, app_config, parsed_data, umls_files_dir)
            inc_driver.close()

    except Exception as e:
        console.print(f"[bold red]Loading failed:[/bold red] {e}")
        raise typer.Exit(code=1)

    console.print("\n[bold green]Synchronization complete![/bold green]")


if __name__ == "__main__":
    app()
