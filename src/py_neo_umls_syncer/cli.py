# Copyright (c) 2025-2026 Gowtham Adamane Rao. All Rights Reserved.
#
# Licensed under the Prosperity Public License 3.0.0 (the "License").
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License in the LICENSE file at the root
# of this repository, or at: https://prosperitylicense.com/versions/3.0.0
#
# Commercial use beyond a 30-day trial requires a separate license.
# Jules was here
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

@app.command(name="full-import", help="Generate CSVs and command for a one-time bulk import.")
def full_import(
    version: str = typer.Option(
        ...,
        "--version",
        "-v",
        help="The version of the UMLS release being imported (e.g., '2025AA'). This is mandatory for future incremental updates."
    )
):
    """
    Generates the CSV files and the neo4j-admin command for a bulk import.
    This is the first step in populating a new database. After running this,
    you must manually run the generated command and then run `init-meta`.
    """
    console.print(Panel(f"[bold cyan]Starting Full UMLS Bulk Import Process for Version: {version}[/bold cyan]", border_style="cyan"))

    try:
        # Step 1: Download UMLS data
        meta_dir = download_umls_if_needed(version)

        # Step 2: Orchestrate the import command generation
        loader = Neo4jLoader()
        loader.run_bulk_import(meta_dir, version)

    except Exception as e:
        console.print_exception()
        console.print(Panel(f"[bold red]An error occurred during the bulk import process: {e}", title="[bold red]Error[/bold red]"))
        raise typer.Exit(code=1)


from neo4j import GraphDatabase, Driver

@app.command(name="init-meta", help="Initialize metadata after a successful bulk import.")
def init_meta(
    version: str = typer.Option(
        ...,
        "--version",
        "-v",
        help="The version of the UMLS release that was just imported (e.g., '2025AA')."
    )
):
    """
    Connects to a database that has been populated via bulk import and:
    1. Creates necessary constraints for Concepts and Codes.
    2. Creates the :UMLS_Meta node to lock in the current version.
    """
    console.print(Panel(f"[bold cyan]Initializing metadata for version: {version}[/bold cyan]", border_style="cyan"))
    driver = None
    try:
        driver = GraphDatabase.driver(settings.neo4j_uri, auth=(settings.neo4j_user, settings.neo4j_password))
        loader = Neo4jLoader(driver=driver)
        loader.update_meta_node_after_bulk(version)
        console.print(Panel(
            f"[bold green]Successfully initialized metadata for version {version}. The database is now ready for incremental syncs.[/bold green]",
            title="[bold green]Metadata Initialized[/bold green]"
        ))
    except Exception as e:
        console.print_exception()
        console.print(Panel(f"[bold red]Failed to initialize metadata: {e}", title="[bold red]Error[/bold red]"))
        raise typer.Exit(code=1)
    finally:
        if driver:
            driver.close()

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
    driver = None
    try:
        # Step 1: Download UMLS data
        meta_dir = download_umls_if_needed(version)

        # Step 2: Orchestrate the incremental sync
        driver = GraphDatabase.driver(settings.neo4j_uri, auth=(settings.neo4j_user, settings.neo4j_password))

        # Pre-flight check: ensure the database has been initialized
        with driver.session(database=settings.neo4j_database) as session:
            meta_node = session.run("MATCH (m:UMLS_Meta) RETURN m").single()
            if not meta_node:
                console.print("[bold red]Error: UMLS_Meta node not found.[/bold red]")
                console.print("Please run `full-import` and `init-meta` first.")
                raise typer.Exit(code=1)

        loader = Neo4jLoader(driver=driver)
        loader.run_incremental_sync(meta_dir, version)

    except Exception as e:
        console.print_exception()
        console.print(Panel(f"[bold red]An error occurred during the incremental sync process: {e}", title="[bold red]Error[/bold red]"))
        raise typer.Exit(code=1)
    finally:
        if driver:
            driver.close()

if __name__ == "__main__":
    app()
