"""
cli.py

This module provides the command-line interface for the pyNeoUmlsSyncer package,
powered by Typer.
"""
import typer
from rich.console import Console
from rich.panel import Panel

from .config import Settings, pydantic
from .loader import UmlsLoader

# Create a Typer application instance
app = typer.Typer(
    name="py-neo-umls-syncer",
    help="A production-ready Python package for creating and maintaining a UMLS Labeled Property Graph in Neo4j.",
    add_completion=False
)

# Create a rich Console for beautiful output
console = Console()

def load_settings() -> Settings | None:
    """Loads settings and validates required fields."""
    try:
        settings = Settings()
        return settings
    except pydantic.ValidationError as e:
        console.print(Panel(
            "[bold red]Configuration Error![/bold red]\n\n"
            "Could not load settings. Please check your .env file or environment variables.\n\n"
            f"[yellow]Details:[/yellow]\n{e}",
            title="Error",
            border_style="red"
        ))
        return None

@app.command(
    name="sync",
    help="Run the main synchronization process (full import or incremental)."
)
def sync(
    force_full: bool = typer.Option(
        False,
        "--force-full",
        help="Force a full bulk import, even if the database is not empty. Warning: This is destructive."
    )
):
    """
    Orchestrates the download, parsing, transformation, and loading of UMLS data
    into a Neo4j database.
    """
    console.print(Panel(
        "[bold cyan]pyNeoUmlsSyncer[/bold cyan] - UMLS to Neo4j Synchronizer",
        expand=False
    ))

    settings = load_settings()
    if not settings:
        raise typer.Exit(code=1)

    console.print(f"[info]Loaded configuration for UMLS version [bold]{settings.umls_version}[/bold].")

    loader = None
    try:
        loader = UmlsLoader(settings)

        if force_full:
            console.print("[warning]--force-full flag detected. Preparing for a destructive full import.[/warning]")
            # In a real scenario, you might add a confirmation prompt here.
            loader.run_full_import()
        else:
            # The loader's run() method automatically detects whether to run full or incremental.
            loader.run()

        console.print(Panel(
            "[bold green]Synchronization process completed successfully![/bold green]",
            title="Success",
            border_style="green"
        ))

    except Exception as e:
        console.print(Panel(
            f"[bold red]An unexpected error occurred:[/bold red]\n\n{e}",
            title="Fatal Error",
            border_style="red"
        ))
        raise typer.Exit(code=1)
    finally:
        if loader:
            loader.close()

@app.command(
    name="download",
    help="Only download and extract the UMLS release files."
)
def download():
    """
    Runs only the download and extraction part of the process.
    """
    console.print("[info]Starting UMLS download and extraction only...[/info]")
    settings = load_settings()
    if not settings:
        raise typer.Exit(code=1)

    from .downloader import UmlsDownloader
    try:
        downloader = UmlsDownloader(settings)
        downloader.download_and_extract_release()
        console.print("[bold green]Download and extraction complete.[/bold green]")
    except Exception as e:
        console.print(Panel(
            f"[bold red]An error occurred during download:[/bold red]\n\n{e}",
            title="Download Error",
            border_style="red"
        ))
        raise typer.Exit(code=1)


if __name__ == "__main__":
    app()
