"""
Command-Line Interface for pyNeoUmlsSyncer.

This module provides the main entry point for users to interact with the
UMLS synchronization tool, offering commands for initial bulk loading and
subsequent incremental updates.
"""

import typer
import logging
from pathlib import Path
import sys

# Add the src directory to the path to allow for absolute imports
sys.path.append(str(Path(__file__).resolve().parents[1]))

from pyNeoUmlsSyncer.config import settings
from pyNeoUmlsSyncer.downloader import UmlsDownloader
from pyNeoUmlsSyncer.parser import UmlsParser
from pyNeoUmlsSyncer.transformer import UmlsTransformer
from pyNeoUmlsSyncer.loader import Neo4jLoader
from pyNeoUmlsSyncer.delta_strategy import DeltaStrategy

# --- Configure Logging ---
# A more advanced application might use a file-based config (e.g., logging.conf)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)-18s - %(levelname)-7s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    stream=sys.stdout
)
# Quieten the chatty neo4j driver during info-level logging
logging.getLogger("neo4j").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

app = typer.Typer(
    help="A tool for creating and maintaining a UMLS Labeled Property Graph in Neo4j.",
    add_completion=False
)

@app.command(help="Perform a full initial bulk load using neo4j-admin.")
def initial_load(
    version: str = typer.Option(lambda: settings.umls_version, "--version", "-v", help="UMLS release version to load."),
    output_dir: Path = typer.Option(Path("./umls_import_files"), "--output-dir", "-o", help="Directory to save the generated CSV files for neo4j-admin.")
):
    """
    Orchestrates the entire ETL pipeline for a one-time bulk import.
    This generates CSV files and an executable import script.
    """
    typer.secho(f"Starting initial bulk load for UMLS version: {version}", fg=typer.colors.CYAN)
    settings.umls_version = version  # Override default setting if provided by user

    # 1. Download (mocked) and Extract
    downloader = UmlsDownloader()
    rrf_path = downloader.run()
    if not rrf_path:
        typer.secho("Failed to acquire UMLS data. Aborting.", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=1)

    # 2. Parse
    parser = UmlsParser(rrf_path)
    cui_terms = parser.get_cui_terms()
    cui_stys = parser.get_cui_semantic_types()
    cui_rels = parser.get_cui_relationships()

    # 3. Transform
    transformer = UmlsTransformer(version=version)
    concepts, codes, has_code_rels, concept_rels = transformer.transform_data(
        cui_terms, cui_stys, cui_rels
    )

    # 4. Generate CSVs
    loader = Neo4jLoader()
    try:
        loader.generate_bulk_import_files(
            output_dir=output_dir,
            concepts=concepts,
            codes=codes,
            has_code_rels=has_code_rels,
            concept_rels=concept_rels
        )
    finally:
        loader.close()

    typer.secho(f"\nSUCCESS: Bulk import files generated in '{output_dir}'.", fg=typer.colors.GREEN)
    typer.echo("\nTo complete the import, please follow these steps:")
    typer.echo(f"  1. Ensure your Neo4j database instance is STOPPED.")
    typer.echo(f"  2. From your project root, run the generated script: ./{output_dir}/import.sh")
    typer.echo(f"  3. Start your Neo4j database.")


@app.command(help="Perform an incremental update from a previous version using the 'Snapshot Diff' strategy.")
def incremental_update(
    version: str = typer.Option(lambda: settings.umls_version, "--version", "-v", help="The NEW UMLS release version to update to.")
):
    """
    Orchestrates the incremental update pipeline using APOC procedures.
    """
    typer.secho(f"Starting incremental update to UMLS version: {version}", fg=typer.colors.CYAN)
    settings.umls_version = version

    loader = Neo4jLoader()
    try:
        # Check current version in the database
        current_version = loader.get_current_umls_version()
        if current_version:
            typer.echo(f"Database is currently at version: {current_version}")
            if version <= current_version:
                typer.secho(f"Target version {version} is not newer than current version {current_version}. Aborting.", fg=typer.colors.YELLOW)
                raise typer.Exit(code=1)
        else:
            typer.secho("No existing UMLS version found in the database. Consider running `initial-load` first.", fg=typer.colors.YELLOW)

        # 1. Download and Extract new version
        downloader = UmlsDownloader()
        rrf_path = downloader.run()
        if not rrf_path:
            typer.secho("Failed to acquire UMLS data. Aborting.", fg=typer.colors.RED, err=True)
            raise typer.Exit(code=1)

        # 2. Parse new version's data
        parser = UmlsParser(rrf_path)
        cui_terms = parser.get_cui_terms()
        cui_stys = parser.get_cui_semantic_types()
        cui_rels = parser.get_cui_relationships()

        # 3. Transform data into graph entities
        transformer = UmlsTransformer(version=version)
        concepts, codes, has_code_rels, concept_rels = transformer.transform_data(
            cui_terms, cui_stys, cui_rels
        )

        # 4. Execute the delta strategy
        delta_strategy = DeltaStrategy(loader, rrf_path, version)
        delta_strategy.run_incremental_update(
            concepts=concepts,
            codes=codes,
            has_code_rels=has_code_rels,
            concept_rels=concept_rels
        )

    except Exception as e:
        logging.getLogger(__name__).critical(f"A critical error occurred during the incremental update: {e}", exc_info=True)
        typer.secho(f"ERROR: A critical error occurred. Check logs for details. Message: {e}", fg=typer.colors.RED)
        raise typer.Exit(code=1)
    finally:
        loader.close()

    typer.secho(f"\nSUCCESS: Incremental update to version {version} complete.", fg=typer.colors.GREEN)


if __name__ == "__main__":
    app()
