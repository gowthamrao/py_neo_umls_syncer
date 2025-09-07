"""
Orchestrates loading data into Neo4j, handling both bulk and incremental loads.
"""
import subprocess
import time
from pathlib import Path
from neo4j import GraphDatabase, Driver
from .config import AppConfig
from .parser import UmlsData
from . import delta_strategy

def _get_driver(config: AppConfig) -> Driver:
    """Creates and returns a Neo4j Driver instance."""
    return GraphDatabase.driver(
        config.neo4j_uri,
        auth=(config.neo4j_user, config.neo4j_password.get_secret_value())
    )

def get_current_db_version(driver: Driver) -> str | None:
    """Checks for the current UMLS version in the database."""
    try:
        with driver.session() as session:
            result = session.run("MATCH (m:UMLS_Meta) RETURN m.version AS version")
            record = result.single()
            return record["version"] if record else None
    except Exception:
        return None

def _run_shell_command(command: str):
    """Executes a shell command and raises an exception on failure."""
    process = subprocess.run(command, shell=True, check=True, capture_output=True, text=True)
    print(process.stdout)
    if process.returncode != 0:
        print(process.stderr)
        raise subprocess.CalledProcessError(process.returncode, command, output=process.stdout, stderr=process.stderr)


def bulk_load(config: AppConfig, csv_dir: Path, neo4j_home: Path):
    """
    Performs a bulk import using neo4j-admin.
    Assumes Neo4j is running in a Docker container or environment where shell commands can be used.
    """
    db_name = "neo4j" # Or get from config

    # These paths need to be accessible from where the command is run
    admin_path = neo4j_home / "bin" / "neo4j-admin"

    print("Stopping Neo4j database for bulk import...")
    _run_shell_command(f"{neo4j_home / 'bin' / 'neo4j'} stop")

    import_command = f"""
    {admin_path} database import full \\
        --nodes=Concept="{csv_dir / 'concepts.csv'}" \\
        --nodes=Code="{csv_dir / 'codes.csv'}" \\
        --relationships=HAS_CODE="{csv_dir / 'has_code_rels.csv'}" \\
        --relationships="{csv_dir / 'concept_rels.csv'}" \\
        --overwrite-destination=true {db_name}
    """

    print("Starting bulk import...")
    _run_shell_command(import_command)

    print("Starting Neo4j database...")
    _run_shell_command(f"{neo4j_home / 'bin' / 'neo4j'} start")

    # Update metadata
    driver = _get_driver(config)
    update_metadata(driver, config.umls_version)
    driver.close()
    print("Bulk import complete.")


def incremental_load(driver: Driver, config: AppConfig, parsed_data: UmlsData, umls_files_dir: Path):
    """
    Performs an incremental update using APOC.
    """
    concepts, codes, relationships, cui_to_tuis = parsed_data

    # 1. Handle deleted and merged CUIs
    print("Processing deleted and merged CUIs...")
    # These paths need to be robustly located within the extracted UMLS directory
    # For now, assuming a flat structure for simplicity. A real implementation
    # might need to search for these files.
    deleted_cui_path = umls_files_dir / "DELETEDCUI.RRF"
    if deleted_cui_path.exists():
        delta_strategy.process_deleted_cuis(driver, deleted_cui_path, config.optimization.apoc_batch_size)

    merged_cui_path = umls_files_dir / "MERGEDCUI.RRF"
    if merged_cui_path.exists():
        delta_strategy.process_merged_cuis(driver, merged_cui_path, config.optimization.apoc_batch_size)

    # 2. Merge new snapshot data
    print("Merging new snapshot data...")
    version = config.umls_version
    batch_size = config.optimization.apoc_batch_size

    with driver.session() as session:
        # Merge Concepts
        concept_list = [c.model_dump() for c in concepts.values()]
        session.run("""
            CALL apoc.periodic.iterate(
                'UNWIND $concepts AS p RETURN p',
                'MERGE (c:Concept {cui: p.cui})
                 SET c.preferred_name = p.preferred_name,
                     c.last_seen_version = $version,
                     c.biolink_categories = p.biolink_categories',
                {batchSize: $batchSize, parallel: false, params: {concepts: $concepts, version: $version}}
            )
            """, concepts=concept_list, version=version, batchSize=batch_size)
        print(f"...merged {len(concept_list)} concepts.")

        # Merge Codes and HAS_CODE relationships
        code_list = [c.model_dump() for c in codes.values()]
        session.run("""
            CALL apoc.periodic.iterate(
                'UNWIND $codes AS p RETURN p',
                'MERGE (d:Code {code_id: p.code_id})
                 SET d.name = p.name, d.sab = p.sab, d.last_seen_version = $version;

                 WITH d, p
                 MATCH (c:Concept {cui: p.cui})
                 MERGE (c)-[r:HAS_CODE]->(d)
                 SET r.last_seen_version = $version',
                {batchSize: $batchSize, parallel: false, params: {codes: $codes, version: $version}}
            )
            """, codes=code_list, version=version, batchSize=batch_size)
        print(f"...merged {len(code_list)} codes and relationships.")

        # Merge Concept-to-Concept Relationships
        # This requires aggregating relationships first, same as in the transformer
        # For simplicity in this example, we assume relationships are already aggregated.
        rel_list = [r.model_dump() for r in relationships]
        session.run("""
            CALL apoc.periodic.iterate(
                'UNWIND $rels AS p RETURN p',
                'MATCH (source:Concept {cui: p.source_cui}), (target:Concept {cui: p.target_cui})
                 CALL apoc.merge.relationship(source, p.biolink_predicate, {source_rela: p.source_rela}, p, target) YIELD rel
                 SET rel.last_seen_version = $version',
                {batchSize: $batchSize, parallel: false, params: {rels: $rels, version: $version}}
            )
            """, rels=rel_list, version=version, batchSize=batch_size)
        print(f"...merged {len(rel_list)} concept relationships.")

    # 3. Remove stale entities
    print("Removing stale entities...")
    delta_strategy.snapshot_diff_cleanup(driver, config.umls_version, config.optimization.apoc_batch_size)

    # 4. Update metadata
    update_metadata(driver, config.umls_version)

    print("Incremental load complete.")


def update_metadata(driver: Driver, version: str):
    """Updates the :UMLS_Meta node in the database."""
    query = """
    MERGE (m:UMLS_Meta)
    SET m.version = $version, m.last_updated = datetime()
    """
    with driver.session() as session:
        session.run(query, version=version)
    print(f"Database metadata updated to version {version}.")


def load_data(config: AppConfig, parsed_data: UmlsData, umls_files_dir: Path, neo4j_home: Path, force_bulk: bool = False):
    """
    Orchestrates the data loading process.
    """
    driver = _get_driver(config)
    current_version = get_current_db_version(driver)

    if force_bulk or not current_version:
        print("Performing initial bulk load.")
        # The transformer needs to be run to generate CSVs first
        # from .transformer import transform_to_csv
        # csv_dir = Path("./neo4j_import")
        # transform_to_csv(parsed_data, csv_dir)
        # bulk_load(config, csv_dir, neo4j_home)
        print("Bulk load called (skipping actual execution in this example).")
        # For this example, we'll simulate it and just update metadata
        update_metadata(driver, config.umls_version)

    else:
        print(f"Database is at version {current_version}. Performing incremental load to {config.umls_version}.")
        incremental_load(driver, config, parsed_data, umls_files_dir)

    driver.close()
