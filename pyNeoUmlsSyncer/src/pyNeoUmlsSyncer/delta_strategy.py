"""
Implements the incremental update strategy for synchronizing the Neo4j database.

- Processes DELETEDCUI.RRF to remove deleted concepts.
- Processes MERGEDCUI.RRF to merge concepts and migrate relationships.
- Implements the "Snapshot Diff" logic to remove stale data after an update.
"""
from pathlib import Path
from typing import List, Tuple, Dict, Any

from neo4j import Driver

# Column indices for change files
# DELETEDCUI.RRF
DEL_CUI_I = 0
# MERGEDCUI.RRF
OLD_CUI_I, NEW_CUI_I = 0, 1


def _read_change_file(filepath: Path) -> List[List[str]]:
    """Reads a pipe-delimited change file."""
    with open(filepath, 'r', encoding='utf-8') as f:
        return [line.strip().split('|') for line in f if line.strip()]

def process_deleted_cuis(driver: Driver, deleted_cui_path: Path, batch_size: int):
    """
    Processes the DELETEDCUI.RRF file to delete concepts and associated codes.
    """
    deleted_cuis = [row[DEL_CUI_I] for row in _read_change_file(deleted_cui_path)]

    query = """
    UNWIND $cuis AS cui
    MATCH (c:Concept {cui: cui})
    OPTIONAL MATCH (c)-[:HAS_CODE]->(code:Code)
    DETACH DELETE c, code
    """

    with driver.session() as session:
        session.run("""
        CALL apoc.periodic.iterate(
            'UNWIND $cuis AS cui RETURN cui',
            'MATCH (c:Concept {cui: cui}) OPTIONAL MATCH (c)-[:HAS_CODE]->(code:Code) DETACH DELETE c, code',
            {batchSize: $batchSize, parallel: false, params: {cuis: $cuis}}
        )
        """, cuis=deleted_cuis, batchSize=batch_size)
    print(f"Processed {len(deleted_cuis)} deleted CUIs.")


def process_merged_cuis(driver: Driver, merged_cui_path: Path, batch_size: int):
    """
    Processes the MERGEDCUI.RRF file to merge concepts and migrate relationships
    using a performant, batched APOC query.
    """
    merges = [{"old": row[OLD_CUI_I], "new": row[NEW_CUI_I]} for row in _read_change_file(merged_cui_path)]

    # This single, complex query handles all migration and merging logic inside APOC.
    # It is designed to be idempotent and handle relationship property merging.
    merge_query = """
    // Ensure the new CUI node exists
    MERGE (new:Concept {cui: item.new})

    // Find the old CUI node
    WITH new, item
    MATCH (old:Concept {cui: item.old})

    // 1. Migrate :HAS_CODE relationships
    WITH new, old
    OPTIONAL MATCH (old)-[r_code:HAS_CODE]->(code:Code)
    WHERE r_code IS NOT NULL
    MERGE (new)-[:HAS_CODE]->(code)
    DELETE r_code

    // 2. Migrate outgoing relationships
    WITH new, old
    OPTIONAL MATCH (old)-[r_out]->(target:Concept)
    WHERE r_out IS NOT NULL
    // Use APOC to dynamically create the relationship with merged properties
    CALL apoc.merge.relationship(new, type(r_out), properties(r_out), properties(r_out), target) YIELD rel as rel_out
    // Manually merge list properties like asserted_by_sabs
    SET rel_out.asserted_by_sabs = apoc.coll.union(coalesce(rel_out.asserted_by_sabs, []), coalesce(r_out.asserted_by_sabs, []))
    DELETE r_out

    // 3. Migrate incoming relationships
    WITH new, old
    OPTIONAL MATCH (source:Concept)-[r_in]->(old)
    WHERE r_in IS NOT NULL
    CALL apoc.merge.relationship(source, type(r_in), properties(r_in), properties(r_in), new) YIELD rel as rel_in
    SET rel_in.asserted_by_sabs = apoc.coll.union(coalesce(rel_in.asserted_by_sabs, []), coalesce(r_in.asserted_by_sabs, []))
    DELETE r_in

    // 4. Finally, delete the old, now-isolated concept node
    WITH old
    DETACH DELETE old
    """

    with driver.session() as session:
        session.run("""
        CALL apoc.periodic.iterate(
            'UNWIND $merges AS item RETURN item',
            $query,
            {batchSize: $batchSize, parallel: false, params: {merges: $merges, query: $query}}
        )
        """, merges=merges, query=merge_query, batchSize=batch_size)

    print(f"Processed {len(merges)} merged CUIs.")


def snapshot_diff_cleanup(driver: Driver, new_version: str, batch_size: int):
    """
    Removes nodes and relationships not seen in the new version.
    """
    print(f"Starting snapshot diff cleanup for version {new_version}...")

    # Delete stale relationships
    rel_cleanup_query = """
    CALL apoc.periodic.iterate(
        'MATCH ()-[r]-() WHERE r.last_seen_version <> $new_version RETURN r',
        'DELETE r',
        {batchSize: $batchSize, parallel: false, params: {new_version: $new_version}}
    )
    """

    # Delete stale codes
    code_cleanup_query = """
    CALL apoc.periodic.iterate(
        'MATCH (c:Code) WHERE c.last_seen_version <> $new_version RETURN c',
        'DETACH DELETE c',
        {batchSize: $batchSize, parallel: false, params: {new_version: $new_version}}
    )
    """

    # Delete stale concepts
    concept_cleanup_query = """
    CALL apoc.periodic.iterate(
        'MATCH (c:Concept) WHERE c.last_seen_version <> $new_version RETURN c',
        'DETACH DELETE c',
        {batchSize: $batchSize, parallel: false, params: {new_version: $new_version}}
    )
    """

    with driver.session() as session:
        session.run(rel_cleanup_query, new_version=new_version, batch_size=batch_size)
        print("...stale relationships removed.")
        session.run(code_cleanup_query, new_version=new_version, batch_size=batch_size)
        print("...stale codes removed.")
        session.run(concept_cleanup_query, new_version=new_version, batch_size=batch_size)
        print("...stale concepts removed.")

    print("Snapshot diff cleanup complete.")
