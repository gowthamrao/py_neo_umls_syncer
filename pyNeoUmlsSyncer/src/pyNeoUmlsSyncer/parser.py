"""
Handles the multiprocessing-based parsing of UMLS RRF files.

- Parses MRCONSO.RRF for concept and code information.
- Parses MRREL.RRF for relationships between concepts.
- Parses MRSTY.RRF for semantic type (TUI) assignments.
- Uses multiprocessing to accelerate the parsing of large files.
- Implements logic for preferred name selection based on SAB priority.
"""

import multiprocessing
import os
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Set, Tuple, Any, Optional

from .config import AppConfig
from .models import Concept, Code, ConceptRelationship

# Define data structures for parsed data
UmlsData = Tuple[
    Dict[str, Concept],
    Dict[str, Code],
    List[ConceptRelationship],
    Dict[str, Set[str]] # CUI -> Set[TUI]
]

# Column indices for RRF files
# MRCONSO.RRF
CUI_I, LAT_I, TS_I, LUI_I, STT_I, SUI_I, ISPREF_I, AUI_I, SAUI_I, SCUI_I, SDUI_I, SAB_I, TTY_I, CODE_I, STR_I, SRL_I, SUPPRESS_I, CVF_I = range(18)
# MRREL.RRF
CUI1_I, AUI1_I, STYPE1_I, REL_I, CUI2_I, AUI2_I, STYPE2_I, RELA_I, RUI_I, SRUI_I, SAB_REL_I, SL_I, RG_I, DIR_I, SUPPRESS_REL_I, CVF_REL_I = range(16)
# MRSTY.RRF
CUI_STY_I, TUI_I, STN_I, STY_I, ATUI_I, CVF_STY_I = range(6)


def _get_rrf_path(umls_version_dir: Path, filename: str) -> Path:
    """Finds the path to a specific RRF file within the UMLS release."""
    # The RRF files are usually in a subdirectory like '2025AA/2025AA/META' or '2025AA/META'
    meta_path = umls_version_dir / "META"
    if not meta_path.exists():
         meta_path = umls_version_dir / umls_version_dir.name / "META"

    if not meta_path.exists():
        raise FileNotFoundError(f"Could not find META directory in {umls_version_dir}")

    rrf_file = meta_path / filename
    if not rrf_file.exists():
        raise FileNotFoundError(f"File not found: {rrf_file}")
    return rrf_file

def _parse_mrsty_chunk(chunk: List[str]) -> Dict[str, Set[str]]:
    """Parses a chunk of the MRSTY.RRF file."""
    cui_to_tuis = defaultdict(set)
    for line in chunk:
        fields = line.strip().split('|')
        cui = fields[CUI_STY_I]
        tui = fields[TUI_I]
        cui_to_tuis[cui].add(tui)
    return cui_to_tuis

def _parse_mrconso_chunk(chunk: List[str], config: AppConfig) -> Tuple[defaultdict, List]:
    """
    Parses a chunk of the MRCONSO.RRF file and groups atoms by CUI.
    This is the target for multiprocessing.
    """
    cui_groups = defaultdict(list)
    all_atoms = []
    for line in chunk:
        fields = line.strip().split('|')
        if fields[SUPPRESS_I] in config.filters.suppression_handling:
            continue
        # We parse all SABs in the file, filtering will happen in the processing step
        cui = fields[CUI_I]
        cui_groups[cui].append(fields)
        all_atoms.append(fields)
    return cui_groups, all_atoms


def _process_atoms_for_preferred_names(
    cui_groups: Dict[str, List[List[str]]],
    all_atoms: List[List[str]],
    config: AppConfig
) -> Tuple[Dict[str, Concept], Dict[str, Code]]:
    """
    Selects preferred names based on SAB priority and UMLS ranks,
    and creates Concept and Code objects.
    """
    concepts = {}
    codes = {}
    sab_priority_map = {sab: i for i, sab in enumerate(config.sab_priority.sab_priority)}

    def get_atom_rank(atom: List[str]) -> tuple:
        """
        Calculates a rank for an atom to determine preference.
        Lower rank is better.
        """
        sab = atom[SAB_I]
        ts = atom[TS_I]
        stt = atom[STT_I]
        ispref = atom[ISPREF_I]

        # Priority 1: SAB priority
        sab_rank = sab_priority_map.get(sab, len(sab_priority_map))

        # Priority 2: Term Status (TS)
        ts_rank = 0 if ts == 'P' else 1 if ts == 'S' else 2

        # Priority 3: String Type (STT)
        stt_rank = 0 if stt == 'PF' else 1 if stt == 'VO' else 2

        # Priority 4: ISPREF flag
        ispref_rank = 0 if ispref == 'Y' else 1

        return (sab_rank, ts_rank, stt_rank, ispref_rank)

    # Select preferred name for each CUI
    for cui, cui_atoms in cui_groups.items():
        if not cui_atoms:
            continue

        # Sort atoms by rank to find the best one
        sorted_atoms = sorted(cui_atoms, key=get_atom_rank)
        best_atom = sorted_atoms[0]

        concepts[cui] = Concept(
            cui=cui,
            preferred_name=best_atom[STR_I],
            last_seen_version=config.umls_version
        )

    # Create Code objects from all relevant atoms
    for atom in all_atoms:
        if atom[SAB_I] in config.filters.sab_filter:
            code_id = f"{atom[SAB_I]}:{atom[CODE_I]}"
            # Avoid overwriting codes if multiple lines define the same one
            if code_id not in codes:
                 codes[code_id] = Code(
                    cui=atom[CUI_I], # Temporarily store CUI here
                    code_id=code_id,
                    sab=atom[SAB_I],
                    name=atom[STR_I],
                    last_seen_version=config.umls_version
                )

    return concepts, codes


def _parse_mrrel_chunk(chunk: List[str], config: AppConfig) -> List[ConceptRelationship]:
    """Parses a chunk of the MRREL.RRF file."""
    relationships = []
    for line in chunk:
        fields = line.strip().split('|')
        if fields[SUPPRESS_REL_I] in config.filters.suppression_handling:
            continue
        if fields[SAB_REL_I] not in config.filters.sab_filter:
            continue

        relationships.append(ConceptRelationship(
            source_cui=fields[CUI1_I],
            target_cui=fields[CUI2_I],
            source_rela=fields[RELA_I] or fields[REL_I], # Use RELA if available
            asserted_by_sabs={fields[SAB_REL_I]},
            last_seen_version=config.umls_version,
            biolink_predicate="" # Will be filled in by the transformer
        ))
    return relationships


def _read_file_in_chunks(filepath: Path, chunk_size: int = 100000) -> List[List[str]]:
    """Reads a file and splits it into chunks."""
    with open(filepath, 'r', encoding='utf-8') as f:
        while True:
            lines = f.readlines(chunk_size)
            if not lines:
                break
            yield lines


def parse_umls_files(
    umls_version_dir: Path,
    config: AppConfig,
    max_workers: Optional[int] = None
) -> UmlsData:
    """
    Orchestrates the parallel parsing of UMLS RRF files.
    """
    if max_workers is None:
        max_workers = os.cpu_count() or 1

    pool = multiprocessing.Pool(processes=max_workers)

    # --- Parse MRSTY for TUI mappings ---
    mrsty_path = _get_rrf_path(umls_version_dir, "MRSTY.RRF")
    mrsty_chunks = _read_file_in_chunks(mrsty_path)
    mrsty_results = pool.map(_parse_mrsty_chunk, mrsty_chunks)

    cui_to_tuis = defaultdict(set)
    for res in mrsty_results:
        for cui, tuis in res.items():
            cui_to_tuis[cui].update(tuis)

    # --- Parse MRCONSO for concepts and codes ---
    mrconso_path = _get_rrf_path(umls_version_dir, "MRCONSO.RRF")
    mrconso_chunks = _read_file_in_chunks(mrconso_path)
    # The config object needs to be passed to the worker processes
    mrconso_results = pool.starmap(_parse_mrconso_chunk, [(chunk, config) for chunk in mrconso_chunks])

    # Aggregate results from all chunks
    aggregated_cui_groups = defaultdict(list)
    aggregated_atoms = []
    for cui_groups, all_atoms in mrconso_results:
        for cui, atoms in cui_groups.items():
            aggregated_cui_groups[cui].extend(atoms)
        aggregated_atoms.extend(all_atoms)

    # Process aggregated atoms to get final concepts and codes
    all_concepts, all_codes = _process_atoms_for_preferred_names(
        aggregated_cui_groups, aggregated_atoms, config
    )

    # --- Parse MRREL for relationships ---
    mrrel_path = _get_rrf_path(umls_version_dir, "MRREL.RRF")
    mrrel_chunks = _read_file_in_chunks(mrrel_path)
    mrrel_results = pool.starmap(_parse_mrrel_chunk, [(chunk, config) for chunk in mrrel_chunks])

    all_relationships = []
    for res in mrrel_results:
        all_relationships.extend(res)

    pool.close()
    pool.join()

    return all_concepts, all_codes, all_relationships, cui_to_tuis
