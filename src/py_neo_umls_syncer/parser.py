# Copyright (c) 2025-2026 Gowtham Adamane Rao. All Rights Reserved.
#
# Licensed under the Prosperity Public License 3.0.0 (the "License").
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License in the LICENSE file at the root
# of this repository, or at: https://prosperitylicense.com/versions/3.0.0
#
# Commercial use beyond a 30-day trial requires a separate license.
# Jules was here
import csv
from pathlib import Path
from typing import Dict, List, Tuple, Iterator, Any
from concurrent.futures import ProcessPoolExecutor, as_completed
from collections import defaultdict
import os
from .config import settings
from .models import Concept, Code, InterConceptRelationship, SemanticType, ConceptToCodeRelationship
from rich.progress import Progress, BarColumn, TextColumn, TimeElapsedColumn
from rich.console import Console

console = Console()

# Define column indices for RRF files for clarity
# MRCONSO: https://www.ncbi.nlm.nih.gov/books/NBK9685/table/ch03.T.mrconso_rrf_file_descriptions_and/
CUI_I, LAT_I, TS_I, LUI_I, STT_I, SUI_I, ISPREF_I, AUI_I, SAUI_I, SCUI_I, SDUI_I, SAB_I, TTY_I, CODE_I, STR_I, SRL_I, SUPPRESS_I, CVF_I = range(18)
# MRREL: https://www.ncbi.nlm.nih.gov/books/NBK9685/table/ch03.T.mrrel_rrf_file_descriptions_and/
CUI1_I, AUI1_I, STYPE1_I, REL_I, CUI2_I, AUI2_I, STYPE2_I, RELA_I, RUI_I, SRUI_I, SAB_REL_I, SL_I, RG_I, DIR_I, SUPPRESS_REL_I, CVF_REL_I = range(16)
# MRSTY: https://www.ncbi.nlm.nih.gov/books/NBK9685/table/ch03.T.mrsty_rrf_file_descriptions_and/
CUI_STY_I, TUI_I, STN_I, STY_I, ATUI_I, CVF_STY_I = range(6)


def _process_mrconso_chunk(chunk_info: Tuple[str, int, int]) -> List[Tuple]:
    """Worker function to process a chunk of MRCONSO.RRF."""
    filepath, start, end = chunk_info
    results = []
    with open(filepath, 'rb') as f:
        f.seek(start)
        # Read the entire chunk into memory. This is safe because _get_file_chunks ensures chunks are reasonable sizes.
        chunk_content = f.read(end - start).decode('utf-8', errors='ignore')
        reader = csv.reader(chunk_content.splitlines(), delimiter='|', quotechar='\x00')
        for row in reader:
            try:
                # Per RRF format, a trailing pipe means csv.reader gives 19 fields
                if len(row) != 19:
                    continue
                if row[SUPPRESS_I] in settings.suppression_handling or row[SAB_I] not in settings.sab_filter:
                    continue

                # We only need a subset of info for the reduction phase
                term_info = {
                    "sab": row[SAB_I],
                    "name": row[STR_I],
                    "code": row[CODE_I],
                    "ts": row[TS_I],
                    "stt": row[STT_I],
                    "ispref": row[ISPREF_I],
                    "tty": row[TTY_I]
                }
                results.append((row[CUI_I], term_info))
            except IndexError:
                continue # Skip malformed lines
    return results

def _process_mrrel_chunk(chunk_info: Tuple[str, int, int]) -> List[InterConceptRelationship]:
    """Worker function to process a chunk of MRREL.RRF."""
    filepath, start, end = chunk_info
    results = []
    with open(filepath, 'rb') as f:
        f.seek(start)
        chunk_content = f.read(end - start).decode('utf-8', errors='ignore')
        reader = csv.reader(chunk_content.splitlines(), delimiter='|', quotechar='\x00')
        for row in reader:
            try:
                # Per RRF format, a trailing pipe means csv.reader gives 17 fields
                if len(row) != 17:
                    continue
                # Filter based on SAB and ensure both concepts are in scope
                if row[SAB_REL_I] not in settings.sab_filter:
                    continue

                results.append(InterConceptRelationship(
                    source_cui=row[CUI1_I],
                    target_cui=row[CUI2_I],
                    source_rela=row[RELA_I] or row[REL_I], # Fallback to REL if RELA is empty
                    sab=row[SAB_REL_I]
                ))
            except IndexError:
                continue
    return results

def _get_file_chunks(filepath: str, num_chunks: int) -> List[Tuple[str, int, int]]:
    """Splits a file into byte-offset chunks for parallel processing."""
    file_size = os.path.getsize(filepath)
    chunk_size = file_size // num_chunks
    chunks = []
    start = 0
    with open(filepath, 'rb') as f:
        while start < file_size:
            end = min(start + chunk_size, file_size)
            # Align end to the next newline character
            if end < file_size:
                f.seek(end)
                f.readline()
                end = f.tell()

            chunks.append((filepath, start, end))
            start = end
            if start >= file_size:
                break
    return chunks


class RRFParser:
    """Parses UMLS RRF files into structured Pydantic models."""

    def __init__(self, meta_dir: Path):
        self.meta_dir = meta_dir
        self.mrconso_path = str(meta_dir / "MRCONSO.RRF")
        self.mrrel_path = str(meta_dir / "MRREL.RRF")
        self.mrsty_path = str(meta_dir / "MRSTY.RRF")

    def parse_mrsty(self) -> Dict[str, List[SemanticType]]:
        """Parses MRSTY.RRF to get CUI -> [SemanticType] mapping."""
        console.log("Parsing MRSTY.RRF...")
        sty_map = defaultdict(list)
        with open(self.mrsty_path, 'r', encoding='utf-8') as f:
            reader = csv.reader(f, delimiter='|', quotechar='\x00')
            for row in reader:
                sty_map[row[CUI_STY_I]].append(SemanticType(
                    cui=row[CUI_STY_I],
                    tui=row[TUI_I],
                    sty=row[STY_I]
                ))
        console.log(f"Parsed {len(sty_map)} CUIs with semantic types.")
        return sty_map

    def _reduce_mrconso_results(self, all_term_info: List[Tuple]) -> Tuple[Dict[str, Concept], List[Code], List[ConceptToCodeRelationship]]:
        """
        Reduces the mapped MRCONSO data to produce Concepts (with preferred names) and Codes.
        This function ensures that the returned lists of codes and relationships contain only unique items.
        """
        console.log("Reducing MRCONSO data to select preferred names...")
        cui_terms = defaultdict(list)
        for cui, term_info in all_term_info:
            cui_terms[cui].append(term_info)

        concepts = {}
        unique_codes = {}  # Using a dict as an ordered set: {code_id: Code}
        unique_concept_to_code_rels = set()  # Using a set for relation tuples: {(cui, code_id)}


        sab_priority_map = {sab: i for i, sab in enumerate(settings.sab_priority)}

        for cui, terms in cui_terms.items():
            # Generate all code nodes and relationships
            for term in terms:
                code_id = f"{term['sab']}:{term['code']}"
                # Add code if not seen before
                if code_id not in unique_codes:
                    unique_codes[code_id] = Code(code_id=code_id, sab=term['sab'], name=term['name'])
                # Add relationship if not seen before
                rel_tuple = (cui, code_id)
                unique_concept_to_code_rels.add(rel_tuple)

            # Determine preferred name
            terms.sort(key=lambda t: (
                sab_priority_map.get(t['sab'], 999),  # Lower is better
                t['ts'] != 'P',  # P is preferred
                t['stt'] != 'PF',  # PF is preferred
                t['ispref'] != 'Y'  # Y is preferred
            ))
            preferred_term = terms[0]
            concepts[cui] = Concept(cui=cui, preferred_name=preferred_term['name'])

        # Convert the unique collections to lists for the return type
        codes = list(unique_codes.values())
        concept_to_code_rels = [ConceptToCodeRelationship(cui=cui, code_id=code_id) for cui, code_id in unique_concept_to_code_rels]

        console.log(f"Reduced to {len(concepts)} concepts and {len(codes)} unique codes.")
        return concepts, codes, concept_to_code_rels

    def parse_files(self) -> Tuple[Dict[str, Concept], List[Code], List[ConceptToCodeRelationship], List[InterConceptRelationship], Dict[str, List[SemanticType]]]:
        """Orchestrates the parallel parsing of all required RRF files."""
        sty_map = self.parse_mrsty()

        num_workers = settings.max_parallel_processes

        all_mrconso_terms = []
        all_mrrel_rels = []

        with Progress(
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            "[progress.percentage]{task.percentage:>3.0f}%",
            TimeElapsedColumn(),
        ) as progress:
            with ProcessPoolExecutor(max_workers=num_workers) as executor:
                # MRCONSO
                mrconso_chunks = _get_file_chunks(self.mrconso_path, num_workers * 4) # More chunks than workers
                task1 = progress.add_task("Parsing MRCONSO...", total=len(mrconso_chunks))
                futures1 = [executor.submit(_process_mrconso_chunk, chunk) for chunk in mrconso_chunks]
                for future in as_completed(futures1):
                    all_mrconso_terms.extend(future.result())
                    progress.update(task1, advance=1)

                # MRREL
                mrrel_chunks = _get_file_chunks(self.mrrel_path, num_workers * 4)
                task2 = progress.add_task("Parsing MRREL...", total=len(mrrel_chunks))
                futures2 = [executor.submit(_process_mrrel_chunk, chunk) for chunk in mrrel_chunks]
                for future in as_completed(futures2):
                    all_mrrel_rels.extend(future.result())
                    progress.update(task2, advance=1)

        concepts, codes, concept_to_code_rels = self._reduce_mrconso_results(all_mrconso_terms)

        # Filter relationships to only include concepts we actually parsed
        valid_cuis = set(concepts.keys())
        final_rels = [
            rel for rel in all_mrrel_rels
            if rel.source_cui in valid_cuis and rel.target_cui in valid_cuis
        ]
        console.log(f"Filtered relationships to {len(final_rels)} entries between known concepts.")

        return concepts, codes, concept_to_code_rels, final_rels, sty_map
