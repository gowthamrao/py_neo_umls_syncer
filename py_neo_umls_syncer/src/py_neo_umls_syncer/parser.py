# Copyright (c) 2025-2026 Gowtham Adamane Rao. All Rights Reserved.
#
# Licensed under the Prosperity Public License 3.0.0 (the "License").
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License in the LICENSE file at the root
# of this repository, or at: https://prosperitylicense.com/versions/3.0.0
#
# Commercial use beyond a 30-day trial requires a separate license.
"""
Handles the parsing of UMLS Rich Release Format (RRF) files.

This module is designed for high-performance, parallel processing of large
RRF files. It uses Python's multiprocessing capabilities to distribute the
workload across multiple CPU cores, which is crucial for handling the
multi-gigabyte UMLS data files efficiently.

Key Features:
- Parallelized parsing of MRCONSO.RRF, MRREL.RRF, and MRSTY.RRF.
- A sophisticated parser for MRCONSO.RRF that correctly groups data by CUI
  to apply the preferred name selection logic.
- Filtering based on language (LAT), source vocabularies (SAB), and
  suppressibility flags, as defined in the application configuration.
- Yields structured data objects, making it memory-efficient for downstream
  processing by the transformer.
"""
import concurrent.futures
from typing import Iterator, List, Dict, Any, Generator, Tuple
from pathlib import Path
import logging

from .config import get_settings
from .models import Code

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Constants for RRF file column indices
MRCONSO_COLUMNS = {
    "CUI": 0, "LAT": 1, "TS": 2, "LUI": 3, "STT": 4, "SUI": 5,
    "ISPREF": 6, "AUI": 7, "SAUI": 8, "SCUI": 9, "SDUI": 10,
    "SAB": 11, "TTY": 12, "CODE": 13, "STR": 14, "SRL": 15,
    "SUPPRESS": 16, "CVF": 17,
}

MRREL_COLUMNS = {
    "CUI1": 0, "AUI1": 1, "STYPE1": 2, "REL": 3, "CUI2": 4,
    "AUI2": 5, "STYPE2": 6, "RELA": 7, "RUI": 8, "SRUI": 9,
    "SAB": 10, "SL": 11, "RG": 12, "DIR": 13, "SUPPRESS": 14,
    "CVF": 15,
}

MRSTY_COLUMNS = {
    "CUI": 0, "TUI": 1, "STN": 2, "STY": 3, "ATUI": 4, "CVF": 5,
}


def _read_chunks(file_path: Path, chunk_size: int) -> Generator[List[str], None, None]:
    """Reads a file in chunks of lines."""
    try:
        with file_path.open('r', encoding='utf-8') as f:
            while True:
                lines = f.readlines(chunk_size)
                if not lines:
                    break
                yield lines
    except FileNotFoundError:
        logging.error(f"File not found: {file_path}")
        return

def _process_mrsty_chunk(chunk: List[str]) -> Dict[str, str]:
    """Processes a chunk of MRSTY.RRF data."""
    cui_to_tui = {}
    for line in chunk:
        fields = line.strip().split('|')
        cui = fields[MRSTY_COLUMNS["CUI"]]
        tui = fields[MRSTY_COLUMNS["TUI"]]
        if cui not in cui_to_tui:
            cui_to_tui[cui] = tui
    return cui_to_tui

def parse_mrsty(file_path: Path, chunk_size: int = 1024 * 1024 * 100) -> Dict[str, str]:
    """
    Parses the MRSTY.RRF file to create a CUI to TUI mapping.
    """
    settings = get_settings()
    logging.info(f"Starting parsing of {file_path}...")
    cui_to_tui_map = {}
    with concurrent.futures.ProcessPoolExecutor(max_workers=settings.MAX_PARALLEL_PROCESSES) as executor:
        future_to_chunk = {
            executor.submit(_process_mrsty_chunk, chunk): i
            for i, chunk in enumerate(_read_chunks(file_path, chunk_size))
        }
        for future in concurrent.futures.as_completed(future_to_chunk):
            chunk_index = future_to_chunk[future]
            try:
                result = future.result()
                cui_to_tui_map.update(result)
            except Exception as exc:
                logging.error(f'Chunk {chunk_index} generated an exception: {exc}')
    logging.info(f"Finished parsing {file_path}. Found mappings for {len(cui_to_tui_map)} CUIs.")
    return cui_to_tui_map

def _process_mrrel_chunk(chunk: List[str]) -> List[Dict[str, Any]]:
    """Processes a chunk of MRREL.RRF data."""
    settings = get_settings()
    relationships = []
    for line in chunk:
        fields = line.strip().split('|')
        sab = fields[MRREL_COLUMNS["SAB"]]
        suppress = fields[MRREL_COLUMNS["SUPPRESS"]]
        if sab not in settings.SAB_FILTER or suppress in settings.SUPPRESSION_HANDLING:
            continue

        rel_data = {
            "source_cui": fields[MRREL_COLUMNS["CUI1"]],
            "target_cui": fields[MRREL_COLUMNS["CUI2"]],
            "rela": fields[MRREL_COLUMNS["RELA"]],
            "rel": fields[MRREL_COLUMNS["REL"]],
            "sab": sab,
        }
        relationships.append(rel_data)
    return relationships

def parse_mrrel(file_path: Path, chunk_size: int = 1024 * 1024 * 100) -> Iterator[Dict[str, Any]]:
    """
    Parses the MRREL.RRF file to extract relationships.
    """
    settings = get_settings()
    logging.info(f"Starting parsing of {file_path}...")
    total_rels = 0
    with concurrent.futures.ProcessPoolExecutor(max_workers=settings.MAX_PARALLEL_PROCESSES) as executor:
        future_to_chunk = {
            executor.submit(_process_mrrel_chunk, chunk): i
            for i, chunk in enumerate(_read_chunks(file_path, chunk_size))
        }
        for future in concurrent.futures.as_completed(future_to_chunk):
            chunk_index = future_to_chunk[future]
            try:
                results = future.result()
                total_rels += len(results)
                for res in results:
                    yield res
            except Exception as exc:
                logging.error(f'Chunk {chunk_index} generated an exception: {exc}')
    logging.info(f"Finished parsing {file_path}. Found {total_rels} relationships.")

def _group_by_cui(file_path: Path) -> Generator[List[List[str]], None, None]:
    """
    Reads MRCONSO.RRF and yields groups of lines belonging to the same CUI.
    """
    current_cui = None
    cui_group = []
    try:
        with file_path.open('r', encoding='utf-8') as f:
            for line in f:
                fields = line.strip().split('|')
                cui = fields[MRCONSO_COLUMNS["CUI"]]
                if current_cui is None:
                    current_cui = cui

                if cui == current_cui:
                    cui_group.append(fields)
                else:
                    yield cui_group
                    current_cui = cui
                    cui_group = [fields]
        if cui_group:
            yield cui_group
    except FileNotFoundError:
        logging.error(f"File not found: {file_path}")
        return

def _get_term_rank(term: Dict[str, Any]) -> int:
    """Assigns a rank to a term for preferred name selection."""
    rank = 0
    if term["ts"] == "P":
        rank += 4
    if term["stt"] == "PF":
        rank += 2
    if term["ispref"] == "Y":
        rank += 1
    return rank

def _process_cui_group(cui_group: List[List[str]]) -> Tuple[str, str, List[Code]]:
    """
    Processes a group of MRCONSO rows for a single CUI to find the
    preferred name and all associated codes.
    """
    settings = get_settings()
    cui = cui_group[0][MRCONSO_COLUMNS["CUI"]]
    valid_terms = []

    for fields in cui_group:
        if (fields[MRCONSO_COLUMNS["LAT"]] == "ENG" and
            fields[MRCONSO_COLUMNS["SAB"]] in settings.SAB_FILTER and
            fields[MRCONSO_COLUMNS["SUPPRESS"]] not in settings.SUPPRESSION_HANDLING):

            valid_terms.append({
                "sab": fields[MRCONSO_COLUMNS["SAB"]],
                "name": fields[MRCONSO_COLUMNS["STR"]],
                "code": fields[MRCONSO_COLUMNS["CODE"]],
                "tty": fields[MRCONSO_COLUMNS["TTY"]],
                "ts": fields[MRCONSO_COLUMNS["TS"]],
                "stt": fields[MRCONSO_COLUMNS["STT"]],
                "ispref": fields[MRCONSO_COLUMNS["ISPREF"]],
            })

    if not valid_terms:
        return cui, "", []

    preferred_name = ""
    for sab in settings.SAB_PRIORITY:
        terms_in_sab = [term for term in valid_terms if term["sab"] == sab]
        if not terms_in_sab:
            continue

        best_term_in_priority_sab = max(terms_in_sab, key=_get_term_rank)
        preferred_name = best_term_in_priority_sab["name"]
        break

    if not preferred_name:
        best_term = max(valid_terms, key=_get_term_rank)
        preferred_name = best_term["name"]

    codes = [
        Code(
            code_id=f'{term["sab"]}:{term["code"]}',
            sab=term["sab"],
            name=term["name"],
            last_seen_version=""
        )
        for term in valid_terms
    ]

    return cui, preferred_name, codes


def parse_mrconso(file_path: Path) -> Iterator[Tuple[str, str, List[Code]]]:
    """
    Parses the MRCONSO.RRF file to extract concepts, codes, and preferred names.
    """
    settings = get_settings()
    logging.info(f"Starting parsing of {file_path}...")
    count = 0
    with concurrent.futures.ProcessPoolExecutor(max_workers=settings.MAX_PARALLEL_PROCESSES) as executor:
        future_to_cui_group = {
            executor.submit(_process_cui_group, group): i
            for i, group in enumerate(_group_by_cui(file_path))
        }
        for future in concurrent.futures.as_completed(future_to_cui_group):
            try:
                cui, preferred_name, codes = future.result()
                if preferred_name:
                    yield cui, preferred_name, codes
                    count += 1
                    if count % 100000 == 0:
                        logging.info(f"Parsed {count} concepts...")
            except Exception as exc:
                logging.error(f'A CUI group processing generated an exception: {exc}')
    logging.info(f"Finished parsing {file_path}. Parsed a total of {count} concepts.")
