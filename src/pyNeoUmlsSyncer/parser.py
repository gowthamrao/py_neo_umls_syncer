"""
Parallelized Parser for UMLS Rich Release Format (RRF) files.

This module is responsible for parsing the key RRF files (MRCONSO, MRSTY, MRREL)
efficiently using multiprocessing. It filters and structures the data into
a format ready for the transformation step.
"""
import logging
import os
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import Dict, List, Set, Tuple, Iterator, Any, Callable
from collections import defaultdict
from pydantic import BaseModel
from tqdm import tqdm
from functools import partial

from .config import settings

logger = logging.getLogger(__name__)

# --- Data structures for parsing ---

class UmlsTerm(BaseModel):
    """A temporary model to hold salient information from an MRCONSO row."""
    cui: str
    sab: str
    tty: str
    code: str
    name: str
    ts: str
    stt: str
    ispref: str

# --- Column indices for RRF files for readability ---

class RRF_COLS:
    class MRCONSO:
        CUI = 0
        LAT = 1
        TS = 4
        STT = 5
        ISPREF = 6
        SAB = 11
        TTY = 12
        CODE = 13
        STR = 14
        SUPPRESS = 16

    class MRREL:
        CUI1 = 0
        CUI2 = 4
        RELA = 7
        SAB = 10
        SUPPRESS = 14

    class MRSTY:
        CUI = 0
        TUI = 1

# --- Top-level worker functions for multiprocessing ---

def _process_mrconso_chunk(
    lines: List[str], sab_filter: Set[str], suppress_filter: Set[str]
) -> Dict[str, List[UmlsTerm]]:
    """Worker function to parse a chunk of MRCONSO.RRF lines."""
    cui_terms: Dict[str, List[UmlsTerm]] = defaultdict(list)

    for line in lines:
        fields = line.strip().split('|')
        if len(fields) <= RRF_COLS.MRCONSO.SUPPRESS: continue
        if fields[RRF_COLS.MRCONSO.LAT] != 'ENG': continue

        sab = fields[RRF_COLS.MRCONSO.SAB]
        suppress_flag = fields[RRF_COLS.MRCONSO.SUPPRESS]

        if sab not in sab_filter or suppress_flag in suppress_filter:
            continue

        term = UmlsTerm(
            cui=fields[RRF_COLS.MRCONSO.CUI],
            sab=sab,
            tty=fields[RRF_COLS.MRCONSO.TTY],
            code=fields[RRF_COLS.MRCONSO.CODE],
            name=fields[RRF_COLS.MRCONSO.STR],
            ts=fields[RRF_COLS.MRCONSO.TS],
            stt=fields[RRF_COLS.MRCONSO.STT],
            ispref=fields[RRF_COLS.MRCONSO.ISPREF]
        )
        cui_terms[term.cui].append(term)

    return cui_terms

def _process_mrsty_chunk(lines: List[str]) -> Dict[str, Set[str]]:
    """Worker function to parse a chunk of MRSTY.RRF lines."""
    cui_stys: Dict[str, Set[str]] = defaultdict(set)
    for line in lines:
        fields = line.strip().split('|')
        if len(fields) <= RRF_COLS.MRSTY.TUI: continue
        cui = fields[RRF_COLS.MRSTY.CUI]
        tui = fields[RRF_COLS.MRSTY.TUI]
        cui_stys[cui].add(tui)
    return cui_stys

def _process_mrrel_chunk(
    lines: List[str], sab_filter: Set[str], suppress_filter: Set[str]
) -> List[Tuple[str, str, str, str]]:
    """Worker function to parse a chunk of MRREL.RRF lines."""
    relationships = []

    for line in lines:
        fields = line.strip().split('|')
        if len(fields) <= RRF_COLS.MRREL.SUPPRESS: continue

        sab = fields[RRF_COLS.MRREL.SAB]
        suppress_flag = fields[RRF_COLS.MRREL.SUPPRESS]

        if sab not in sab_filter or suppress_flag in suppress_filter:
            continue

        if fields[2] != 'CUI' or fields[6] != 'CUI': continue

        cui1 = fields[RRF_COLS.MRREL.CUI1]
        cui2 = fields[RRF_COLS.MRREL.CUI2]
        rela = fields[RRF_COLS.MRREL.RELA]

        relationships.append((cui1, cui2, rela, sab))

    return relationships

# --- Main Parser Class ---

class UmlsParser:
    """Orchestrates the parallel parsing of UMLS RRF files."""
    def __init__(self, rrf_path: Path):
        if not rrf_path or not rrf_path.is_dir():
            raise FileNotFoundError(f"RRF path does not exist or is not a directory: {rrf_path}")
        self.rrf_path = rrf_path
        self.max_workers = settings.max_parallel_processes or os.cpu_count()
        self.chunk_size = 250_000
        logger.info(f"UmlsParser initialized with {self.max_workers} workers.")

    def _parse_file_parallel(self, file_name: str, worker_func: Callable, result_aggregator: Callable):
        """Generic parallel file parser."""
        file_path = self.rrf_path / file_name
        if not file_path.exists():
            raise FileNotFoundError(f"Required RRF file not found: {file_path}")

        results = []
        with ProcessPoolExecutor(max_workers=self.max_workers) as executor, open(file_path, 'r', encoding='utf-8') as f:
            futures = {
                executor.submit(worker_func, list(chunk))
                for chunk in self._chunk_iterator(f, self.chunk_size)
            }

            progress_bar = tqdm(total=len(futures), desc=f"Parsing {file_name}")
            for future in as_completed(futures):
                try:
                    result = future.result()
                    results.append(result)
                except Exception as e:
                    logger.error(f"A worker process failed for {file_name}: {e}", exc_info=True)
                progress_bar.update(1)
            progress_bar.close()

        return result_aggregator(results)

    def _chunk_iterator(self, iterator: Iterator, chunk_size: int) -> Iterator[List[Any]]:
        """Yields chunks of a given size from an iterator."""
        chunk = []
        for item in iterator:
            chunk.append(item)
            if len(chunk) >= chunk_size:
                yield chunk
                chunk = []
        if chunk:
            yield chunk

    def get_cui_terms(self) -> Dict[str, List[UmlsTerm]]:
        """Parses MRCONSO.RRF to get all terms for each CUI."""
        worker = partial(
            _process_mrconso_chunk,
            sab_filter=set(settings.sab_filter),
            suppress_filter=set(settings.suppression_handling)
        )
        def aggregate_terms(results: List[Dict[str, List[UmlsTerm]]]) -> Dict[str, List[UmlsTerm]]:
            logger.info("Aggregating term data from all workers...")
            aggregated = defaultdict(list)
            for result_dict in results:
                for cui, terms in result_dict.items():
                    aggregated[cui].extend(terms)
            return aggregated

        return self._parse_file_parallel('MRCONSO.RRF', worker, aggregate_terms)

    def get_cui_semantic_types(self) -> Dict[str, Set[str]]:
        """Parses MRSTY.RRF to get all semantic types for each CUI."""
        def aggregate_stys(results: List[Dict[str, Set[str]]]) -> Dict[str, Set[str]]:
            logger.info("Aggregating semantic type data from all workers...")
            aggregated = defaultdict(set)
            for result_dict in results:
                for cui, stys in result_dict.items():
                    aggregated[cui].update(stys)
            return aggregated

        return self._parse_file_parallel('MRSTY.RRF', _process_mrsty_chunk, aggregate_stys)

    def get_cui_relationships(self) -> List[Tuple[str, str, str, str]]:
        """Parses MRREL.RRF to get all CUI-to-CUI relationships."""
        worker = partial(
            _process_mrrel_chunk,
            sab_filter=set(settings.sab_filter),
            suppress_filter=set(settings.suppression_handling)
        )
        def aggregate_rels(results: List[List[Tuple[str, str, str, str]]]) -> List[Tuple[str, str, str, str]]:
            logger.info("Aggregating relationship data from all workers...")
            return [item for sublist in results for item in sublist]

        return self._parse_file_parallel('MRREL.RRF', worker, aggregate_rels)

    @staticmethod
    def select_preferred_name(terms: List[UmlsTerm]) -> UmlsTerm:
        """Selects the best term to represent a CUI based on a priority ranking."""
        sab_priority_map = {sab: i for i, sab in enumerate(settings.sab_priority)}
        best_term = None
        best_rank = (float('inf'), float('inf'), float('inf'), float('inf'))

        for term in terms:
            sab_rank = sab_priority_map.get(term.sab, float('inf'))
            ts_rank = 0 if term.ts == 'P' else 1
            stt_rank = 0 if term.stt == 'PF' else (1 if term.stt == 'VO' else 2)
            ispref_rank = 0 if term.ispref == 'Y' else 1
            current_rank = (sab_rank, ts_rank, stt_rank, ispref_rank)

            if best_term is None or current_rank < best_rank:
                best_rank = current_rank
                best_term = term

        if not best_term: return terms[0]
        return best_term
