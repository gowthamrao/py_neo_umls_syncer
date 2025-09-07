"""
parser.py

This module is responsible for the high-performance parsing of UMLS RRF files.
It uses Python's multiprocessing to parallelize the reading and processing of
these large, pipe-delimited files.
"""
import logging
import csv
from pathlib import Path
from typing import Iterator, Dict, Any, List, Callable
from multiprocessing import Pool, cpu_count

from .config import Settings

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Define column indices for clarity
# MRCONSO.RRF columns
MRCONSO_CUI = 0
MRCONSO_LAT = 1
MRCONSO_TS = 2
MRCONSO_LUI = 3
MRCONSO_STT = 4
MRCONSO_SUI = 5
MRCONSO_ISPREF = 6
MRCONSO_AUI = 7
MRCONSO_SAUI = 8
MRCONSO_SCUI = 9
MRCONSO_SDUI = 10
MRCONSO_SAB = 11
MRCONSO_TTY = 12
MRCONSO_CODE = 13
MRCONSO_STR = 14
MRCONSO_SRL = 15
MRCONSO_SUPPRESS = 16
MRCONSO_CVF = 17

# MRREL.RRF columns
MRREL_CUI1 = 0
MRREL_AUI1 = 1
MRREL_STYPE1 = 2
MRREL_REL = 3
MRREL_CUI2 = 4
MRREL_AUI2 = 5
MRREL_STYPE2 = 6
MRREL_RELA = 7
MRREL_RUI = 8
MRREL_SRUI = 9
MRREL_SAB = 10
MRREL_SL = 11
MRREL_RG = 12
MRREL_DIR = 13
MRREL_SUPPRESS = 14
MRREL_CVF = 15

# MRSTY.RRF columns
MRSTY_CUI = 0
MRSTY_TUI = 1
MRSTY_STN = 2
MRSTY_STY = 3
MRSTY_ATUI = 4
MRSTY_CVF = 5


from itertools import islice

def _parse_chunk(
    chunk: List[str],
    parser_func: Callable[[List[str], Settings], Dict[str, Any]],
    settings: Settings
) -> List[Dict[str, Any]]:
    """Worker function to parse a chunk of lines."""
    # This function remains the same, it processes a list of lines.
    results = []
    for line in chunk:
        # RRF files have a trailing pipe, so the last element is empty.
        fields = line.strip().split('|')[:-1]
        if not fields:
            continue
        parsed = parser_func(fields, settings)
        if parsed:
            results.append(parsed)
    return results


def _parse_mrconso_line(fields: List[str], settings: Settings) -> Dict[str, Any] | None:
    """Parses a single line from MRCONSO.RRF."""
    # Apply suppression filter
    if fields[MRCONSO_SUPPRESS] in settings.suppression_handling:
        return None
    # Apply SAB filter if it's defined
    sab = fields[MRCONSO_SAB]
    if settings.sab_filter and sab not in settings.sab_filter:
        return None

    return {
        "cui": fields[MRCONSO_CUI],
        "sab": sab,
        "tty": fields[MRCONSO_TTY],
        "code": fields[MRCONSO_CODE],
        "str": fields[MRCONSO_STR],
        "ispref": fields[MRCONSO_ISPREF],
        "ts": fields[MRCONSO_TS],
        "stt": fields[MRCONSO_STT],
    }

def _parse_mrrel_line(fields: List[str], settings: Settings) -> Dict[str, Any] | None:
    """Parses a single line from MRREL.RRF."""
    if fields[MRREL_SUPPRESS] == 'O': # Often only 'O' is relevant for MRREL
        return None
    sab = fields[MRREL_SAB]
    if settings.sab_filter and sab not in settings.sab_filter:
        return None

    return {
        "cui1": fields[MRREL_CUI1],
        "cui2": fields[MRREL_CUI2],
        "rela": fields[MRREL_RELA],
        "sab": sab,
    }

def _parse_mrsty_line(fields: List[str], settings: Settings) -> Dict[str, Any] | None:
    """Parses a single line from MRSTY.RRF."""
    return {"cui": fields[MRSTY_CUI], "tui": fields[MRSTY_TUI]}


class UmlsParser:
    """
    A parser for UMLS RRF files that uses multiprocessing for efficiency.
    """
    def __init__(self, settings: Settings):
        self.settings = settings
        self.data_dir = Path(self.settings.data_dir) / self.settings.umls_version
        self.pool = Pool(processes=self.settings.max_parallel_processes)

    def _parse_file(
        self,
        filename: str,
        parser_func: Callable[[List[str], Settings], Dict[str, Any]],
        chunk_size: int = 100000
    ) -> Iterator[Dict[str, Any]]:
        """
        A generator that parses a file in parallel chunks.
        """
        filepath = self.data_dir / filename
        if not filepath.exists():
            raise FileNotFoundError(f"Required RRF file not found: {filepath}")

        logger.info(f"Starting parallel parsing of {filename} with {self.settings.max_parallel_processes} processes.")

        with open(filepath, 'r', encoding='utf-8') as f:
            # Create a generator that reads the file in chunks
            chunk_generator = (
                list(islice(f, chunk_size)) for _ in iter(lambda: True, False)
            )

            # Process the chunks in parallel using imap_unordered for lazy evaluation
            for result_chunk in self.pool.starmap(
                _parse_chunk,
                ((chunk, parser_func, self.settings) for chunk in chunk_generator if chunk)
            ):
                for result in result_chunk:
                    yield result

        logger.info(f"Finished parsing {filename}.")

    def parse_mrconso(self) -> Iterator[Dict[str, Any]]:
        """Parses MRCONSO.RRF and yields processed records."""
        return self._parse_file("MRCONSO.RRF", _parse_mrconso_line)

    def parse_mrrel(self) -> Iterator[Dict[str, Any]]:
        """Parses MRREL.RRF and yields processed records."""
        return self._parse_file("MRREL.RRF", _parse_mrrel_line)

    def parse_mrsty(self) -> Iterator[Dict[str, Any]]:
        """Parses MRSTY.RRF and yields processed records."""
        return self._parse_file("MRSTY.RRF", _parse_mrsty_line)

    def __del__(self):
        """Ensure the multiprocessing pool is closed when the object is destroyed."""
        self.pool.close()
        self.pool.join()
