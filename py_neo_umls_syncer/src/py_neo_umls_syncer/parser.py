import logging
from pathlib import Path
from typing import Iterator, Dict, Any, List
import pandas as pd

from .config import Settings

logger = logging.getLogger(__name__)

# Define column names for clarity and maintainability, based on UMLS RRF documentation
MRCONSO_COLS = [
    "CUI", "LAT", "TS", "LUI", "STT", "SUI", "ISPREF", "AUI", "SAUI",
    "SCUI", "SDUI", "SAB", "TTY", "CODE", "STR", "SRL", "SUPPRESS", "CVF"
]
MRSTY_COLS = ["CUI", "TUI", "STN", "STY", "ATUI", "CVF"]
MRREL_COLS = [
    "CUI1", "AUI1", "STYPE1", "REL", "CUI2", "AUI2", "STYPE2", "RELA",
    "RUI", "SRUI", "SAB", "SL", "RG", "DIR", "SUPPRESS", "CVF"
]

def _stream_rrf_file(file_path: Path, columns: List[str], chunk_size: int = 100000) -> Iterator[Dict[str, Any]]:
    """
    A generic generator to stream a pipe-delimited RRF file using pandas.
    This is a private helper function.

    Args:
        file_path: Path to the RRF file.
        columns: A list of column names for the file.
        chunk_size: The number of rows per chunk to read into memory.

    Yields:
        A dictionary representing a row in the file.
    """
    if not file_path.exists():
        logger.error(f"RRF file not found: {file_path}")
        return

    logger.info(f"Streaming data from {file_path}...")
    try:
        with pd.read_csv(
            file_path,
            sep="|",
            header=None,
            names=columns,
            chunksize=chunk_size,
            dtype=str,
            on_bad_lines='warn',
            index_col=False # Ensure no column is used as index
        ) as reader:
            for chunk in reader:
                # The RRF format has a trailing delimiter, which causes pandas to read
                # an extra, unnamed column full of NaNs. We drop it if it exists.
                if chunk.columns[-1].startswith('Unnamed'):
                    chunk = chunk.iloc[:, :-1]

                # Convert NaN to None for consistency, then yield records
                chunk = chunk.where(pd.notna(chunk), None)
                for record in chunk.to_dict("records"):
                    yield record
    except Exception as e:
        logger.error(f"Failed to stream file {file_path}: {e}", exc_info=True)
        raise

def stream_mrconso(settings: Settings) -> Iterator[Dict[str, Any]]:
    """Streams records from MRCONSO.RRF."""
    file_path = Path(settings.input_dir) / "MRCONSO.RRF"
    return _stream_rrf_file(file_path, MRCONSO_COLS)

def stream_mrsty(settings: Settings) -> Iterator[Dict[str, Any]]:
    """Streams records from MRSTY.RRF."""
    file_path = Path(settings.input_dir) / "MRSTY.RRF"
    return _stream_rrf_file(file_path, MRSTY_COLS)

def stream_mrrel(settings: Settings) -> Iterator[Dict[str, Any]]:
    """Streams records from MRREL.RRF."""
    file_path = Path(settings.input_dir) / "MRREL.RRF"
    return _stream_rrf_file(file_path, MRREL_COLS)
