"""
Biolink Model Mapping Service.

This module provides a service for mapping UMLS identifiers to their
corresponding Biolink Model concepts. It uses simple, file-based mappings
to allow for easy updates and configuration without changing the code.
"""

import csv
from pathlib import Path
from typing import Dict, Optional
import logging

# Set up logger
logger = logging.getLogger(__name__)

class BiolinkMapper:
    """
    Loads and provides access to UMLS-to-Biolink mappings.
    """
    def __init__(self, resource_dir: Optional[Path] = None):
        """
        Initializes the mapper by loading mapping files from the resource directory.

        Args:
            resource_dir: The directory containing mapping files. If None, defaults
                          to the 'resources' subdirectory next to this file.
        """
        if resource_dir is None:
            resource_dir = Path(__file__).parent / "resources"

        self.tui_to_category: Dict[str, str] = self._load_mapping(
            resource_dir / "tui_to_biolink.tsv", "TUI to Category"
        )
        self.rela_to_predicate: Dict[str, str] = self._load_mapping(
            resource_dir / "rela_to_biolink.tsv", "RELA to Predicate"
        )

        self.default_category = "biolink:NamedThing"
        self.default_predicate = "biolink:related_to"

    def _load_mapping(self, file_path: Path, mapping_name: str) -> Dict[str, str]:
        """Loads a two-column TSV mapping file into a dictionary."""
        mapping: Dict[str, str] = {}
        if not file_path.exists():
            logger.warning(
                f"Mapping file not found for '{mapping_name}': {file_path}. "
                "The mapping will be empty."
            )
            return mapping

        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.reader(f, delimiter='\t')
            for i, row in enumerate(reader):
                if not row or row[0].strip().startswith('#'):
                    continue
                if len(row) >= 2:
                    key = row[0].strip()
                    value = row[1].strip()
                    mapping[key] = value
                else:
                    logger.warning(
                        f"Skipping malformed row {i+1} in '{file_path}': {row}"
                    )

        logger.info(f"Successfully loaded {len(mapping)} entries for '{mapping_name}' from {file_path}")
        return mapping

    def get_biolink_category(self, tui: str) -> str:
        """
        Maps a UMLS TUI (Semantic Type Identifier) to a Biolink Model category.

        If no specific mapping is found, returns a default category.
        """
        return self.tui_to_category.get(tui, self.default_category)

    def get_biolink_predicate(self, rela: str) -> str:
        """
        Maps a UMLS RELA (Relationship Attribute) to a Biolink Model predicate.

        Note: This is a simplified direct mapping. A production system may
        require more complex logic considering the CUI's types and the REL.
        If no specific mapping is found, returns a default predicate.
        """
        return self.rela_to_predicate.get(rela, self.default_predicate)

# Create a single, importable instance for the application to use.
# This avoids reloading the mapping files repeatedly.
mapper = BiolinkMapper()
