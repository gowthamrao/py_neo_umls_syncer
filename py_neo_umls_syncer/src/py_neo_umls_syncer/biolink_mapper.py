import logging
from typing import Optional, Dict, Tuple

from bmt import Toolkit

# Set up a basic logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class BiolinkMapper:
    """
    A class to handle mapping from UMLS identifiers to the Biolink Model.
    It uses the biolink-model-toolkit (bmt) to perform the lookups and
    caches the results for performance.
    """

    _toolkit_instance: Optional[Toolkit] = None
    _tui_to_category_cache: Dict[str, Optional[str]] = {}
    _rela_to_predicate_cache: Dict[Tuple[str, str], Optional[str]] = {}

    # Manual overrides for when bmt returns a mixin or an otherwise non-ideal mapping
    _manual_tui_map = {
        "T028": "biolink:Gene",  # T028 (Gene or Genome) maps to mixin 'GenomicEntity', override to concrete 'Gene'
    }

    def __init__(self):
        if BiolinkMapper._toolkit_instance is None:
            logger.info("Initializing BiolinkMapper and bmt.Toolkit for the first time...")
            try:
                BiolinkMapper._toolkit_instance = Toolkit()
                logger.info("bmt.Toolkit initialized successfully.")
            except Exception as e:
                logger.error(f"Failed to initialize bmt.Toolkit: {e}")
                raise
        self.toolkit = BiolinkMapper._toolkit_instance

    def get_biolink_category(self, tui: str) -> Optional[str]:
        """
        Maps a UMLS Semantic Type Identifier (TUI) to a Biolink Model category.
        """
        if tui in self._tui_to_category_cache:
            return self._tui_to_category_cache[tui]

        # Check manual override map first
        if tui in self._manual_tui_map:
            result = self._manual_tui_map[tui]
            self._tui_to_category_cache[tui] = result
            return result

        # Use STY prefix for semantic types
        umls_curie = f"STY:{tui}"
        try:
            # get_all_elements_by_mapping is the most reliable function
            mapped_elements = self.toolkit.get_all_elements_by_mapping(umls_curie, formatted=True)

            categories = [elem for elem in mapped_elements if self.toolkit.is_category(elem) and not self.toolkit.is_mixin(elem)]
            if categories:
                if len(categories) > 1:
                    logger.warning(f"TUI '{tui}' mapped to multiple concrete categories: {categories}. Using the first one: {categories[0]}")
                result = categories[0]
                self._tui_to_category_cache[tui] = result
                return result

            logger.debug(f"TUI '{tui}' ({umls_curie}) did not map to a valid concrete Biolink category.")
            self._tui_to_category_cache[tui] = None
            return None
        except Exception as e:
            logger.error(f"An error occurred while mapping TUI '{tui}': {e}")
            self._tui_to_category_cache[tui] = None
            return None

    def get_biolink_predicate(self, rela: str, sab: str) -> Optional[str]:
        """
        Maps a UMLS Relationship Attribute (RELA) from a specific source (SAB)
        to a Biolink Model predicate.
        """
        cache_key = (rela, sab)
        if cache_key in self._rela_to_predicate_cache:
            return self._rela_to_predicate_cache[cache_key]

        # Construct a source-specific CURIE
        source_curie = f"{sab}:{rela.replace(' ', '_')}"

        try:
            # First, try the source-specific CURIE
            mapped_elements = self.toolkit.get_all_elements_by_mapping(source_curie, formatted=True)

            # If that fails, try a generic UMLS prefix as a fallback
            if not mapped_elements:
                generic_curie = f"UMLS:{rela.replace(' ', '_')}"
                mapped_elements = self.toolkit.get_all_elements_by_mapping(generic_curie, formatted=True)

            predicates = [elem for elem in mapped_elements if self.toolkit.is_predicate(elem)]
            if predicates:
                if len(predicates) > 1:
                    logger.warning(f"RELA '{rela}' from SAB '{sab}' mapped to multiple predicates: {predicates}. Using the first one: {predicates[0]}")
                result = predicates[0]
                self._rela_to_predicate_cache[cache_key] = result
                return result

            logger.debug(f"RELA '{rela}' from SAB '{sab}' did not map to a valid Biolink predicate.")
            self._rela_to_predicate_cache[cache_key] = None
            return None
        except Exception as e:
            logger.error(f"An error occurred while mapping RELA '{rela}' from SAB '{sab}': {e}")
            self._rela_to_predicate_cache[cache_key] = None
            return None
