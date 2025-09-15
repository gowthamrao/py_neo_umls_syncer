import logging
from pathlib import Path
from typing import Dict, List, Set, Tuple
from collections import defaultdict
import pandas as pd

from .config import Settings
from .parser import stream_mrconso, stream_mrsty, stream_mrrel
from .biolink_mapper import BiolinkMapper

logger = logging.getLogger(__name__)

class Transformer:
    """
    Handles the transformation of raw UMLS RRF data into CSV files
    formatted for Neo4j's admin bulk import.
    """

    def __init__(self, settings: Settings):
        self.settings = settings
        self.mapper = BiolinkMapper()
        self.output_dir = Path(self.settings.output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"Transformer initialized. Output directory: {self.output_dir}")

    def transform_for_bulk_import(self):
        """
        Orchestrates the full transformation process.
        """
        logger.info("Starting data transformation for bulk import...")

        # Process nodes and HAS_CODE relationships first
        self._process_mrconso_and_mrsty()

        # Process concept-to-concept relationships
        self._process_mrrel()

        logger.info("Data transformation complete.")

    def _write_csv(self, data: List[Dict], filename: str):
        """Writes a list of dictionaries to a CSV file."""
        if not data:
            logger.warning(f"No data to write for {filename}")
            return

        filepath = self.output_dir / filename
        logger.info(f"Writing {len(data)} records to {filepath}...")
        df = pd.DataFrame(data)
        df.to_csv(filepath, index=False, sep=',')

    def _write_node_header(self, id_space: str, properties: List[str], filename: str):
        """Writes a header file for a node CSV."""
        header_map = {prop: f"{prop}:string" for prop in properties}
        header_map[id_space] = f"{id_space}:ID({id_space})"
        header_map['LABEL'] = ':LABEL'

        filepath = self.output_dir / filename
        logger.info(f"Writing header to {filepath}...")
        pd.DataFrame([header_map]).to_csv(filepath, index=False, sep=',')

    def _write_rel_header(self, start_id_space: str, end_id_space: str, properties: List[str], filename: str):
        """Writes a header file for a relationship CSV."""
        header_map = {prop: f"{prop}:string" for prop in properties}
        header_map['START_ID'] = f":START_ID({start_id_space})"
        header_map['END_ID'] = f":END_ID({end_id_space})"

        filepath = self.output_dir / filename
        logger.info(f"Writing header to {filepath}...")
        pd.DataFrame([header_map]).to_csv(filepath, index=False, sep=',')

    def _process_mrconso_and_mrsty(self):
        """
        Processes MRCONSO and MRSTY files in a streaming fashion to generate
        Concept nodes, Code nodes, and HAS_CODE relationships.
        """
        logger.info("Processing MRSTY to build CUI->TUI mapping...")
        cui_to_tuis = defaultdict(set)
        for record in stream_mrsty(self.settings):
            cui_to_tuis[record['CUI']].add(record['TUI'])
        logger.info(f"Built map for {len(cui_to_tuis)} CUIs from MRSTY.")

        logger.info("Streaming MRCONSO to generate nodes and HAS_CODE rels...")

        # For simplicity in this implementation, we collect nodes in memory.
        # A true large-scale implementation would write to CSVs in batches.
        concepts = []
        codes = []
        has_code_rels = []

        current_cui = None
        cui_term_buffer = []

        for record in stream_mrconso(self.settings):
            # Basic filtering
            if record['LAT'] != 'ENG' or record['SAB'] not in self.settings.sab_filter or record['SUPPRESS'] in self.settings.suppress_flags:
                continue

            if record['CUI'] != current_cui and current_cui is not None:
                # Process the completed CUI group
                self._process_cui_group(current_cui, cui_term_buffer, cui_to_tuis, concepts, codes, has_code_rels)
                cui_term_buffer = []

            current_cui = record['CUI']
            cui_term_buffer.append(record)

        # Process the last CUI group
        if current_cui and cui_term_buffer:
            self._process_cui_group(current_cui, cui_term_buffer, cui_to_tuis, concepts, codes, has_code_rels)

        # Write files
        self._write_node_header("cui", ["preferred_name", "last_seen_version"], "nodes_concepts_header.csv")
        self._write_csv(concepts, "nodes_concepts.csv")

        self._write_node_header("code_id", ["sab", "name", "last_seen_version"], "nodes_codes_header.csv")
        self._write_csv(codes, "nodes_codes.csv")

        self._write_rel_header("Concept", "Code", [], "rels_has_code_header.csv")
        self._write_csv(has_code_rels, "rels_has_code.csv")

    def _process_cui_group(self, cui: str, terms: List[Dict], cui_to_tuis: Dict[str, Set[str]], concepts: List, codes: List, has_code_rels: List):
        """Processes a group of terms for a single CUI."""

        # 1. Determine Preferred Name
        preferred_name = "No preferred name found"
        # Try finding by SAB priority first
        for sab in self.settings.sab_priority:
            for term in terms:
                if term['SAB'] == sab:
                    preferred_name = term['STR']
                    break
            if preferred_name != "No preferred name found":
                break
        # Fallback to UMLS standard flags if no name found via SAB priority
        if preferred_name == "No preferred name found":
            for term in terms:
                if term['TS'] == 'P' and term['STT'] == 'PF' and term['ISPREF'] == 'Y':
                    preferred_name = term['STR']
                    break

        # 2. Get Biolink Categories
        tuis = cui_to_tuis.get(cui, set())
        biolink_categories = {self.mapper.get_biolink_category(tui) for tui in tuis}
        biolink_categories.add("Concept") # Add base Concept label
        # Remove None if a mapping failed
        labels = ";".join(sorted([cat for cat in biolink_categories if cat]))

        # 3. Create Concept Node
        concepts.append({
            "cui": cui,
            "preferred_name": preferred_name,
            "LABEL": labels,
            "last_seen_version": self.settings.release_version
        })

        # 4. Create Code Nodes and HAS_CODE Relationships
        for term in terms:
            code_id = f"{term['SAB']}:{term['CODE']}"
            codes.append({
                "code_id": code_id,
                "sab": term['SAB'],
                "name": term['STR'],
                "LABEL": "Code",
                "last_seen_version": self.settings.release_version
            })
            has_code_rels.append({
                "START_ID": cui,
                "END_ID": code_id
            })

    def _process_mrrel(self):
        """
        Processes MRREL file to generate concept-to-concept relationships,
        aggregating by source SABs.
        """
        logger.info("Processing MRREL to generate concept-to-concept relationships...")
        # Key: (CUI1, RELA, CUI2), Value: Set of SABs
        rel_aggregator: Dict[Tuple[str, str, str], Set[str]] = defaultdict(set)
        record_count = 0
        filtered_count = 0
        for record in stream_mrrel(self.settings):
            record_count += 1
            logger.debug(f"Processing MRREL record: {record}") # DEBUG
            if record['SAB'] not in self.settings.sab_filter:
                filtered_count += 1
                logger.debug(f"Filtering out record due to SAB: {record['SAB']}") # DEBUG
                continue

            # We are interested in relationships between concepts (CUIs)
            if record.get('CUI1') and record.get('CUI2') and record.get('RELA'):
                key = (record['CUI1'], record['RELA'], record['CUI2'])
                rel_aggregator[key].add(record['SAB'])
            else:
                logger.warning(f"Skipping MRREL record due to missing CUI1, CUI2, or RELA: {record}")

        logger.info(f"Processed {record_count} records from MRREL. Filtered out {filtered_count}. Aggregated {len(rel_aggregator)} unique relationships.")

        # Group relationships by their mapped Biolink predicate
        rels_by_type: Dict[str, List[Dict]] = defaultdict(list)

        for (cui1, rela, cui2), sabs in rel_aggregator.items():
            # Use the first SAB for mapping, as the predicate should be consistent
            first_sab = next(iter(sabs))
            predicate = self.mapper.get_biolink_predicate(rela, first_sab)

            if predicate:
                rel_data = {
                    "START_ID": cui1,
                    "END_ID": cui2,
                    "source_rela": rela,
                    "asserted_by_sabs": ";".join(sorted(list(sabs))),
                    "last_seen_version": self.settings.release_version
                }
                rels_by_type[predicate].append(rel_data)

        # Write files for each relationship type
        for predicate, rels in rels_by_type.items():
            clean_predicate = predicate.replace(":", "_") # for valid filenames
            filename = f"rels_concept_{clean_predicate}.csv"
            header_filename = f"rels_concept_{clean_predicate}_header.csv"

            self._write_rel_header("Concept", "Concept", ["source_rela", "asserted_by_sabs", "last_seen_version"], header_filename)
            self._write_csv(rels, filename)
