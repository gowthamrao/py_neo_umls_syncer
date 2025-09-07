"""
transformer.py

This module transforms the parsed RRF data into a format suitable for loading
into Neo4j. It handles data aggregation, business logic (like preferred name
selection), and the generation of CSV files for the neo4j-admin bulk importer.
"""
import csv
import logging
from pathlib import Path
from collections import defaultdict
from typing import Dict, List, Any, Set, Tuple, Iterator

from .config import Settings
from .parser import UmlsParser
from .biolink_mapper import biolink_mapper

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class UmlsTransformer:
    """
    Orchestrates the transformation of parsed UMLS data into CSV files for bulk import.
    """

    def __init__(self, settings: Settings, parser: UmlsParser):
        self.settings = settings
        self.parser = parser
        self.version = self.settings.umls_version
        self.output_dir = Path(self.settings.data_dir) / "transformed" / self.version
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def _select_preferred_name(self, terms: List[Dict[str, Any]]) -> str:
        """
        Selects the preferred name for a CUI based on SAB priority and UMLS ranking.
        """
        # Group terms by SAB
        terms_by_sab = defaultdict(list)
        for term in terms:
            terms_by_sab[term['sab']].append(term)

        # 1. Try to find a name from the prioritized SABs
        for sab in self.settings.sab_priority:
            if sab in terms_by_sab:
                sab_terms = terms_by_sab[sab]
                # Rank terms within the SAB by TTY
                for tty in self.settings.preferred_name_tty_ranking:
                    for term in sab_terms:
                        if term['tty'] == tty:
                            return term['str']

        # 2. Fallback to standard UMLS ranking logic if no priority SABs match
        for term in terms:
            if term['ts'] == 'P' and term['stt'] == 'PF' and term['ispref'] == 'Y':
                return term['str']

        # 3. If still no name, return the first term found (last resort)
        return terms[0]['str'] if terms else "No preferred name found"

    def _write_csv(self, filename: str, header: List[str], rows: List[Dict | List]):
        """Utility to write data to a CSV file."""
        filepath = self.output_dir / filename
        logger.info(f"Writing {len(rows)} records to {filepath}...")
        is_dict = isinstance(rows[0], dict) if rows else False
        with open(filepath, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            writer.writerow(header)
            for row in rows:
                writer.writerow(row.values() if is_dict else row)

    def transform_for_bulk_import(self):
        """
        Main method to run the full transformation and CSV generation process.
        """
        logger.info("Starting transformation process for bulk import...")

        # 1. Parse and aggregate data from RRF files
        logger.info("Parsing MRSTY.RRF to map CUIs to TUIs...")
        cui_to_tuis = defaultdict(set)
        for sty in self.parser.parse_mrsty():
            cui_to_tuis[sty['cui']].add(sty['tui'])

        logger.info("Parsing MRCONSO.RRF to gather all terms and codes...")
        cui_to_terms = defaultdict(list)
        codes = []
        concept_has_code_rels = []
        processed_codes = set()

        for conso in self.parser.parse_mrconso():
            cui = conso['cui']
            cui_to_terms[cui].append(conso)
            code_id = f"{conso['sab']}:{conso['code']}"
            if code_id not in processed_codes:
                codes.append([code_id, conso['sab'], conso['str'], self.version])
                concept_has_code_rels.append([cui, code_id, self.version])
                processed_codes.add(code_id)

        # 2. Generate Concept nodes
        logger.info("Generating Concept nodes with preferred names and Biolink categories...")
        concepts = []
        for cui, terms in cui_to_terms.items():
            preferred_name = self._select_preferred_name(terms)
            tuis = cui_to_tuis.get(cui, [])
            biolink_labels = biolink_mapper.get_biolink_categories(list(tuis))
            # The :LABEL column expects a semicolon-delimited list of labels
            labels = ";".join(["Concept"] + biolink_labels)
            concepts.append([cui, preferred_name, labels, self.version])

        # 3. Parse and aggregate relationships from MRREL.RRF
        logger.info("Parsing MRREL.RRF to aggregate relationships...")
        # Key: (cui1, cui2, rela), Value: set of SABs
        rel_aggregator: Dict[Tuple[str, str, str], Set[str]] = defaultdict(set)
        for rel in self.parser.parse_mrrel():
            # Ensure we only model relationships between concepts we have processed
            if rel['cui1'] in cui_to_terms and rel['cui2'] in cui_to_terms:
                key = (rel['cui1'], rel['cui2'], rel['rela'])
                rel_aggregator[key].add(rel['sab'])

        logger.info("Generating relationship data...")
        concept_rels = []
        for (cui1, cui2, rela), sabs in rel_aggregator.items():
            predicate = biolink_mapper.get_biolink_predicate(rela)
            # The relationship type must be a single label
            concept_rels.append([cui1, cui2, predicate, rela, ";".join(sorted(sabs)), self.version])

        # 4. Write all data to CSV files with headers
        logger.info("Writing header and data files for neo4j-admin import...")
        # Node headers and data
        self._write_csv("concept_nodes_header.csv", ["cui:ID(Concept)", "preferred_name:string", ":LABEL", "last_seen_version:string"], [])
        self._write_csv("concept_nodes_data.csv", [], concepts)
        self._write_csv("code_nodes_header.csv", ["code_id:ID(Code)", "sab:string", "name:string", "last_seen_version:string"], [])
        self._write_csv("code_nodes_data.csv", [], codes)

        # Relationship headers and data
        self._write_csv("concept_has_code_rels_header.csv", [":START_ID(Concept)", ":END_ID(Code)", "last_seen_version:string"], [])
        self._write_csv("concept_has_code_rels_data.csv", [], concept_has_code_rels)
        self._write_csv("concept_concept_rels_header.csv", [":START_ID(Concept)", ":END_ID(Concept)", ":TYPE", "source_rela:string", "asserted_by_sabs:string[]", "last_seen_version:string"], [])
        self._write_csv("concept_concept_rels_data.csv", [], concept_rels)

        logger.info("Transformation complete. CSV files are ready for bulk import.")

    def stream_transformed_data(self, batch_size=50000) -> Iterator[Tuple[str, List[Dict[str, Any]]]]:
        """
        Parses and transforms UMLS data, yielding it in batches for incremental loading.
        This is a memory-intensive operation.
        """
        logger.info("Starting data transformation for streaming...")

        # 1. Parse and aggregate data (similar to bulk import)
        logger.info("Parsing MRSTY.RRF...")
        cui_to_tuis = defaultdict(set)
        for sty in self.parser.parse_mrsty():
            cui_to_tuis[sty['cui']].add(sty['tui'])

        logger.info("Parsing MRCONSO.RRF...")
        cui_to_terms = defaultdict(list)
        processed_codes = set()
        codes_batch = []
        rels_batch = []

        for conso in self.parser.parse_mrconso():
            cui = conso['cui']
            cui_to_terms[cui].append(conso)
            code_id = f"{conso['sab']}:{conso['code']}"
            if code_id not in processed_codes:
                codes_batch.append({
                    "code_id": code_id, "sab": conso['sab'], "name": conso['str']
                })
                rels_batch.append({"start_id": cui, "end_id": code_id})
                processed_codes.add(code_id)

                if len(codes_batch) >= batch_size:
                    yield "codes", codes_batch
                    yield "concept_has_code_rels", rels_batch
                    codes_batch, rels_batch = [], []

        if codes_batch:
            yield "codes", codes_batch
            yield "concept_has_code_rels", rels_batch

        # 2. Yield Concept nodes
        logger.info("Yielding Concept nodes...")
        concepts_batch = []
        for cui, terms in cui_to_terms.items():
            preferred_name = self._select_preferred_name(terms)
            tuis = cui_to_tuis.get(cui, [])
            biolink_labels = biolink_mapper.get_biolink_categories(list(tuis))
            concepts_batch.append({
                "cui": cui,
                "preferred_name": preferred_name,
                ":LABEL": ";".join(["Concept"] + biolink_labels)
            })
            if len(concepts_batch) >= batch_size:
                yield "concepts", concepts_batch
                concepts_batch = []
        if concepts_batch:
            yield "concepts", concepts_batch

        # 3. Yield Concept-Concept relationships
        logger.info("Parsing and yielding MRREL.RRF...")
        rel_aggregator = defaultdict(set)
        concept_rels_batch = []
        for rel in self.parser.parse_mrrel():
            if rel['cui1'] in cui_to_terms and rel['cui2'] in cui_to_terms:
                key = (rel['cui1'], rel['cui2'], rel['rela'])
                rel_aggregator[key].add(rel['sab'])

        for (cui1, cui2, rela), sabs in rel_aggregator.items():
            predicate = biolink_mapper.get_biolink_predicate(rela)
            concept_rels_batch.append({
                "start_id": cui1,
                "end_id": cui2,
                "type": predicate,
                "props": {
                    "source_rela": rela,
                    "asserted_by_sabs": ";".join(sorted(sabs))
                }
            })
            if len(concept_rels_batch) >= batch_size:
                yield "concept_concept_rels", concept_rels_batch
                concept_rels_batch = []
        if concept_rels_batch:
            yield "concept_concept_rels", concept_rels_batch

        logger.info("Finished streaming transformed data.")
