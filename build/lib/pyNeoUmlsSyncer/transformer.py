import csv
from pathlib import Path
from typing import Dict, List, Tuple
from collections import defaultdict
from .models import Concept, Code, InterConceptRelationship, SemanticType, ConceptToCodeRelationship
from .biolink_mapper import get_biolink_category, get_biolink_predicate
from rich.console import Console

console = Console()

class CSVTransformer:
    """
    Transforms parsed UMLS data into CSV files suitable for Neo4j's bulk import.
    """
    def __init__(self, csv_dir: str):
        self.csv_dir = Path(csv_dir)
        self.csv_dir.mkdir(parents=True, exist_ok=True)
        console.log(f"CSV output directory set to: {self.csv_dir.resolve()}")

    def _write_csv(self, filename: str, header: List[str], rows: List[List[str]]):
        """Utility to write data to a CSV file."""
        filepath = self.csv_dir / filename
        with open(filepath, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(header)
            writer.writerows(rows)
        console.log(f"Wrote {len(rows)} rows to {filepath.name}")

    def _write_concept_nodes_csv(self, concepts: Dict[str, Concept], sty_map: Dict[str, List[SemanticType]]):
        """Generates the CSV for Concept nodes."""
        header = ["cui:ID(Concept-ID)", "preferred_name:string", ":LABEL"]
        rows = []
        for cui, concept in concepts.items():
            semantic_types = sty_map.get(cui, [])
            biolink_labels = {get_biolink_category(st.tui) for st in semantic_types}
            # Ensure base :Concept label is always present
            all_labels = ["Concept"] + sorted(list(biolink_labels))
            rows.append([
                concept.cui,
                concept.preferred_name,
                ";".join(all_labels)
            ])
        self._write_csv("nodes_concepts.csv", header, rows)

    def _write_code_nodes_csv(self, codes: List[Code]):
        """Generates the CSV for Code nodes."""
        header = ["code_id:ID(Code-ID)", "sab:string", "name:string"]
        # Use a set to handle duplicate codes that might arise from different term types
        unique_codes = { (c.code_id, c.sab, c.name) for c in codes }
        rows = [[code_id, sab, name] for code_id, sab, name in unique_codes]
        self._write_csv("nodes_codes.csv", header, rows)

    def _write_has_code_rels_csv(self, rels: List[ConceptToCodeRelationship]):
        """Generates the CSV for (:Concept)-[:HAS_CODE]->(:Code) relationships."""
        header = [":START_ID(Concept-ID)", ":END_ID(Code-ID)", ":TYPE"]
        unique_rels = { (r.cui, r.code_id) for r in rels }
        rows = [[cui, code_id, "HAS_CODE"] for cui, code_id in unique_rels]
        self._write_csv("rels_has_code.csv", header, rows)

    def _write_inter_concept_rels_csv(self, rels: List[InterConceptRelationship]):
        """
        Generates the CSV for inter-concept relationships, aggregating provenance.
        """
        header = [":START_ID(Concept-ID)", ":END_ID(Concept-ID)", "source_rela:string", "asserted_by_sabs:string[]", ":TYPE"]

        # Aggregate relationships to merge provenance
        agg_rels = defaultdict(set)
        # We group by the core relationship tuple to collect all asserting SABs
        for rel in rels:
            # Key: (source_cui, target_cui, biolink_predicate, source_rela)
            # The biolink predicate is part of the key to avoid merging, e.g., 'treats' and 'related_to'
            # if their source_rela was different but mapped to the same predicate.
            # A simpler key (source_cui, target_cui, source_rela) is also valid if we trust the source_rela.
            key = (rel.source_cui, rel.target_cui, rel.source_rela)
            agg_rels[key].add(rel.sab)

        rows = []
        for (source_cui, target_cui, source_rela), sabs in agg_rels.items():
            biolink_predicate = get_biolink_predicate(source_rela)
            rows.append([
                source_cui,
                target_cui,
                source_rela,
                ";".join(sorted(list(sabs))),
                biolink_predicate
            ])
        self._write_csv("rels_inter_concept.csv", header, rows)

    def transform_to_csvs(
        self,
        concepts: Dict[str, Concept],
        codes: List[Code],
        concept_to_code_rels: List[ConceptToCodeRelationship],
        inter_concept_rels: List[InterConceptRelationship],
        sty_map: Dict[str, List[SemanticType]]
    ):
        """
        Orchestrates the transformation of all parsed data into CSV files.
        """
        console.log("Starting transformation of parsed data to CSV files...")
        self._write_concept_nodes_csv(concepts, sty_map)
        self._write_code_nodes_csv(codes)
        self._write_has_code_rels_csv(concept_to_code_rels)
        self._write_inter_concept_rels_csv(inter_concept_rels)
        console.log("[green]CSV transformation complete.[/green]")
