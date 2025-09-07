"""
ETL Transformer for UMLS Data.

This module takes the parsed RRF data and transforms it into the target
Labeled Property Graph (LPG) schema defined in `models.py`. It handles
the logic for selecting preferred names, mapping to Biolink, and aggregating
relationship provenance.
"""
import logging
from typing import Dict, List, Set, Tuple
from tqdm import tqdm

from .models import Concept, Code, HasCodeRelationship, ConceptRelationship
from .parser import UmlsParser, UmlsTerm
from .biolink_mapper import mapper as biolink_mapper

logger = logging.getLogger(__name__)

class UmlsTransformer:
    """
    Orchestrates the transformation of parsed UMLS data into graph-ready objects.
    """
    def __init__(self, version: str):
        """
        Initializes the transformer for a specific UMLS version.

        Args:
            version: The UMLS release version string (e.g., "2024AA").
                     This will be stamped on all created entities.
        """
        self.version = version
        self.seen_codes: Set[str] = set()  # Tracks created Code nodes to prevent duplicates

    def transform_data(
        self,
        cui_terms: Dict[str, List[UmlsTerm]],
        cui_stys: Dict[str, Set[str]],
        cui_rels: List[Tuple[str, str, str, str]]
    ) -> Tuple[List[Concept], List[Code], List[HasCodeRelationship], List[ConceptRelationship]]:
        """
        Main transformation method.

        Args:
            cui_terms: Parsed data from MRCONSO.RRF.
            cui_stys: Parsed data from MRSTY.RRF.
            cui_rels: Parsed data from MRREL.RRF.

        Returns:
            A tuple containing lists of all transformed graph entities.
        """
        logger.info("Starting data transformation process...")

        concepts, codes, has_code_rels = self._transform_concepts_and_codes(cui_terms, cui_stys)
        concept_rels = self._transform_relationships(cui_rels, set(cui_terms.keys()))

        logger.info(f"Transformation complete. Generated {len(concepts)} concepts, "
                    f"{len(codes)} codes, and {len(concept_rels)} unique relationships.")
        return concepts, codes, has_code_rels, concept_rels

    def _transform_concepts_and_codes(
        self,
        cui_terms: Dict[str, List[UmlsTerm]],
        cui_stys: Dict[str, Set[str]]
    ) -> Tuple[List[Concept], List[Code], List[HasCodeRelationship]]:
        """Transforms CUIs into Concept nodes, Code nodes, and HAS_CODE relationships."""
        all_concepts: List[Concept] = []
        all_codes: List[Code] = []
        all_has_code_rels: List[HasCodeRelationship] = []

        logger.info(f"Transforming {len(cui_terms)} CUIs into Concepts and Codes...")
        for cui, terms in tqdm(cui_terms.items(), desc="Transforming Concepts"):
            # 1. Select the preferred term for the Concept's name
            preferred_term = UmlsParser.select_preferred_name(terms)

            # 2. Map semantic types to Biolink categories
            semantic_types = cui_stys.get(cui, set())
            biolink_categories = {biolink_mapper.get_biolink_category(tui) for tui in semantic_types}
            if not biolink_categories:
                biolink_categories.add(biolink_mapper.default_category)

            concept = Concept(
                cui=cui,
                preferred_name=preferred_term.name,
                biolink_categories=biolink_categories,
                last_seen_version=self.version
            )
            all_concepts.append(concept)

            # 3. Create Code nodes and HAS_CODE relationships for all associated terms
            for term in terms:
                code_id = f"{term.sab}:{term.code}"

                if code_id not in self.seen_codes:
                    code = Code(
                        code_id=code_id,
                        sab=term.sab,
                        name=term.name,
                        last_seen_version=self.version
                    )
                    all_codes.append(code)
                    self.seen_codes.add(code_id)

                has_code_rel = HasCodeRelationship(cui=cui, code_id=code_id)
                all_has_code_rels.append(has_code_rel)

        return all_concepts, all_codes, all_has_code_rels

    def _transform_relationships(
        self,
        cui_rels: List[Tuple[str, str, str, str]],
        valid_cuis: Set[str]
    ) -> List[ConceptRelationship]:
        """Transforms raw relationship tuples into aggregated ConceptRelationship objects."""
        logger.info(f"Transforming and aggregating {len(cui_rels)} raw relationships...")

        # Key: (source_cui, target_cui, rel_type), Value: ConceptRelationship
        aggregated_rels: Dict[Tuple[str, str, str], ConceptRelationship] = {}

        for cui1, cui2, rela, sab in tqdm(cui_rels, desc="Aggregating Relationships"):
            # Ensure relationships only connect concepts that are actually in our dataset
            if cui1 not in valid_cuis or cui2 not in valid_cuis:
                continue

            rel_type = biolink_mapper.get_biolink_predicate(rela)
            key = (cui1, cui2, rel_type)

            if key not in aggregated_rels:
                new_rel = ConceptRelationship(
                    source_cui=cui1,
                    target_cui=cui2,
                    rel_type=rel_type,
                    source_rela=rela,
                    asserted_by_sabs={sab},
                    last_seen_version=self.version
                )
                aggregated_rels[key] = new_rel
            else:
                # Aggregate provenance by adding the SAB
                aggregated_rels[key].asserted_by_sabs.add(sab)

        return list(aggregated_rels.values())
