import argparse
import logging
import sys
from pathlib import Path

# Add the src directory to the Python path to allow importing the package
src_path = Path(__file__).parent / "src"
sys.path.insert(0, str(src_path))

from py_neo_umls_syncer.config import Settings
from py_neo_umls_syncer.transformer import Transformer

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(
        description="Parse UMLS RRF files and generate CSVs for a Neo4j bulk import."
    )
    parser.add_argument(
        "-i", "--input",
        dest="input_dir",
        default="data/input",
        help="Directory containing the RRF files. (default: data/input)"
    )
    parser.add_argument(
        "-o", "--output",
        dest="output_dir",
        default="data/output",
        help="Directory to save the generated CSV files. (default: data/output)"
    )
    args = parser.parse_args()

    logger.info("Starting the bulk import file generation process...")

    try:
        settings = Settings(input_dir=args.input_dir, output_dir=args.output_dir)
        logger.info(f"Input directory: {settings.input_dir}")
        logger.info(f"Output directory: {settings.output_dir}")
        logger.info(f"Using SAB filter: {', '.join(settings.sab_filter)}")

        transformer = Transformer(settings)
        transformer.transform_for_bulk_import()

        logger.info(f"Successfully generated CSV files in {settings.output_dir}")
        print("\nProcess complete. You can now use the generated files with the 'neo4j-admin database import' command.")

    except FileNotFoundError as e:
        logger.error(f"A required file was not found. Please check your input directory.", exc_info=True)
        print(f"\nError: A required file was not found. Details: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error("An unexpected error occurred during the transformation process.", exc_info=True)
        print(f"\nAn unexpected error occurred: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
