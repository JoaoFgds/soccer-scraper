import argparse
import logging

from src.utils import logger
from src.scraper.main import scraper_pipeline

# from src.analysis.main import analysis_pipeline
from src.processor.main import pre_processor_pipeline


logger.setup_logging()
logger = logging.getLogger(__name__)


def main():
    """Main entry point for the soccer data processing application.

    This function orchestrates the execution of the different data pipelines
    based on command-line arguments. It allows the user to run the scraper,
    the pre-processor, the analysis, or all three in sequence.
    """
    parser = argparse.ArgumentParser(
        description="Run the soccer scraper, pre-processor, and analysis pipelines."
    )
    parser.add_argument(
        "pipeline",
        choices=["scrape", "process", "analysis", "all"],
        help=(
            "The pipeline to run: 'scrape' to gather data, 'process' to clean "
            "it, 'analysis' to generate insights, or 'all' to run all three "
            "sequentially."
        ),
    )
    args = parser.parse_args()

    if args.pipeline in ["scrape", "all"]:
        logger.info("Starting the scraper pipeline...")
        scraper_pipeline()
        logger.info("Scraper pipeline finished.")

    if args.pipeline in ["process", "all"]:
        logger.info("Starting the pre-processor pipeline...")
        pre_processor_pipeline()
        logger.info("Pre-processor pipeline finished.")

    # if args.pipeline in ["analysis", "all"]:
    #     logger.info("Starting the analysis pipeline...")
    #     analysis_pipeline()
    #     logger.info("Analysis pipeline finished.")


if __name__ == "__main__":
    main()
