import argparse
import logging

from src.utils import logger_setup
from src.scraper.main import scraper_pipeline
from src.pre_processor.main import pre_processor_pipeline
from src.analysis.main import analysis_pipeline


logger_setup.setup_logging()
logger = logging.getLogger(__name__)


def main():
    """
    Parses command-line arguments and runs the selected pipeline(s).
    """
    parser = argparse.ArgumentParser(
        description="Run the soccer scraper, pre-processor, and analysis pipelines."
    )
    parser.add_argument(
        "pipeline",
        choices=["scrape", "process", "analysis", "all"],
        help=(
            "The pipeline to run: 'scrape', 'process', 'analysis', "
            "or 'all' to run all sequentially."
        ),
    )
    args = parser.parse_args()

    if args.pipeline in ["scrape", "all"]:
        scraper_pipeline()

    if args.pipeline in ["process", "all"]:
        pre_processor_pipeline()

    if args.pipeline in ["analysis", "all"]:
        analysis_pipeline()


if __name__ == "__main__":
    main()
