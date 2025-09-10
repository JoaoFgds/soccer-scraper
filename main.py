# File: main.py
"""
Main entry point for the soccer-scraper application.
Orchestrates the scraper and pre-processor pipelines based on user input.
"""
import argparse
import logging

from src.scraper.main import scraper_pipeline
from src.pre_processor.main import pre_processor_pipeline


def configure_logging():
    """Configures the root logger for the application."""
    log_format = "[%(asctime)s] - %(levelname)s - %(name)s - %(message)s"
    logging.basicConfig(
        level=logging.INFO,
        format=log_format,
        handlers=[logging.StreamHandler()],
    )


def main():
    """
    Parses command-line arguments and runs the selected pipeline(s).
    """
    parser = argparse.ArgumentParser(
        description="Run the soccer scraper and data pre-processor pipelines."
    )
    parser.add_argument(
        "pipeline",
        choices=["scrape", "process", "all"],
        help=(
            "The pipeline to run: 'scrape' for the scraper, "
            "'process' for the pre-processor, or 'all' to run both sequentially."
        ),
    )
    args = parser.parse_args()

    configure_logging()

    if args.pipeline in ["scrape", "all"]:
        scraper_pipeline()

    if args.pipeline in ["process", "all"]:
        pre_processor_pipeline()


if __name__ == "__main__":
    main()
