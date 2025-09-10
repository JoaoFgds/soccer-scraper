# File: src/pre_processor/main.py
"""
Orchestrates the entire data pre-processing workflow.
"""
import logging
from src.pre_processor import config
from src.pre_processor import processors

logger = logging.getLogger(__name__)


def pre_processor_pipeline():
    """
    Main entry point for the pre-processing workflow.
    Generates a summary of standings data and saves it to a CSV file.
    """
    logger.info("--- Starting Data Pre-processing Workflow ---")
    summary_df = processors.create_standings_summary()

    if not summary_df.empty:
        # Save the summary to the processed data directory
        summary_df.to_csv(config.FINAL_STANDINGS_SUMMARY_CSV, index=False)
        logger.info(
            f"Final standings summary saved to: {config.FINAL_STANDINGS_SUMMARY_CSV.relative_to(config.BASE_DIR)}"
        )
    else:
        logger.warning("No summary data was generated.")

    logger.info("--- Data Pre-processing Workflow Complete ---")


if __name__ == "__main__":
    config.configure_logging()
    pre_processor_pipeline()
