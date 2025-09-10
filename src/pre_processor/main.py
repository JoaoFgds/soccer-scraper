# File: src/pre_processor/main.py
"""
Orchestrates the entire data pre-processing workflow.
"""
import logging
import pandas as pd
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
        # Save the full summary file
        summary_df.to_csv(config.FINAL_STANDINGS_SUMMARY_CSV, index=False)
        logger.info(
            f"Final standings summary saved to: {config.FINAL_STANDINGS_SUMMARY_CSV.relative_to(config.BASE_DIR)}"
        )

        # Filter for valid seasons
        valid_standings_df = summary_df[
            (summary_df["is_valid_url"])
            & (summary_df["is_double_rounded"])
            & (summary_df["is_valid_attendance"])
        ]

        # Select and save the reference file for valid leagues
        if not valid_standings_df.empty:
            valid_cols = ["source_id", "source_csv_file", "league_name", "season_year"]
            valid_standings_ref_df = valid_standings_df[valid_cols]

            valid_standings_ref_df.to_csv(config.FINAL_STANDINGS_VALID_CSV, index=False)

            logger.info(
                f"Successfully saved {len(valid_standings_ref_df)} valid seasons."
            )
            logger.info(
                f"Reference file for valid standings saved to: {config.FINAL_STANDINGS_VALID_CSV.relative_to(config.BASE_DIR)}"
            )
        else:
            logger.warning(
                "No seasons passed all validation criteria. 'final_standings_valid.csv' will not be created."
            )

    else:
        logger.warning("No summary data was generated.")

    logger.info("--- Data Pre-processing Workflow Complete ---")


if __name__ == "__main__":
    config.configure_logging()
    pre_processor_pipeline()
