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
    Generates all summary, valid, and complete processed files.
    """
    logger.info("--- Starting Data Pre-processing Workflow ---")

    # --- Task 1 & 2: Generate Standings Summary and Complete Files ---
    summary_df = processors.create_standings_summary()
    complete_standings_df = processors.create_standings_complete()
    valid_standings_ref_df = pd.DataFrame()  # Ensure it's defined

    if not summary_df.empty:
        summary_df.to_csv(config.FINAL_STANDINGS_SUMMARY_CSV, index=False)
        logger.info(
            f"Final standings summary saved to: {config.FINAL_STANDINGS_SUMMARY_CSV.relative_to(config.BASE_DIR)}"
        )

        valid_standings_df = summary_df[
            (summary_df["is_valid_url"])
            & (summary_df["is_double_rounded"])
            & (summary_df["is_valid_attendance"])
        ]

        if not valid_standings_df.empty:
            valid_cols = ["source_id", "source_csv_file", "league_name", "season_year"]
            valid_standings_ref_df = valid_standings_df[valid_cols].copy()
            valid_standings_ref_df.to_csv(config.FINAL_STANDINGS_VALID_CSV, index=False)
            logger.info(
                f"Reference file for valid standings saved to: {config.FINAL_STANDINGS_VALID_CSV.relative_to(config.BASE_DIR)}"
            )
            logger.info(
                f"Successfully saved {len(valid_standings_ref_df)} valid seasons."
            )
    else:
        logger.warning("No summary data was generated.")

    if not complete_standings_df.empty:
        complete_standings_df.to_csv(config.FINAL_STANDINGS_COMPLETE_CSV, index=False)
        logger.info(
            f"Complete standings file saved to: {config.FINAL_STANDINGS_COMPLETE_CSV.relative_to(config.BASE_DIR)}"
        )
    else:
        logger.warning("No complete standings data was generated.")

    # --- Task 3: Create Validated Versions of Complete Files ---
    if not complete_standings_df.empty and not valid_standings_ref_df.empty:
        valid_source_ids = valid_standings_ref_df["source_id"].unique()
        final_valid_df = complete_standings_df[
            complete_standings_df["source_id"].isin(valid_source_ids)
        ].copy()
        final_valid_df.to_csv(config.FINAL_STANDINGS_COMPLETE_VALID_CSV, index=False)
        logger.info(
            f"Validated complete standings file saved to: {config.FINAL_STANDINGS_COMPLETE_VALID_CSV.relative_to(config.BASE_DIR)}"
        )
        logger.info(
            f"Successfully saved {len(final_valid_df)} rows in the validated complete file."
        )
    else:
        logger.warning(
            "Could not create 'final_standings_complete_valid.csv' due to missing source data."
        )

    # --- Task 4: Generate Complete Team Games File ---
    team_games_df = processors.create_team_games_complete()
    if not team_games_df.empty:
        team_games_df.to_csv(config.TEAM_GAMES_COMPLETED_CSV, index=False)
        logger.info(
            f"Complete team games file saved to: {config.TEAM_GAMES_COMPLETED_CSV.relative_to(config.BASE_DIR)}"
        )
    else:
        logger.warning("No completed team games data was generated.")

    logger.info("--- Data Pre-processing Workflow Complete ---")


if __name__ == "__main__":
    config.configure_logging()
    pre_processor_pipeline()
