import logging
import pandas as pd

from . import mapping
from src.utils import paths
from src.processor import processors
from src.utils.logger import setup_logging


logger = logging.getLogger(__name__)


def pre_processor_pipeline():
    """Orchestrates the entire data pre-processing and validation workflow.

    This function serves as the main entry point for transforming the raw,
    scraped data into a clean, structured, and validated dataset. It executes a
    sequence of tasks to consolidate, enrich, standardize, and filter the data.

    The pipeline performs the following key steps:
    1.  Generates a data quality summary for each scraped season.
    2.  Consolidates all individual standings and game files into two master
        DataFrames.
    3.  Standardizes team names by generating and applying a mapping between
        different name variations and a canonical name.
    4.  Identifies "valid" seasons by filtering the summary based on a set of
        predefined business rules (e.g., data completeness, URL correctness).
    5.  Saves the final, validated datasets (standings and games) containing
        only the data from high-quality seasons.
    6.  Saves the complete, mapped datasets and the season summary as
        intermediate artifacts for analysis and debugging.
    """
    setup_logging()
    logger.info("--- Starting Data Pre-processing Workflow ---")

    logger.info("Task 1: Generating season summary and complete standings.")
    summary_df = processors.create_standings_summary()

    print("\n")
    logger.info("Task 2: Generating complete standings file.")
    complete_standings_df = processors.create_standings_complete()

    print("\n")
    logger.info("Task 3: Generating complete team games file.")
    complete_games_df = processors.create_team_games_complete()

    print("\n")
    logger.info("Task 4: Generating and applying team name mappings.")
    mappings_df = pd.DataFrame()

    if not complete_standings_df.empty and not complete_games_df.empty:

        mappings_df = mapping.generate_and_save_name_mappings(
            standings_df=complete_standings_df, games_df=complete_games_df
        )
        standings_mapped_df, games_mapped_df = mapping.apply_name_mappings(
            standings_df=complete_standings_df,
            games_df=complete_games_df,
            mappings_df=mappings_df,
        )
    else:
        logger.warning("Skipping name mapping due to empty standings or games data.")
        standings_mapped_df = complete_standings_df.rename(
            columns={"team_sanitized": "team_canonical"}
        )
        games_mapped_df = complete_games_df

    print("\n")
    logger.info("Task 5: Identifying valid seasons and creating validated data files.")
    valid_standings_ref_df = pd.DataFrame()

    if not summary_df.empty:
        summary_df.to_csv(paths.SEASON_SUMMARY_PATH, index=False)
        logger.info("Season summary saved to: %s", paths.SEASON_SUMMARY_PATH)

        # Business Rule: Define what constitutes a "valid" season for promotion.
        valid_seasons_df = summary_df[
            (summary_df["has_all_teams_files"])
            & (summary_df["is_valid_url"])
            & (summary_df["is_double_rounded"])
            & (summary_df["is_valid_attendance"])
        ]

        if not valid_seasons_df.empty:
            valid_cols = ["source_id", "source_csv_file", "league_name", "season_year"]
            valid_standings_ref_df = valid_seasons_df[valid_cols].copy()

            # Save the validated versions of the mapped data.
            valid_source_ids = valid_standings_ref_df["source_id"].unique()

            standings_valid_df = standings_mapped_df[
                standings_mapped_df["source_id"].isin(valid_source_ids)
            ].copy()

            games_valid_df = games_mapped_df[
                games_mapped_df["standings_id"].isin(valid_source_ids)
            ].copy()

            standings_valid_df.to_csv(paths.STANDINGS_VALID_PATH, index=False)
            logger.info(
                "Validated standings file saved to: %s", paths.STANDINGS_VALID_PATH
            )
            games_valid_df.to_csv(paths.GAMES_VALID_PATH, index=False)
            logger.info("Validated games file saved to: %s", paths.GAMES_VALID_PATH)

        else:
            logger.warning(
                "No seasons met the criteria for being 'valid'. No valid files created."
            )
    else:
        logger.warning(
            "Standings summary generation yielded no data. Cannot determine valid seasons."
        )

    if not standings_mapped_df.empty:
        standings_mapped_df.to_csv(paths.STANDINGS_COMPLETE_PATH, index=False)
        logger.info(
            "Complete mapped standings file saved to: %s", paths.STANDINGS_COMPLETE_PATH
        )

    if not games_mapped_df.empty:
        games_mapped_df.to_csv(paths.GAMES_COMPLETE_PATH, index=False)
        logger.info(
            "Complete mapped games file saved to: %s", paths.GAMES_COMPLETE_PATH
        )

    logger.info("--- Data Pre-processing Workflow Complete ---")
