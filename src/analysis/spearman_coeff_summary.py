import logging
import pandas as pd
from src.utils import paths


logger = logging.getLogger(__name__)


def create_g_type_summary():
    """Aggregates Spearman's G coefficient results into a summary table.

    This function takes the detailed output from the strength-of-schedule
    balance analysis and creates a multi-level summary to provide insights at
    different granularities.

    The aggregation is performed in three stages:
    1.  **By Season:** Groups data by league, season, and G-type to show the
        distribution of schedule types within each specific season.
    2.  **By League:** Groups data by league and G-type across all available
        seasons to provide a long-term view of schedule balance for each league.
    3.  **Overall:** Creates a global summary across all leagues and seasons to
        show the overall distribution of schedule types in the entire dataset.

    The results from these three levels are then combined, sorted, and saved
    to a new summary CSV file, providing a comprehensive overview of the
    analysis results.
    """
    logger.info("Starting creation of G-type summary.")
    try:
        df = pd.read_csv(paths.SPEARMAN_BALANCE_PATH)
        logger.info("Loaded '%s' with %d rows.", paths.SPEARMAN_BALANCE_PATH, len(df))
    except FileNotFoundError:
        logger.error(
            "Input file not found: %s. Cannot create summary.",
            paths.SPEARMAN_BALANCE_PATH,
        )
        return

    # Level 1: Group by league, season, and G_type.
    agg_season = (
        df.groupby(["league_name", "season_year", "G_type"])
        .agg(n_samples=("G", "count"), G_avg=("G", "mean"))
        .reset_index()
    )

    # Level 2: Group by league across all seasons.
    agg_league = (
        df.groupby(["league_name", "G_type"])
        .agg(n_samples=("G", "count"), G_avg=("G", "mean"))
        .reset_index()
    )
    agg_league["season_year"] = "all_seasons"

    # Level 3: Group across all leagues and seasons.
    agg_all = (
        df.groupby("G_type")
        .agg(n_samples=("G", "count"), G_avg=("G", "mean"))
        .reset_index()
    )
    agg_all["league_name"] = "all_leagues"
    agg_all["season_year"] = "all_seasons"

    summary_df = pd.concat([agg_season, agg_league, agg_all], ignore_index=True)

    final_columns = ["league_name", "season_year", "G_type", "n_samples", "G_avg"]
    summary_df = summary_df.reindex(columns=final_columns)

    summary_df.sort_values(by=["league_name", "season_year", "G_type"], inplace=True)
    summary_df.reset_index(drop=True, inplace=True)

    summary_df.to_csv(paths.SPEARMAN_SUMMARY_PATH, index=False, float_format="%.4f")
    logger.info(
        "Summary table with %d rows saved to '%s'.",
        len(summary_df),
        paths.SPEARMAN_SUMMARY_PATH,
    )
