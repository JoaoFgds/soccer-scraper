import logging
import pandas as pd
from src.utils import paths
from typing import List, Dict, Any

logger = logging.getLogger(__name__)


def _calculate_group_stats(group_df: pd.DataFrame, g_type_cols: List[str]) -> pd.Series:
    """
    Calculates summary statistics for a given DataFrame group.

    This helper function computes the total number of tests, the number of
    significant tests, and the value counts for each G-type classification
    (balanced, unbalanced_strong, unbalanced_weak) across all provided
    threshold columns.

    Args:
        group_df: A pandas DataFrame subset (e.g., for a specific
            league/season) containing the data to summarize.
        g_type_cols: A list of column names that store the G-type
            classifications (e.g., 'G_type_0.300').

    Returns:
        A pandas Series containing the aggregated statistics for the group.
    """
    if group_df.empty:
        return pd.Series(dtype="float64")

    stats: Dict[str, Any] = {}

    stats["total_tests"] = len(group_df)
    stats["significant_tests"] = group_df["is_significant"].sum()

    for col in g_type_cols:
        counts = group_df[col].value_counts()

        stats[f"{col}_balanced"] = counts.get("balanced", 0)
        stats[f"{col}_unbalanced_strong"] = counts.get("unbalanced_strong", 0)
        stats[f"{col}_unbalanced_weak"] = counts.get("unbalanced_weak", 0)

    return pd.Series(stats)


def create_g_type_summary():
    """
    Aggregates G-coefficient results across multiple thresholds into a summary table.

    This function loads the detailed G-coefficient results (from
    SPEARMAN_BALANCE_PATH), identifies all G-type classification columns,
    and then aggregates the data at three different levels:
    1.  Overall (all leagues, all seasons)
    2.  By League (all seasons)
    3.  By League and Season

    It calculates total tests, significant tests, and the counts for each
    G-type classification at each aggregation level. The final summary
    DataFrame is then saved to SPEARMAN_SUMMARY_PATH.
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

    if df.empty:
        logger.warning(
            "Input file '%s' is empty. No summary will be created.",
            paths.SPEARMAN_BALANCE_PATH,
        )
        return

    g_type_cols = sorted([col for col in df.columns if col.startswith("G_type_")])

    if not g_type_cols:
        logger.error(
            "No 'G_type_' columns found in '%s'. Aborting summary.",
            paths.SPEARMAN_BALANCE_PATH,
        )
        return

    logger.info(
        "Found %d G-type columns to summarize: %s", len(g_type_cols), g_type_cols
    )

    agg_season = (
        df.groupby(["league_name", "season_year"])
        .apply(_calculate_group_stats, g_type_cols=g_type_cols, include_groups=False)
        .reset_index()
    )

    agg_league = (
        df.groupby("league_name")
        .apply(_calculate_group_stats, g_type_cols=g_type_cols, include_groups=False)
        .reset_index()
    )
    agg_league["season_year"] = "all_seasons"

    agg_all_series = _calculate_group_stats(df, g_type_cols)
    agg_all = agg_all_series.to_frame().T
    agg_all["league_name"] = "all_leagues"
    agg_all["season_year"] = "all_seasons"

    for col in agg_all.columns:
        if col not in ["league_name", "season_year"]:
            try:
                agg_all[col] = pd.to_numeric(agg_all[col])
            except ValueError:
                pass

    summary_df = pd.concat([agg_season, agg_league, agg_all], ignore_index=True)

    id_cols = ["league_name", "season_year", "total_tests", "significant_tests"]

    metric_cols = []
    for col_name in g_type_cols:
        metric_cols.extend(
            [
                f"{col_name}_balanced",
                f"{col_name}_unbalanced_strong",
                f"{col_name}_unbalanced_weak",
            ]
        )

    final_columns = id_cols + metric_cols
    summary_df = summary_df.reindex(columns=final_columns)

    summary_df.sort_values(by=["league_name", "season_year"], inplace=True)
    summary_df.reset_index(drop=True, inplace=True)

    int_cols = [
        col for col in summary_df.columns if col not in ["league_name", "season_year"]
    ]
    summary_df[int_cols] = summary_df[int_cols].astype(int)

    summary_df.to_csv(paths.SPEARMAN_SUMMARY_PATH, index=False)
    logger.info(
        "Summary table with %d rows saved to '%s'.",
        len(summary_df),
        paths.SPEARMAN_SUMMARY_PATH,
    )
    return summary_df
