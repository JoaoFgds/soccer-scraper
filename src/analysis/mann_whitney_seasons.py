import logging
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

from pathlib import Path
from typing import Optional
from src.utils import paths
from itertools import combinations
from scipy.stats import mannwhitneyu


logger = logging.getLogger(__name__)


GROUP_MAP = {"unbalanced_weak": "G-", "balanced": "G0", "unbalanced_strong": "G+"}
GROUP_ORDER = ["G-", "G0", "G+"]


def _generate_boxplot(df: pd.DataFrame, output_path: Path, title: str) -> None:
    """Generates and saves a customized boxplot of final rank distribution by group.

    This function creates a boxplot to visualize the distribution of final team
    positions for each schedule balance group (G-, G0, G+). It applies custom
    styling and saves the resulting plot to the specified file path.

    Args:
        df (pd.DataFrame): The DataFrame containing the data to plot. Must
            include 'group_name' and 'final_position' columns.
        output_path (Path): The file path where the generated plot image
            will be saved.
        title (str): The title to be displayed on the plot.
    """
    if df.empty or df["group_name"].nunique() < 2:
        logger.warning(
            "Skipping boxplot generation for '%s' due to insufficient data.", title
        )
        return

    plt.rcParams["font.family"] = "DejaVu Sans"
    plt.figure(figsize=(10, 7))
    sns.set_style("whitegrid", {"axes.grid": True, "grid.linestyle": "--"})

    sns.boxplot(
        x="group_name",
        y="final_position",
        data=df,
        order=GROUP_ORDER,
        palette="viridis",
        hue="group_name",
        legend=False,
    )

    plt.title(title, fontsize=16, pad=20)
    plt.xlabel("Group", fontsize=12)
    plt.ylabel("Team Final Rank", fontsize=12)
    plt.tight_layout()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(output_path)
    plt.close()
    logger.info("Boxplot saved to: %s", output_path)


def _run_tests_on_subset(data: pd.DataFrame) -> Optional[pd.DataFrame]:
    """Performs pairwise Mann-Whitney U tests on a subset of data.

    This function takes a DataFrame, groups it by the 'group_name' column, and
    runs a two-sided Mann-Whitney U test for all combinations of the predefined
    groups (G-, G0, G+). The test determines if the distributions of
    'final_position' are significantly different between the groups.

    Args:
        data (pd.DataFrame): A subset of the analysis data for a specific
            scope (e.g., a single season, a league, or overall). Must contain
            'group_name' and 'final_position' columns.

    Returns:
        Optional[pd.DataFrame]: A single-row DataFrame containing the p-values
            for each pairwise comparison (e.g., 'G-_vs_G0'). Returns `None`
            if there are fewer than two distinct groups to compare in the data.
    """
    groups = {
        name: group_data["final_position"]
        for name, group_data in data.groupby("group_name")
    }
    if len(groups) < 2:
        return None

    p_values = {}
    for g1, g2 in combinations(GROUP_ORDER, 2):
        col_name = f"{g1}_vs_{g2}"
        if g1 in groups and g2 in groups:
            _, p_value = mannwhitneyu(groups[g1], groups[g2], alternative="two-sided")
            p_values[col_name] = p_value
        else:
            p_values[col_name] = None

    p_values_df = pd.DataFrame([p_values])

    for col in p_values_df.columns:
        if "_vs_" in col:
            p_values_df[col] = p_values_df[col].astype(float)

    return p_values_df


def run_statistical_analysis() -> None:
    """Orchestrates the statistical analysis of schedule balance on final team ranks.

    This function serves as the main entry point for the statistical testing
    phase. It loads the detailed Spearman analysis results and conducts a
    multi-level analysis to determine if there is a statistically significant
    difference in the final league standings of teams based on their schedule
    balance type (unbalanced-weak, balanced, or unbalanced-strong).

    The analysis is performed at three granularities:
    1.  **Overall:** All leagues and seasons combined.
    2.  **Per-League:** Each league's data aggregated across all its seasons.
    3.  **Per-Season:** Each individual league-season combination.

    For each level, it runs Mann-Whitney U tests and generates boxplots to
    visualize the distributions. The results (p-values and plots) are saved to
    the 'gold' data directory.
    """
    logger.info("Starting statistical significance analysis (Mann-Whitney U).")
    try:
        df = pd.read_csv(paths.SPEARMAN_BALANCE_PATH)
        df["group_name"] = df["G_type"].map(GROUP_MAP)
        df.dropna(subset=["final_position", "group_name"], inplace=True)
        logger.info(
            "Loaded '%s' with %d valid rows.", paths.SPEARMAN_BALANCE_PATH, len(df)
        )
    except FileNotFoundError:
        logger.error(
            "Input file not found: %s. Aborting analysis.", paths.SPEARMAN_BALANCE_PATH
        )
        return

    # --- Level 1: Overall Analysis ---
    logger.info("--- Running Overall Statistical Analysis ---")
    if (overall_p := _run_tests_on_subset(df)) is not None:
        overall_p.to_csv(
            paths.OVERALL_MANN_WHITNEY_PATH, index=False, float_format="%.4f"
        )
        logger.info("Overall p-values saved to: %s", paths.OVERALL_MANN_WHITNEY_PATH)

    plot_path = paths.MANN_WHITNEY_SEASONS_PLOTS / "rank_dist_overall.png"
    _generate_boxplot(df, plot_path, "Overall Team Final Rank Distribution by Group")

    # --- Level 2: Per-League Analysis ---
    logger.info("--- Running Per-League Statistical Analysis ---")
    per_league_results = []
    for league, league_df in df.groupby("league_name"):
        if (p_league := _run_tests_on_subset(league_df)) is not None:
            p_league["league_name"] = league
            per_league_results.append(p_league)

        plot_path = (
            paths.MANN_WHITNEY_SEASONS_PLOTS / f"rank_dist_{league}_all_seasons.png"
        )
        title = f"Final Rank Distribution for {league.replace('_', ' ').title()} (All Seasons)"
        _generate_boxplot(league_df, plot_path, title)

    if per_league_results:
        league_df = pd.concat(per_league_results, ignore_index=True)
        league_df.to_csv(
            paths.PER_LEAGUE_MANN_WHITNEY_PATH, index=False, float_format="%.4f"
        )
        logger.info(
            "Per-league p-values saved to: %s", paths.PER_LEAGUE_MANN_WHITNEY_PATH
        )

    # --- Level 3: Per-Season Analysis ---
    logger.info("--- Running Per-Season Statistical Analysis ---")
    per_season_results = []
    for (league, season), season_df in df.groupby(["league_name", "season_year"]):
        if (p_season := _run_tests_on_subset(season_df)) is not None:
            p_season["league_name"] = league
            p_season["season_year"] = season
            per_season_results.append(p_season)

    if per_season_results:
        season_df = pd.concat(per_season_results, ignore_index=True)
        season_df.to_csv(
            paths.PER_SEASON_MANN_WHITNEY_PATH, index=False, float_format="%.4f"
        )
        logger.info(
            "Per-season p-values saved to: %s", paths.PER_SEASON_MANN_WHITNEY_PATH
        )

    logger.info("Statistical analysis complete.")
