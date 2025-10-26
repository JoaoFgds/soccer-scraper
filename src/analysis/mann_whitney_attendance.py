import logging
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

from typing import Dict, List
from itertools import combinations

from src.utils import paths
from scipy.stats import mannwhitneyu


logger = logging.getLogger(__name__)


GROUP_MAP = {"unbalanced_weak": "G-", "balanced": "G0", "unbalanced_strong": "G+"}
GROUP_ORDER = ["G-", "G0", "G+"]


def _calculate_occupancy(games_df: pd.DataFrame, attendance_col: str) -> pd.DataFrame:
    """Calculates the average stadium occupancy for each team-season.

    This function processes a games DataFrame to determine the average occupancy
    rate for each team during a season. Occupancy is defined as the ratio of
    the team's mean home attendance to their maximum home attendance for that
    season. An audit file containing the intermediate statistics (mean, max)
    is saved for verification.

    Args:
        games_df (pd.DataFrame): The DataFrame containing validated game data.
        attendance_col (str): The specific imputed attendance column to use for
            the calculation (e.g., 'audience_filled_mean').

    Returns:
        pd.DataFrame: A DataFrame with the calculated 'average_occupancy' for
            each team and season.
    """
    logger.info("Calculating average occupancy using column: '%s'", attendance_col)
    home_games = games_df.dropna(subset=[attendance_col]).copy()
    home_games = home_games[home_games[attendance_col] > 0]

    occupancy_stats = (
        home_games.groupby(["league_name", "season_year", "home_team_canonical"])
        .agg(
            mean_attendance=(attendance_col, "mean"),
            max_attendance=(attendance_col, "max"),
        )
        .reset_index()
    )

    occupancy_stats = occupancy_stats[occupancy_stats["max_attendance"] > 0]
    occupancy_stats["average_occupancy"] = (
        occupancy_stats["mean_attendance"] / occupancy_stats["max_attendance"]
    )
    occupancy_stats.rename(
        columns={"home_team_canonical": "team_canonical"}, inplace=True
    )

    audit_path = paths.ANALYSIS_AUDIT_DIR / f"occupancy_audit_{attendance_col}.csv"
    occupancy_stats.to_csv(audit_path, index=False, float_format="%.2f")
    logger.info("Occupancy audit table saved to: %s", audit_path)

    return occupancy_stats[
        ["league_name", "season_year", "team_canonical", "average_occupancy"]
    ]


def _generate_plot(
    df: pd.DataFrame, file_key: str, attendance_col: str, title: str
) -> None:
    """Generates and saves a boxplot for a given subset of occupancy data.

    Args:
        df (pd.DataFrame): The DataFrame containing the data to plot.
        file_key (str): A unique key for the output filename (e.g., 'overall').
        attendance_col (str): The attendance column used, for the filename.
        title (str): The title for the plot.
    """
    if df.empty or df["group_name"].nunique() < 2:
        return

    plot_path = (
        paths.MANN_WHITNEY_ATTENDANCE_PLOTS
        / f"occupancy_{attendance_col}_{file_key}.png"
    )
    plt.rcParams["font.family"] = "DejaVu Sans"
    plt.figure(figsize=(10, 7))
    sns.set_style("whitegrid", {"axes.grid": True, "grid.linestyle": "--"})
    sns.boxplot(
        x="group_name",
        y="average_occupancy",
        data=df,
        order=GROUP_ORDER,
        palette="viridis",
        hue="group_name",
        legend=False,
    )
    plt.title(title, fontsize=16, pad=20)
    plt.xlabel("Group", fontsize=12)
    plt.ylabel("Average Occupancy", fontsize=12)
    plt.tight_layout()
    plt.savefig(plot_path)
    plt.close()


def _run_tests(df: pd.DataFrame) -> pd.DataFrame:
    """Runs pairwise Mann-Whitney U tests and returns a row of p-values.

    This function compares the distributions of 'average_occupancy' between
    all pairs of schedule balance groups (G-, G0, G+).

    Args:
        df (pd.DataFrame): The input DataFrame for a specific analysis scope.

    Returns:
        pd.DataFrame: A single-row DataFrame containing the p-values for each
            pairwise comparison.
    """
    groups_data = {
        name: data["average_occupancy"] for name, data in df.groupby("group_name")
    }
    p_values: Dict[str, float | None] = {}

    for g1, g2 in combinations(GROUP_ORDER, 2):
        col_name = f"{g1}_vs_{g2}".replace("-", "neg").replace("+", "pos")
        p_values[col_name] = None
        if g1 in groups_data and g2 in groups_data:
            _, p_value = mannwhitneyu(groups_data[g1], groups_data[g2])
            p_values[col_name] = p_value

    p_values_df = pd.DataFrame([p_values])

    for col in p_values_df.columns:
        if "_vs_" in col:
            p_values_df[col] = p_values_df[col].astype(float)

    return p_values_df


def run_occupancy_analysis() -> None:
    """Orchestrates the stadium occupancy sensitivity analysis.

    This function serves as the main entry point to analyze the relationship
    between schedule balance and stadium occupancy. It performs a sensitivity

    analysis by repeating the entire workflow for different imputed attendance
    columns ('audience_filled_fb', 'audience_filled_mean',
    'audience_filled_median').

    For each attendance type, it calculates occupancy, merges it with the
    schedule balance data, and runs Mann-Whitney U tests at both an overall
    and a per-league level. All resulting p-values are consolidated into a
    single summary CSV file for easy comparison.
    """
    logger.info("--- Starting Stadium Occupancy Analysis ---")
    try:
        games_df = pd.read_csv(paths.GAMES_VALID_PATH)
        g_coeff_df = pd.read_csv(paths.SPEARMAN_BALANCE_PATH)
    except FileNotFoundError as e:
        logger.error("Input file not found: %s. Aborting analysis.", e)
        return

    attendance_columns = [
        "audience_filled_fb",
    ]
    all_p_value_results: List[pd.DataFrame] = []

    for col in attendance_columns:
        if col not in games_df.columns:
            logger.warning("Attendance column '%s' not found. Skipping.", col)
            continue

        logger.info("--- Processing for attendance column: [%s] ---", col)
        occupancy_df = _calculate_occupancy(games_df, col)
        merged_df = pd.merge(
            g_coeff_df,
            occupancy_df,
            on=["league_name", "season_year", "team_canonical"],
            how="inner",
        )
        merged_df["group_name"] = merged_df["G_type"].map(GROUP_MAP)

        # Level 1: Overall Analysis
        p_overall = _run_tests(merged_df)
        p_overall["league_name"] = "all_leagues"
        p_overall["season_year"] = "all_seasons"
        p_overall["audience_column"] = col
        all_p_value_results.append(p_overall)
        _generate_plot(merged_df, "overall", col, f"Overall Occupancy\n(using {col})")

        # Level 2: Per-League Analysis
        for league, league_df in merged_df.groupby("league_name"):
            p_league = _run_tests(league_df)
            p_league["league_name"] = league
            p_league["season_year"] = "all_seasons"
            p_league["audience_column"] = col
            all_p_value_results.append(p_league)

        # Level 3: Per-League / Season

    if not all_p_value_results:
        logger.warning("No p-value results were generated.")
        return

    final_df = pd.concat(all_p_value_results, ignore_index=True)

    final_df.rename(
        columns={
            "Gneg_vs_G0": "Gneg_vs_G0",
            "Gneg_vs_Gpos": "Gneg_vs_Gpos",
            "G0_vs_Gpos": "G0_vs_Gpos",
        },
        inplace=True,
    )

    final_cols = [
        "league_name",
        "season_year",
        "audience_column",
        "Gneg_vs_G0",
        "Gneg_vs_Gpos",
        "G0_vs_Gpos",
    ]
    final_df = final_df.reindex(columns=final_cols)

    final_df.to_csv(paths.OCCUPANCY_P_VALUES_PATH, index=False, float_format="%.4f")
    logger.info("Consolidated p-values saved to: %s", paths.OCCUPANCY_P_VALUES_PATH)
    logger.info("--- Stadium Occupancy Analysis Complete ---")
