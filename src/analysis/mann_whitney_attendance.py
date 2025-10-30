import logging
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick

from pathlib import Path
from itertools import combinations
from typing import Dict, List, Optional, TYPE_CHECKING

from src.utils import paths
from scipy.stats import mannwhitneyu

if TYPE_CHECKING:
    from pandas import DataFrame


logger = logging.getLogger(__name__)


GROUP_ORDER = ["G-", "G0", "G+"]
COMPARISON_PAIRS = [("G-", "G0"), ("G-", "G+"), ("G0", "G+")]
GROUP_MAP = {"unbalanced_weak": "G-", "balanced": "G0", "unbalanced_strong": "G+"}

P_VALUE_COLS = [f"{g1}_vs_{g2}" for g1, g2 in COMPARISON_PAIRS]

G_TYPE_COLUMN_TO_ANALYZE = "G_type_0.300"
ATTENDANCE_COLUMN = "audience_filled_fb"


def _calculate_occupancy(games_df: pd.DataFrame, attendance_col: str) -> "DataFrame":
    """
    Calculates the average stadium occupancy for each team-season combination.

    This function filters home games, calculates mean and max attendance,
    computes the average occupancy (mean/max), and saves an audit CSV.

    Args:
        games_df: DataFrame containing individual game records.
        attendance_col: The name of the column to use for attendance data.

    Returns:
        A DataFrame with ['league_name', 'season_year', 'team_canonical',
        'average_occupancy'].
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

    paths.MANN_WHITNEY_ATT_AUDIT_DIR.mkdir(parents=True, exist_ok=True)
    audit_path = (
        paths.MANN_WHITNEY_ATT_AUDIT_DIR / f"occupancy_audit_{attendance_col}.csv"
    )
    occupancy_stats.to_csv(audit_path, index=False, float_format="%.2f")
    logger.info("Occupancy audit table saved to: %s", audit_path)

    return occupancy_stats[
        ["league_name", "season_year", "team_canonical", "average_occupancy"]
    ]


def _run_tests_on_subset(data: pd.DataFrame) -> Optional["DataFrame"]:
    """
    Runs pairwise Mann-Whitney U tests on a subset of data.

    Compares the 'average_occupancy' metric between predefined groups (G-, G0, G+).

    Args:
        data: A DataFrame subset for a specific league/season, containing
              'group_name' and 'average_occupancy' columns.

    Returns:
        A DataFrame with a single row containing p-values for each
        comparison pair, or None if insufficient data.
    """
    groups = {
        name: group_data["average_occupancy"]
        for name, group_data in data.groupby("group_name")
    }
    if len(groups) < 2:
        return None

    p_values = {}
    for g1, g2 in COMPARISON_PAIRS:
        col_name = f"{g1}_vs_{g2}"
        if g1 in groups and g2 in groups:
            if groups[g1].empty or groups[g2].empty:
                p_values[col_name] = None
            else:
                _, p_value = mannwhitneyu(
                    groups[g1], groups[g2], alternative="two-sided"
                )
                p_values[col_name] = p_value
        else:
            p_values[col_name] = None

    p_values_df = pd.DataFrame([p_values])
    return p_values_df.astype(float)


def _calculate_significance_stats(group: pd.DataFrame) -> pd.Series:
    """
    Calculates significance statistics for a group of p-values.

    Counts total tests, significant tests (p < 0.05), and provides
    a breakdown of significant results by comparison pair.

    Args:
        group: A DataFrame of p-values, typically the 'melted' DataFrame
               containing 'p_value' and 'comparison' columns.

    Returns:
        A pandas Series containing the calculated statistics
        (e.g., 'total_comparisons', 'significant_percentage').
    """
    if group.empty:
        return pd.Series(dtype="float64")

    stats = {}
    total_tests = len(group)
    significant_mask = group["p_value"] < 0.05
    total_significant = significant_mask.sum()

    stats["total_comparisons"] = total_tests
    stats["significant_comparisons"] = total_significant

    if total_tests > 0:
        stats["significant_percentage"] = total_significant / total_tests
    else:
        stats["significant_percentage"] = 0.0

    stats["significant_G-_vs_G0"] = significant_mask[
        group["comparison"] == "G-_vs_G0"
    ].sum()
    stats["significant_G-_vs_G+"] = significant_mask[
        group["comparison"] == "G-_vs_G+"
    ].sum()
    stats["significant_G0_vs_G+"] = significant_mask[
        group["comparison"] == "G0_vs_G+"
    ].sum()

    return pd.Series(stats)


def _generate_occupancy_boxplot(
    df: pd.DataFrame, output_path: Path, title: str
) -> None:
    """
    (PLOT 1) Generates and saves a three-group boxplot for occupancy.

    Args:
        df: The DataFrame containing data to plot (must include
            'group_name' and 'average_occupancy').
        output_path: The Path object where the plot image will be saved.
        title: The title for the chart.
    """
    if df.empty or df["group_name"].nunique() < 2:
        logger.warning(
            "Skipping occupancy boxplot generation for '%s' (insufficient data).", title
        )
        return

    logger.info("Generating occupancy boxplot: %s", title)
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
    plt.xlabel("Grupo de Tabela (Schedule Group)", fontsize=12)
    plt.ylabel("Ocupação Média do Estádio", fontsize=12)
    plt.ylim(0, 1.05)
    plt.gca().yaxis.set_major_formatter(mtick.PercentFormatter(1.0))
    plt.tight_layout()

    plt.savefig(output_path)
    plt.close()
    logger.info("Occupancy boxplot saved to: %s", output_path)


def _plot_violin_occupancy_G_vs_Gplus(
    df: pd.DataFrame, output_path: Path, title: str
) -> None:
    """
    (PLOT 2 - VIOLIN) Generates a violin plot comparing G- and G+ occupancy.

    This plot is designed to show the full distribution shape of average
    occupancy for the two extreme groups.

    Args:
        df: The DataFrame containing data to plot (must include
            'group_name' and 'average_occupancy').
        output_path: The Path object where the plot image will be saved.
        title: The title for the chart.
    """
    logger.info("Generating occupancy violin plot (G- vs G+): %s", title)

    df_plot = df[df["group_name"].isin(["G-", "G+"])].copy()

    if df_plot.empty or df_plot["group_name"].nunique() < 2:
        logger.warning(
            "Skipping occupancy violin plot generation for '%s' (insufficient data).",
            title,
        )
        return

    plt.rcParams["font.family"] = "DejaVu Sans"
    plt.figure(figsize=(10, 7))
    sns.set_style("whitegrid", {"axes.grid": True, "grid.linestyle": "--"})

    palette = {"G-": "#91cf60", "G+": "#d01c8b"}

    sns.violinplot(
        x="group_name",
        y="average_occupancy",
        data=df_plot,
        order=["G-", "G+"],
        palette=palette,
        hue="group_name",
        legend=False,
        inner="quartile",
        cut=0,
    )

    plt.ylim(0, 1.05)
    plt.gca().yaxis.set_major_formatter(mtick.PercentFormatter(1.0))

    plt.title(title, fontsize=16, pad=20)
    plt.xlabel("Grupo de Tabela (Schedule Group)", fontsize=12)
    plt.ylabel("Ocupação Média do Estádio", fontsize=12)
    plt.tight_layout()

    plt.savefig(output_path)
    plt.close()
    logger.info("Occupancy violin plot saved to: %s", output_path)


def _plot_significance_by_league(summary_df: pd.DataFrame, output_path: Path) -> None:
    """
    (PLOT 3 - Summary) Generates a bar chart of significance rate by league.

    This plot answers "Which league shows the strongest effect?" by plotting
    the 'significant_percentage' for all leagues (excluding 'all_leagues')
    from the 'all_seasons' aggregation.

    Args:
        summary_df: The aggregated summary DataFrame.
        output_path: The Path object where the plot image will be saved.
    """
    logger.info("Generating occupancy significance summary plot by league...")

    df_plot = summary_df[
        (summary_df["season_year"] == "all_seasons")
        & (summary_df["league_name"] != "all_leagues")
    ].sort_values(by="significant_percentage", ascending=False)

    if df_plot.empty:
        logger.warning("No aggregated league data to plot occupancy summary.")
        return

    plt.figure(figsize=(12, 8))
    sns.set_style("whitegrid")

    ax = sns.barplot(
        data=df_plot,
        x="significant_percentage",
        y="league_name",
        palette="coolwarm",
        hue="league_name",
        legend=False,
    )

    ax.set_title(
        f"Taxa de Significância (p < 0.05) na Ocupação por Liga\n(Limiar: {G_TYPE_COLUMN_TO_ANALYZE})",
        fontsize=16,
        pad=20,
    )
    ax.set_xlabel("Taxa de Significância (Comparações com p < 0.05)", fontsize=12)
    ax.set_ylabel("Liga", fontsize=12)
    ax.xaxis.set_major_formatter(mtick.PercentFormatter(1.0))
    ax.set_xlim(0, 1)

    for container in ax.containers:
        labels = [f"{v*100:.1f}%" if v > 0 else "" for v in container.datavalues]
        ax.bar_label(container, labels=labels, padding=5)

    plt.tight_layout()
    plt.savefig(output_path)
    plt.close()
    logger.info("Occupancy significance by league plot saved to: %s", output_path)


def _plot_pvalue_distribution(melted_df: pd.DataFrame, output_path: Path) -> None:
    """
    (PLOT 4 - Summary) Generates a stripplot of the p-value distribution.

    Shows all individual p-values from the league/season tests,
    separated by comparison pair (e.g., 'G-_vs_G0').

    Args:
        melted_df: The long-format DataFrame containing all p-values.
        output_path: The Path object where the plot image will be saved.
    """
    logger.info("Generating occupancy p-value distribution plot...")
    plt.figure(figsize=(14, 8))
    sns.set_style("whitegrid")

    ax = sns.stripplot(
        data=melted_df,
        x="comparison",
        y="p_value",
        hue="comparison",
        legend=False,
        jitter=0.2,
        alpha=0.6,
        palette="muted",
    )

    ax.axhline(
        0.05, ls="--", color="red", linewidth=2, label="Nível de Significância (0.05)"
    )

    ax.set_title(
        f"Distribuição P-Values (Ocupação por Liga/Temporada)\n(Limiar: {G_TYPE_COLUMN_TO_ANALYZE})",
        fontsize=16,
        pad=20,
    )
    ax.set_xlabel("Comparação de Grupos", fontsize=12)
    ax.set_ylabel("P-Value (Teste Mann-Whitney U)", fontsize=12)
    ax.set_ylim(-0.02, 1.02)

    handles, labels = ax.get_legend_handles_labels()
    ax.legend(handles=handles, labels=labels, loc="upper right")

    plt.tight_layout()
    plt.savefig(output_path)
    plt.close()
    logger.info("Occupancy p-value distribution plot saved to: %s", output_path)


def _analyze_occupancy_results(
    results_df: pd.DataFrame, plot_dir: Path, df_thresh: pd.DataFrame
) -> None:
    """
    Analyzes occupancy p-values, saves summary CSVs, and generates summary plots.

    This function takes the raw p-value results, calculates aggregate statistics
    (by season, by league, and overall), saves the aggregated data,
    and orchestrates the generation of summary plots
    (_plot_significance_by_league, _plot_pvalue_distribution).

    It also logs the main business answers derived from the aggregates.

    Args:
        results_df: DataFrame of raw p-values (one row per league/season).
        plot_dir: The Path directory to save summary plots.
        df_thresh: The full merged DataFrame (used for generating
                   overall boxplot/violin plot if analysis fails early).
    """
    logger.info("--- Starting Aggregated P-Value Analysis (Occupancy) ---")

    original_row_count = len(results_df)
    analysis_df = results_df.dropna(subset=P_VALUE_COLS)
    dropped_rows = original_row_count - len(analysis_df)

    if dropped_rows > 0:
        logger.info("Removed %d rows with NA p-values.", dropped_rows)

    if analysis_df.empty:
        logger.warning("No rows with a complete set of p-values found...")
        plot_path_box = (
            plot_dir / f"occupancy_dist_overall_{G_TYPE_COLUMN_TO_ANALYZE}.png"
        )
        title_box = f"Distribuição Geral da Ocupação Média\n(Limiar: {G_TYPE_COLUMN_TO_ANALYZE})"
        _generate_occupancy_boxplot(df_thresh, plot_path_box, title_box)
        plot_path_violin = (
            plot_dir
            / f"occupancy_violin_Gminus_vs_Gplus_{G_TYPE_COLUMN_TO_ANALYZE}.png"
        )
        title_violin = f"Comparação Distribuição Ocupação (G- vs G+)\n(Limiar: {G_TYPE_COLUMN_TO_ANALYZE})"
        _plot_violin_occupancy_G_vs_Gplus(df_thresh, plot_path_violin, title_violin)
        return

    melted_df = analysis_df.melt(
        id_vars=["league_name", "season_year"],
        value_vars=P_VALUE_COLS,
        var_name="comparison",
        value_name="p_value",
    )

    agg_season = (
        melted_df.groupby(["league_name", "season_year"])
        .apply(_calculate_significance_stats, include_groups=False)
        .reset_index()
    )
    agg_league = (
        melted_df.groupby("league_name")
        .apply(_calculate_significance_stats, include_groups=False)
        .reset_index()
    )
    agg_league["season_year"] = "all_seasons"
    agg_all_series = _calculate_significance_stats(melted_df)
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
    id_cols = ["league_name", "season_year"]
    metric_cols = [
        "total_comparisons",
        "significant_comparisons",
        "significant_percentage",
        "significant_G-_vs_G0",
        "significant_G-_vs_G+",
        "significant_G0_vs_G+",
    ]
    summary_df = summary_df.reindex(columns=id_cols + metric_cols)
    summary_df.sort_values(by=["league_name", "season_year"], inplace=True)
    summary_df.reset_index(drop=True, inplace=True)
    int_cols = [
        "total_comparisons",
        "significant_comparisons",
        "significant_G-_vs_G0",
        "significant_G-_vs_G+",
        "significant_G0_vs_G+",
    ]
    summary_df[int_cols] = summary_df[int_cols].fillna(0).astype(int)

    paths.MANN_WHITNEY_ATT_METRICS_DIR.mkdir(parents=True, exist_ok=True)
    output_path_csv = (
        paths.MANN_WHITNEY_ATT_METRICS_DIR
        / "per_season_mann_whitney_p_values_summary.csv"
    )
    summary_df.to_csv(output_path_csv, index=False, float_format="%.4f")
    logger.info("Occupancy significance summary CSV saved to: %s", output_path_csv)

    plot_path_league_summary = plot_dir / "summary_occupancy_significance_by_league.png"
    _plot_significance_by_league(summary_df, plot_path_league_summary)
    plot_path_pvalue_dist = plot_dir / "summary_occupancy_pvalue_distribution.png"
    _plot_pvalue_distribution(melted_df, plot_path_pvalue_dist)

    overall_stats = summary_df[
        (summary_df["league_name"] == "all_leagues")
        & (summary_df["season_year"] == "all_seasons")
    ]
    if not overall_stats.empty:
        stats = overall_stats.iloc[0]
        logger.info(
            "[Overall Occupancy Analysis] Percentage of significant comparisons (p < 0.05): %.2f%%",
            stats["significant_percentage"] * 100,
        )

    league_summary = (
        summary_df[
            (summary_df["season_year"] == "all_seasons")
            & (summary_df["league_name"] != "all_leagues")
        ]
        .set_index("league_name")["significant_percentage"]
        .sort_values(ascending=False)
    )
    if not league_summary.empty:
        logger.info(
            "[League Occupancy Analysis] League with HIGHEST effect: %s (%.2f%%)",
            league_summary.index[0],
            league_summary.iloc[0] * 100,
        )
        logger.info(
            "[League Occupancy Analysis] League with LOWEST effect: %s (%.2f%%)",
            league_summary.index[-1],
            league_summary.iloc[-1] * 100,
        )
        league_summary_log = (league_summary * 100).to_string(float_format="%.2f%%")
        logger.info(
            "Full occupancy significance rate summary by league:\n%s",
            league_summary_log,
        )

    logger.info("--- Aggregated Occupancy Analysis Concluded ---")


def run_occupancy_analysis() -> None:
    """
    Orchestrates the entire stadium occupancy analysis.

    Loads data, calculates occupancy, merges with G-coefficients,
    runs Mann-Whitney U tests for each league/season, saves raw p-values,
    and triggers the final aggregate analysis and plotting.
    """
    logger.info("--- Starting Stadium Occupancy Analysis (Mann-Whitney U) ---")

    plot_dir = paths.MANN_WHITNEY_ATT_PLOTS_DIR
    plot_dir.mkdir(parents=True, exist_ok=True)

    try:
        games_df = pd.read_csv(paths.GAMES_VALID_PATH)
        g_coeff_df = pd.read_csv(paths.SPEARMAN_BALANCE_PATH)
    except FileNotFoundError as e:
        logger.error("Required data file not found: %s", e)
        return

    if ATTENDANCE_COLUMN not in games_df.columns:
        logger.error(
            "Required attendance column not in games_df: %s", ATTENDANCE_COLUMN
        )
        return
    if G_TYPE_COLUMN_TO_ANALYZE not in g_coeff_df.columns:
        logger.error(
            "Required G_type column not in g_coeff_df: %s", G_TYPE_COLUMN_TO_ANALYZE
        )
        return

    logger.info("Processing with attendance column: [%s]", ATTENDANCE_COLUMN)
    logger.info("Processing with G_type threshold: [%s]", G_TYPE_COLUMN_TO_ANALYZE)

    occupancy_df = _calculate_occupancy(games_df, ATTENDANCE_COLUMN)

    merged_df = pd.merge(
        g_coeff_df,
        occupancy_df,
        on=["league_name", "season_year", "team_canonical"],
        how="inner",
    )

    merged_df["group_name"] = merged_df[G_TYPE_COLUMN_TO_ANALYZE].map(GROUP_MAP)
    merged_df.dropna(subset=["average_occupancy", "group_name"], inplace=True)

    if merged_df.empty:
        logger.warning("No valid data after merging and cleaning. Aborting.")
        return

    plot_path_box = plot_dir / f"occupancy_dist_overall_{G_TYPE_COLUMN_TO_ANALYZE}.png"
    title_box = (
        f"Distribuição Geral da Ocupação Média\n(Limiar: {G_TYPE_COLUMN_TO_ANALYZE})"
    )
    _generate_occupancy_boxplot(merged_df, plot_path_box, title_box)

    plot_path_violin = (
        plot_dir / f"occupancy_violin_Gminus_vs_Gplus_{G_TYPE_COLUMN_TO_ANALYZE}.png"
    )
    title_violin = f"Comparação Distribuição Ocupação (G- vs G+)\n(Limiar: {G_TYPE_COLUMN_TO_ANALYZE})"
    _plot_violin_occupancy_G_vs_Gplus(merged_df, plot_path_violin, title_violin)

    all_p_value_results: List["DataFrame"] = []

    for (league, season), season_df in merged_df.groupby(
        ["league_name", "season_year"]
    ):
        if (p_season := _run_tests_on_subset(season_df)) is not None:
            p_season["league_name"] = league
            p_season["season_year"] = season
            all_p_value_results.append(p_season)

    if not all_p_value_results:
        logger.warning("No p-value results were generated.")
        logger.info(
            "--- Stadium Occupancy Analysis Concluded (no statistical tests) ---"
        )
        return

    final_df = pd.concat(all_p_value_results, ignore_index=True)
    final_cols = ["league_name", "season_year"] + P_VALUE_COLS
    final_df = final_df.reindex(columns=final_cols)

    final_df.to_csv(paths.OCCUPANCY_P_VALUES_PATH, index=False, float_format="%.4f")
    logger.info(
        "Occupancy analysis p-values saved to: %s", paths.OCCUPANCY_P_VALUES_PATH
    )

    _analyze_occupancy_results(final_df, plot_dir, merged_df)

    logger.info("--- Stadium Occupancy Analysis Concluded ---")


if __name__ == "__main__":
    run_occupancy_analysis()
