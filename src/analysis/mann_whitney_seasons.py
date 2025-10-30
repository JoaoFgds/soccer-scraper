import logging
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick

from pathlib import Path
from src.utils import paths
from itertools import combinations
from scipy.stats import mannwhitneyu
from typing import Optional, List, Dict, Any, TYPE_CHECKING


if TYPE_CHECKING:
    from pandas import DataFrame


logger = logging.getLogger(__name__)


GROUP_MAP = {"unbalanced_weak": "G-", "balanced": "G0", "unbalanced_strong": "G+"}
GROUP_ORDER = ["G-", "G0", "G+"]
COMPARISON_PAIRS = [("G-", "G0"), ("G-", "G+"), ("G0", "G+")]
P_VALUE_COLS = [f"{g1}_vs_{g2}" for g1, g2 in COMPARISON_PAIRS]


G_TYPE_COLUMN_TO_ANALYZE = "G_type_0.300"


def _run_tests_on_subset(data: pd.DataFrame) -> Optional["DataFrame"]:
    """
    Runs pairwise Mann-Whitney U tests on a subset of data.

    Compares the 'final_position' metric between predefined groups (G-, G0, G+).

    Args:
        data: A DataFrame subset for a specific league/season, containing
              'group_name' and 'final_position' columns.

    Returns:
        A DataFrame with a single row containing p-values for each
        comparison pair, or None if insufficient data.
    """
    groups = {
        name: group_data["final_position"]
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
        (e.g., 'total_tests', 'significant_percentage').
    """
    if group.empty:
        return pd.Series(dtype="float64")

    stats = {}
    total_tests = len(group)
    significant_mask = group["p_value"] < 0.05
    total_significant = significant_mask.sum()

    stats["total_tests"] = total_tests
    stats["significant_tests"] = total_significant

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


def _generate_boxplot(df: pd.DataFrame, output_path: Path, title: str) -> None:
    """
    (PLOT 1) Generates and saves a three-group boxplot for final position.

    Args:
        df: The DataFrame containing data to plot (must include
            'group_name' and 'final_position').
        output_path: The Path object where the plot image will be saved.
        title: The title for the chart.
    """
    if df.empty or df["group_name"].nunique() < 2:
        logger.warning(
            "Skipping boxplot generation for '%s' (insufficient data).", title
        )
        return

    logger.info("Generating boxplot: %s", title)
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

    plt.gca().invert_yaxis()
    plt.title(title, fontsize=16, pad=20)
    plt.xlabel("Grupo de Tabela (Schedule Group)", fontsize=12)
    plt.ylabel("Posição Final no Torneio (Rank)", fontsize=12)
    plt.tight_layout()

    plt.savefig(output_path)
    plt.close()
    logger.info("Boxplot saved to: %s", output_path)


def _plot_violin_G_vs_Gplus(df: pd.DataFrame, output_path: Path, title: str) -> None:
    """
    (PLOT 2 - VIOLIN) Generates a violin plot comparing G- and G+ final position.

    This plot is designed to show the full distribution shape of final ranks
    for the two extreme groups.

    Args:
        df: The DataFrame containing data to plot (must include
            'group_name' and 'final_position').
        output_path: The Path object where the plot image will be saved.
        title: The title for the chart.
    """
    logger.info("Generating violin plot (G- vs G+): %s", title)

    df_plot = df[df["group_name"].isin(["G-", "G+"])].copy()

    if df_plot.empty or df_plot["group_name"].nunique() < 2:
        logger.warning(
            "Skipping violin plot generation for '%s' (insufficient data).", title
        )
        return

    plt.rcParams["font.family"] = "DejaVu Sans"
    plt.figure(figsize=(10, 7))
    sns.set_style("whitegrid", {"axes.grid": True, "grid.linestyle": "--"})

    palette = {"G-": "#91cf60", "G+": "#d01c8b"}

    sns.violinplot(
        x="group_name",
        y="final_position",
        data=df_plot,
        order=["G-", "G+"],
        palette=palette,
        hue="group_name",
        legend=False,
        inner="quartile",
        cut=0,
    )

    plt.gca().invert_yaxis()

    plt.title(title, fontsize=16, pad=20)
    plt.xlabel("Grupo de Tabela (Schedule Group)", fontsize=12)
    plt.ylabel("Posição Final no Torneio (Rank)", fontsize=12)
    plt.tight_layout()

    plt.savefig(output_path)
    plt.close()
    logger.info("Violin plot saved to: %s", output_path)


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
    logger.info("Generating significance summary plot by league...")

    df_plot = summary_df[
        (summary_df["season_year"] == "all_seasons")
        & (summary_df["league_name"] != "all_leagues")
    ].sort_values(by="significant_percentage", ascending=False)

    if df_plot.empty:
        logger.warning("No aggregated league data to plot summary.")
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
        f"Taxa de Significância (p < 0.05) por Liga\n(Limiar: {G_TYPE_COLUMN_TO_ANALYZE})",
        fontsize=16,
        pad=20,
    )
    ax.set_xlabel("Taxa de Significância (Testes com p < 0.05)", fontsize=12)
    ax.set_ylabel("Liga", fontsize=12)
    ax.xaxis.set_major_formatter(mtick.PercentFormatter(1.0))
    ax.set_xlim(0, 1)

    for container in ax.containers:
        labels = [f"{v*100:.1f}%" if v > 0 else "" for v in container.datavalues]
        ax.bar_label(container, labels=labels, padding=5)

    plt.tight_layout()
    plt.savefig(output_path)
    plt.close()
    logger.info("Significance by league plot saved to: %s", output_path)


def _plot_pvalue_distribution(melted_df: pd.DataFrame, output_path: Path) -> None:
    """
    (PLOT 4 - Summary) Generates a stripplot of the p-value distribution.

    Shows all individual p-values from the league/season tests,
    separated by comparison pair (e.g., 'G-_vs_G0').

    Args:
        melted_df: The long-format DataFrame containing all p-values.
        output_path: The Path object where the plot image will be saved.
    """
    logger.info("Generating p-value distribution plot...")
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
        f"Distribuição de Todos P-Values (Testes por Liga/Temporada)\n"
        f"(Limiar: {G_TYPE_COLUMN_TO_ANALYZE})",
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
    logger.info("P-value distribution plot saved to: %s", output_path)


def _analyze_and_log_results(results_df: pd.DataFrame, plot_dir: Path) -> None:
    """
    Analyzes p-values, saves summary CSVs, and generates summary plots.

    This function takes the raw p-value results, calculates aggregate statistics
    (by season, by league, and overall), saves the aggregated data,
    and orchestrates the generation of summary plots
    (_plot_significance_by_league, _plot_pvalue_distribution).

    It also logs the main business answers derived from the aggregates.

    Args:
        results_df: DataFrame of raw p-values (one row per league/season).
        plot_dir: The Path directory to save summary plots.
    """
    logger.info(
        "--- Starting Aggregated P-Value Analysis (for %s) ---",
        G_TYPE_COLUMN_TO_ANALYZE,
    )

    original_row_count = len(results_df)
    analysis_df = results_df.dropna(subset=P_VALUE_COLS)
    dropped_rows = original_row_count - len(analysis_df)

    if dropped_rows > 0:
        logger.info(
            "Removed %d rows (seasons) with null p-values "
            "(incomplete tests) before analysis. "
            "%d complete seasons remaining.",
            dropped_rows,
            len(analysis_df),
        )

    if analysis_df.empty:
        logger.warning("No rows with a complete set of p-values found to analyze.")
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
        "total_tests",
        "significant_tests",
        "significant_percentage",
        "significant_G-_vs_G0",
        "significant_G-_vs_G+",
        "significant_G0_vs_G+",
    ]
    summary_df = summary_df.reindex(columns=id_cols + metric_cols)
    summary_df.sort_values(by=["league_name", "season_year"], inplace=True)
    summary_df.reset_index(drop=True, inplace=True)
    int_cols = [
        "total_tests",
        "significant_tests",
        "significant_G-_vs_G0",
        "significant_G-_vs_G+",
        "significant_G0_vs_G+",
    ]
    summary_df[int_cols] = summary_df[int_cols].fillna(0).astype(int)

    summary_df.to_csv(paths.OVERALL_MANN_WHITNEY_PATH, index=False, float_format="%.4f")
    logger.info(
        "Significance summary CSV saved to: %s",
        paths.OVERALL_MANN_WHITNEY_PATH,
    )

    plot_path_league_summary = plot_dir / "summary_significance_by_league.png"
    _plot_significance_by_league(summary_df, plot_path_league_summary)

    plot_path_pvalue_dist = plot_dir / "summary_pvalue_distribution.png"
    _plot_pvalue_distribution(melted_df, plot_path_pvalue_dist)

    overall_stats = summary_df[
        (summary_df["league_name"] == "all_leagues")
        & (summary_df["season_year"] == "all_seasons")
    ]

    if not overall_stats.empty:
        stats = overall_stats.iloc[0]
        logger.info(
            "[Overall Analysis (Threshold %s)] "
            "Percentage of significant tests (p < 0.05): "
            "%.2f%% "
            "(%d of %d tests)",
            G_TYPE_COLUMN_TO_ANALYZE,
            stats["significant_percentage"] * 100,
            stats["significant_tests"],
            stats["total_tests"],
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
            "[League Analysis (Threshold %s)] "
            "League with HIGHEST effect (most significant tests): "
            "%s (%.2f%%)",
            G_TYPE_COLUMN_TO_ANALYZE,
            league_summary.index[0],
            league_summary.iloc[0] * 100,
        )
        logger.info(
            "[League Analysis (Threshold %s)] "
            "League with LOWEST effect (least significant tests): "
            "%s (%.2f%%)",
            G_TYPE_COLUMN_TO_ANALYZE,
            league_summary.index[-1],
            league_summary.iloc[-1] * 100,
        )
        league_summary_log = (league_summary * 100).to_string(float_format="%.2f%%")
        logger.info(
            "Full significance rate summary by league (for %s):\n%s",
            G_TYPE_COLUMN_TO_ANALYZE,
            league_summary_log,
        )

    logger.info("--- Aggregated Analysis Concluded ---")


def run_seasons_analysis() -> None:
    """
    Orchestrates the statistical analysis of schedule balance on final ranks.

    This function focuses on a single G-type threshold (G_type_0.300).
    It loads the data, generates overall distribution plots (boxplot and violin),
    then iterates through each league/season to perform Mann-Whitney U tests.
    Finally, it aggregates all p-values, saves the raw and summary
    results, and logs the key findings.
    """
    logger.info("Starting statistical significance analysis (Mann-Whitney U).")

    plot_dir = paths.MANN_WHITNEY_PLOTS_DIR
    plot_dir.mkdir(parents=True, exist_ok=True)

    try:
        df = pd.read_csv(paths.SPEARMAN_BALANCE_PATH)
        logger.info("Loaded '%s' with %d rows.", paths.SPEARMAN_BALANCE_PATH, len(df))
    except FileNotFoundError:
        logger.error(
            "Input file not found: %s. Aborting analysis.",
            paths.SPEARMAN_BALANCE_PATH,
        )
        return

    if G_TYPE_COLUMN_TO_ANALYZE not in df.columns:
        logger.error(
            "Analysis column '%s' not found in '%s'. Aborting.",
            G_TYPE_COLUMN_TO_ANALYZE,
            paths.SPEARMAN_BALANCE_PATH,
        )
        return

    all_season_results: List["DataFrame"] = []

    logger.info("--- Processing single threshold: %s ---", G_TYPE_COLUMN_TO_ANALYZE)

    df_thresh = df[
        ["league_name", "season_year", "final_position", G_TYPE_COLUMN_TO_ANALYZE]
    ].copy()
    df_thresh["group_name"] = df_thresh[G_TYPE_COLUMN_TO_ANALYZE].map(GROUP_MAP)
    df_thresh.dropna(subset=["final_position", "group_name"], inplace=True)

    if df_thresh.empty:
        logger.warning(
            "No valid data for '%s'. Ending analysis.", G_TYPE_COLUMN_TO_ANALYZE
        )
        return

    plot_path_box = plot_dir / f"rank_dist_overall_{G_TYPE_COLUMN_TO_ANALYZE}.png"
    title = f"Distribuição Geral do Rank Final (Limiar: {G_TYPE_COLUMN_TO_ANALYZE})"
    _generate_boxplot(df_thresh, plot_path_box, title)

    plot_path_violin = (
        plot_dir / f"violin_Gminus_vs_Gplus_{G_TYPE_COLUMN_TO_ANALYZE}.png"
    )
    title_violin = (
        f"Comparação da Distribuição de Ranks (G- vs G+)\n"
        f"(Limiar: {G_TYPE_COLUMN_TO_ANALYZE})"
    )
    _plot_violin_G_vs_Gplus(df_thresh, plot_path_violin, title_violin)

    for (league, season), season_df in df_thresh.groupby(
        ["league_name", "season_year"]
    ):
        if (p_season := _run_tests_on_subset(season_df)) is not None:
            p_season["league_name"] = league
            p_season["season_year"] = season
            all_season_results.append(p_season)

    if not all_season_results:
        logger.warning("Analysis complete, but no statistical results were generated.")
        return

    results_df = pd.concat(all_season_results, ignore_index=True)

    id_cols = ["league_name", "season_year"]
    results_df = results_df.reindex(columns=id_cols + P_VALUE_COLS)

    results_df.to_csv(
        paths.PER_SEASON_MANN_WHITNEY_PATH, index=False, float_format="%.4f"
    )
    logger.info(
        "P-value results for '%s' saved to: %s",
        G_TYPE_COLUMN_TO_ANALYZE,
        paths.PER_SEASON_MANN_WHITNEY_PATH,
    )

    _analyze_and_log_results(results_df, plot_dir)

    logger.info("--- Tournament Efficacy Analysis Concluded ---")


if __name__ == "__main__":
    run_seasons_analysis()
