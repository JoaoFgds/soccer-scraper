import re
import logging
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick

from pathlib import Path
from src.utils import paths
from typing import List, Dict, Any, Set


logger = logging.getLogger(__name__)


TYPE_COLORS = {
    "balanced": "#1f77b4",
    "unbalanced_strong": "#d62728",
    "unbalanced_weak": "#2ca02c",
}
TYPE_NAMES_MAP = {
    "balanced": "Balanceada",
    "unbalanced_strong": "Forte",
    "unbalanced_weak": "Fraca",
}
PLOT_TYPES = ["balanced", "unbalanced_strong", "unbalanced_weak"]

OVERALL_LABEL = "Overall"


def _plot_stacked_distribution(
    summary_df: pd.DataFrame, threshold: float, output_path: Path
):
    """
    (Graph 1) Generates a 100% stacked bar chart for a single threshold.

    This plot compares the distribution of schedule types (balanced,
    unbalanced_strong, unbalanced_weak) across all leagues and includes
    an 'Overall' summary bar.

    Args:
        summary_df: The summary DataFrame containing aggregated data
                    (must include 'all_seasons' rows).
        threshold: The specific G-coefficient threshold (e.g., 0.300)
                   to plot.
        output_path: The Path object where the plot image will be saved.
    """
    logger.info(
        "Generating 100%% stacked distribution plot for threshold %.3f...",
        threshold,
    )

    df_plot = summary_df[(summary_df["season_year"] == "all_seasons")].copy()

    if df_plot.empty:
        logger.warning("No 'all_seasons' data found to plot distribution.")
        return

    df_plot["league_name"] = df_plot["league_name"].replace(
        {"all_leagues": OVERALL_LABEL}
    )

    base_col_prefix = f"G_type_{threshold:.3f}_"

    count_cols = []
    plot_types_found = []

    for plot_type in PLOT_TYPES:
        col_name = f"{base_col_prefix}{plot_type}"
        if col_name in df_plot.columns:
            count_cols.append(col_name)
            plot_types_found.append(plot_type)
        else:
            df_plot[col_name] = 0
            count_cols.append(col_name)
            plot_types_found.append(plot_type)
            logger.debug("Column '%s' not found. Adding with zeros.", col_name)

    if not count_cols:
        logger.warning(
            "No valid count columns found for threshold %.3f. Skipping plot.",
            threshold,
        )
        return

    df_plot["total_tests_safe"] = df_plot["total_tests"].replace(0, 1)

    prop_cols = {}
    for col in count_cols:
        prop_col_name = f"prop_{col}"
        df_plot[prop_col_name] = df_plot[col] / df_plot["total_tests_safe"]
        prop_cols[col] = prop_col_name

    df_to_plot = df_plot[["league_name"] + list(prop_cols.values())].set_index(
        "league_name"
    )

    if OVERALL_LABEL in df_to_plot.index:
        other_leagues = sorted(
            [idx for idx in df_to_plot.index if idx != OVERALL_LABEL]
        )
        df_to_plot = df_to_plot.reindex(other_leagues + [OVERALL_LABEL])
    else:
        df_to_plot.sort_index(inplace=True)

    df_to_plot.columns = [TYPE_NAMES_MAP.get(pt, pt) for pt in plot_types_found]
    plot_colors = [TYPE_COLORS.get(pt) for pt in plot_types_found]

    sns.set_theme(style="whitegrid")
    ax = df_to_plot.plot(
        kind="bar", stacked=True, figsize=(12, 7), color=plot_colors, width=0.8
    )

    ax.set_title(
        f"Distribuição de Tipos de Tabela por Liga (Limiar G = {threshold:.3f})",
        fontsize=16,
        pad=20,
    )
    ax.set_xlabel("Liga", fontsize=12)
    ax.set_ylabel("Proporção de Tabelas", fontsize=12)
    ax.yaxis.set_major_formatter(mtick.PercentFormatter(1.0))
    ax.set_ylim(0, 1)

    plt.xticks(rotation=45, ha="right")

    ax.legend(
        title="Tipo de Tabela",
        bbox_to_anchor=(1.02, 1),
        loc="upper left",
        borderaxespad=0.0,
    )

    plt.tight_layout(rect=[0, 0, 0.85, 1])
    plt.savefig(output_path)
    plt.close()
    logger.info("Plot saved to: %s", output_path)


def _plot_threshold_sensitivity(
    summary_df: pd.DataFrame, thresholds: List[float], output_path: Path
):
    """
    (Graph 2) Generates a line plot showing classification sensitivity.

    This plot visualizes how the proportion of each schedule type
    (balanced, unbalanced_strong, unbalanced_weak) changes as the
    G-coefficient threshold increases. It plots a separate line for
    the 'Overall' data and for each individual league.

    Args:
        summary_df: The summary DataFrame containing aggregated data.
        thresholds: A list of all thresholds (e.g., [0.2, 0.25])
                    used for the x-axis.
        output_path: The Path object where the plot image will be saved.
    """
    logger.info("Generating threshold sensitivity line plot...")

    df_filtered = summary_df[(summary_df["season_year"] == "all_seasons")].copy()

    if df_filtered.empty:
        logger.warning("No 'all_seasons' data found to plot sensitivity.")
        return

    df_filtered["league_name"] = df_filtered["league_name"].replace(
        {"all_leagues": OVERALL_LABEL}
    )

    id_vars = ["league_name", "total_tests"]
    value_vars = [col for col in df_filtered.columns if col.startswith("G_type_")]
    df_melted = df_filtered.melt(
        id_vars=id_vars,
        value_vars=value_vars,
        var_name="g_type_full",
        value_name="count",
    )

    df_melted["total_tests_safe"] = df_melted["total_tests"].replace(0, 1)
    df_melted["percentage"] = df_melted["count"] / df_melted["total_tests_safe"]

    regex_pattern = r"G_type_(\d+\.\d+)_(balanced|unbalanced_strong|unbalanced_weak)$"
    extract_df = df_melted["g_type_full"].str.extract(regex_pattern)

    df_melted["threshold"] = pd.to_numeric(extract_df[0])
    df_melted["g_type"] = extract_df[1].map(TYPE_NAMES_MAP)

    df_plot = df_melted.dropna(subset=["threshold", "g_type", "percentage"])

    sns.set_theme(style="whitegrid")
    plt.figure(figsize=(14, 8))

    mapped_palette = {
        portuguese_name: TYPE_COLORS[english_key]
        for english_key, portuguese_name in TYPE_NAMES_MAP.items()
        if english_key in TYPE_COLORS
    }

    df_leagues = df_plot[df_plot["league_name"] != OVERALL_LABEL]
    df_overall = df_plot[df_plot["league_name"] == OVERALL_LABEL]

    ax = sns.lineplot(
        data=df_leagues,
        x="threshold",
        y="percentage",
        hue="g_type",
        style="league_name",
        markers=True,
        markersize=7,
        linewidth=2,
        palette=mapped_palette,
        dashes=False,
    )

    if not df_overall.empty:
        sns.lineplot(
            data=df_overall,
            ax=ax,
            x="threshold",
            y="percentage",
            hue="g_type",
            style="league_name",
            markers=True,
            markersize=10,
            linewidth=4,
            palette=mapped_palette,
            dashes=False,
            legend=False,
        )

    ax.set_title(
        "Análise de Sensibilidade: Proporção de Tipos de Tabela vs. Limiar (G)",
        fontsize=16,
        pad=20,
    )
    ax.set_xlabel("Limiar de Correlação (G)", fontsize=12)
    ax.set_ylabel("Proporção de Tabelas", fontsize=12)

    ax.yaxis.set_major_formatter(mtick.PercentFormatter(1.0))
    ax.set_xticks(thresholds)
    ax.set_ylim(0, 1)

    try:
        handles, labels = ax.get_legend_handles_labels()

        idx_gtype_start = labels.index("g_type")
        idx_league_name_start = -1
        for i, label in enumerate(labels[idx_gtype_start + 1 :]):
            if label == "league_name":
                idx_league_name_start = idx_gtype_start + 1 + i
                break

        if idx_league_name_start == -1:
            raise ValueError("Simple legend format.")

        g_type_handles = handles[idx_gtype_start + 1 : idx_league_name_start]
        g_type_labels = labels[idx_gtype_start + 1 : idx_league_name_start]

        league_name_handles = handles[idx_league_name_start + 1 :]
        league_name_labels = labels[idx_league_name_start + 1 :]

        legend1 = ax.legend(
            g_type_handles,
            g_type_labels,
            title="Tipo de Tabela",
            bbox_to_anchor=(1.02, 1),
            loc="upper left",
            borderaxespad=0.0,
        )

        ax.add_artist(legend1)
        legend2 = ax.legend(
            league_name_handles,
            league_name_labels,
            title="Liga",
            bbox_to_anchor=(1.02, 0.65),
            loc="upper left",
            borderaxespad=0.0,
        )
    except ValueError:
        ax.legend(bbox_to_anchor=(1.02, 1), loc="upper left", borderaxespad=0.0)

    plt.tight_layout(rect=[0, 0, 0.8, 1])
    plt.savefig(output_path)
    plt.close()
    logger.info("Plot saved to: %s", output_path)


def _plot_significance_rate(summary_df: pd.DataFrame, output_path: Path):
    """
    (Graph 3) Generates a horizontal bar chart of the significance rate.

    This plot shows the proportion of statistically significant (p <= 0.05)
    G-coefficients for each league and 'Overall', based on the
    'all_seasons' aggregation. The 'Overall' bar is highlighted.

    Args:
        summary_df: The summary DataFrame containing aggregated data.
        output_path: The Path object where the plot image will be saved.
    """
    logger.info("Generating significance rate bar plot...")

    df_plot = summary_df[(summary_df["season_year"] == "all_seasons")].copy()

    if df_plot.empty:
        logger.warning("No 'all_seasons' data found to plot significance.")
        return

    df_plot["league_name"] = df_plot["league_name"].replace(
        {"all_leagues": OVERALL_LABEL}
    )

    df_plot["total_tests_safe"] = df_plot["total_tests"].replace(0, 1)
    df_plot["significance_rate"] = (
        df_plot["significant_tests"] / df_plot["total_tests_safe"]
    )

    df_leagues = df_plot[df_plot["league_name"] != OVERALL_LABEL].sort_values(
        "significance_rate", ascending=False
    )
    df_overall = df_plot[df_plot["league_name"] == OVERALL_LABEL]

    df_plot = pd.concat([df_overall, df_leagues], ignore_index=True)

    sns.set_theme(style="whitegrid")
    plt.figure(figsize=(10, 6))

    palette = {lg: "#aaaaaa" for lg in df_plot["league_name"] if lg != OVERALL_LABEL}
    palette[OVERALL_LABEL] = "#3498db"

    ax = sns.barplot(
        data=df_plot,
        x="significance_rate",
        y="league_name",
        palette=palette,
        hue="league_name",
        legend=False,
    )

    ax.set_title(
        "Taxa de Significância (p <= 0.05) por Liga (Todas as Temporadas)",
        fontsize=16,
        pad=20,
    )
    ax.set_xlabel("Taxa de Significância", fontsize=12)
    ax.set_ylabel("Liga", fontsize=12)
    ax.xaxis.set_major_formatter(mtick.PercentFormatter(1.0))
    ax.set_xlim(0, 1)

    for container in ax.containers:
        labels = [f"{v*100:.1f}%" if v > 0 else "" for v in container.datavalues]

        ax.bar_label(container, labels=labels, padding=5)

    plt.tight_layout()
    plt.savefig(output_path)
    plt.close()
    logger.info("Plot saved to: %s", output_path)


def generate_all_visualizations():
    """
    Orchestrates the generation of all summary analysis visualizations.

    This main function loads the summary CSV, detects the correlation
    thresholds used from the column names, and then calls the respective
    plotting functions to generate:
    1. A stacked bar chart for each threshold.
    2. A line plot for threshold sensitivity.
    3. A bar chart for p-value significance rates.
    """
    logger.info("Starting generation of analysis visualizations...")

    summary_path = paths.SPEARMAN_SUMMARY_PATH
    output_dir = paths.SPEARMAN_COEFFICIENT_PLOTS

    try:
        output_dir.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        logger.error("Failed to create plot directory: %s", e)
        return

    try:
        df = pd.read_csv(summary_path)
        logger.info("Loaded '%s' with %d rows.", summary_path, len(df))
    except FileNotFoundError:
        logger.error("Summary file not found: %s. Aborting.", summary_path)
        return
    except pd.errors.EmptyDataError:
        logger.error("Summary file '%s' is empty. Aborting.", summary_path)
        return

    g_type_cols = [col for col in df.columns if col.startswith("G_type_")]

    thresholds_str = set()
    for col in g_type_cols:
        match = re.search(r"G_type_(\d+\.\d+)_", col)
        if match:
            thresholds_str.add(match.group(1))

    try:
        thresholds = sorted([float(t) for t in thresholds_str])
    except (ValueError, IndexError) as e:
        logger.error(
            "Failed to parse thresholds from columns: %s. Columns: %s", e, g_type_cols
        )
        return

    if not thresholds:
        logger.error("No thresholds detected in columns. Aborting.")
        return

    logger.info("Detected thresholds for analysis: %s", thresholds)

    for t in thresholds:
        fname = output_dir / f"1_plot_distribuicao_thresh_{t:.3f}.png"
        _plot_stacked_distribution(df, t, fname)

    fname_sens = output_dir / "2_plot_sensibilidade_limiar.png"
    _plot_threshold_sensitivity(df, thresholds, fname_sens)

    fname_sig = output_dir / "3_plot_taxa_significancia.png"
    _plot_significance_rate(df, fname_sig)

    logger.info("Visualization generation complete.")
