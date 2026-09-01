"""Generate Figure 2 significance results for every schedule classification."""

import logging
from pathlib import Path

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
import numpy as np
import pandas as pd
import seaborn as sns
from scipy.stats import mannwhitneyu

from src.analysis.cliffs_delta import COMPARISONS, cliffs_delta
from src.analysis.plot_style import COLORS, apply_plot_style, save_figure
from src.analysis.schedule_classifications import iter_schedule_classifications
from src.utils import paths


logger = logging.getLogger(__name__)

MARKERS = (
    "o", "s", "^", "D", "v", "p", "*", "h", "X", "P",
    "<", ">", "8", "d", "H", "1", "2", "3", "4", "+",
)
UNFILLED_MARKERS = {"1", "2", "3", "4", "+"}


def calculate_stratified_league_effects(
    schedule_balance: pd.DataFrame,
    groups: pd.Series,
) -> pd.DataFrame:
    """Calculate season-stratified delta and pooled Mann-Whitney by league."""
    classified = schedule_balance.assign(schedule_group=groups)
    rows = []

    for league_name, league in classified.groupby("league_name", sort=True):
        balanced_league_positions = league.loc[
            league["schedule_group"].eq("balanced"),
            "final_position",
        ]
        for comparison, schedule_group in COMPARISONS.items():
            comparison_league_positions = league.loc[
                league["schedule_group"].eq(schedule_group),
                "final_position",
            ]
            weighted_delta_sum = 0.0
            valid_pair_count = 0

            for _, season in league.groupby("season_year", sort=True):
                balanced_positions = season.loc[
                    season["schedule_group"].eq("balanced"),
                    "final_position",
                ]
                comparison_positions = season.loc[
                    season["schedule_group"].eq(schedule_group),
                    "final_position",
                ]
                season_pair_count = len(comparison_positions) * len(balanced_positions)
                if season_pair_count:
                    weighted_delta_sum += (
                        cliffs_delta(comparison_positions, balanced_positions)
                        * season_pair_count
                    )
                    valid_pair_count += season_pair_count

            if not valid_pair_count:
                continue

            mann_whitney = mannwhitneyu(
                comparison_league_positions,
                balanced_league_positions,
                alternative="two-sided",
                method="asymptotic",
            )
            p_value = float(mann_whitney.pvalue)
            rows.append(
                {
                    "league_name": league_name,
                    "comparison": comparison,
                    "delta_L": weighted_delta_sum / valid_pair_count,
                    "valid_pair_count": valid_pair_count,
                    "mann_whitney_u": float(mann_whitney.statistic),
                    "p_value": p_value,
                    "negative_log10_p_value": -np.log10(p_value),
                }
            )

    return pd.DataFrame(rows)


def plot_significancy(results: pd.DataFrame, output_path: Path) -> None:
    """Render the notebook's league-level significance figure."""
    apply_plot_style()
    league_names = sorted(results["league_name"].unique())
    league_colors = dict(
        zip(league_names, sns.color_palette("tab20", n_colors=len(league_names)))
    )
    league_markers = {
        league_name: MARKERS[index % len(MARKERS)]
        for index, league_name in enumerate(league_names)
    }
    figure, axes = plt.subplots(
        2,
        1,
        figsize=(8, 6),
        sharex=True,
        sharey=True,
        gridspec_kw={"hspace": 0.24},
    )
    significance_x = -np.log10(0.05)

    for axis, comparison in zip(axes, COMPARISONS, strict=True):
        panel = results.loc[results["comparison"].eq(comparison)]
        for row in panel.itertuples(index=False):
            marker = league_markers[row.league_name]
            scatter_style = {
                "color": league_colors[row.league_name],
                "marker": marker,
                "linewidth": 0.6,
                "s": 52,
                "zorder": 3,
            }
            if marker not in UNFILLED_MARKERS:
                scatter_style["edgecolor"] = "#555555"
            axis.scatter(row.negative_log10_p_value, row.delta_L, **scatter_style)

        axis.axvline(
            significance_x,
            color=COLORS["red"],
            linestyle="--",
            linewidth=1.3,
        )
        axis.axhline(0.147, color=COLORS["purple"], linestyle="--", linewidth=1.2)
        axis.axhline(-0.147, color=COLORS["purple"], linestyle="--", linewidth=1.2)
        axis.set_title(comparison, loc="left")
        axis.set_ylim(-0.75, 0.75)
        axis.set_yticks([-0.50, -0.25, 0.00, 0.25, 0.50])

    axes[0].text(significance_x + 0.03, 0.62, r"$p = 0.05$", color=COLORS["red"])
    axes[0].text(
        -0.25,
        0.17,
        r"$|\delta| < 0.147$: neg.",
        color=COLORS["purple"],
        fontsize=8,
    )
    axes[1].set_xlim(-0.30, 7.10)
    axes[1].set_xticks(range(8))
    axes[1].set_xlabel(r"$-\log_{10}(p\mathrm{-value})$")
    figure.supylabel(r"Cliff's $\delta$", x=0.02)

    legend_handles = [
        Line2D(
            [0],
            [0],
            linestyle="none",
            marker=league_markers[league_name],
            markerfacecolor=league_colors[league_name],
            markeredgecolor="#555555",
            markersize=6,
            label=league_name,
        )
        for league_name in league_names
    ]
    figure.legend(
        handles=legend_handles,
        title="League",
        loc="center left",
        bbox_to_anchor=(0.75, 0.52),
        edgecolor="#D8D8D8",
        fontsize=8,
        title_fontsize=9,
    )
    figure.subplots_adjust(left=0.11, right=0.72, bottom=0.12, top=0.96)
    save_figure(figure, output_path)


def write_significancy_analysis(
    schedule_balance: pd.DataFrame,
    output_dir: Path,
) -> list[Path]:
    """Write significance data and plot into each classification folder."""
    output_paths = []

    for classification, groups in iter_schedule_classifications(schedule_balance):
        results = calculate_stratified_league_effects(schedule_balance, groups)
        classification_dir = output_dir / classification
        classification_dir.mkdir(parents=True, exist_ok=True)
        data_path = classification_dir / "significancy_analysis.csv"
        plot_path = classification_dir / "significancy_analysis.png"
        results.to_csv(data_path, index=False)
        plot_significancy(results, plot_path)
        output_paths.extend((data_path, plot_path))

    return output_paths


def generate_significancy_analysis() -> list[Path]:
    """Generate Figure 2 outputs for both strength proxies."""
    market_balance = pd.read_csv(paths.SPEARMAN_BALANCE_MARKET_PATH)
    ranking_balance = pd.read_csv(paths.SPEARMAN_BALANCE_RANKING_PATH)

    market_output_paths = write_significancy_analysis(
        market_balance,
        paths.SIGNIFICANCY_ANALYSIS_MARKET_DIR,
    )
    ranking_output_paths = write_significancy_analysis(
        ranking_balance,
        paths.SIGNIFICANCY_ANALYSIS_FINAL_RANKING_DIR,
    )
    output_paths = market_output_paths + ranking_output_paths
    logger.info("Generated %d significance-analysis outputs.", len(output_paths))
    return output_paths
