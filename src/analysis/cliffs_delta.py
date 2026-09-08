"""Generate Cliff's delta results and Figure 1 for every classification."""

import logging
from pathlib import Path

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

from src.analysis.plot_style import COLORS, apply_plot_style, save_figure
from src.analysis.schedule_classifications import iter_schedule_classifications
from src.utils import paths


logger = logging.getLogger(__name__)

COMPARISONS = {
    r"$G^-$ vs $G^0$": "unbalanced_weak",
    r"$G^+$ vs $G^0$": "unbalanced_strong",
}


def cliffs_delta(
    comparison_ranks: pd.Series,
    balanced_ranks: pd.Series,
) -> float | None:
    """Return pairwise rank dominance, where negative means better ranks."""
    comparison_count = len(comparison_ranks) * len(balanced_ranks)
    if comparison_count == 0:
        return None

    dominance_sum = sum(
        int(comparison_rank > balanced_rank)
        - int(comparison_rank < balanced_rank)
        for comparison_rank in comparison_ranks
        for balanced_rank in balanced_ranks
    )
    return dominance_sum / comparison_count


def calculate_season_deltas(
    schedule_balance: pd.DataFrame,
    groups: pd.Series,
    proxy: str,
) -> pd.DataFrame:
    """Calculate both group comparisons for every league-season."""
    classified = schedule_balance.assign(schedule_group=groups)
    rows = []

    for (season_year, league_name), season in classified.groupby(
        ["season_year", "league_name"]
    ):
        balanced_ranks = season.loc[
            season["schedule_group"].eq("balanced"),
            "final_position",
        ]
        for comparison, schedule_group in COMPARISONS.items():
            delta = cliffs_delta(
                season.loc[
                    season["schedule_group"].eq(schedule_group),
                    "final_position",
                ],
                balanced_ranks,
            )
            if delta is not None:
                rows.append(
                    {
                        "proxy": proxy,
                        "season_year": season_year,
                        "league_name": league_name,
                        "comparison": comparison,
                        "delta": delta,
                    }
                )

    return pd.DataFrame(rows)


def plot_cliffs_delta(delta_results: pd.DataFrame, output_path: Path) -> None:
    """Render the notebook's Cliff's delta distribution boxplot."""
    apply_plot_style()
    figure, axis = plt.subplots(figsize=(7, 4.5))
    sns.boxplot(
        data=delta_results,
        x="proxy",
        y="delta",
        hue="comparison",
        ax=axis,
        palette={
            r"$G^-$ vs $G^0$": COLORS["muted_red"],
            r"$G^+$ vs $G^0$": COLORS["blue"],
        },
        width=0.45,
        whis=(0, 100),
        boxprops={"edgecolor": "#4A4A4A", "linewidth": 1.2},
        whiskerprops={"color": "#4A4A4A", "linewidth": 1.2},
        capprops={"color": "#4A4A4A", "linewidth": 1.2},
        medianprops={"color": "black", "linewidth": 2.0},
    )
    axis.axhline(0.00, color="#8E8E93", linestyle="--", linewidth=1.5)
    for threshold, color in ((0.33, COLORS["orange"]), (0.474, COLORS["red"])):
        axis.axhline(threshold, color=color, linestyle=":", linewidth=1.5)
        axis.axhline(-threshold, color=color, linestyle=":", linewidth=1.5)
    axis.set_ylim(-1.08, 1.08)
    axis.set_yticks([-1.00, -0.75, -0.50, -0.25, 0.00, 0.25, 0.50, 0.75, 1.00])
    axis.yaxis.set_major_formatter("{x:.2f}")
    axis.set_ylabel(r"Cliff's $\delta$")
    axis.set_xlabel("")
    axis.legend(loc="upper right", edgecolor="#E0E0E0")
    figure.tight_layout()
    save_figure(figure, output_path)


def write_cliffs_delta_analysis(
    ranking_balance: pd.DataFrame,
    market_balance: pd.DataFrame,
    output_dir: Path,
) -> list[Path]:
    """Write Cliff's delta data and plot into each classification folder."""
    ranking_groups = dict(iter_schedule_classifications(ranking_balance))
    market_groups = dict(iter_schedule_classifications(market_balance))
    output_paths = []

    for classification, ranking_classification in ranking_groups.items():
        delta_results = pd.concat(
            [
                calculate_season_deltas(
                    ranking_balance,
                    ranking_classification,
                    "Final ranking",
                ),
                calculate_season_deltas(
                    market_balance,
                    market_groups[classification],
                    "Market value",
                ),
            ],
            ignore_index=True,
        )
        classification_dir = output_dir / classification
        classification_dir.mkdir(parents=True, exist_ok=True)
        data_path = classification_dir / "cliffs_delta.csv"
        plot_path = classification_dir / "cliffs_delta.png"
        delta_results.to_csv(data_path, index=False)
        plot_cliffs_delta(delta_results, plot_path)
        output_paths.extend((data_path, plot_path))

    return output_paths


def generate_cliffs_delta_analysis() -> list[Path]:
    """Load schedule-balance inputs and generate all Cliff's delta outputs."""
    output_paths = write_cliffs_delta_analysis(
        pd.read_csv(paths.SPEARMAN_BALANCE_RANKING_PATH),
        pd.read_csv(paths.SPEARMAN_BALANCE_MARKET_PATH),
        paths.CLIFFS_DELTA_DIR,
    )
    logger.info("Generated %d Cliff's delta outputs.", len(output_paths))
    return output_paths
