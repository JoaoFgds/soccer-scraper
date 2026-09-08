"""Generate the paper's Tables 2 and 3 for every schedule classification."""

import logging
from pathlib import Path

import pandas as pd

from src.analysis.schedule_classifications import (
    GROUP_LABELS,
    iter_schedule_classifications,
)
from src.utils import paths


logger = logging.getLogger(__name__)


def compute_group_proportions(groups: pd.Series) -> dict[str, float]:
    """Calculate the overall proportion of schedules in G0, G-, and G+."""
    proportions = groups.value_counts(normalize=True).reindex(
        GROUP_LABELS,
        fill_value=0,
    )
    return {
        label: float(proportions[group])
        for group, label in GROUP_LABELS.items()
    }


def compute_league_proportions(
    schedule_balance: pd.DataFrame,
    groups: pd.Series,
) -> pd.DataFrame:
    """Calculate G0, G-, and G+ proportions within each league."""
    counts = (
        schedule_balance.assign(schedule_group=groups)
        .groupby(["league_name", "schedule_group"])
        .size()
        .unstack(fill_value=0)
        .reindex(columns=GROUP_LABELS, fill_value=0)
        .rename(columns=GROUP_LABELS)
    )
    denominator = counts.sum(axis=1).rename("N")
    return counts.div(denominator, axis=0).assign(N=denominator).reset_index()


def write_ssb_proportion_tables(
    ranking_balance: pd.DataFrame,
    market_balance: pd.DataFrame,
    output_dir: Path,
) -> list[Path]:
    """Write Tables 2 and 3 into one folder per classification."""
    ranking_groups = dict(iter_schedule_classifications(ranking_balance))
    market_groups = dict(iter_schedule_classifications(market_balance))
    output_paths = []

    for classification, ranking_classification in ranking_groups.items():
        market_classification = market_groups[classification]
        classification_dir = output_dir / classification
        classification_dir.mkdir(parents=True, exist_ok=True)

        table_2 = pd.DataFrame(
            [
                {
                    "proxy": "final_standings",
                    **compute_group_proportions(ranking_classification),
                },
                {
                    "proxy": "market_value",
                    **compute_group_proportions(market_classification),
                },
            ]
        )
        table_3 = pd.concat(
            [
                compute_league_proportions(
                    ranking_balance,
                    ranking_classification,
                ).assign(proxy="final_standings"),
                compute_league_proportions(
                    market_balance,
                    market_classification,
                ).assign(proxy="market_value"),
            ],
            ignore_index=True,
        )
        table_3.insert(0, "proxy", table_3.pop("proxy"))

        for filename, table in (("table_2.csv", table_2), ("table_3.csv", table_3)):
            output_path = classification_dir / filename
            table.to_csv(output_path, index=False)
            output_paths.append(output_path)

    return output_paths


def generate_ssb_proportion_tables() -> list[Path]:
    """Load schedule-balance inputs and generate all Table 2 and 3 files."""
    output_paths = write_ssb_proportion_tables(
        pd.read_csv(paths.SPEARMAN_BALANCE_RANKING_PATH),
        pd.read_csv(paths.SPEARMAN_BALANCE_MARKET_PATH),
        paths.SPEARMAN_COEFFICIENT_DIR,
    )
    logger.info("Generated %d SSB proportion tables.", len(output_paths))
    return output_paths
