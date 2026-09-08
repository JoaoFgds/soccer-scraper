#!/usr/bin/env python3
"""Build the consolidated football team-season analytics dataset.

The final standings table is the driving dataset. The pipeline derives each
team's market-value ranking from the exact preceding season and enriches every
driving row with attendance and schedule-balance metrics through left joins.
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Final, Sequence

import pandas as pd


LOGGER = logging.getLogger(__name__)

PROJECT_ROOT: Final[Path] = Path(__file__).resolve().parents[1]
JOIN_KEYS: Final[list[str]] = [
    "league_name",
    "season_year",
    "team_canonical",
]

REFERENCE_COLUMNS: Final[list[str]] = [
    *JOIN_KEYS,
    "position_old",
    "position",
    "total_market_value_euros",
]
OCCUPANCY_COLUMNS: Final[list[str]] = [
    *JOIN_KEYS,
    "mean_attendance",
    "max_attendance",
    "average_occupancy",
]
STRENGTH_COLUMNS: Final[list[str]] = [*JOIN_KEYS, "G", "P_value"]

OUTPUT_COLUMNS: Final[list[str]] = [
    "league_name",
    "season_year",
    "team_canonical",
    "ranking",
    "ranking_last_season",
    "ranking_market",
    "ranking_market_last_season",
    "total_market_value_euros",
    "mean_attendance",
    "max_attendance",
    "avg_attendance",
    "g_value_ranking",
    "p_value_ranking",
    "g_value_market",
    "p_value_market",
]


class DataValidationError(ValueError):
    """Indicate that an input file violates the pipeline's data contract."""


def parse_args() -> argparse.Namespace:
    """Parse input and output paths with repository-relative defaults."""

    parser = argparse.ArgumentParser(
        description=(
            "Consolidate standings, attendance, and schedule-balance data into "
            "one team-season CSV."
        )
    )
    parser.add_argument(
        "--reference",
        type=Path,
        default=(
            PROJECT_ROOT
            / "data/silver/final_standings_valid_market_ranking.csv"
        ),
        help="Driving standings and market-ranking CSV.",
    )
    parser.add_argument(
        "--occupancy",
        type=Path,
        default=(
            PROJECT_ROOT
            / "data/gold/analysis/mann_whitney/attendance/audit/"
            "occupancy_audit_audience_filled_fb.csv"
        ),
        help="Attendance and occupancy CSV.",
    )
    parser.add_argument(
        "--strength-market",
        type=Path,
        default=(
            PROJECT_ROOT
            / "data/gold/analysis/spearman_coefficient/metrics/"
            "strength_schedule_balance_market.csv"
        ),
        help="Market-based schedule-balance CSV.",
    )
    parser.add_argument(
        "--strength-ranking",
        type=Path,
        default=(
            PROJECT_ROOT
            / "data/gold/analysis/spearman_coefficient/metrics/"
            "strength_schedule_balance_ranking.csv"
        ),
        help="Standings-based schedule-balance CSV.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=(
            PROJECT_ROOT
            / "data/gold/analysis/team_season_consolidated.csv"
        ),
        help="Destination for the consolidated CSV.",
    )
    return parser.parse_args()


def require_columns(
    frame: pd.DataFrame,
    required_columns: Sequence[str],
    source_path: Path,
) -> None:
    """Verify that a loaded table contains every required source column."""

    missing = sorted(set(required_columns).difference(frame.columns))
    if missing:
        raise DataValidationError(
            f"{source_path} is missing required columns: {', '.join(missing)}"
        )


def coerce_numeric_column(
    frame: pd.DataFrame,
    column: str,
    dtype: str,
    source_path: Path,
) -> None:
    """Convert one column to a numeric nullable dtype without hiding bad data."""

    source_values = frame[column]
    numeric_values = pd.to_numeric(source_values, errors="coerce")
    invalid = source_values.notna() & numeric_values.isna()
    if invalid.any():
        examples = source_values.loc[invalid].astype(str).drop_duplicates().head(5)
        raise DataValidationError(
            f"{source_path}: column '{column}' contains non-numeric values: "
            f"{examples.tolist()}"
        )

    try:
        frame[column] = numeric_values.astype(dtype)
    except (TypeError, ValueError) as exc:
        raise DataValidationError(
            f"{source_path}: column '{column}' cannot be represented as {dtype}"
        ) from exc


def validate_and_align_keys(frame: pd.DataFrame, source_path: Path) -> None:
    """Align join-key types and reject null or blank keys."""

    coerce_numeric_column(frame, "season_year", "Int64", source_path)

    for column in ("league_name", "team_canonical"):
        frame[column] = frame[column].astype("string")
        invalid = frame[column].isna() | frame[column].str.strip().eq("")
        if invalid.any():
            raise DataValidationError(
                f"{source_path}: join key '{column}' contains "
                f"{int(invalid.sum())} null or blank values"
            )

    if frame["season_year"].isna().any():
        raise DataValidationError(
            f"{source_path}: join key 'season_year' contains null values"
        )


def require_unique_keys(frame: pd.DataFrame, source_path: Path) -> None:
    """Ensure one row exists per league, season, and canonical team."""

    duplicate_mask = frame.duplicated(JOIN_KEYS, keep=False)
    if duplicate_mask.any():
        examples = (
            frame.loc[duplicate_mask, JOIN_KEYS]
            .drop_duplicates()
            .head(5)
            .to_dict(orient="records")
        )
        raise DataValidationError(
            f"{source_path}: duplicate join keys would make the merge "
            f"non-deterministic. Examples: {examples}"
        )


def read_selected_csv(
    source_path: Path,
    required_columns: Sequence[str],
) -> pd.DataFrame:
    """Load and validate a CSV, returning only pipeline-owned columns."""

    if not source_path.is_file():
        raise FileNotFoundError(f"Input CSV does not exist: {source_path}")

    try:
        frame = pd.read_csv(source_path, low_memory=False)
    except pd.errors.EmptyDataError as exc:
        raise DataValidationError(f"Input CSV is empty: {source_path}") from exc
    except pd.errors.ParserError as exc:
        raise DataValidationError(f"Input CSV is malformed: {source_path}") from exc

    require_columns(frame, required_columns, source_path)
    frame = frame.loc[:, list(required_columns)].copy()
    validate_and_align_keys(frame, source_path)
    require_unique_keys(frame, source_path)
    return frame


def load_reference(source_path: Path) -> pd.DataFrame:
    """Load the driving table and derive both exact prior-season rankings."""

    frame = read_selected_csv(source_path, REFERENCE_COLUMNS)
    coerce_numeric_column(frame, "position_old", "Int64", source_path)
    coerce_numeric_column(frame, "position", "Int64", source_path)
    coerce_numeric_column(
        frame, "total_market_value_euros", "Float64", source_path
    )

    frame = frame.rename(
        columns={
            "position_old": "ranking",
            "position": "ranking_market",
        }
    )

    history = frame.loc[:, JOIN_KEYS + ["ranking", "ranking_market"]].rename(
        columns={
            "ranking": "ranking_last_season",
            "ranking_market": "ranking_market_last_season",
        }
    )
    history["season_year"] = history["season_year"] + 1

    frame = frame.merge(
        history,
        how="left",
        on=JOIN_KEYS,
        sort=False,
        validate="one_to_one",
    )
    for column in ("ranking_last_season", "ranking_market_last_season"):
        frame[column] = frame[column].fillna(-1).astype("Int64")
    return frame


def load_occupancy(source_path: Path) -> pd.DataFrame:
    """Load attendance metrics and preserve the historical output naming."""

    frame = read_selected_csv(source_path, OCCUPANCY_COLUMNS)
    coerce_numeric_column(frame, "mean_attendance", "Float64", source_path)
    coerce_numeric_column(frame, "max_attendance", "Int64", source_path)
    coerce_numeric_column(frame, "average_occupancy", "Float64", source_path)
    return frame.rename(columns={"average_occupancy": "avg_attendance"})


def load_strength(
    source_path: Path,
    metric_suffix: str,
) -> pd.DataFrame:
    """Load one schedule-balance table and assign metric-specific names."""

    if metric_suffix not in {"market", "ranking"}:
        raise ValueError(
            "metric_suffix must be either 'market' or 'ranking', "
            f"not {metric_suffix!r}"
        )

    frame = read_selected_csv(source_path, STRENGTH_COLUMNS)
    coerce_numeric_column(frame, "G", "Float64", source_path)
    coerce_numeric_column(frame, "P_value", "Float64", source_path)
    return frame.rename(
        columns={
            "G": f"g_value_{metric_suffix}",
            "P_value": f"p_value_{metric_suffix}",
        }
    )


def build_consolidated_dataset(
    reference_path: Path,
    occupancy_path: Path,
    strength_market_path: Path,
    strength_ranking_path: Path,
) -> pd.DataFrame:
    """Create the output table using validated one-to-one left joins."""

    reference = load_reference(reference_path)
    occupancy = load_occupancy(occupancy_path)
    strength_market = load_strength(strength_market_path, "market")
    strength_ranking = load_strength(strength_ranking_path, "ranking")

    result = reference
    for source_name, enrichment in (
        ("occupancy", occupancy),
        ("ranking strength", strength_ranking),
        ("market strength", strength_market),
    ):
        row_count_before = len(result)
        result = result.merge(
            enrichment,
            how="left",
            on=JOIN_KEYS,
            sort=False,
            validate="one_to_one",
        )
        if len(result) != row_count_before:
            raise DataValidationError(
                f"The {source_name} left join changed the driving row count "
                f"from {row_count_before} to {len(result)}"
            )

    missing_output_columns = sorted(set(OUTPUT_COLUMNS).difference(result.columns))
    if missing_output_columns:
        raise DataValidationError(
            "Pipeline did not produce required output columns: "
            + ", ".join(missing_output_columns)
        )

    return result.loc[:, OUTPUT_COLUMNS]


def write_csv_atomically(frame: pd.DataFrame, output_path: Path) -> None:
    """Write a CSV through a temporary file and replace the target atomically."""

    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path: Path | None = None

    try:
        with NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            newline="",
            dir=output_path.parent,
            prefix=f".{output_path.name}.",
            suffix=".tmp",
            delete=False,
        ) as temporary_file:
            temporary_path = Path(temporary_file.name)
            frame.to_csv(temporary_file, index=False, lineterminator="\n")

        temporary_path.chmod(0o644)
        temporary_path.replace(output_path)
    finally:
        if temporary_path is not None and temporary_path.exists():
            temporary_path.unlink()


def main() -> int:
    """Run validation, transformation, and atomic output writing."""

    args = parse_args()
    result = build_consolidated_dataset(
        reference_path=args.reference,
        occupancy_path=args.occupancy,
        strength_market_path=args.strength_market,
        strength_ranking_path=args.strength_ranking,
    )
    write_csv_atomically(result, args.output)
    LOGGER.info("Wrote %s rows and %s columns to %s", *result.shape, args.output)
    return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    raise SystemExit(main())
