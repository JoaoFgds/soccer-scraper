"""Consolidate Bronze club market-value CSVs into one Silver table."""

import logging

import pandas as pd

from .config import DATA_DIR, SILVER_PATH


LOGGER = logging.getLogger(__name__)
REQUIRED_COLUMNS = [
    "league_name",
    "season_year",
    "club_name",
    "total_market_value_euros",
]


def process_bronze_to_silver() -> pd.DataFrame | None:
    """Build the historical market-values Silver dataset."""

    bronze_files = sorted(
        path for path in DATA_DIR.glob("*.csv") if path.name != "scraping_status.csv"
    )
    if not bronze_files:
        LOGGER.warning("No market-value Bronze CSV files were found in %s", DATA_DIR)
        return None

    all_frames = []
    for source_path in bronze_files:
        try:
            frame = pd.read_csv(source_path)
        except pd.errors.EmptyDataError:
            LOGGER.warning("Ignoring empty file %s", source_path.name)
            continue

        missing = sorted(set(REQUIRED_COLUMNS).difference(frame.columns))
        if missing:
            LOGGER.warning(
                "Ignoring %s because columns are missing: %s",
                source_path.name,
                ", ".join(missing),
            )
            continue

        filtered = frame.loc[:, REQUIRED_COLUMNS].copy()
        filtered["total_market_value_euros"] = (
            filtered["total_market_value_euros"].fillna(0.0).astype(float)
        )
        has_missing_values = filtered["total_market_value_euros"].eq(0.0).any()
        filtered["is_complete"] = not has_missing_values
        all_frames.append(filtered)

    if not all_frames:
        LOGGER.warning("No valid market-value Bronze data could be processed")
        return None

    result = pd.concat(all_frames, ignore_index=True)
    result = result.sort_values(
        by=["league_name", "season_year", "total_market_value_euros"],
        ascending=[True, True, False],
        kind="stable",
    )
    SILVER_PATH.parent.mkdir(parents=True, exist_ok=True)
    result.to_csv(SILVER_PATH, index=False, encoding="utf-8")
    LOGGER.info("Wrote %s rows to %s", len(result), SILVER_PATH)
    return result


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    process_bronze_to_silver()
