"""
Data pre-processing module for the soccer scraper project.

This module unifies, cleans, and transforms raw soccer data from CSV files
into a structured format for analysis.
"""

import pandas as pd
import json
import re
import logging
from pathlib import Path
from src.pre_processor import config
from src.scraper.utils import sanitize_filename

logger = logging.getLogger(__name__)


def extract_metadata_from_filename(file_path: Path) -> dict:
    """
    Extracts league and season information from a standardized filename.

    Args:
        file_path: The Path object of the file.

    Returns:
        A dictionary containing 'league_name' and 'season_year'.
    """
    filename = file_path.stem
    match = re.match(r"([a-z0-9]+)_(\d{4})_standings", filename)
    if not match:
        raise ValueError(f"Filename does not match expected pattern: {file_path}")

    return {"league_name": match.group(1), "season_year": int(match.group(2))}


def validate_url_year(url: str, season_year: int) -> bool:
    """
    Validates if the year in a URL is less than or equal to the season year.

    Args:
        url: The team URL string.
        season_year: The season year from the file metadata.

    Returns:
        True if the URL's year is <= season year, otherwise False.
    """
    if not isinstance(url, str):
        return False

    url_year_match = re.search(r"saison_id/(\d{4})", url)
    if not url_year_match:
        return False

    url_year = int(url_year_match.group(1))
    return url_year <= season_year


def process_standings_data():
    """
    Processes all league standings data, unifies it into a single DataFrame,
    and generates CSV and JSON files.
    """
    logger.info("Starting standings data processing...")
    all_data_frames = []

    for league_dir in config.RAW_DATA_DIR.iterdir():
        if not league_dir.is_dir():
            continue

        for year_dir in league_dir.iterdir():
            if not year_dir.is_dir():
                continue

            standings_dir = year_dir / "final_standings"
            if not standings_dir.is_dir():
                logger.warning(
                    f"Skipping '{standings_dir.relative_to(config.BASE_DIR)}' as it is not a directory."
                )
                continue

            for standings_file in standings_dir.glob("*.csv"):
                logger.info(
                    f"Processing file: {standings_file.relative_to(config.BASE_DIR)}"
                )

                try:
                    metadata = extract_metadata_from_filename(standings_file)

                    df = pd.read_csv(standings_file, encoding="utf-8")
                    if df.empty:
                        logger.warning(
                            f"File is empty. Skipping: {standings_file.name}"
                        )
                        continue

                    df["csv_file_name"] = standings_file.name
                    df["league_name"] = metadata["league_name"]
                    df["season_year"] = metadata["season_year"]

                    df["team_sanitized"] = df["team"].apply(sanitize_filename)
                    df["is_valid_url"] = df.apply(
                        lambda row: validate_url_year(
                            row["team_url"], row["season_year"]
                        ),
                        axis=1,
                    )

                    all_data_frames.append(df)
                    logger.info(f"Successfully processed file: {standings_file.name}")

                except FileNotFoundError:
                    logger.error(f"File not found: {standings_file.name}")
                    continue
                except (pd.errors.EmptyDataError, pd.errors.ParserError) as e:
                    logger.warning(
                        f"Skipping malformed or empty file: {standings_file.name}. Error: {e}"
                    )
                    continue
                except ValueError as e:
                    logger.warning(
                        f"Skipping file due to metadata extraction error: {standings_file.name}. Error: {e}"
                    )
                    continue

    if not all_data_frames:
        logger.info("No standings data found to process. Exiting.")
        return

    logger.info(f"Found {len(all_data_frames)} data frames. Concatenating...")
    unified_df = pd.concat(all_data_frames, ignore_index=True)

    # Save the concatenated DataFrame to CSV
    logger.info("Saving concatenated DataFrame to CSV...")
    unified_df.to_csv(config.STANDINGS_CONCAT_CSV, index=False)
    logger.info(
        f"DataFrame saved successfully at: {config.STANDINGS_CONCAT_CSV.relative_to(config.BASE_DIR)}"
    )

    # Generate JSON from a filtered version of the DataFrame
    generate_standings_json(unified_df)


def generate_standings_json(df: pd.DataFrame):
    """
    Generates a nested JSON structure from the unified standings DataFrame,
    filtering for rows where 'is_valid_url' is True.

    Args:
        df: The unified pandas DataFrame containing standings data.
    """
    logger.info("Generating standings JSON file from valid URLs...")

    # Filter the DataFrame to include only valid URLs
    filtered_df = df.loc[df["is_valid_url"] == True].copy()

    if filtered_df.empty:
        logger.warning("Filtered DataFrame is empty. No JSON will be generated.")
        return

    output_data = {}

    for (league, year), group_df in filtered_df.groupby(["league_name", "season_year"]):
        if league not in output_data:
            output_data[league] = {}

        season_ranks = {
            row["team_sanitized"]: row["position"] for _, row in group_df.iterrows()
        }
        output_data[league][str(year)] = season_ranks

    output_path = config.STANDINGS_JSON
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(output_data, f, indent=4)
        logger.info(
            f"Standings JSON file created successfully at: {output_path.relative_to(config.BASE_DIR)}"
        )


if __name__ == "__main__":
    config.configure_logging()
    process_standings_data()
    logger.info("Standings data processing complete.")
