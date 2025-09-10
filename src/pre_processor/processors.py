# File: src/pre_processor/processors.py
"""
Core processing logic for the soccer scraper data.
Contains functions for handling standings and schedule data to generate a summary.
"""
import logging
import pandas as pd
from pathlib import Path
from src.pre_processor import config
from src.pre_processor import utils

logger = logging.getLogger(__name__)


def _load_and_consolidate_games(team_games_dir: Path) -> pd.DataFrame | None:
    """
    Loads all game files from a directory, consolidates them, and returns a
    DataFrame with distinct matches.

    Args:
        team_games_dir: Path to the directory containing team game CSVs.

    Returns:
        A DataFrame of distinct games, or None if no valid games are found.
    """
    all_games_list = []
    for team_file in team_games_dir.glob("*.csv"):
        try:
            game_df = pd.read_csv(team_file, encoding="utf-8")
            if not game_df.empty:
                all_games_list.append(game_df)
        except pd.errors.EmptyDataError:
            logger.warning(f"Skipping empty team game file: {team_file.name}")
        except Exception as e:
            logger.error(f"Error processing team file {team_file.name}: {e}")

    if not all_games_list:
        return None

    consolidated_games_df = pd.concat(all_games_list, ignore_index=True)
    # Create an explicit copy to avoid SettingWithCopyWarning later
    distinct_games_df = consolidated_games_df.drop_duplicates(
        subset=["date", "home_team", "away_team", "result"]
    ).copy()

    return distinct_games_df


def process_season_data(standings_file: Path) -> dict | None:
    """
    Processes the data for a single season to generate a summary dictionary.

    Args:
        standings_file: The Path object for the league standings CSV file.

    Returns:
        A dictionary containing the summarized data for the season, or None if processing fails.
    """
    try:
        logger.info(f"Processing season from: {standings_file.name}")
        metadata = utils.extract_metadata_from_filename(standings_file)
        league_name = metadata["league_name"]
        season_year = metadata["season_year"]

        # 1. Read and validate standings data
        standings_df = pd.read_csv(standings_file, encoding="utf-8")
        if standings_df.empty:
            logger.warning(f"Skipping empty standings file: {standings_file.name}")
            return None

        num_total_teams = len(standings_df)
        is_valid_url = all(
            standings_df.apply(
                lambda row: utils.validate_url_year(row["team_url"], season_year),
                axis=1,
            )
        )

        # 2. Validate and load game data
        team_games_dir = standings_file.parent.parent / "team_games"
        if not team_games_dir.is_dir():
            logger.warning(
                f"Team games directory not found for {standings_file.name}. Skipping season."
            )
            return None

        team_files = list(team_games_dir.glob("*.csv"))
        has_all_teams_files = len(team_files) == num_total_teams

        distinct_games_df = _load_and_consolidate_games(team_games_dir)
        if distinct_games_df is None:
            logger.warning(
                f"No valid game data found for season: {league_name}/{season_year}. Skipping."
            )
            return None

        # 3. Calculate metrics
        num_total_games = len(distinct_games_df)
        is_double_rounded = num_total_games == num_total_teams * (num_total_teams - 1)

        distinct_games_df["audience"] = distinct_games_df["audience"].fillna(0)
        num_null_attendance_games = int((distinct_games_df["audience"] == 0).sum())

        pct_null_attendance_games = (
            (num_null_attendance_games / num_total_games) * 100
            if num_total_games > 0
            else 0
        )
        is_valid_attendance = pct_null_attendance_games < 5.0

        # 4. Assemble result
        return {
            "source_id": utils.generate_id(standings_file.name),
            "source_csv_file": standings_file.name,
            "league_name": league_name,
            "season_year": season_year,
            "has_all_teams_files": has_all_teams_files,
            "num_total_teams": num_total_teams,
            "num_total_games": num_total_games,
            "num_null_attendance_games": num_null_attendance_games,
            "pct_null_attendance_games": round(pct_null_attendance_games, 2),
            "is_valid_url": is_valid_url,
            "is_double_rounded": is_double_rounded,
            "is_valid_attendance": is_valid_attendance,
        }

    except FileNotFoundError:
        logger.error(f"Standings file not found: {standings_file}")
        return None
    except pd.errors.EmptyDataError:
        logger.error(f"Standings file is empty or malformed: {standings_file.name}")
        return None
    except Exception as e:
        logger.error(
            f"An unexpected error occurred while processing {standings_file.name}: {e}"
        )
        return None


def create_standings_summary() -> pd.DataFrame:
    """
    Orchestrates the processing of all league standings to create a unified summary.

    Returns:
        pd.DataFrame: A DataFrame containing the summary of all processed seasons.
    """
    logger.info("Starting standings summary creation...")
    summary_data = []

    for league_dir in config.RAW_DATA_DIR.iterdir():
        if not league_dir.is_dir():
            continue

        for year_dir in league_dir.iterdir():
            if not year_dir.is_dir():
                continue

            standings_dir = year_dir / "final_standings"
            if not standings_dir.is_dir():
                continue

            for standings_file in standings_dir.glob("*_standings.csv"):
                season_summary = process_season_data(standings_file)
                if season_summary:
                    summary_data.append(season_summary)

    if not summary_data:
        logger.warning("No data processed. Returning an empty DataFrame.")
        return pd.DataFrame()

    summary_df = pd.DataFrame(summary_data)
    logger.info(f"Successfully created summary for {len(summary_df)} seasons.")
    return summary_df


def create_standings_complete() -> pd.DataFrame:
    """
    Concatenates all raw standings files, enriches them with metadata and new IDs,
    and returns a single complete DataFrame.

    Returns:
        A DataFrame containing all standings data, or an empty DataFrame if no data is found.
    """
    logger.info("Starting complete standings data creation...")
    all_standings_dfs = []

    standings_files = Path(config.RAW_DATA_DIR).rglob("*_standings.csv")

    for standings_file in standings_files:
        try:
            df = pd.read_csv(standings_file, encoding="utf-8")
            if df.empty:
                logger.warning(
                    f"Skipping empty standings file for concatenation: {standings_file.name}"
                )
                continue

            metadata = utils.extract_metadata_from_filename(standings_file)

            # Enrich DataFrame with new columns
            df["team_sanitized"] = df["team"].apply(utils.sanitize_filename)
            df["league_name"] = metadata["league_name"]
            df["season_year"] = metadata["season_year"]
            df["source_csv_file"] = standings_file.name
            df["source_id"] = utils.generate_id(standings_file.name)
            df["id"] = df.apply(
                lambda row: utils.generate_id(
                    f"{row['team_sanitized']}_{row['source_csv_file']}"
                ),
                axis=1,
            )

            # Ensure 'position' is a nullable integer
            df["position"] = pd.to_numeric(df["position"], errors="coerce").astype(
                "Int64"
            )

            all_standings_dfs.append(df)

        except Exception as e:
            logger.error(
                f"Failed to process {standings_file.name} for concatenation: {e}"
            )

    if not all_standings_dfs:
        logger.warning("No standings data found to concatenate.")
        return pd.DataFrame()

    final_df = pd.concat(all_standings_dfs, ignore_index=True)

    # Reorder columns to match the final schema
    final_schema = [
        "position",
        "team",
        "team_sanitized",
        "played",
        "won",
        "drawn",
        "lost",
        "goal_ratio",
        "goal_difference",
        "points",
        "team_url",
        "league_name",
        "season_year",
        "source_csv_file",
        "source_id",
        "id",
    ]
    # Handle 'draw' vs 'drawn' discrepancy
    if "draw" in final_df.columns and "drawn" not in final_df.columns:
        final_df.rename(columns={"draw": "drawn"}, inplace=True)

    final_df = final_df.reindex(columns=final_schema)

    logger.info(
        f"Successfully created complete standings file with {len(final_df)} rows."
    )
    return final_df
