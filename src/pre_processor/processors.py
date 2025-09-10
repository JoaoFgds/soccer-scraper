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


def process_season_data(standings_file: Path) -> dict | None:
    """
    Processes the data for a single season to generate a summary dictionary.

    Args:
        standings_file: The Path object for the league standings CSV file.

    Returns:
        A dictionary containing the summarized data for the season, or None if processing fails.
    """
    try:
        metadata = utils.extract_metadata_from_filename(standings_file)
        league_name = metadata["league_name"]
        season_year = metadata["season_year"]

        # Read standings data
        standings_df = pd.read_csv(standings_file, encoding="utf-8")
        if standings_df.empty:
            logger.warning(f"Skipping empty standings file: {standings_file.name}")
            return None

        # Basic metrics from standings
        num_total_teams = len(standings_df)
        is_valid_url = all(
            standings_df.apply(
                lambda row: utils.validate_url_year(row["team_url"], season_year),
                axis=1,
            )
        )

        # Team games path and validation
        team_games_dir = standings_file.parent.parent / "team_games"
        if not team_games_dir.is_dir():
            logger.warning(f"Team games directory not found for {standings_file.name}")
            return None

        team_files = list(team_games_dir.glob("*.csv"))
        has_all_teams_files = len(team_files) == num_total_teams

        # Robustly process distinct games and attendance
        all_games_list = []
        for team_file in team_files:
            try:
                game_df = pd.read_csv(team_file, encoding="utf-8")
                if not game_df.empty:
                    all_games_list.append(game_df)
            except Exception as e:
                logger.error(f"Error processing team file {team_file.name}: {e}")

        if not all_games_list:
            logger.warning(
                f"No game data found for season: {league_name}/{season_year}"
            )
            return None

        # Consolidate all games and drop duplicates to get a unique set of matches
        consolidated_games_df = pd.concat(all_games_list, ignore_index=True)
        # CORRECTION: Add .copy() to prevent SettingWithCopyWarning
        distinct_games_df = consolidated_games_df.drop_duplicates(
            subset=["date", "home_team", "away_team", "result"]
        ).copy()

        num_total_games = len(distinct_games_df)
        # A perfect double-rounded season has N * (N-1) games.
        is_double_rounded = num_total_games == num_total_teams * (num_total_teams - 1)

        # Attendance metrics
        distinct_games_df["audience"] = distinct_games_df["audience"].fillna(0)
        num_null_attendance_games = int((distinct_games_df["audience"] == 0).sum())

        pct_null_attendance_games = (
            (num_null_attendance_games / num_total_games) * 100
            if num_total_games > 0
            else 0
        )
        is_valid_attendance = pct_null_attendance_games < 5.0

        return {
            "id": utils.generate_id(standings_file.name),
            "source_csv_file": standings_file.name,
            "league_name": league_name,
            "season_year": season_year,
            "num_total_teams": num_total_teams,
            "num_total_games": num_total_games,
            "has_all_teams_files": has_all_teams_files,
            "num_null_attendance_games": num_null_attendance_games,
            "pct_null_attendance_games": round(pct_null_attendance_games, 2),
            "is_double_rounded": is_double_rounded,
            "is_valid_url": is_valid_url,
            "is_valid_attendance": is_valid_attendance,
        }

    except Exception as e:
        logger.error(f"Failed to process season from {standings_file.name}: {e}")
        return None


def create_standings_summary():
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
