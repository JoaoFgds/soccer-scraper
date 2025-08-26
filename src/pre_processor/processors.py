# File: src/pre_processor/processors.py
"""
Core processing logic for the soccer scraper data.
Contains functions for handling standings and schedule data.
"""
import logging
import pandas as pd
from pathlib import Path
from src.pre_processor import config
from src.pre_processor import utils
import numpy as np
import json

logger = logging.getLogger(__name__)


def process_standings_data():
    """
    Processes all league standings data, unifies it into a single DataFrame,
    and returns it.

    Returns:
        pd.DataFrame: The unified DataFrame with standings data.
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
                    metadata = utils.extract_metadata_from_filename(standings_file)

                    df = pd.read_csv(standings_file, encoding="utf-8")
                    if df.empty:
                        logger.warning(
                            f"File is empty. Skipping: {standings_file.name}"
                        )
                        continue

                    df["csv_file_name"] = standings_file.name
                    df["league_name"] = metadata["league_name"]
                    df["season_year"] = metadata["season_year"]

                    df["team_sanitized"] = df["team"].apply(utils.sanitize_filename)
                    df["is_valid_url"] = df.apply(
                        lambda row: utils.validate_url_year(
                            row["team_url"], row["season_year"]
                        ),
                        axis=1,
                    )
                    df["id"] = df.apply(
                        lambda row: utils.generate_id(
                            f"{row['team_sanitized']}_{row['season_year']}_{row['league_name']}"
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
        return pd.DataFrame()

    logger.info(f"Found {len(all_data_frames)} data frames. Concatenating...")
    unified_df = pd.concat(all_data_frames, ignore_index=True)

    return unified_df


def process_schedules_data(standings_df: pd.DataFrame):
    """
    Processes all team schedules data, filtering by valid seasons,
    deduplicating, and calculating attendance metrics.

    Args:
        standings_df: DataFrame containing all processed standings data.

    Returns:
        tuple[pd.DataFrame, pd.DataFrame]: The unified schedules DataFrame
        and the attendance summary DataFrame.
    """
    logger.info("Starting schedules data processing...")

    # Identify valid seasons based on is_valid_url from the standings
    valid_seasons = standings_df.loc[standings_df["is_valid_url"] == True]
    valid_seasons_set = set(
        zip(valid_seasons["league_name"], valid_seasons["season_year"])
    )

    if not valid_seasons_set:
        logger.warning(
            "No valid seasons found in standings data. Skipping schedules processing."
        )
        return pd.DataFrame(), pd.DataFrame()

    all_schedules_frames = []
    attendance_summary_list = []

    for league_dir in config.RAW_DATA_DIR.iterdir():
        if not league_dir.is_dir():
            continue

        for year_dir in league_dir.iterdir():
            if not year_dir.is_dir():
                continue

            current_league = league_dir.name
            try:
                current_year = int(year_dir.name)
            except ValueError:
                logger.warning(f"Skipping non-numeric directory: {year_dir.name}")
                continue

            if (current_league, current_year) not in valid_seasons_set:
                logger.info(
                    f"Skipping schedules for invalid season: {current_league}/{current_year}"
                )
                continue

            team_games_dir = year_dir / "team_games"
            if not team_games_dir.is_dir():
                logger.warning(
                    f"Skipping '{team_games_dir.relative_to(config.BASE_DIR)}' as it is not a directory."
                )
                continue

            season_games_df_list = []
            cont_teams = 0

            for games_file in team_games_dir.glob("*.csv"):
                try:
                    metadata = utils.extract_metadata_from_filename(games_file)
                    team_name = metadata["team_sanitized_name"]

                    df = pd.read_csv(games_file, encoding="utf-8")
                    if df.empty:
                        logger.warning(f"File is empty. Skipping: {games_file.name}")
                        continue

                    df["csv_file_name"] = games_file.name
                    df["league_name"] = metadata["league_name"]
                    df["season_year"] = metadata["season_year"]

                    df["home_team_sanitized"] = df["home_team"].apply(
                        utils.sanitize_filename
                    )
                    df["away_team_sanitized"] = df["away_team"].apply(
                        utils.sanitize_filename
                    )
                    df["id"] = df.apply(
                        lambda row: utils.generate_id(
                            f"{row['round']}_{row['home_team_sanitized']}_{row['season_year']}_{row['league_name']}"
                        ),
                        axis=1,
                    )
                    cont_teams += 1
                    df_home_games = df.loc[df["home_team_sanitized"] == team_name]
                    season_games_df_list.append(df_home_games)

                    logger.info(
                        f"Processed home games for team: {team_name} from {games_file.name}"
                    )

                except (pd.errors.EmptyDataError, pd.errors.ParserError) as e:
                    logger.warning(
                        f"Skipping malformed or empty file: {games_file.name}. Error: {e}"
                    )
                    continue
                except ValueError as e:
                    logger.warning(
                        f"Skipping file due to metadata extraction error: {games_file.name}. Error: {e}"
                    )
                    continue

            # Aggregate and calculate metrics for the season
            if season_games_df_list:
                season_df = pd.concat(season_games_df_list, ignore_index=True)

                # Add datetime column
                season_df["date_part"] = season_df["date"].str.split(" ").str[1]
                season_df["datetime_str"] = (
                    season_df["date_part"] + " " + season_df["time"]
                )
                season_df["datetime"] = pd.to_datetime(
                    season_df["datetime_str"], format="%d/%m/%Y %H:%M"
                )

                # Metrics for attendance summary (before imputation)
                season_df["audience"] = season_df["audience"].replace(0, np.nan)

                num_total_games = len(season_df)
                num_null_attendance_games = season_df["audience"].isna().sum()

                pct_null_attendance_games = (
                    (num_null_attendance_games / num_total_games) * 100
                    if num_total_games > 0
                    else 100
                )
                is_valid_attendance = pct_null_attendance_games <= 5.0

                # Impute null values for valid seasons
                season_df["audience_filled"] = season_df["audience"].copy()
                if is_valid_attendance:
                    season_df["audience_filled"] = (
                        season_df["audience_filled"].ffill().bfill()
                    )
                else:
                    logger.warning(
                        f"Season {current_league}/{current_year} exceeds 5% null attendance. Skipping imputation for audience_filled."
                    )

                attendance_total_games = season_df["audience"].sum()
                attendance_total_games_filled = season_df["audience_filled"].sum()

                all_schedules_frames.append(season_df)

                attendance_summary_list.append(
                    {
                        "league_name": current_league,
                        "season_year": current_year,
                        "num_total_teams": cont_teams,
                        "num_total_games": num_total_games,
                        "num_null_attendance_games": num_null_attendance_games,
                        "pct_null_attendance_games": round(
                            pct_null_attendance_games, 2
                        ),
                        "attendance_total_games": attendance_total_games,
                        "attendance_total_games_filled": attendance_total_games_filled,
                        "is_valid_attendance": is_valid_attendance,
                        "is_double_rounded": (num_total_games / cont_teams)
                        == (cont_teams - 1),
                        "csv_file_name": f"{current_league}_{current_year}_standings.csv",
                    }
                )

    if not all_schedules_frames:
        logger.warning("No valid schedules to process.")
        return pd.DataFrame(), pd.DataFrame()

    unified_schedules_df = pd.concat(all_schedules_frames, ignore_index=True)
    attendance_summary_df = pd.DataFrame(attendance_summary_list)

    return unified_schedules_df, attendance_summary_df


def generate_final_json(standings_df: pd.DataFrame):
    """
    Generates a final JSON structure from the processed standings DataFrame,
    filtering for seasons where 'is_valid_url' and 'is_valid_attendance' are True.

    Args:
        standings_df: The unified standings DataFrame.
    """
    logger.info("Generating final JSON from valid seasons...")

    # Filter the DataFrame to include only valid seasons
    filtered_df = standings_df.loc[
        (standings_df["is_valid_url"] == True)
        & (standings_df["is_valid_attendance"] == True)
    ].copy()

    if filtered_df.empty:
        logger.warning("Filtered DataFrame is empty. No JSON will be generated.")
        return

    output_data = {}

    # Group by league and year to build the nested structure
    for (league, year), group_df in filtered_df.groupby(["league_name", "season_year"]):
        if league not in output_data:
            output_data[league] = {}

        season_data = {
            row["team_sanitized"]: row["position"] for _, row in group_df.iterrows()
        }

        output_data[league][str(year)] = season_data

    output_path = config.FINAL_JSON
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(output_data, f, indent=4)
        logger.info(
            f"Final JSON file created successfully at: {output_path.relative_to(config.BASE_DIR)}"
        )
