import logging
import traceback
from pathlib import Path
from typing import Dict, List, Optional, Union

import numpy as np
import pandas as pd

from src.utils import helpers
from src.utils import paths


logger = logging.getLogger(__name__)


# ***************************************************************************


def _read_csv_file(file_path: Path) -> Optional[pd.DataFrame]:
    """Safely reads a CSV file into a pandas DataFrame with robust error handling.

    This function attempts to load a CSV file, gracefully handling common issues
    that can occur during file reading. It specifically catches and logs
    parsing errors, file encoding problems (assuming UTF-8), and handles cases
    where a file might be empty. Instead of raising exceptions, it logs the
    issue and returns None, allowing batch processes to continue without
    interruption.

    Args:
        file_path (Path): The path to the CSV file to be read.

    Returns:
        Optional[pd.DataFrame]: A pandas DataFrame containing the data from the
            CSV file. Returns `None` if the file is empty, cannot be parsed,
            or if any other reading error occurs.
    """
    try:
        df = pd.read_csv(file_path, encoding="utf-8")
        if df.empty:
            logger.warning("Empty file skipped: %s", file_path.name)
            return None
        logger.info(
            "Successfully loaded file: %s with %d rows", file_path.name, len(df)
        )
        return df
    except pd.errors.ParserError as e:
        logger.error(
            "Malformed CSV file could not be parsed: %s. Error: %s", file_path.name, e
        )
        return None
    except UnicodeDecodeError as e:
        logger.error("Encoding error in file: %s. Error: %s", file_path.name, e)
        return None
    except Exception as e:
        logger.error(
            "An unexpected error occurred while reading file: %s. Error: %s",
            file_path.name,
            e,
        )
        return None


def _validate_input_df(df: pd.DataFrame, required_columns: List[str]) -> bool:
    """Checks if a DataFrame contains all required columns.

    This utility function serves as a prerequisite check to ensure that a
    DataFrame has the necessary structure for further processing. By verifying
    the presence of all specified columns, it helps prevent downstream
    `KeyError` exceptions and ensures data integrity before any operations are
    performed on the DataFrame.

    Args:
        df (pd.DataFrame): The pandas DataFrame to be validated.
        required_columns (List[str]): A list of strings, where each string is a
            column name that must be present in the DataFrame.

    Returns:
        bool: True if all required columns are found in the DataFrame's
            columns, False otherwise.
    """
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        logger.error(
            "DataFrame validation failed. Missing required columns: %s", missing_columns
        )
        return False
    return True


# ***************************************************************************


def _process_season_metrics(
    distinct_games_df: pd.DataFrame,
    standings_df: pd.DataFrame,
    league_name: str,
    season_year: int,
    standings_file_name: str,
    num_team_files: int,
) -> Dict[str, Union[int, float, bool, str]]:
    """Calculates and summarizes key data quality metrics for a single season.

    This function acts as a data quality gatekeeper by computing a series of
    validation metrics based on specific business rules. These metrics are used
    to assess the completeness and correctness of the scraped data for a given
    league and season before it is promoted to the next data layer.

    Metrics checked include:
    -   Completeness of team game files.
    -   Conformity to a double round-robin tournament structure.
    -   Validity and completeness of game attendance data.
    -   Correctness of team URLs against the season year.

    Args:
        distinct_games_df (pd.DataFrame): A DataFrame containing the unique games
            played during the season.
        standings_df (pd.DataFrame): A DataFrame with the final league standings
            for the season.
        league_name (str): The sanitized, URL-friendly name of the league.
        season_year (int): The starting year of the season (e.g., 2023 for 2023/24).
        standings_file_name (str): The original filename of the standings CSV,
            used to generate a unique source ID.
        num_team_files (int): The total count of individual team game files found
            in the source directory for the season.

    Returns:
        Dict[str, Union[int, float, bool, str]]: A dictionary where keys are
            metric names and values are the calculated summary statistics for
            the season.
    """
    num_total_games = len(distinct_games_df)
    num_total_teams = len(standings_df)

    # Business Rule: Check if a game file exists for every team in the standings.
    has_all_teams_files = num_team_files == num_total_teams

    # Business Rule: A "double round-robin" tournament has N*(N-1) games.
    is_double_rounded = num_total_games == num_total_teams * (num_total_teams - 1)

    # Coerce 'audience' column to numeric, turning non-numeric values into NaN.
    # This is a data cleaning step to handle malformed audience strings.
    audience_numeric = pd.to_numeric(distinct_games_df["audience"], errors="coerce")

    # Business Rule: A null attendance is defined as either NaN (after coercion) or zero.
    num_null_attendance_games = int(
        audience_numeric.isna().sum() + (audience_numeric == 0).sum()
    )

    pct_null_attendance_games = (
        (num_null_attendance_games / num_total_games) * 100
        if num_total_games > 0
        else 0
    )
    # Business Rule: Attendance data is considered valid if less than 5% of games have null values.
    is_valid_attendance = pct_null_attendance_games < 5.0

    # Business Rule: All team URLs must point to a year less than or equal to the current season year.
    is_valid_url = all(
        standings_df.apply(
            lambda row: helpers.validate_url_year(row["team_url"], season_year), axis=1
        )
    )

    return {
        "source_id": helpers.generate_id(standings_file_name),
        "source_csv_file": standings_file_name,
        "league_name": league_name,
        "season_year": season_year,
        "num_total_teams": num_total_teams,
        "num_total_games": num_total_games,
        "num_null_attendance_games": num_null_attendance_games,
        "pct_null_attendance_games": round(pct_null_attendance_games, 2),
        "is_valid_url": is_valid_url,
        "is_double_rounded": is_double_rounded,
        "is_valid_attendance": is_valid_attendance,
        "has_all_teams_files": has_all_teams_files,
    }


def _load_and_consolidate_games(team_games_dir: Path) -> Optional[pd.DataFrame]:
    """Loads, validates, and consolidates all team game CSVs from a directory.

    This function iterates through all CSV files within a specified directory,
    each expected to contain the game schedule for a single team. It reads and
    validates each file, requiring a standard set of columns for consistency.
    All valid DataFrames are then concatenated into a single master DataFrame.

    Since each game is present in the schedule file for both the home and away
    teams, duplicate entries are expected. The function removes these duplicates
    based on key match identifiers to produce a final, unique set of games for
    the season.

    Args:
        team_games_dir (Path): The directory path containing the individual team
            game CSV files for a single season.

    Returns:
        Optional[pd.DataFrame]: A consolidated DataFrame containing unique game
            records. Returns `None` if no valid game data could be loaded from
            the directory.
    """
    all_games_dfs: List[pd.DataFrame] = []
    required_columns = ["date", "home_team", "away_team", "result", "audience"]

    game_files = list(team_games_dir.glob("*.csv"))
    logger.info(
        "Starting to load %d game files from %s", len(game_files), team_games_dir
    )

    for team_file in game_files:
        game_df = _read_csv_file(team_file)
        if game_df is not None and _validate_input_df(game_df, required_columns):
            all_games_dfs.append(game_df)

    if not all_games_dfs:
        logger.warning("No valid game files found in directory: %s", team_games_dir)
        return None

    consolidated_games_df = pd.concat(all_games_dfs, ignore_index=True)

    # A single game is reported in the file of each participating team, so duplicates are expected.
    # Dropping them based on key match identifiers provides a unique set of games.

    distinct_games_df = consolidated_games_df.drop_duplicates(
        subset=["date", "home_team", "away_team", "result"]
    ).copy()

    logger.info(
        "Consolidated %d unique games from %d valid files.",
        len(distinct_games_df),
        len(all_games_dfs),
    )
    return distinct_games_df


def _process_season_data(
    standings_file: Path,
) -> Optional[Dict[str, Union[int, float, bool, str]]]:
    """Processes data for a single season to generate summary metrics.

    This function serves as a pipeline for a single season's data. It begins by
    reading a standings file and extracting metadata (league name, season year).
    Based on a conventional directory structure, it locates the corresponding
    'team_games' directory situated parallel to the 'final_standings' directory.

    It then consolidates all individual team game files from that directory,
    calculates a series of validation and summary metrics, and returns them as a
    dictionary. The function is designed to be robust, returning None if any
    critical step fails, such as missing files or invalid data formats.

    Args:
        standings_file (Path): The path to the league standings CSV file for a
            specific season.

    Returns:
        Optional[Dict[str, Union[int, float, bool, str]]]: A dictionary
            containing the summary metrics for the season. Returns None if the
            data is incomplete, invalid, or a processing error occurs.
    """
    print("\n")
    logger.info("Processing standings file: %s", standings_file.name)
    try:
        metadata = helpers.extract_metadata_from_filename(standings_file)
        league_name = metadata["league_name"]
        season_year = metadata["season_year"]

        standings_df = _read_csv_file(standings_file)

        if standings_df is None or not _validate_input_df(
            standings_df, ["team", "team_url", "position"]
        ):
            logger.warning(
                "Invalid or unreadable standings file: %s", standings_file.name
            )
            return None

        team_games_dir = standings_file.parent.parent / "team_games"
        if not team_games_dir.is_dir():
            logger.warning(
                "Associated team games directory not found for: %s", standings_file.name
            )
            return None

        num_team_files = len(list(team_games_dir.glob("*.csv")))
        distinct_games_df = _load_and_consolidate_games(team_games_dir)
        if distinct_games_df is None:
            logger.warning(
                "No valid game data found for %s/%s", league_name, season_year
            )
            return None

        season_metrics = _process_season_metrics(
            distinct_games_df,
            standings_df,
            league_name,
            season_year,
            standings_file.name,
            num_team_files,
        )
        logger.info(
            "Successfully processed season %s/%s: %d games, %d teams.",
            league_name,
            season_year,
            season_metrics["num_total_games"],
            season_metrics["num_total_teams"],
        )
        return season_metrics

    except (FileNotFoundError, pd.errors.EmptyDataError) as e:
        logger.error("File access error for %s: %s", standings_file.name, e)
        return None
    except Exception as e:
        logger.error(
            "Unexpected error processing %s: %s\n%s",
            standings_file.name,
            e,
            traceback.format_exc(),
        )
        return None


def create_standings_summary() -> pd.DataFrame:
    """Generates a summary DataFrame from all available season standings files.

    This function scans the scraper's output directory for all files matching the
    `*_standings.csv` pattern. It iterates through each file, processes it to
    extract summary metrics for that specific league and season, and compiles the
    results into a single comprehensive DataFrame. This summary is primarily
    used to validate the completeness and quality of the scraped data for each
    season.

    Returns:
        pd.DataFrame: A DataFrame where each row represents the summary of a
            single season. Returns an empty DataFrame if no valid standings
            files are found or processed successfully.
    """
    logger.info("Starting creation of the complete season standings summary.")
    summary_data: List[Dict] = []
    leagues_processed = set()
    standings_files = list(paths.SCRAPER_OUTPUT_DIR.rglob("*_standings.csv"))

    logger.info("Found %d potential standings files to process.", len(standings_files))

    for standings_file in standings_files:
        leagues_processed.add(standings_file.parent.parent.parent.name)
        season_summary = _process_season_data(standings_file)
        if season_summary:
            summary_data.append(season_summary)

    if not summary_data:
        logger.warning("No season data could be processed to generate a summary.")
        return pd.DataFrame()

    summary_df = pd.DataFrame(summary_data)
    print("\n")
    logger.info(
        "Standings summary created successfully: %d seasons from %d leagues.",
        len(summary_df),
        len(leagues_processed),
    )
    return summary_df


# ***************************************************************************


def _enrich_standings_df(df: pd.DataFrame, standings_file: Path) -> pd.DataFrame:
    """Enriches a standings DataFrame with metadata and generated IDs.

    This function takes a raw standings DataFrame from a single season file and
    augments it with crucial metadata. This process makes the data ready for
    consolidation and integration with other datasets.

    The enrichment steps are:
    1.  Extracts `league_name` and `season_year` from the source filename.
    2.  Creates a sanitized team name (`team_sanitized`) for reliable joins.
    3.  Generates a unique ID for the source file (`source_id`).
    4.  Generates a unique ID for each row (team-season combination).
    5.  Converts the 'position' column to a proper integer type.

    Args:
        df (pd.DataFrame): The input standings DataFrame from a single season.
        standings_file (Path): The path to the source standings CSV file from which
            the DataFrame was loaded.

    Returns:
        pd.DataFrame: The enriched DataFrame with new metadata columns.
    """
    metadata = helpers.extract_metadata_from_filename(standings_file)
    df_enriched = df.copy()
    df_enriched["team_sanitized"] = df_enriched["team"].apply(helpers.sanitize_filename)
    df_enriched["league_name"] = metadata["league_name"]
    df_enriched["season_year"] = metadata["season_year"]
    df_enriched["source_csv_file"] = standings_file.name
    df_enriched["source_id"] = helpers.generate_id(standings_file.name)
    df_enriched["id"] = df_enriched.apply(
        lambda row: helpers.generate_id(
            f"{row['team_sanitized']}_{row['source_csv_file']}"
        ),
        axis=1,
    )
    df_enriched["position"] = pd.to_numeric(
        df_enriched["position"], errors="coerce"
    ).astype("Int64")
    return df_enriched


def create_standings_complete() -> pd.DataFrame:
    """Consolidates all individual standings files into a single master DataFrame.

    This function orchestrates the creation of the complete standings dataset. It
    scans the scraper's output directory for all individual season standings
    files (`*_standings.csv`). Each file is read, validated, enriched with
    metadata via `_enrich_standings_df`, and then concatenated into a single,
    unified DataFrame. Finally, it standardizes the schema to ensure column
    consistency across all data.

    Returns:
        pd.DataFrame: A comprehensive DataFrame containing all standings data from
            all leagues and seasons. Returns an empty DataFrame if no valid
            standings files are found or processed.
    """
    logger.info("Starting creation of the complete standings dataset.")
    all_standings_dfs: List[pd.DataFrame] = []
    standings_files = list(paths.SCRAPER_OUTPUT_DIR.rglob("*_standings.csv"))

    logger.info("Found %d standings files to combine.", len(standings_files))

    for standings_file in standings_files:
        try:
            df = _read_csv_file(standings_file)
            if df is not None and _validate_input_df(df, ["team", "position"]):
                enriched_df = _enrich_standings_df(df, standings_file)
                all_standings_dfs.append(enriched_df)
        except Exception as e:
            logger.error(
                "Failed to process standings file %s: %s\n%s",
                standings_file.name,
                e,
                traceback.format_exc(),
            )

    if not all_standings_dfs:
        logger.warning("No valid standings data found to create a complete dataset.")
        return pd.DataFrame()

    final_df = pd.concat(all_standings_dfs, ignore_index=True)

    # Standardize column naming ('draw' vs 'drawn') for consistency.
    if "draw" in final_df.columns and "drawn" not in final_df.columns:
        final_df.rename(columns={"draw": "drawn"}, inplace=True)

    # The 'team_sanitized' column is the key for the name mapping process.
    # It will be renamed to 'team_canonical' after the mapping is applied in the main pipeline.
    final_schema = [
        "id",
        "source_id",
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
    ]
    # Reindex to ensure a consistent column order and add any missing columns as NaN.
    final_df = final_df.reindex(columns=final_schema)
    logger.info("Complete standings dataset created with %d rows.", len(final_df))
    return final_df


# ***************************************************************************


def _enrich_season_games(
    df: pd.DataFrame, league_name: str, season_year: int
) -> pd.DataFrame:
    """Enriches a season's games DataFrame with metadata and generated IDs.

    This function adds several columns to the input DataFrame to provide
    context and create unique, deterministic identifiers for each record and
    its related entities. It links games back to their source files and the
    season's main standings file.

    Args:
        df (pd.DataFrame): The input DataFrame containing game data for a
            single season.
        league_name (str): The sanitized, URL-friendly name of the league.
        season_year (int): The starting year of the season.

    Returns:
        pd.DataFrame: The enriched DataFrame with new columns for league,
            season, and various unique IDs.
    """
    standings_csv_file = f"{league_name}_{season_year}_standings.csv"
    df["league_name"] = league_name
    df["season_year"] = season_year
    df["standings_csv_file"] = standings_csv_file
    df["standings_id"] = helpers.generate_id(standings_csv_file)
    df["source_id"] = df["source_csv_file"].apply(helpers.generate_id)
    df["id"] = df.apply(
        lambda row: helpers.generate_id(
            f"{row['round']}_{row['home_team_sanitized']}_{row['standings_csv_file']}"
        ),
        axis=1,
    )
    return df


def _parse_datetime(df: pd.DataFrame) -> pd.DataFrame:
    """Parses date and time string columns into a single datetime object column.

    This function combines the 'date' and 'time' string columns into a unified
    'datetime' column with a proper `datetime64[ns]` dtype. It is designed to
    be robust against common data inconsistencies by:
    -   Replacing missing or unknown times with a default of "00:00".
    -   Cleaning date strings that include day-of-the-week abbreviations (e.g.,
        'sáb, 29/04/2023' becomes '29/04/2023').
    -   Coercing parsing errors into `NaT` (Not a Time) values to prevent
        the process from failing on malformed entries.

    Args:
        df (pd.DataFrame): The input DataFrame, which is expected to have
            'date' and 'time' string columns.

    Returns:
        pd.DataFrame: The DataFrame with an added 'datetime' column. Rows that
            could not be parsed will have `NaT` in this column.
    """
    logger.debug("Starting explicit datetime parsing.")
    if "date" not in df.columns or "time" not in df.columns:
        logger.error("DataFrame is missing 'date' or 'time' columns for parsing.")
        df["datetime"] = pd.NaT
        return df

    # Normalize time and combine with date string.
    df["time"] = df["time"].replace("desconhecido", "00:00").fillna("00:00")
    datetime_str = df["date"].fillna("") + " " + df["time"].fillna("")
    # Handles formats like "sáb, 29/04/2023" by removing the day abbreviation.
    cleaned_datetime_str = datetime_str.str.split(n=1).str[1].str.strip()
    date_format = "%d/%m/%Y %H:%M"

    df["datetime"] = pd.to_datetime(
        cleaned_datetime_str, format=date_format, errors="coerce"
    )
    invalid_dates_count = df["datetime"].isna().sum()
    if invalid_dates_count > 0:
        logger.warning(
            "%d rows could not be parsed to datetime with format '%s' and were set to NaT.",
            invalid_dates_count,
            date_format,
        )
    return df


def _impute_audience(df: pd.DataFrame) -> pd.DataFrame:
    """Imputes missing audience values for a specific team's home games.

    This function identifies the primary team within the input DataFrame (assumed
    to be the team appearing most frequently in the 'home_team_sanitized'
    column). It then filters the data to include only this team's home games.

    For these home games, it calculates the mean and median audience, ignoring
    zeros and non-numeric values. It then creates several new columns with the
    missing audience values imputed using the mean, median, and a
    forward/backward fill strategy.

    Args:
        df (pd.DataFrame): The input DataFrame of game data, typically from a
            single team's schedule file. It must contain 'home_team_sanitized'
            and 'audience' columns.

    Returns:
        pd.DataFrame: A new DataFrame containing only the home games of the
            primary team, now with additional columns for imputed audience
            values (`audience_filled_mean`, `audience_filled_median`,
            `audience_filled_fb`).

    Raises:
        KeyError: If the required columns 'home_team_sanitized' or 'audience'
            are missing from the input DataFrame.
    """
    logger.debug("Starting audience imputation for DataFrame with %d rows.", len(df))
    if not _validate_input_df(df, ["home_team_sanitized", "audience"]):
        raise KeyError("Missing required columns for audience imputation.")

    # In a team-specific game file, the primary team is the one that appears most as the home team.
    principal_team = df["home_team_sanitized"].value_counts().idxmax()
    home_df = df[df["home_team_sanitized"] == principal_team].copy()

    # Convert audience to a numeric type, coercing errors to NaN.
    home_df["audience"] = pd.to_numeric(home_df["audience"], errors="coerce")

    # Treat 0 as NaN for statistical calculations to avoid skewing the results.
    audience_clean_home = home_df["audience"].replace(0, np.nan)

    if audience_clean_home.notna().any():
        mean_val = audience_clean_home.mean()
        median_val = audience_clean_home.median()

        home_df["audience_filled_mean"] = audience_clean_home.fillna(mean_val)
        home_df["audience_filled_median"] = audience_clean_home.fillna(median_val)
    else:
        # If all audience data is missing, the imputed columns will also be NaN.
        home_df["audience_filled_mean"] = np.nan
        home_df["audience_filled_median"] = np.nan

    home_df["audience_filled_fb"] = audience_clean_home.ffill().bfill()

    for col in ["audience_filled_fb", "audience_filled_mean", "audience_filled_median"]:
        if col in home_df.columns:
            home_df[col] = home_df[col].round(0).astype("Int64")

    logger.debug(
        "Imputed audience for team %s in %d home games.", principal_team, len(home_df)
    )
    return home_df


def _process_season_games(
    team_games_dir: Path, league_name: str, season_year: int
) -> Optional[pd.DataFrame]:
    """Processes all team game files for a single season into a master DataFrame.

    This function serves as a pipeline for consolidating and enriching all game
    data for a given league and season. It iterates through each team's game
    file in the specified directory, performs data cleaning and imputation, and
    then combines the results.

    The key processing steps include:
    1.  Reading each team's game file.
    2.  Sanitizing team names.
    3.  Calling `_impute_audience` to fill missing attendance data for home games.
    4.  Concatenating the processed home-game data from all teams.
    5.  Performing final enrichments, such as parsing datetimes and generating IDs.

    Args:
        team_games_dir (Path): The directory path containing the individual team
            game CSV files.
        league_name (str): The sanitized, URL-friendly name of the league.
        season_year (int): The starting year of the season.

    Returns:
        Optional[pd.DataFrame]: A single, enriched DataFrame containing all
            processed game data for the season. Returns `None` if no valid game
            files are found or processed successfully.
    """
    print("\n")
    logger.info("Processing games for season: %s/%s", league_name, season_year)
    all_games_enriched: List[pd.DataFrame] = []
    game_files = list(team_games_dir.glob("*.csv"))

    for team_file in game_files:
        try:
            df = _read_csv_file(team_file)
            metadata = helpers.extract_metadata_from_filename(team_file)
            if df is not None:
                df["home_team_sanitized"] = df["home_team"].apply(
                    helpers.sanitize_filename
                )
                df["source_csv_file"] = team_file.name
                df_imputed = _impute_audience(df)
                all_games_enriched.append(df_imputed)
        except Exception as e:
            logger.error(
                "Failed to process game file %s: %s\n%s",
                team_file.name,
                e,
                traceback.format_exc(),
            )

    if not all_games_enriched:
        logger.warning(
            "No valid game files processed for season: %s/%s", league_name, season_year
        )
        return None

    season_df = pd.concat(all_games_enriched, ignore_index=True)
    season_df["away_team_sanitized"] = season_df["away_team"].apply(
        helpers.sanitize_filename
    )
    season_df["coach_sanitized"] = season_df["coach"].apply(helpers.sanitize_filename)
    season_df = _parse_datetime(season_df)
    season_df = _enrich_season_games(season_df, league_name, season_year)

    logger.info(
        "Completed processing for %s/%s, with %d game rows.",
        league_name,
        season_year,
        len(season_df),
    )
    return season_df


def create_team_games_complete() -> pd.DataFrame:
    """Consolidates all team game files from all seasons into a master DataFrame.

    This function orchestrates the creation of the complete games dataset by
    navigating the scraper's output directory structure. It iterates through
    each league and season, processing the corresponding 'team_games'
    directory using the `_process_season_games` helper function.

    The resulting DataFrames from each season are then concatenated into a
    single, unified dataset. Finally, the schema of this master DataFrame is
    standardized to ensure a consistent column order and structure.

    Returns:
        pd.DataFrame: A comprehensive DataFrame containing all processed and
            enriched game data from all available leagues and seasons. Returns
            an empty DataFrame if no valid game data is found.
    """
    logger.info("Starting creation of the complete team games dataset.")
    all_seasons_dfs: List[pd.DataFrame] = []
    processed_leagues = set()

    for league_dir in paths.SCRAPER_OUTPUT_DIR.iterdir():
        if not league_dir.is_dir():
            continue
        processed_leagues.add(league_dir.name)
        for year_dir in league_dir.iterdir():
            if not year_dir.is_dir():
                continue
            team_games_dir = year_dir / "team_games"
            if team_games_dir.is_dir():
                season_df = _process_season_games(
                    team_games_dir, league_dir.name, int(year_dir.name)
                )
                if season_df is not None:
                    all_seasons_dfs.append(season_df)

    if not all_seasons_dfs:
        logger.warning("No valid team game data found to create a complete dataset.")
        return pd.DataFrame()

    final_df = pd.concat(all_seasons_dfs, ignore_index=True)

    # The final schema includes columns for canonical team names, which will be populated
    # by the mapping process in the main pipeline. The 'sanitized' columns are
    # intermediate keys used for this mapping.

    final_schema = [
        "id",
        "source_id",
        "standings_id",
        "round",
        "date",
        "time",
        "datetime",
        "home_team",
        "home_team_sanitized",
        "home_team_canonical",
        "away_team",
        "away_team_sanitized",
        "away_team_canonical",
        "formation",
        "coach",
        "coach_sanitized",
        "result",
        "audience",
        "audience_filled_fb",
        "league_name",
        "season_year",
        "source_csv_file",
        "standings_csv_file",
    ]
    final_df = final_df.reindex(columns=final_schema)
    print("\n")
    logger.info(
        "Complete team games dataset created with %d rows from %d leagues.",
        len(final_df),
        len(processed_leagues),
    )
    return final_df


# ***************************************************************************
