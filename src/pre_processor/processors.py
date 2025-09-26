import logging
import numpy as np
import pandas as pd
from pathlib import Path
from typing import List, Dict, Optional, Union
from src.pre_processor import config
from src.pre_processor import utils
import traceback

logger = logging.getLogger(__name__)


def _validate_input_df(df: pd.DataFrame, required_columns: List[str]) -> bool:
    """Validates if the DataFrame contains all required columns.

    Args:
        df (pd.DataFrame): Input DataFrame to validate.
        required_columns (List[str]): List of required column names.

    Returns:
        bool: True if all columns are present, False otherwise.
    """
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        logger.error(f"Missing required columns: {missing_columns}")
        return False
    return True


def _read_game_file(file_path: Path) -> Optional[pd.DataFrame]:
    """Reads a single game CSV file with error handling.

    Args:
        file_path (Path): Path to the CSV file.

    Returns:
        Optional[pd.DataFrame]: DataFrame if successful, None if failed.

    Raises:
        pd.errors.ParserError: If the CSV file is malformed.
        UnicodeDecodeError: If encoding fails.
    """
    try:
        df = pd.read_csv(file_path, encoding="utf-8", chunksize=10000)
        combined_df = pd.concat(df, ignore_index=True)
        if combined_df.empty:
            logger.warning(f"Empty file skipped: {file_path.name}")
            return None
        logger.info(f"Loaded file: {file_path.name} with {len(combined_df)} rows")
        return combined_df
    except pd.errors.ParserError as e:
        logger.error(f"Malformed CSV {file_path.name}: {str(e)}")
        return None
    except UnicodeDecodeError as e:
        logger.error(f"Encoding error {file_path.name}: {str(e)}")
        return None
    except Exception as e:
        logger.error(f"Error reading {file_path.name}: {str(e)}")
        return None


def _load_and_consolidate_games(team_games_dir: Path) -> Optional[pd.DataFrame]:
    """Loads and consolidates game files from a directory into a single DataFrame with unique matches.

    Args:
        team_games_dir (Path): Directory path containing team game CSV files.

    Returns:
        Optional[pd.DataFrame]: Consolidated DataFrame with unique games, or None if no valid data is found.

    Raises:
        FileNotFoundError: If the directory is inaccessible.
    """
    all_games_dfs: List[pd.DataFrame] = []
    required_columns = ["date", "home_team", "away_team", "result", "audience"]

    game_files = list(team_games_dir.glob("*.csv"))
    logger.info(f"Starting to load {len(game_files)} game files from {team_games_dir}")
    for team_file in game_files:
        game_df = _read_game_file(team_file)
        if game_df is not None and _validate_input_df(game_df, required_columns):
            all_games_dfs.append(game_df)

    if not all_games_dfs:
        logger.warning(f"No valid game files in {team_games_dir}")
        return None

    consolidated_games_df = pd.concat(all_games_dfs, ignore_index=True)
    distinct_games_df = consolidated_games_df.drop_duplicates(
        subset=["date", "home_team", "away_team", "result"]
    ).copy()
    logger.info(
        f"Consolidated {len(distinct_games_df)} unique games from {len(all_games_dfs)} files"
    )
    return distinct_games_df


def _validate_standings_df(standings_df: pd.DataFrame) -> bool:
    """Validates the standings DataFrame structure.

    Args:
        standings_df (pd.DataFrame): Input standings DataFrame.

    Returns:
        bool: True if valid, False otherwise.
    """
    required_columns = ["team", "team_url", "position"]
    if not _validate_input_df(standings_df, required_columns):
        return False
    return True


def _process_season_metrics(
    distinct_games_df: pd.DataFrame,
    standings_df: pd.DataFrame,
    league_name: str,
    season_year: int,
    standings_file_name: str,
    num_team_files: int,
) -> Dict[str, Union[int, float, bool, str]]:
    """Calculates season metrics based on game and standings data.

    Args:
        distinct_games_df (pd.DataFrame): DataFrame with unique games.
        standings_df (pd.DataFrame): DataFrame with standings data.
        league_name (str): Name of the league.
        season_year (int): Year of the season.
        standings_file_name (str): Name of the standings CSV file.
        num_team_files (int): Number of team game files.

    Returns:
        Dict[str, Union[int, float, bool, str]]: Dictionary with calculated metrics.
    """
    num_total_games = len(distinct_games_df)
    num_total_teams = len(standings_df)
    has_all_teams_files = num_team_files == num_total_teams
    is_double_rounded = num_total_games == num_total_teams * (num_total_teams - 1)

    distinct_games_df["audience"] = distinct_games_df["audience"].fillna(0)
    num_null_attendance_games = int((distinct_games_df["audience"] == 0).sum())
    pct_null_attendance_games = (
        (num_null_attendance_games / num_total_games) * 100
        if num_total_games > 0
        else 0
    )
    is_valid_attendance = pct_null_attendance_games < 5.0

    return {
        "source_id": utils.generate_id(standings_file_name),
        "source_csv_file": standings_file_name,
        "league_name": league_name,
        "season_year": season_year,
        "has_all_teams_files": has_all_teams_files,
        "num_total_teams": num_total_teams,
        "num_total_games": num_total_games,
        "num_null_attendance_games": num_null_attendance_games,
        "pct_null_attendance_games": round(pct_null_attendance_games, 2),
        "is_valid_url": all(
            standings_df.apply(
                lambda row: utils.validate_url_year(row["team_url"], season_year),
                axis=1,
            )
        ),
        "is_double_rounded": is_double_rounded,
        "is_valid_attendance": is_valid_attendance,
    }


def process_season_data(
    standings_file: Path,
) -> Optional[Dict[str, Union[int, float, bool, str]]]:
    """Processes season data from a standings file to generate a summary dictionary.

    Args:
        standings_file (Path): Path to the league standings CSV file.

    Returns:
        Optional[Dict[str, Union[int, float, bool, str]]]: Dictionary with season summary metrics, or None if processing fails.

    Raises:
        FileNotFoundError: If the standings file does not exist.
        pd.errors.EmptyDataError: If the standings file is empty or malformed.
    """
    logger.info(f"Starting processing standings file: {standings_file.name}")
    try:
        metadata = utils.extract_metadata_from_filename(standings_file)
        league_name = metadata["league_name"]
        season_year = metadata["season_year"]

        standings_df = pd.read_csv(standings_file, encoding="utf-8")
        if standings_df.empty or not _validate_standings_df(standings_df):
            logger.warning(f"Invalid standings file: {standings_file.name}")
            return None

        team_games_dir = standings_file.parent.parent / "team_games"
        if not team_games_dir.is_dir():
            logger.warning(f"No team games directory for {standings_file.name}")
            return None

        num_team_files = len(list(team_games_dir.glob("*.csv")))

        distinct_games_df = _load_and_consolidate_games(team_games_dir)
        if distinct_games_df is None:
            logger.warning(f"No valid game data for {league_name}/{season_year}")
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
            f"Processed {league_name}/{season_year}: {season_metrics['num_total_games']} games, {season_metrics['num_total_teams']} teams"
        )
        return season_metrics

    except FileNotFoundError:
        logger.error(f"File not found: {standings_file}")
        return None
    except pd.errors.EmptyDataError:
        logger.error(f"Empty standings file: {standings_file.name}")
        return None
    except Exception as e:
        logger.error(
            f"Error processing {standings_file.name}: {str(e)}\n{traceback.format_exc()}"
        )
        return None


def create_standings_summary() -> pd.DataFrame:
    """Creates a unified summary DataFrame from all league standings files.

    Returns:
        pd.DataFrame: Summary of processed seasons, or empty DataFrame if no data is found.
    """
    logger.info("Starting standings summary creation")
    summary_data: List[Dict] = []
    leagues_processed = set()

    for league_dir in config.RAW_DATA_DIR.iterdir():
        if not league_dir.is_dir():
            continue
        leagues_processed.add(league_dir.name)
        logger.info(f"Processing league: {league_dir.name}")

        for year_dir in league_dir.iterdir():
            if not year_dir.is_dir():
                continue
            standings_dir = year_dir / "final_standings"
            if not standings_dir.is_dir():
                continue

            for standings_file in standings_dir.glob("*_standings.csv"):
                logger.info(f"Processing file: {standings_file.name}")
                season_summary = process_season_data(standings_file)
                if season_summary:
                    summary_data.append(season_summary)

    if not summary_data:
        logger.warning("No season data processed")
        return pd.DataFrame()

    summary_df = pd.DataFrame(summary_data)
    logger.info(
        f"Completed summary: {len(summary_df)} seasons from {len(leagues_processed)} leagues"
    )
    return summary_df


def _enrich_standings_df(df: pd.DataFrame, standings_file: Path) -> pd.DataFrame:
    """Enriches a standings DataFrame with metadata and IDs.

    Args:
        df (pd.DataFrame): Input standings DataFrame.
        standings_file (Path): Path to the standings CSV file.

    Returns:
        pd.DataFrame: Enriched DataFrame.
    """
    metadata = utils.extract_metadata_from_filename(standings_file)
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
    df["position"] = pd.to_numeric(df["position"], errors="coerce").astype("Int64")
    return df


def create_standings_complete() -> pd.DataFrame:
    """Concatenates and enriches all standings files into a single DataFrame.

    Returns:
        pd.DataFrame: Enriched standings data, or empty DataFrame if no data is found.
    """
    logger.info("Starting complete standings creation")
    all_standings_dfs: List[pd.DataFrame] = []
    leagues_processed = set()

    standings_files = list(Path(config.RAW_DATA_DIR).rglob("*_standings.csv"))
    logger.info(f"Found {len(standings_files)} standings files")

    for standings_file in standings_files:
        league_name = standings_file.parent.parent.parent.name
        leagues_processed.add(league_name)
        try:
            df = _read_game_file(standings_file)
            if df is not None and _validate_standings_df(df):
                enriched_df = _enrich_standings_df(df, standings_file)
                all_standings_dfs.append(enriched_df)
                logger.info(f"Processed standings: {standings_file.name}")
        except Exception as e:
            logger.error(
                f"Error processing {standings_file.name}: {str(e)}\n{traceback.format_exc()}"
            )

    if not all_standings_dfs:
        logger.warning("No valid standings data")
        return pd.DataFrame()

    final_df = pd.concat(all_standings_dfs, ignore_index=True)

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
    if "draw" in final_df.columns and "drawn" not in final_df.columns:
        final_df.rename(columns={"draw": "drawn"}, inplace=True)

    final_df = final_df.reindex(columns=final_schema)
    logger.info(
        f"Completed standings: {len(final_df)} rows from {len(leagues_processed)} leagues"
    )
    return final_df


def _inpute_audience(df: pd.DataFrame) -> pd.DataFrame:
    """Imputes missing audience values for home games of the most frequent team.

    Args:
        df (pd.DataFrame): Input DataFrame with game data, including 'home_team_sanitized' and 'audience' columns.

    Returns:
        pd.DataFrame: Filtered DataFrame with imputed audience columns for home games.

    Raises:
        KeyError: If required columns are missing.
    """
    logger.debug(f"Starting imputation for DataFrame with {len(df)} rows")
    required_columns = ["home_team_sanitized", "audience"]
    if not _validate_input_df(df, required_columns):
        raise KeyError("Missing columns for imputation")

    team_principal = df["home_team_sanitized"].value_counts().idxmax()
    home_df = df[df["home_team_sanitized"] == team_principal].copy()

    audience_clean_home = home_df["audience"].replace(0, np.nan)

    if audience_clean_home.notna().any():
        mean_val = audience_clean_home.mean()
        median_val = audience_clean_home.median()

        home_df["audience_filled_mean"] = audience_clean_home.fillna(mean_val)
        home_df["audience_filled_median"] = audience_clean_home.fillna(median_val)
    else:
        home_df["audience_filled_mean"] = np.nan
        home_df["audience_filled_median"] = np.nan

    home_df["audience_filled_fb"] = audience_clean_home.ffill().bfill()

    for col in [
        "audience_filled_fb",
        "audience_filled_mean",
        "audience_filled_median",
    ]:
        if col in home_df:
            home_df[col] = home_df[col].round(0).astype("Int64")

    logger.info(f"Imputed audience for team {team_principal}, {len(home_df)} rows")
    return home_df


def _enrich_season_games(
    df: pd.DataFrame, league_name: str, season_year: int
) -> pd.DataFrame:
    """Enriches a season's game DataFrame with metadata and IDs.

    Args:
        df (pd.DataFrame): Input game DataFrame.
        league_name (str): Name of the league.
        season_year (int): Year of the season.

    Returns:
        pd.DataFrame: Enriched DataFrame.
    """
    standings_csv_file = f"{league_name}_{season_year}_standings.csv"
    df["league_name"] = league_name
    df["season_year"] = season_year
    df["standings_csv_file"] = standings_csv_file
    df["standings_id"] = utils.generate_id(standings_csv_file)
    df["source_id"] = df["source_csv_file"].apply(utils.generate_id)
    df["id"] = df.apply(
        lambda row: utils.generate_id(
            f"{row['round']}_{row['home_team_sanitized']}_{row['standings_csv_file']}"
        ),
        axis=1,
    )
    return df


def _parse_datetime(df: pd.DataFrame) -> pd.DataFrame:
    """
    Parses date and time columns into a robust datetime column using an explicit format.

    This function combines 'date' and 'time' string columns and converts
    them into a single datetime object column. It is specifically designed to
    handle formats that include a day-of-the-week abbreviation (e.g., 'sáb', 'sex')
    by removing it before parsing.

    Args:
        df (pd.DataFrame): Input DataFrame with 'date' and 'time' columns.

    Returns:
        pd.DataFrame: The DataFrame with an added 'datetime' column of
                      dtype datetime64[ns].
    """
    logger.info("Starting explicit datetime parsing for formats with weekdays...")

    if "date" not in df.columns or "time" not in df.columns:
        logger.error("Input DataFrame is missing required 'date' or 'time' columns.")
        df["datetime"] = pd.NaT
        return df

    df["time"] = df["time"].replace("desconhecido", "00:00").fillna("00:00")
    datetime_str = df["date"].fillna("") + " " + df["time"].fillna("")
    cleaned_datetime_str = datetime_str.str.split(n=1).str[1]
    date_format = "%d/%m/%Y %H:%M"

    try:
        df["datetime"] = pd.to_datetime(
            cleaned_datetime_str, format=date_format, errors="coerce"
        )

        invalid_dates_count = df["datetime"].isna().sum()
        if invalid_dates_count > 0:
            logger.warning(
                f"Datetime parsing complete. Found {invalid_dates_count} rows "
                f"that did not match the format '{date_format}' and were set to NaT."
            )
        else:
            logger.info(
                "Successfully parsed datetime column for all rows using the specified format."
            )

    except Exception as e:
        logger.error(
            f"An unexpected error occurred during datetime parsing. Error: {e}",
            exc_info=True,
        )
        df["datetime"] = pd.NaT

    return df


def _process_season_games(
    team_games_dir: Path, league_name: str, season_year: int
) -> Optional[pd.DataFrame]:
    """Processes team game files for a single season, enriching with metadata and imputing audience data.

    Args:
        team_games_dir (Path): Directory containing team game CSV files.
        league_name (str): Name of the league.
        season_year (int): Year of the season.

    Returns:
        Optional[pd.DataFrame]: Enriched DataFrame with game data, or None if no valid data is found.
    """
    logger.info(f"Starting games processing for {league_name}/{season_year}")
    all_games_enriched: List[pd.DataFrame] = []

    game_files = list(team_games_dir.glob("*.csv"))
    logger.info(f"Found {len(game_files)} game files for {league_name}/{season_year}")
    for team_file in game_files:
        try:
            df = _read_game_file(team_file)
            if df is not None:
                df["home_team_sanitized"] = df["home_team"].apply(
                    utils.sanitize_filename
                )
                df["source_csv_file"] = team_file.name
                df = _inpute_audience(df)
                all_games_enriched.append(df)
                logger.info(f"Processed game file: {team_file.name}")
        except Exception as e:
            logger.error(
                f"Error processing {team_file.name}: {str(e)}\n{traceback.format_exc()}"
            )

    if not all_games_enriched:
        logger.warning(f"No valid games for {league_name}/{season_year}")
        return None

    season_df = pd.concat(all_games_enriched, ignore_index=True)
    season_df["away_team_sanitized"] = season_df["away_team"].apply(
        utils.sanitize_filename
    )
    season_df["coach_sanitized"] = season_df["coach"].apply(utils.sanitize_filename)
    season_df = _parse_datetime(season_df)
    season_df = _enrich_season_games(season_df, league_name, season_year)

    logger.info(f"Completed {len(season_df)} games for {league_name}/{season_year}")
    return season_df


def create_team_games_complete() -> pd.DataFrame:
    """Creates a unified DataFrame with all distinct team games across seasons.

    Returns:
        pd.DataFrame: Consolidated DataFrame with enriched game data, or empty if no data is found.
    """
    logger.info("Starting team games completion")
    all_seasons_dfs: List[pd.DataFrame] = []
    leagues_processed = set()

    for league_dir in config.RAW_DATA_DIR.iterdir():
        if not league_dir.is_dir():
            continue
        leagues_processed.add(league_dir.name)
        logger.info(f"Processing league: {league_dir.name}")

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
        logger.warning("No valid team game data")
        return pd.DataFrame()

    final_df = pd.concat(all_seasons_dfs, ignore_index=True)

    final_schema = [
        "round",
        "date",
        "time",
        "datetime",
        "home_team",
        "home_team_sanitized",
        "away_team",
        "away_team_sanitized",
        "formation",
        "coach",
        "coach_sanitized",
        "result",
        "audience",
        "audience_filled_fb",
        "audience_filled_mean",
        "audience_filled_median",
        "league_name",
        "season_year",
        "source_csv_file",
        "standings_csv_file",
        "source_id",
        "standings_id",
        "id",
    ]

    final_df = final_df.reindex(columns=final_schema)
    logger.info(
        f"Completed team games: {len(final_df)} rows from {len(leagues_processed)} leagues"
    )

    return final_df


def create_team_games_mid_valid(
    valid_games_df: pd.DataFrame,
) -> Optional[pd.DataFrame]:
    """
    Filters a DataFrame of valid games to include only the first half of each season.

    Args:
        valid_games_df (pd.DataFrame): DataFrame containing games from valid seasons.

    Returns:
        Optional[pd.DataFrame]: A DataFrame with only the first-round games, or None if input is empty.
    """
    if valid_games_df.empty:
        logger.warning("Input DataFrame for mid-season split is empty. Skipping.")
        return None

    logger.info("Starting mid-season (first round) data creation.")
    df = valid_games_df.copy()
    df["round"] = pd.to_numeric(df["round"], errors="coerce")
    df.dropna(subset=["round"], inplace=True)
    df["round"] = df["round"].astype(int)

    def filter_first_half(group: pd.DataFrame) -> pd.DataFrame:
        max_round = group["round"].max()
        mid_point = max_round / 2
        return group[group["round"] <= mid_point]

    mid_season_df = (
        df.groupby(["league_name", "season_year"])
        .apply(filter_first_half, include_groups=False)
        .reset_index(drop=True)
    )

    logger.info(
        f"Successfully created mid-season DataFrame with {len(mid_season_df)} rows."
    )
    return mid_season_df
