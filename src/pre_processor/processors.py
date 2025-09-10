import logging
import numpy as np
import pandas as pd
from pathlib import Path
from src.pre_processor import config
from src.pre_processor import utils

logger = logging.getLogger(__name__)


def _load_and_consolidate_games(team_games_dir: Path) -> pd.DataFrame | None:
    """Loads and consolidates game files from a directory into a single DataFrame with unique matches.

    Args:
        team_games_dir (Path): Directory path containing team game CSV files.

    Returns:
        pd.DataFrame | None: Consolidated DataFrame with unique games, or None if no valid data is found.

    Raises:
        pd.errors.EmptyDataError: If a CSV file is empty or malformed.
        Exception: For other unexpected errors during file processing.
    """
    all_games_dfs = []
    for team_file in team_games_dir.glob("*.csv"):
        try:
            game_df = pd.read_csv(team_file, encoding="utf-8")
            if not game_df.empty:
                all_games_dfs.append(game_df)
                logger.debug(f"Successfully loaded game file: {team_file.name}")
            else:
                logger.warning(f"Empty game file skipped: {team_file.name}")
        except pd.errors.EmptyDataError:
            logger.warning(f"Empty or malformed CSV file skipped: {team_file.name}")
        except Exception as e:
            logger.error(f"Failed to process game file {team_file.name}: {str(e)}")

    if not all_games_dfs:
        logger.info(f"No valid game files found in directory: {team_games_dir}")
        return None

    consolidated_games_df = pd.concat(all_games_dfs, ignore_index=True)
    distinct_games_df = consolidated_games_df.drop_duplicates(
        subset=["date", "home_team", "away_team", "result"]
    ).copy()
    logger.info(
        f"Consolidated {len(distinct_games_df)} unique games from {len(all_games_dfs)} files"
    )

    return distinct_games_df


def process_season_data(standings_file: Path) -> dict | None:
    """Processes season data from a standings file to generate a summary dictionary.

    Args:
        standings_file (Path): Path to the league standings CSV file.

    Returns:
        dict | None: Dictionary with season summary metrics, or None if processing fails.

    Raises:
        FileNotFoundError: If the standings file does not exist.
        pd.errors.EmptyDataError: If the standings file is empty or malformed.
        Exception: For other unexpected errors during processing.
    """
    logger.info(f"Starting processing for standings file: {standings_file.name}")
    try:
        metadata = utils.extract_metadata_from_filename(standings_file)
        league_name = metadata["league_name"]
        season_year = metadata["season_year"]

        standings_df = pd.read_csv(standings_file, encoding="utf-8")
        if standings_df.empty:
            logger.warning(f"Empty standings file: {standings_file.name}")
            return None

        num_total_teams = len(standings_df)
        is_valid_url = all(
            standings_df.apply(
                lambda row: utils.validate_url_year(row["team_url"], season_year),
                axis=1,
            )
        )

        team_games_dir = standings_file.parent.parent / "team_games"
        if not team_games_dir.is_dir():
            logger.warning(f"Team games directory not found for {standings_file.name}")
            return None

        team_files = list(team_games_dir.glob("*.csv"))
        has_all_teams_files = len(team_files) == num_total_teams

        distinct_games_df = _load_and_consolidate_games(team_games_dir)
        if distinct_games_df is None:
            logger.warning(f"No valid game data for {league_name}/{season_year}")
            return None

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

        logger.info(
            f"Processed season {league_name}/{season_year} with {num_total_games} games"
        )
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
        logger.error(f"Empty or malformed standings file: {standings_file.name}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error processing {standings_file.name}: {str(e)}")
        return None


def create_standings_summary() -> pd.DataFrame:
    """Creates a unified summary DataFrame from all league standings files.

    Returns:
        pd.DataFrame: Summary of processed seasons, or empty DataFrame if no data is found.

    Raises:
        Exception: For unexpected errors during directory iteration or file processing.
    """
    logger.info("Initiating standings summary creation")
    summary_data = []

    for league_dir in config.RAW_DATA_DIR.iterdir():
        if not league_dir.is_dir():
            logger.debug(f"Skipping non-directory: {league_dir}")
            continue

        for year_dir in league_dir.iterdir():
            if not year_dir.is_dir():
                logger.debug(f"Skipping non-directory: {year_dir}")
                continue

            standings_dir = year_dir / "final_standings"
            if not standings_dir.is_dir():
                logger.debug(f"No standings directory found: {standings_dir}")
                continue

            for standings_file in standings_dir.glob("*_standings.csv"):
                logger.debug(f"Processing standings file: {standings_file.name}")
                season_summary = process_season_data(standings_file)
                if season_summary:
                    summary_data.append(season_summary)

    if not summary_data:
        logger.warning("No season data processed; returning empty DataFrame")
        return pd.DataFrame()

    summary_df = pd.DataFrame(summary_data)
    logger.info(f"Created summary DataFrame with {len(summary_df)} seasons")
    return summary_df


def create_standings_complete() -> pd.DataFrame:
    """Concatenates and enriches all standings files into a single DataFrame.

    Returns:
        pd.DataFrame: Enriched standings data, or empty DataFrame if no data is found.

    Raises:
        Exception: For unexpected errors during file reading or processing.
    """
    logger.info("Initiating complete standings data creation")
    all_standings_dfs = []

    standings_files = Path(config.RAW_DATA_DIR).rglob("*_standings.csv")

    for standings_file in standings_files:
        try:
            df = pd.read_csv(standings_file, encoding="utf-8")
            if df.empty:
                logger.warning(f"Empty standings file skipped: {standings_file.name}")
                continue

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

            df["position"] = pd.to_numeric(df["position"], errors="coerce").astype(
                "Int64"
            )

            all_standings_dfs.append(df)
            logger.debug(f"Processed standings file: {standings_file.name}")

        except Exception as e:
            logger.error(
                f"Failed to process standings file {standings_file.name}: {str(e)}"
            )

    if not all_standings_dfs:
        logger.warning("No valid standings data found for concatenation")
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
    logger.info(f"Created complete standings DataFrame with {len(final_df)} rows")

    return final_df


def _impute_audience(df: pd.DataFrame) -> pd.DataFrame:
    """Imputes missing audience values for home games of the most frequent team.

    Args:
        df (pd.DataFrame): Input DataFrame with game data, including 'home_team_sanitized' and 'audience' columns.

    Returns:
        pd.DataFrame: Filtered DataFrame with imputed audience columns for home games.

    Raises:
        KeyError: If required columns are missing.
        ValueError: If audience data cannot be processed.
    """
    logger.debug("Imputing audience values for DataFrame")
    team_principal = df["home_team_sanitized"].value_counts().idxmax()
    home_df = df[df["home_team_sanitized"] == team_principal].copy()

    audience_clean_home = home_df["audience"].replace(0, np.nan)

    if audience_clean_home.notna().any():
        mean_val = audience_clean_home.mean()
        median_val = audience_clean_home.median()
        mode_val = (
            audience_clean_home.mode().iloc[0]
            if not audience_clean_home.mode().empty
            else np.nan
        )

        home_df["audience_filled_mean"] = audience_clean_home.fillna(mean_val)
        home_df["audience_filled_median"] = audience_clean_home.fillna(median_val)
        home_df["audience_filled_mode"] = audience_clean_home.fillna(mode_val)
    else:
        home_df["audience_filled_mean"] = np.nan
        home_df["audience_filled_median"] = np.nan
        home_df["audience_filled_mode"] = np.nan

    home_df["audience_filled_fb"] = audience_clean_home.ffill().bfill()

    for col in [
        "audience_filled_fb",
        "audience_filled_mean",
        "audience_filled_mode",
        "audience_filled_median",
    ]:
        if col in home_df:
            home_df[col] = home_df[col].round(0).astype("Int64")

    logger.debug(f"Completed audience imputation for team {team_principal}")
    return home_df


def _process_season_games(
    team_games_dir: Path, league_name: str, season_year: int
) -> pd.DataFrame | None:
    """Processes team game files for a single season, enriching with metadata and imputing audience data.

    Args:
        team_games_dir (Path): Directory containing team game CSV files.
        league_name (str): Name of the league.
        season_year (int): Year of the season.

    Returns:
        pd.DataFrame | None: Enriched DataFrame with game data, or None if no valid data is found.

    Raises:
        Exception: For errors during file reading, data processing, or datetime parsing.
    """
    logger.info(f"Processing season games for {league_name}/{season_year}")
    all_games_enriched = []

    for team_file in team_games_dir.glob("*.csv"):
        try:
            df = pd.read_csv(team_file, encoding="utf-8")
            if df.empty:
                logger.warning(f"Empty game file skipped: {team_file.name}")
                continue

            df["home_team_sanitized"] = df["home_team"].apply(utils.sanitize_filename)
            df["source_csv_file"] = team_file.name

            df = _impute_audience(df)
            all_games_enriched.append(df)
            logger.debug(f"Processed game file: {team_file.name}")

        except Exception as e:
            logger.error(f"Failed to process game file {team_file.name}: {str(e)}")

    if not all_games_enriched:
        logger.warning(f"No valid game data found for {league_name}/{season_year}")
        return None

    season_df = pd.concat(all_games_enriched, ignore_index=True)

    season_df["away_team_sanitized"] = season_df["away_team"].apply(
        utils.sanitize_filename
    )
    season_df["coach_sanitized"] = season_df["coach"].apply(utils.sanitize_filename)

    try:
        date_part = season_df["date"].str.split(" ").str[1]
        datetime_str = date_part + " " + season_df["time"]
        season_df["datetime"] = pd.to_datetime(
            datetime_str, format="%d/%m/%Y %H:%M", errors="coerce"
        )
        logger.debug(f"Successfully parsed datetime for {league_name}/{season_year}")
    except Exception as e:
        logger.warning(
            f"Failed to parse datetime for {league_name}/{season_year}: {str(e)}"
        )
        season_df["datetime"] = pd.NaT

    standings_csv_file = f"{league_name}_{season_year}_standings.csv"
    season_df["league_name"] = league_name
    season_df["season_year"] = season_year
    season_df["standings_csv_file"] = standings_csv_file
    season_df["standings_id"] = utils.generate_id(standings_csv_file)
    season_df["source_id"] = season_df["source_csv_file"].apply(utils.generate_id)
    season_df["id"] = season_df.apply(
        lambda row: utils.generate_id(
            f"{row['round']}_{row['home_team_sanitized']}_{row['standings_csv_file']}"
        ),
        axis=1,
    )

    logger.info(
        f"Completed processing {len(season_df)} games for {league_name}/{season_year}"
    )
    return season_df


def create_team_games_complete() -> pd.DataFrame:
    """Creates a unified DataFrame with all distinct team games across seasons.

    Returns:
        pd.DataFrame: Consolidated DataFrame with enriched game data, or empty if no data is found.

    Raises:
        Exception: For errors during directory iteration or file processing.
    """
    logger.info("Initiating team games completion process")
    all_seasons_dfs = []

    for league_dir in config.RAW_DATA_DIR.iterdir():
        if not league_dir.is_dir():
            logger.debug(f"Skipping non-directory: {league_dir}")
            continue
        for year_dir in league_dir.iterdir():
            if not year_dir.is_dir():
                logger.debug(f"Skipping non-directory: {year_dir}")
                continue

            team_games_dir = year_dir / "team_games"
            if team_games_dir.is_dir():
                logger.info(f"Processing games for {league_dir.name}/{year_dir.name}")
                season_df = _process_season_games(
                    team_games_dir, league_dir.name, int(year_dir.name)
                )
                if season_df is not None:
                    all_seasons_dfs.append(season_df)

    if not all_seasons_dfs:
        logger.warning("No valid team game data found")
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
        "audience_filled_mode",
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
    logger.info(f"Created complete team games DataFrame with {len(final_df)} rows")

    return final_df
