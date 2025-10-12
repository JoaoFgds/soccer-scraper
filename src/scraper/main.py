import time
import logging

from . import constants
from src.utils import paths
from src.utils.logger import setup_logging
from src.utils.helpers import sanitize_filename

from .exceptions import ScrapingError
from .parsers import fetch_league_standings, fetch_team_schedules


logger = logging.getLogger(__name__)


def run_scraper_for_season(
    league_name: str, league_slug: str, league_code: str, season_year: int
):
    """Orchestrates the scraping process for a single league and season.

    This function manages the entire workflow for scraping data for one
    competition season. It performs the following steps:
    1.  Constructs the necessary output directories for standings and matches.
    2.  Determines the correct 'season_id' for the URL, handling exceptions
        for leagues with calendar-year seasons.
    3.  Fetches the main league standings table.
    4.  Saves the standings to a CSV file.
    5.  Iterates through each team in the standings to fetch their individual
        match schedules for that season.
    6.  Saves each team's schedule to a separate CSV file.
    7.  Logs errors gracefully to allow the wider scraping process to continue.

    Args:
        league_name (str): The human-readable name of the league (e.g., "Premier League").
        league_slug (str): The URL-friendly slug for the league (e.g., "premier-league").
        league_code (str): The unique competition code used by the website (e.g., "GB1").
        season_year (int): The starting year of the season to be scraped (e.g., 2023
            for the 2023/24 season).
    """
    safe_league_slug = sanitize_filename(league_slug)

    season_output_dir = paths.SCRAPER_OUTPUT_DIR / safe_league_slug / str(season_year)
    standings_output_dir = season_output_dir / "final_standings"
    matches_output_dir = season_output_dir / "team_games"

    standings_output_dir.mkdir(parents=True, exist_ok=True)
    matches_output_dir.mkdir(parents=True, exist_ok=True)

    season_id = season_year
    if league_code in ["BRA1", "BRA2", "JAP1", "JAP2", "CLPD"]:
        season_id = season_year - 1

    standings_url = f"{constants.BASE_URL}/{league_slug}/tabelle/wettbewerb/{league_code}/saison_id/{season_id}"
    logger.info("Extracting data for %s - Season %s", league_name, season_year)
    logger.info("Standings URL: %s", standings_url)

    try:
        standings_df = fetch_league_standings(standings_url)
        if standings_df.empty:
            logger.warning(
                "No standings data found for %s %s. Skipping season.",
                league_name,
                season_year,
            )
            return

        logger.info("Found %d teams in the league table.", len(standings_df))

        standings_path = (
            standings_output_dir / f"{safe_league_slug}_{season_year}_standings.csv"
        )
        standings_df.to_csv(standings_path, index=False)
        logger.info("League standings saved to %s", standings_path)

        for _, row in standings_df.iterrows():
            team_name, team_url = row["team"], row["team_url"]
            if not team_url:
                logger.warning("No URL for team '%s'. Skipping.", team_name)
                continue

            schedule_url = f"{team_url.replace('/startseite/', '/spielplan/')}/plus/1#{league_code}"
            logger.info("Fetching schedule for '%s'...", team_name)

            schedule_df = fetch_team_schedules(schedule_url, league_name, league_code)

            if not schedule_df.empty:
                safe_team_name = sanitize_filename(team_name)
                output_path = (
                    matches_output_dir
                    / f"{safe_league_slug}_{season_year}_{safe_team_name}.csv"
                )
                schedule_df.to_csv(output_path, index=False)
                logger.info("Schedule for '%s' saved to %s", team_name, output_path)
            else:
                logger.warning("No schedule data extracted for '%s'.", team_name)

    except ScrapingError as e:
        logger.error(
            "A critical scraping error occurred for %s %s: %s",
            league_name,
            season_year,
            e,
        )
    except Exception as e:
        logger.critical(
            "An unexpected error occurred for %s %s: %s",
            league_name,
            season_year,
            e,
            exc_info=True,
        )


def scraper_pipeline():
    """Runs the full scraping pipeline for all configured leagues.

    This function serves as the main entry point for the scraper. It initializes
    logging and then iterates through all leagues defined in the
    `constants.LEAGUES` configuration dictionary. For each league, it scrapes
    data for a range of seasons, from a defined start year up to a final
    year. The pipeline introduces delays between requests for different seasons
    and leagues to avoid overwhelming the target server.
    """
    setup_logging()

    FINAL_YEAR = 2024
    MIN_START_YEAR = 1990

    for league_key, league_info in constants.LEAGUES.items():
        if league_info.get("processed"):
            logger.info(
                "League '%s' is marked as processed. Skipping.", league_info["name"]
            )
            continue

        league_name = league_info["name"]
        logger.info("--- Starting processing for league: %s ---", league_name)

        start_year = max(league_info["start_year"], MIN_START_YEAR)

        for year in range(start_year, FINAL_YEAR + 1):
            run_scraper_for_season(
                league_name=league_name,
                league_slug=league_info["slug"],
                league_code=league_info["code"],
                season_year=year,
            )
            logger.info("Waiting 30 seconds before next season...")
            time.sleep(30)

        constants.LEAGUES[league_key]["processed"] = True
        logger.info("--- Finished processing for %s. ---", league_name)
        logger.info("Waiting 2 minutes before next league...")
        time.sleep(120)

    logger.info("--- All leagues have been processed. ---")
