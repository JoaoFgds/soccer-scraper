# src/soccer_scraper/main.py

import os
import time
import logging
from . import config
from .utils import sanitize_filename
from .exceptions import ScrapingError
from .parsers import fetch_league_standings, fetch_team_schedules

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


def run_scraper_for_season(
    league_name: str, league_slug: str, league_code: str, season_year: int
):
    """
    Orchestrates the scraping process for a single league and season.

    This function serves as the main workflow controller. It takes league and
    season identifiers to perform a series of scraping tasks:
    1.  Fetches the main league standings table to identify all participating teams
        and their respective URLs.
    2.  Saves the complete league standings to a dedicated CSV file.
    3.  Iterates through each team, constructing the URL for its detailed
        match schedule page.
    4.  Scrapes the schedule for each team and saves the data to a separate
        CSV file.
    5.  All outputs are organized into a structured directory based on the league
        and season. Errors during the process are logged without halting the
        entire execution.

    Args:
        league_name (str): The human-readable name of the league, used for
            logging and informational purposes (e.g., "Premier League").
        league_slug (str): The URL-friendly slug for the league, used in
            constructing URLs and file paths (e.g., "premier-league").
        league_code (str): The unique competition code used by Transfermarkt
            (e.g., "GB1").
        season_year (int): The starting year of the season to be processed
            (e.g., 2023 for the 2023/24 season).
    """

    safe_league_slug = sanitize_filename(league_slug)

    season_output_dir = os.path.join(
        config.OUTPUT_DIR, safe_league_slug, str(season_year)
    )
    matches_output_dir = os.path.join(season_output_dir, "team_games")
    standings_output_dir = os.path.join(season_output_dir, "final_standings")

    os.makedirs(matches_output_dir, exist_ok=True)
    os.makedirs(standings_output_dir, exist_ok=True)

    if league_code in ["BRA1", "BRA2", "JAP1", "JAP2", "CLPD"]:
        season_year_url = season_year - 1
    else:
        season_year_url = season_year

    standings_url = f"https://www.transfermarkt.com.br/{league_slug}/tabelle/wettbewerb/{league_code}/saison_id/{season_year_url}"

    logging.info(f"Starting data extraction for {league_name} - season {season_year}.")

    try:
        standings_df = fetch_league_standings(standings_url)
        logging.info(f"Found {len(standings_df)} teams in the league table.")
        logging.info(f"Link: '{standings_url}'.")

        standings_path = os.path.join(
            standings_output_dir,
            f"{safe_league_slug}_{season_year}_standings.csv",
        )
        standings_df.to_csv(standings_path, index=False)
        logging.info(f"League standings saved to {standings_path}")

        for index, row in standings_df.iterrows():
            team_name = row["team"]
            team_url = row["team_url"]

            if not team_url:
                logging.warning(f"No URL found for team '{team_name}'. Skipping.")
                continue

            schedule_url = f"{team_url.replace('/startseite/', '/spielplan/')}/plus/1#{league_code}"

            logging.info(f"Fetching games for '{team_name}'.")
            logging.info(f"Link: '{schedule_url}'.")

            schedule_df = fetch_team_schedules(schedule_url, league_name, league_code)

            if not schedule_df.empty:
                safe_team_name = sanitize_filename(team_name)
                output_path = os.path.join(
                    matches_output_dir,
                    f"{safe_league_slug}_{season_year}_{safe_team_name}.csv",
                )
                schedule_df.to_csv(output_path, index=False)
                logging.info(f"Games for '{team_name}' saved to {output_path}")
            else:
                logging.warning(f"Could not extract games for '{team_name}'.")

    except ScrapingError as e:
        logging.critical(f"A critical error occurred: {e}")
    except Exception as e:
        logging.critical(f"An unexpected error occurred: {e}")


def scraper_pipeline():
    """Main entry point to run the full scraping process based on config."""

    leagues_to_process = config.LEAGUES

    FINAL_YEAR = 2024
    MIN_START_YEAR = 1990

    for league_key, league_info in leagues_to_process.items():

        if league_info["processed"] == "false":

            league_name = league_info["name"]
            league_slug = league_info["slug"]
            league_code = league_info["code"]
            start_year = int(league_info["start_year"])

            if start_year < MIN_START_YEAR:
                start_year = MIN_START_YEAR

            logging.info(f"--- Starting processing for league: {league_name} ---")

            for year in range(start_year, FINAL_YEAR + 1):
                logging.info(f"Processing season: {year}")

                try:
                    run_scraper_for_season(
                        league_name=league_name,
                        league_slug=league_slug,
                        league_code=league_code,
                        season_year=year,
                    )
                except Exception as e:
                    logging.error(
                        f"An error occurred while processing {league_name} - {year}: {e}"
                    )
                    continue

                logging.info("Waiting 30 seconds before the next season...")
                time.sleep(30)
                print("\n")

            leagues_to_process[league_key]["processed"] = "true"
            logging.info(
                f"--- Finished processing for league: {league_name}. Status updated. ---"
            )

            logging.info("Waiting 2 minutes before the next league...")
            time.sleep(120)

        else:
            logging.info(f"League '{league_info['name']}' already processed. Skipping.")

    logging.info("--- All leagues have been processed. ---")
