import logging
import os
import random
import re
import time
from typing import Dict, List

import pandas as pd
import requests
from bs4 import BeautifulSoup, Tag
from urllib.parse import urljoin

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

MAX_RETRIES = 5
BACKOFF_FACTOR = 2
REQUEST_DELAY_RANGE_SECONDS = (3, 12)

OUTPUT_DIR = "data/raw"
BASE_URL = "https://www.transfermarkt.com.br"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9,pt;q=0.8",
}


class ScrapingError(Exception):
    """Custom exception for errors encountered during the scraping process."""

    pass


def _sanitize_filename(text: str) -> str:
    """
    Cleans a string to be used as a safe filename.

    Replaces spaces with underscores, converts to lowercase, and removes all
    characters that are not alphanumeric or underscores.

    Args:
        text: The original string to be cleaned.

    Returns:
        A clean and safe string for use in filenames.
    """
    text = text.lower().replace(" ", "").replace("-2004", "")
    return re.sub(r"(?u)[^-\w.]", "", text)


def fetch_soup(url: str) -> BeautifulSoup:
    """
    Performs an HTTP GET request and returns a BeautifulSoup object.

    Implements a randomized delay between requests and an exponential backoff
    retry logic to handle server errors or rate limiting, enhancing the
    scraper's robustness.

    Args:
        url: The URL of the website to fetch.

    Returns:
        A BeautifulSoup object containing the parsed HTML of the page.

    Raises:
        ScrapingError: If a network error or an unrecoverable HTTP status
                       error occurs after exhausting all retries.
    """
    delay = random.uniform(*REQUEST_DELAY_RANGE_SECONDS)
    time.sleep(delay)
    last_exception = None

    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(url, headers=HEADERS, timeout=20)
            response.raise_for_status()
            return BeautifulSoup(response.text, "html.parser")
        except requests.exceptions.HTTPError as e:
            last_exception = e
            status_code = e.response.status_code
            if status_code in [429, 503]:
                wait_time = BACKOFF_FACTOR**attempt
                logging.warning(
                    f"Received status {status_code} for {url}. "
                    f"Attempt {attempt + 1}/{MAX_RETRIES}. "
                    f"Waiting {wait_time} seconds."
                )
                time.sleep(wait_time)
            else:
                logging.error(f"Unrecoverable HTTP {status_code} for {url}: {e}")
                raise ScrapingError(f"Unrecoverable HTTP error for {url}") from e
        except requests.exceptions.RequestException as e:
            last_exception = e
            wait_time = BACKOFF_FACTOR**attempt
            logging.warning(
                f"Network error for {url}: {e}. Attempt {attempt + 1}/{MAX_RETRIES}. "
                f"Waiting {wait_time} seconds."
            )
            time.sleep(wait_time)

    logging.error(f"Failed to fetch URL {url} after {MAX_RETRIES} attempts.")
    raise ScrapingError(
        f"Failed to fetch {url} after {MAX_RETRIES} attempts"
    ) from last_exception


def fetch_league_standings(standings_url: str) -> pd.DataFrame:
    """
    Extracts the league standings table from a competition page.

    Also captures the URL for each team's page, which is essential for
    discovering the schedule pages.

    Args:
        standings_url: The URL of the league standings page.

    Returns:
        A pandas DataFrame containing the standings data and team URLs.

    Raises:
        ScrapingError: If the standings table is not found in the HTML.
    """
    soup = fetch_soup(standings_url)
    table_data = []

    table = soup.find("table", class_="items")
    if not table or not isinstance(table, Tag):
        raise ScrapingError(f"Standings table not found at {standings_url}")

    rows = table.find_all("tr")
    if len(rows) < 2:
        return pd.DataFrame()

    for row in rows[1:]:
        cols = row.find_all("td")
        if len(cols) < 10:
            logging.warning(
                f"Unexpected table row structure skipped at {standings_url}"
            )
            continue

        team_anchor = cols[1].find("a", href=True)
        team_name = (
            team_anchor.get("title", cols[1].text.strip()).strip()
            if team_anchor
            else cols[1].text.strip()
        )
        team_url = urljoin(BASE_URL, team_anchor["href"]) if team_anchor else None

        table_data.append(
            {
                "position": cols[0].text.strip(),
                "team": team_name,
                "played": cols[3].text.strip(),
                "won": cols[4].text.strip(),
                "drawn": cols[5].text.strip(),
                "lost": cols[6].text.strip(),
                "goal_ratio": cols[7].text.strip(),
                "goal_difference": cols[8].text.strip(),
                "points": cols[9].text.strip(),
                "team_url": team_url,
            }
        )
    return pd.DataFrame(table_data)


def fetch_team_games(calendar_url: str) -> pd.DataFrame:
    """
    Extracts a team's match schedule from its specific calendar page.

    This function follows a defined logic to find the schedule table: it first
    attempts to locate a div with the id 'GB1'. If that fails, it falls back to
    finding an h2 element containing 'Premier League' and then selects the
    next adjacent table element.

    Args:
        calendar_url: The URL of the team's schedule page.

    Returns:
        A pandas DataFrame containing the details of each match found.

    Raises:
        ValueError: If the competition section or the schedule table cannot be found.
    """
    soup = fetch_soup(calendar_url)
    match_data_list = []

    competition_section = soup.find("div", id="GB2")
    schedule_table = None
    if not competition_section:
        h2_tags = soup.find_all("h2")
        for h2 in h2_tags:
            if "Championship" in h2.text:
                schedule_table = h2.find_next("table")
                break
        else:
            raise ValueError("Premier League section not found.")
    else:
        schedule_table = competition_section.find("table")

    if not schedule_table:
        raise ValueError("Schedule table not found.")

    rows = schedule_table.find_all("tr")[1:]
    for row in rows:
        cols = row.find_all("td")
        if len(cols) >= 11:
            round_anchor = cols[0].find("a")
            round_text = (
                round_anchor.text.strip() if round_anchor else cols[0].text.strip()
            )

            date_text = cols[1].text.strip()
            time_text = cols[2].text.strip()

            home_anchor = cols[4].find("a")
            home_team = (
                home_anchor["title"].strip()
                if home_anchor and "title" in home_anchor.attrs
                else (home_anchor.text.strip() if home_anchor else cols[4].text.strip())
            )

            away_anchor = cols[6].find("a")
            away_team = (
                away_anchor.text.strip() if away_anchor else cols[6].text.strip()
            )

            formation = cols[7].text.strip()

            coach_anchor = cols[8].find("a")
            coach_name = (
                coach_anchor.text.strip() if coach_anchor else cols[8].text.strip()
            )

            audience_str = cols[9].text.strip()

            result_anchor = cols[10].find("a")
            result_text = (
                result_anchor.text.strip() if result_anchor else cols[10].text.strip()
            )

            match_link = (
                urljoin(BASE_URL, result_anchor.get("href", ""))
                if result_anchor
                else ""
            )

            try:
                audience = int(audience_str.replace(".", "")) if audience_str else 0
            except ValueError:
                audience = 0

            match_data_list.append(
                {
                    "round": round_text,
                    "date": date_text,
                    "time": time_text,
                    "home_team": home_team,
                    "away_team": away_team,
                    "formation": formation,
                    "coach": coach_name,
                    "audience": audience,
                    "result": result_text,
                    "match_link": match_link,
                }
            )

    return pd.DataFrame(match_data_list)


def main(championship_name: str, season_id: int):
    """
    Main entry point for the scraping script.

    Orchestrates the process of fetching the league standings to discover all
    teams, then individually scrapes the full schedule for each team.

    Args:
        championship_name: The name of the championship for directory structuring.
        season_id: The starting year of the season to be processed (e.g., 2023).
    """
    season_output_dir = os.path.join(OUTPUT_DIR, championship_name, str(season_id))
    matches_output_dir = os.path.join(season_output_dir, "team_games")
    standings_output_dir = os.path.join(season_output_dir, "final_standings")

    os.makedirs(matches_output_dir, exist_ok=True)
    os.makedirs(standings_output_dir, exist_ok=True)

    standings_url = f"https://www.transfermarkt.com.br/championship/tabelle/wettbewerb/GB2/saison_id/{season_id}"

    logging.info(
        f"Starting data extraction for {championship_name}, season {season_id}."
    )

    try:
        standings_df = fetch_league_standings(standings_url)
        logging.info(f"Found {len(standings_df)} teams in the league table.")

        standings_path = os.path.join(
            standings_output_dir,
            f"{championship_name}_{season_id}_standings.csv",
        )
        standings_df.to_csv(standings_path, index=False)
        logging.info(f"League standings saved to {standings_path}")

        for index, row in standings_df.iterrows():
            team_name = row["team"]
            team_url = row["team_url"]

            if not team_url:
                logging.warning(f"No URL found for team '{team_name}'. Skipping.")
                continue

            schedule_url = f"{team_url.replace('/startseite/', '/spielplan/')}/saison_id/{season_id}/plus/1#GB2"

            logging.info(f"Fetching schedule for '{team_name}'.")
            logging.info(f"Schedule link: '{schedule_url}'.")

            schedule_df = fetch_team_games(schedule_url)

            if not schedule_df.empty:
                safe_team_name = _sanitize_filename(team_name)
                output_path = os.path.join(
                    matches_output_dir,
                    f"{championship_name}_{season_id}_{safe_team_name}.csv",
                )
                schedule_df.to_csv(output_path, index=False)
                logging.info(f"Schedule for '{team_name}' saved to {output_path}")
            else:
                logging.warning(f"Could not extract schedule for '{team_name}'.")

    except ScrapingError as e:
        logging.critical(f"A critical error occurred: {e}")
    except Exception as e:
        logging.critical(f"An unexpected error occurred: {e}")


if __name__ == "__main__":
    CHAMPIONSHIP = "championship"
    START_SEASON = 2004
    END_SEASON = 2023
    for season_id in range(START_SEASON, END_SEASON + 1):
        main(championship_name=CHAMPIONSHIP, season_id=season_id)
        time.sleep(60)
        print("\n")
