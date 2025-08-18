import os
import re
import time
import random
import logging
import requests
import unicodedata
import pandas as pd

from typing import Dict, List
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
    """
    Custom exception for predictable errors during the scraping process.

    This exception is intended to be raised when a scraping operation fails
    for a specific, anticipated reason, such as an HTTP request failure after
    all retries or a required HTML element not being found on the page.

    Using a specific exception type allows for more granular error handling
    in the application's main control flow, distinguishing scraping-related
    failures from other unexpected programming errors.

    Example:
        try:
            standings_df = fetch_league_standings(url)
        except ScrapingError as e:
            logging.error(f"Could not scrape standings: {e}")
            # The application can then handle the failure gracefully.
    """

    pass


def _sanitize_filename(text: str) -> str:
    """
    Cleans and sanitizes a string to be used as a single-word, safe filename.

    This function performs the following steps:
    1.  Normalizes Unicode characters to remove accents (e.g., 'ç' -> 'c').
    2.  Converts the string to lowercase.
    3.  Removes any character that is not a letter, number, whitespace, or hyphen.
    4.  Removes all remaining spaces and hyphens, concatenating the string into a single word.

    Args:
        text: The original string to be cleaned.

    Returns:
        A clean, single-word, and safe string for use in filenames.
    """
    text = str(text)
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("utf-8")
    text = text.lower()
    text = re.sub(r"[^\w\s-]", "", text)
    text = re.sub(r"[\s-]+", "", text)

    return text


def fetch_soup(url: str) -> BeautifulSoup:
    """
    Performs a robust HTTP GET request and returns a parsed BeautifulSoup object.

    This is the central function for all web requests in the scraper. It is
    designed to be resilient and respectful to the target server by incorporating
    several key features:

    1.  **Politeness Delay**: A randomized delay (configured by module-level
        constants) is enforced before each request to mimic human-like
        browsing patterns and avoid overwhelming the server.

    2.  **Automatic Retries with Exponential Backoff**: The function will
        automatically retry requests if it encounters transient issues. The
        waiting time between retries increases exponentially to give the
        server time to recover.

    Retries are attempted for the following conditions:
    -   Rate limiting errors (HTTP 429).
    -   Server availability errors (HTTP 503).
    -   General network connection errors (e.g., DNS failure, connection refused).

    The function will fail immediately for non-transient HTTP errors such as
    'Not Found' (404) or 'Forbidden' (403), as retrying these is futile.

    Args:
        url (str): The URL of the website to fetch.

    Returns:
        BeautifulSoup: A BeautifulSoup object containing the parsed HTML of the
            successfully fetched page.

    Raises:
        ScrapingError: If a non-retriable HTTP error occurs, or if all retry
            attempts for a transient error are exhausted.
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

    This function serves as a foundational step in the scraping process. It
    parses the main classification table for a given league and season to
    gather essential information about each participating team.

    The extracted data includes both the statistical standings (points, wins,
    goals, etc.) and, critically, the unique URL to each team's homepage on
    Transfermarkt. This URL is required by other functions to locate more
    detailed data, such as individual team schedules.

    Args:
        standings_url (str): The URL of the league standings page.

    Returns:
        pd.DataFrame: A pandas DataFrame containing the standings data. The
            DataFrame includes the following columns: 'position', 'team',
            'played', 'won', 'drawn', 'lost', 'goal_ratio', 'goal_difference',
            'points', and 'team_url'.

    Raises:
        ScrapingError: If the main standings table (class='items') cannot be
            found in the page's HTML.
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


def fetch_team_games(
    calendar_url: str, league_name: str, league_code: str
) -> pd.DataFrame:
    """
    Extracts a team's match schedule for a specific league from its calendar page.

    This function employs a two-step strategy to locate the correct schedule
    table on the provided page. It first attempts to find the table's container
    directly by searching for a div element whose ID matches the `league_code`.
    If this primary method fails, it uses a fallback search: it scans all `<h2>`
    headings for the `league_name` and then selects the first table that
    follows the correct heading.

    Args:
        calendar_url (str): The full URL of the team's schedule ('spielplan') page.
        league_name (str): The human-readable name of the league (e.g., "Premier League"),
            used in the fallback search to find the correct section.
        league_code (str): The unique competition code from Transfermarkt (e.g., "GB1"),
            used as the primary method to find the schedule table.

    Returns:
        pd.DataFrame: A pandas DataFrame containing the details of each match,
            including round, date, teams, result, and more.

    Raises:
        ValueError: If both the primary and fallback search methods fail to
            locate the schedule table.
    """
    soup = fetch_soup(calendar_url)
    match_data_list = []

    competition_section = soup.find("div", id=league_code)
    schedule_table = None
    if not competition_section:
        h2_tags = soup.find_all("h2")
        for h2 in h2_tags:
            if league_name in h2.text:
                schedule_table = h2.find_next("table")
                break
        else:
            raise ValueError(f"{league_name} section not found.")
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


def main(league_name: str, league_slug: str, league_code: str, season_year: int):
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

    safe_league_slug = _sanitize_filename(league_slug)

    season_output_dir = os.path.join(OUTPUT_DIR, safe_league_slug, str(season_year))
    matches_output_dir = os.path.join(season_output_dir, "team_games")
    standings_output_dir = os.path.join(season_output_dir, "final_standings")

    os.makedirs(matches_output_dir, exist_ok=True)
    os.makedirs(standings_output_dir, exist_ok=True)

    standings_url = f"https://www.transfermarkt.com.br/{league_slug}/tabelle/wettbewerb/{league_code}/saison_id/{season_year}"

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

            schedule_df = fetch_team_games(schedule_url, league_name, league_code)

            if not schedule_df.empty:
                safe_team_name = _sanitize_filename(team_name)
                output_path = os.path.join(
                    matches_output_dir,
                    f"{league_slug}_{season_year}_{safe_team_name}.csv",
                )
                schedule_df.to_csv(output_path, index=False)
                logging.info(f"Games for '{team_name}' saved to {output_path}")
            else:
                logging.warning(f"Could not extract games for '{team_name}'.")

    except ScrapingError as e:
        logging.critical(f"A critical error occurred: {e}")
    except Exception as e:
        logging.critical(f"An unexpected error occurred: {e}")


if __name__ == "__main__":

    leagues_dict = {
        "premierleague": {
            "name": "Premier League",
            "slug": "premier-league",
            "code": "GB1",
            "start_year": "1992",
            "processed": "true",
        },
        "championship": {
            "name": "Championship",
            "slug": "championship",
            "code": "GB2",
            "start_year": "2004",
            "processed": "true",
        },
        "laliga": {
            "name": "LaLiga",
            "slug": "laliga",
            "code": "ES1",
            "start_year": "2000",
            "processed": "true",
        },
        "laliga2": {
            "name": "LaLiga2",
            "slug": "laliga2",
            "code": "ES2",
            "start_year": "2007",
            "processed": "false",
        },
        "bundesliga": {
            "name": "Bundesliga",
            "slug": "bundesliga",
            "code": "L1",
            "start_year": "1963",
            "processed": "false",
        },
        "2bundesliga": {
            "name": "2. Bundesliga",
            "slug": "2-bundesliga",
            "code": "L2",
            "start_year": "1981",
            "processed": "false",
        },
        "seriea": {
            "name": "Serie A",
            "slug": "serie-a",
            "code": "IT1",
            "start_year": "1946",
            "processed": "false",
        },
        "serieb": {
            "name": "Serie B",
            "slug": "serie-b",
            "code": "IT2",
            "start_year": "2002",
            "processed": "false",
        },
        "ligue1": {
            "name": "Ligue 1",
            "slug": "ligue-1",
            "code": "FR1",
            "start_year": "1948",
            "processed": "false",
        },
        "ligue2": {
            "name": "Ligue 2",
            "slug": "ligue-2",
            "code": "FR2",
            "start_year": "1994",
            "processed": "false",
        },
        "brasileiraoseriea": {
            "name": "Campeonato Brasileiro Série A",
            "slug": "campeonato-brasileiro-serie-a",
            "code": "BRA1",
            "start_year": "2006",
            "processed": "false",
        },
        "brasileiraoserieb": {
            "name": "Campeonato Brasileiro Série B",
            "slug": "campeonato-brasileiro-serie-b",
            "code": "BRA2",
            "start_year": "2009",
            "processed": "false",
        },
    }

    FINAL_YEAR = 2024
    MIN_START_YEAR = 1990

    for league_key, league_info in leagues_dict.items():

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
                    main(
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

            leagues_dict[league_key]["processed"] = "true"
            logging.info(
                f"--- Finished processing for league: {league_name}. Status updated. ---"
            )

            logging.info("Waiting 2 minutes before the next league...")
            time.sleep(120)

        else:
            logging.info(f"League '{league_info['name']}' already processed. Skipping.")

    logging.info("--- All leagues have been processed. ---")
