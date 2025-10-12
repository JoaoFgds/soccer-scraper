import logging
import pandas as pd
from bs4 import Tag
from urllib.parse import urljoin

from . import constants
from .network import fetch_soup
from .exceptions import ScrapingError


logger = logging.getLogger(__name__)


def fetch_league_standings(standings_url: str) -> pd.DataFrame:
    """Extracts the league standings table from a given competition URL.

    This function parses the main classification table for a specific league
    and season. Its primary purpose is to gather team statistics and the unique
    URL for each team's homepage on Transfermarkt, which is essential for
    subsequent scraping tasks.

    Args:
        standings_url (str): The full URL of the league standings page.

    Returns:
        pd.DataFrame: A DataFrame containing the standings data. Key columns
            include 'position', 'team', various match stats, and 'team_url'.
            Returns an empty DataFrame if the table is found but contains no data rows.

    Raises:
        ScrapingError: If the main standings table (identified by the class
            'items') cannot be found in the page's HTML, which likely
            indicates a change in the website's structure.
    """
    soup = fetch_soup(standings_url)
    table = soup.find("table", class_="items")
    if not isinstance(table, Tag):
        raise ScrapingError(f"Standings table not found at {standings_url}")

    table_data = []
    rows = table.find_all("tr")
    if len(rows) < 2:
        logger.warning(f"No data rows found in standings table at {standings_url}")
        return pd.DataFrame()

    for row in rows[1:]:
        cols = row.find_all("td")
        if len(cols) < 10:
            logger.warning(f"Skipping malformed row in table at {standings_url}")
            continue

        team_anchor = cols[1].find("a", href=True)
        team_name = (
            team_anchor.get("title", "").strip()
            if team_anchor
            else cols[1].text.strip()
        )
        team_url = (
            urljoin(constants.BASE_URL, team_anchor["href"]) if team_anchor else None
        )

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


def fetch_team_schedules(
    calendar_url: str, league_name: str, league_code: str
) -> pd.DataFrame:
    """Extracts a team's match schedule for a specific league.

    This function scrapes a team's calendar page to find the match schedule for a
    particular competition. It employs a two-step strategy to locate the correct
    data table. First, it attempts to find the table using the competition's
    unique `league_code`. If that fails, it falls back to searching for the
    human-readable `league_name` within page headings to identify the section.

    Args:
        calendar_url (str): The URL of the team's main schedule ('spielplan') page.
        league_name (str): The display name of the league (e.g., "Premier League"),
            which is used as a fallback search criterion.
        league_code (str): The unique competition code (e.g., "GB1"), used as the
            primary method for locating the schedule table.

    Returns:
        pd.DataFrame: A DataFrame containing match details such as date, teams,
            and result. Returns an empty DataFrame if the schedule table is
            found but contains no match data.

    Raises:
        ScrapingError: If the schedule table cannot be located using either the
            primary `league_code` search or the fallback `league_name` search.
    """
    soup = fetch_soup(calendar_url)
    schedule_table = None

    # Primary strategy: find the data block by the competition's unique ID.
    competition_section = soup.find("div", id=league_code)
    if competition_section and isinstance(competition_section, Tag):
        schedule_table = competition_section.find("table")
    else:
        # Fallback strategy: find the section heading that contains the league name.
        h2_tags = soup.find_all("h2")
        for h2 in h2_tags:
            if h2 and league_name in h2.text:
                schedule_table = h2.find_next("table")
                break

    if not isinstance(schedule_table, Tag):
        raise ScrapingError(
            f"Could not find schedule table for {league_name} at {calendar_url}"
        )

    match_data = []
    rows = schedule_table.find_all("tr")[1:]
    for row in rows:
        cols = row.find_all("td")
        if len(cols) < 11:
            continue

        home_anchor = cols[4].find("a")
        away_anchor = cols[6].find("a")
        result_anchor = cols[10].find("a")

        match_data.append(
            {
                "round": cols[0].text.strip(),
                "date": cols[1].text.strip(),
                "time": cols[2].text.strip(),
                "home_team": (
                    home_anchor.text.strip() if home_anchor else cols[4].text.strip()
                ),
                "away_team": (
                    away_anchor.text.strip() if away_anchor else cols[6].text.strip()
                ),
                "formation": cols[7].text.strip(),
                "coach": cols[8].text.strip(),
                "audience": cols[9].text.strip().replace(".", ""),
                "result": result_anchor.text.strip() if result_anchor else "",
                "match_link": (
                    urljoin(constants.BASE_URL, result_anchor["href"])
                    if result_anchor
                    else ""
                ),
            }
        )

    return pd.DataFrame(match_data)
