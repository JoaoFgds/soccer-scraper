import logging
import pandas as pd

from bs4 import Tag
from urllib.parse import urljoin

from . import config
from .network import fetch_soup
from .exceptions import ScrapingError


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
        team_url = (
            urljoin(config.BASE_URL, team_anchor["href"]) if team_anchor else None
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
                urljoin(config.BASE_URL, result_anchor.get("href", ""))
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
