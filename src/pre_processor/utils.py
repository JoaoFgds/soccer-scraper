# File: src/pre_processor/utils.py
"""
Utility functions for the pre-processor module.
"""
import hashlib
import logging
import re
from pathlib import Path
from src.scraper.utils import sanitize_filename

logger = logging.getLogger(__name__)


def generate_id(data: str) -> str:
    """
    Generates a SHA-256 hash for a given string.

    Args:
        data: The string to be hashed.

    Returns:
        The SHA-256 hash as a hexadecimal string.
    """
    return hashlib.sha256(data.encode("utf-8")).hexdigest()


def extract_metadata_from_filename(file_path: Path) -> dict:
    """
    Extracts league, season, and team information from a standardized filename.

    Args:
        file_path: The Path object of the file.

    Returns:
        A dictionary containing extracted metadata.
    """
    filename = file_path.stem

    # Pattern for standings files
    standings_match = re.match(r"([a-z0-9]+)_(\d{4})_standings", filename)
    if standings_match:
        return {
            "type": "standings",
            "league_name": standings_match.group(1),
            "season_year": int(standings_match.group(2)),
        }

    # Pattern for team games files
    games_match = re.match(r"([a-z0-9]+)_(\d{4})_([a-z0-9]+)", filename)
    if games_match:
        return {
            "type": "games",
            "league_name": games_match.group(1),
            "season_year": int(games_match.group(2)),
            "team_sanitized_name": games_match.group(3),
        }

    raise ValueError(f"Filename does not match expected patterns: {file_path}")


def validate_url_year(url: str, season_year: int) -> bool:
    """
    Validates if the year in a URL is less than or equal to the season year.

    Args:
        url: The team URL string.
        season_year: The season year from the file metadata.

    Returns:
        True if the URL's year is <= season year, otherwise False.
    """
    if not isinstance(url, str):
        return False

    url_year_match = re.search(r"saison_id/(\d{4})", url)
    if not url_year_match:
        return False

    url_year = int(url_year_match.group(1))
    return url_year <= season_year
