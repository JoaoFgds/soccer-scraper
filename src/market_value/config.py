"""Configuration for the Transfermarkt club market-value scraper."""

from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "data" / "bronze" / "market_values"
SILVER_PATH = PROJECT_ROOT / "data" / "silver" / "market_values_silver.csv"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 "
        "Safari/537.36"
    ),
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;q=0.9,"
        "image/avif,image/webp,*/*;q=0.8"
    ),
    "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
}

BASE_URL_TEMPLATE = (
    "https://www.transfermarkt.com.br/{slug}/startseite/wettbewerb/"
    "{code}/plus/?saison_id={saison_id}"
)

# This is the exact historical league/season scope used by the source project.
LEAGUES = {
    "2bundesliga": {
        "code": "L2",
        "slug": "2-bundesliga",
        "seasons": list(range(1990, 2025)),
        "offset": 0,
    },
    "bundesliga": {
        "code": "L1",
        "slug": "bundesliga",
        "seasons": list(range(1990, 2025)),
        "offset": 0,
    },
    "campeonatobrasileiroseriea": {
        "code": "BRA1",
        "slug": "campeonato-brasileiro-serie-a",
        "seasons": list(range(2007, 2025)),
        "offset": -1,
    },
    "campeonatobrasileiroserieb": {
        "code": "BRA2",
        "slug": "campeonato-brasileiro-serie-b",
        "seasons": list(range(2010, 2025)),
        "offset": -1,
    },
    "challengerproleague": {
        "code": "BE2",
        "slug": "challenger-pro-league",
        "seasons": list(range(2006, 2024)),
        "offset": 0,
    },
    "championship": {
        "code": "GB2",
        "slug": "championship",
        "seasons": list(range(2004, 2025)),
        "offset": 0,
    },
    "j1league": {
        "code": "JAP1",
        "slug": "j1-league",
        "seasons": list(range(2005, 2025)),
        "offset": -1,
    },
    "j2league": {
        "code": "JAP2",
        "slug": "j2-league",
        "seasons": list(range(2010, 2025)),
        "offset": -1,
    },
    "jupilerproleague": {
        "code": "BE1",
        "slug": "jupiler-pro-league",
        "seasons": list(range(2008, 2025)),
        "offset": 0,
    },
    "laliga": {
        "code": "ES1",
        "slug": "laliga",
        "seasons": list(range(2001, 2025)),
        "offset": 0,
    },
    "laliga2": {
        "code": "ES2",
        "slug": "laliga2",
        "seasons": list(range(2007, 2025)),
        "offset": 0,
    },
    "ligaportugal": {
        "code": "PO1",
        "slug": "liga-portugal",
        "seasons": list(range(1996, 2025)),
        "offset": 0,
    },
    "ligaportugal2": {
        "code": "PO2",
        "slug": "liga-portugal-2",
        "seasons": list(range(2007, 2025)),
        "offset": 0,
    },
    "ligue1": {
        "code": "FR1",
        "slug": "ligue-1",
        "seasons": list(range(1990, 2025)),
        "offset": 0,
    },
    "ligue2": {
        "code": "FR2",
        "slug": "ligue-2",
        "seasons": list(range(1994, 2023)),
        "offset": 0,
    },
    "premierleague": {
        "code": "GB1",
        "slug": "premier-league",
        "seasons": list(range(1992, 2025)),
        "offset": 0,
    },
    "saudiproleague": {
        "code": "SA1",
        "slug": "saudi-pro-league",
        "seasons": list(range(2014, 2025)),
        "offset": 0,
    },
    "seriea": {
        "code": "IT1",
        "slug": "serie-a",
        "seasons": list(range(1996, 2025)),
        "offset": 0,
    },
    "serieb": {
        "code": "IT2",
        "slug": "serie-b",
        "seasons": list(range(2002, 2025)),
        "offset": 0,
    },
    "superlig": {
        "code": "TR1",
        "slug": "super-lig",
        "seasons": list(range(2014, 2025)),
        "offset": 0,
    },
}


def get_saison_id(season: int, offset: int) -> int:
    """Return Transfermarkt's season identifier after the league offset."""

    return season + offset


def generate_url(league_key: str, season: int) -> str:
    """Build a club market-value page URL for one league-season."""

    league_info = LEAGUES.get(league_key)
    if not league_info:
        raise ValueError(f"League {league_key!r} is not configured")

    saison_id = get_saison_id(season, league_info.get("offset", 0))
    return BASE_URL_TEMPLATE.format(
        slug=league_info["slug"],
        code=league_info["code"],
        saison_id=saison_id,
    )
