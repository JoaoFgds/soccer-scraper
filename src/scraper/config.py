MAX_RETRIES = 5
BACKOFF_FACTOR = 2
REQUEST_DELAY_RANGE_SECONDS = (3, 12)

OUTPUT_DIR = "data/raw"
BASE_URL = "https://www.transfermarkt.com.br"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9,pt;q=0.8",
}

LEAGUES = {
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
        "processed": "true",
    },
    "bundesliga": {
        "name": "Bundesliga",
        "slug": "bundesliga",
        "code": "L1",
        "start_year": "1963",
        "processed": "true",
    },
    "2bundesliga": {
        "name": "2. Bundesliga",
        "slug": "2-bundesliga",
        "code": "L2",
        "start_year": "1981",
        "processed": "true",
    },
    "seriea": {
        "name": "Serie A",
        "slug": "serie-a",
        "code": "IT1",
        "start_year": "1946",
        "processed": "true",
    },
    "serieb": {
        "name": "Serie B",
        "slug": "serie-b",
        "code": "IT2",
        "start_year": "2002",
        "processed": "true",
    },
    "ligue1": {
        "name": "Ligue 1",
        "slug": "ligue-1",
        "code": "FR1",
        "start_year": "1948",
        "processed": "true",
    },
    "ligue2": {
        "name": "Ligue 2",
        "slug": "ligue-2",
        "code": "FR2",
        "start_year": "1994",
        "processed": "true",
    },
    "brasileiraoseriea": {
        "name": "Campeonato Brasileiro Série A",
        "slug": "campeonato-brasileiro-serie-a",
        "code": "BRA1",
        "start_year": "2006",
        "processed": "true",
    },
    "brasileiraoserieb": {
        "name": "Campeonato Brasileiro Série B",
        "slug": "campeonato-brasileiro-serie-b",
        "code": "BRA2",
        "start_year": "2009",
        "processed": "true",
    },
    "ligaportugal": {
        "name": "Liga Portugal",
        "slug": "liga-portugal",
        "code": "PO1",
        "start_year": "1996",
        "processed": "true",
    },
    "ligaportugal2": {
        "name": "Liga Portugal 2",
        "slug": "liga-portugal-2",
        "code": "PO2",
        "start_year": "2007",
        "processed": "true",
    },
    "jupilerproleague": {
        "name": "Jupiler Pro League",
        "slug": "jupiler-pro-league",
        "code": "BE1",
        "start_year": "2008",
        "processed": "true",
    },
    "challenger": {
        "name": "Challenger Pro League",
        "slug": "challenger-pro-league",
        "code": "BE2",
        "start_year": "2006",
        "processed": "true",
    },
    "j1league": {
        "name": "J1 League",
        "slug": "j1-league",
        "code": "JAP1",
        "start_year": "2005",
        "processed": "false",
    },
    "j2league": {
        "name": "J2 League",
        "slug": "j2-league",
        "code": "JAP2",
        "start_year": "2010",
        "processed": "false",
    },
    "superlig": {
        "name": "Süper Lig",
        "slug": "super-lig",
        "code": "TR1",
        "start_year": "2014",
        "processed": "true",
    },
}
