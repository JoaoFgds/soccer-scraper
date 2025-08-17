# Transfermarkt Football Data Scraper

[![Python Version](https://img.shields.io/badge/python-3.8%2B-blue.svg)](https://www.python.org/downloads/)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

This project is a robust web scraper designed to extract football (soccer) data from Transfermarkt.com. It focuses on collecting league standings and team match schedules for various leagues and seasons, particularly the first and second divisions of major world championships. The scraper is built with resilience in mind, including retries, delays, and logging to handle web scraping challenges ethically and efficiently.

**Important Note:** Web scraping should be done responsibly. Transfermarkt's terms of service prohibit automated data extraction for commercial use. This tool is for educational and personal research purposes only. Use it at your own risk, and respect rate limits to avoid IP bans.

## Table of Contents

- [Features](#features)
- [Requirements](#requirements)
- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)
- [Output Structure](#output-structure)
- [Examples](#examples)
- [Troubleshooting](#troubleshooting)
- [Contributing](#contributing)
- [License](#license)

## Features

- **Multi-League Support:** Scrapes data from multiple leagues, including Premier League (GB1), Championship (GB2), LaLiga (ES1), Segunda División (ES2), Bundesliga (L1), 2. Bundesliga (L2), Serie A (IT1), Serie B (IT2), Ligue 1 (FR1), Ligue 2 (FR2), Brasileirão Série A (BRA1), Brasileirão Série B (BRA2), and more. Easily extendable via configuration.
- **Multi-Season Scraping:** Automatically iterates over seasons from a configurable start year to the current year.
- **Data Extracted:**
  - League standings: Position, team name, games played, wins, draws, losses, goals, goal difference, points, and team URLs.
  - Team schedules: Round, date, time, home/away teams, formation, coach, audience, result, and match link.
- **Robustness:** 
  - Randomized delays and exponential backoff retries to handle rate limiting and network issues.
  - Logging for debugging and monitoring.
  - Custom exceptions for scraping errors.
- **Modular Design:** Separate functions for fetching HTML, parsing standings, and extracting schedules.
- **Output:** Saves data as CSV files organized by league, season, and team.
- **Ethical Considerations:** Built-in delays to minimize server load; no parallel processing by default to avoid abuse.

## Requirements

- Python 3.8 or higher
- Libraries:
  - `requests` for HTTP requests
  - `beautifulsoup4` (bs4) for HTML parsing
  - `pandas` for data handling and CSV export
  - `urllib` (standard library) for URL manipulation

No additional installations are needed beyond these (install via `pip` as shown below).

## Installation

1. Clone the repository:
   ```
   git clone https://github.com/yourusername/transfermarkt-scraper.git
   cd transfermarkt-scraper
   ```

2. Create a virtual environment (optional but recommended):
   ```
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. Install dependencies:
   ```
   pip install requests beautifulsoup4 pandas
   ```

4. (Optional) If using YAML/JSON for configuration, install `pyyaml` or ensure `json` is available (standard library).

## Configuration

The scraper is configurable via a separate file for leagues and seasons. By default, use `config.py`, `leagues.json`, or `leagues.yaml` (as per previous instructions). Place this file in the project root.

### Example `leagues.json` Configuration
```json
{
    "premierleague": {
        "name": "Premier League",
        "slug": "premier-league",
        "code": "GB1",
        "start_year": 1992
    },
    "championship": {
        "name": "Championship",
        "slug": "championship",
        "code": "GB2",
        "start_year": 2004
    },
    "brasileiraoseriea": {
        "name": "Campeonato Brasileiro Série A",
        "slug": "campeonato-brasileiro-serie-a",
        "code": "BRA1",
        "start_year": 2003
    }
    // Add more leagues as needed
}
```

- **Keys:** Use a unique name for the league (e.g., "premierleague").
- **name:** Full league name for HTML section identification.
- **slug:** URL slug (e.g., "premier-league").
- **code:** League code (e.g., "GB1").
- **start_year:** Earliest season to scrape (to avoid incomplete historical data).

In the script, load this configuration in the `__main__` block (examples provided in code comments).

Adjust global constants like `OUTPUT_DIR`, `MAX_RETRIES`, `REQUEST_DELAY_RANGE_SECONDS` in the script as needed.

## Usage

Run the script directly:
```
python scraper.py
```

- The script will iterate over all configured leagues and their seasons (from `start_year` to `END_SEASON`).
- Data is saved to `data/raw/{league}/{season}/` directories.
- Logs are printed to console; check for warnings/errors.

### Custom Runs
- Modify `__main__` to target specific leagues/seasons, e.g.:
  ```python
  main(
      championship_name="premierleague",
      season_id=2023,
      league_slug="premier-league",
      league_code="GB1",
      league_name="Premier League"
  )
  ```

- For batch processing, use the loop in `__main__`.

### Adding New Leagues
1. Find the league's slug and code on Transfermarkt (e.g., via browser inspection: standings URL like `/laliga/tabelle/wettbewerb/ES1`).
2. Add to the configuration file.
3. Test with a single season to verify selectors work (HTML structure may vary slightly).

## Output Structure

- **Root:** `data/raw/`
- **Per League/Season:** `{league_name}/{season_id}/`
  - `final_standings/{league_name}_{season_id}_standings.csv`: League-wide standings.
  - `team_games/{league_name}_{season_id}_{team_name}.csv`: Per-team schedules with columns: round, date, time, home_team, away_team, formation, coach, audience, result, match_link.

Example:
```
data/raw/
├── premierleague/
│   ├── 2023/
│   │   ├── final_standings/premierleague_2023_standings.csv
│   │   └── team_games/
│   │       ├── premierleague_2023_manchestercity.csv
│   │       └── ... (other teams)
│   └── 2024/
│       └── ...
└── brasileiraoseriea/
    └── ...
```

## Examples

### Scraping Premier League 2023/24
Run the script with configuration limited to Premier League. Output: Standings CSV and 20 team schedule CSVs.

### Extending to Brasileirão
Add to config:
```json
"brasileiraoseriea": {
    "name": "Campeonato Brasileiro Série A",
    "slug": "campeonato-brasileiro-serie-a",
    "code": "BRA1",
    "start_year": 2003
}
```
Rerun the script.

### Handling Errors
- If a season has no data (e.g., future years), the script logs a warning and skips.
- For site changes, update selectors in `fetch_team_games` or `fetch_league_standings`.

## Troubleshooting

- **IP Ban:** Increase delays (`REQUEST_DELAY_RANGE_SECONDS`) or use proxies (not implemented; add via `requests` proxies param).
- **Selector Errors:** If HTML changes, inspect the page (e.g., via browser dev tools) and update BS4 queries.
- **Empty DataFrames:** Check logs; may indicate incomplete seasons or parsing issues.
- **Dependencies:** Ensure libraries are installed; use `pip list` to verify.
- **Rate Limiting:** If 429 errors occur, extend `BACKOFF_FACTOR` or add longer inter-season sleeps.

For issues, open a GitHub issue with logs and URLs.

## Contributing

Contributions are welcome! Fork the repo, create a branch, and submit a pull request. Focus on:
- Adding new leagues.
- Improving robustness (e.g., parallel processing with threads).
- Unit tests for parsing functions.

Follow PEP 8 style guidelines.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.