# Transfermarkt Soccer Scraper

[![Python Version](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## Overview

This project is a robust and scalable Python-based web scraper designed to extract comprehensive soccer data from [Transfermarkt](https://www.transfermarkt.com.br), one of the most detailed public sources for football statistics. The application systematically processes multiple leagues and historical seasons to build a rich dataset for sports analytics, predictive modeling, or historical research.

The scraper is engineered with politeness and resilience at its core. It incorporates intelligent delays and a sophisticated retry mechanism to ensure stable, long-running execution while respecting the website's servers and minimizing the risk of IP blocks. This project is not just a script, but a small-scale data engineering pipeline, moving from raw web data to structured, usable CSV files.

## Key Features

-   **Modular & Scalable Architecture**: The code is organized into a clean `src` layout, separating distinct responsibilities (configuration, network requests, data parsing, utilities) into dedicated modules for high maintainability and ease of expansion.
-   **Robust Network Handling**: Features a resilient request handler that automatically retries on transient network errors and specific server responses (HTTP 429/503), using an exponential backoff strategy to gracefully handle server load.
-   **Polite & Ethical Scraping**: A randomized delay is enforced between all requests to mimic human-like browsing behavior, ensuring the scraper does not overwhelm the target server. This is crucial for long-term operational stability.
-   **Configuration-Driven Control**: The entire scraping process is controlled via a central configuration file (`config.py`). Users can easily define which leagues to scrape, manage their processing state (`processed` flag), and set historical data limits (`start_year`).
-   **Structured & Organized Data Output**: All extracted data is saved into clean, machine-readable CSV files, organized in a logical directory hierarchy: `data/raw/{league_slug}/{season_year}/`.

## Data Coverage

This project has successfully scraped and processed historical data for the following major European and Brazilian football leagues. The data for each league covers the seasons from a defined start year up to the **2024** season. The scraping process respects a minimum start year of **1990** for historical data.

*Note: For leagues following a mid-year calendar (e.g., most European leagues), the year represents the start of the season. For example, data for the year `2023` corresponds to the `2023/24` season.*


| League                             | Country | Seasons Covered |
| :--------------------------------- | :------ | :-------------- |
| Premier League                     | England | 1992-2024       |
| Championship                       | England | 2004-2024       |
| LaLiga                             | Spain   | 2000-2024       |
| LaLiga2                            | Spain   | 2007-2024       |
| Bundesliga                         | Germany | 1990-2024       |
| 2. Bundesliga                      | Germany | 1990-2024       |
| Serie A                            | Italy   | 1990-2024       |
| Serie B                            | Italy   | 2002-2024       |
| Ligue 1                            | France  | 1990-2024       |
| Ligue 2                            | France  | 1994-2024       |
| Liga Portugal                      | Portugal| 1996-2024       |
| Liga Portugal 2                    | Portugal| 2007-2024       |
| Ligue 2                            | France  | 1994-2024       |
| Campeonato Brasileiro Série A      | Brazil  | 2006-2024       |
| Campeonato Brasileiro Série B      | Brazil  | 2009-2024       |

## Architectural Deep Dive

To meet the goal of being a robust and maintainable application, the project's code is divided into several modules, each with a single responsibility. The data flows between these modules in a logical sequence.

-   `src/soccer_scraper/config.py`: The single source of truth for all operational parameters. It holds constants like request headers, retry settings, and the main `LEAGUES` dictionary that defines the scope of the scraping tasks. No other module should contain hardcoded configuration values.
-   `src/soccer_scraper/network.py`: The communication layer. Its primary function, `fetch_soup`, handles all outgoing HTTP requests, embedding the politeness delay and exponential backoff logic. It is the only module that directly interacts with the internet. It uses parameters from `config.py` and raises exceptions from `exceptions.py`.
-   `src/soccer_scraper/parsers.py`: The core "brain" of the scraper. This module contains functions (`fetch_league_standings`, `fetch_team_games`) responsible for taking raw HTML content (provided by `network.py`) and parsing it with BeautifulSoup to extract structured data into pandas DataFrames. The logic here is specific to Transfermarkt's HTML structure.
-   `src/soccer_scraper/utils.py`: A toolbox for common, reusable tasks. It contains helper functions like `sanitize_filename` that are used across different parts of the application to ensure consistent data cleaning.
-   `src/soccer_scraper/exceptions.py`: Defines custom exceptions like `ScrapingError`. This allows the application to differentiate between predictable scraping failures (e.g., a table not found) and general Python errors.
-   `main.py` (and `src/soccer_scraper/main.py`): The central orchestrator. These modules do not contain any low-level scraping logic. Instead, they import components from the other modules and execute the high-level workflow: reading the configuration, looping through leagues and seasons, calling the appropriate parsers, handling errors, and saving the results.

## Configuration In-Depth

All operational parameters are controlled from `src/soccer_scraper/config.py`.

| Constant                      | Description                                                                                             |
| ----------------------------- | ------------------------------------------------------------------------------------------------------- |
| `MAX_RETRIES`                 | The maximum number of times the scraper will retry a failed network request.                            |
| `BACKOFF_FACTOR`              | A multiplier that determines how quickly the delay between retries increases (e.g., `2**attempt`).      |
| `REQUEST_DELAY_RANGE_SECONDS` | A tuple `(min, max)` defining the random waiting period in seconds before each HTTP request.            |
| `OUTPUT_DIR`                  | The root directory where all scraped data will be saved.                                                |
| `BASE_URL`                    | The base URL for Transfermarkt, used to construct absolute links from relative paths.                   |
| `HEADERS`                     | HTTP headers sent with each request to identify the client as a standard web browser.                   |
| `LEAGUES`                     | The main dictionary defining the scraping tasks. Each key (`name`, `slug`, etc.) is vital for the process. |

##  Erorr Handling & Logging

The application is designed to run for long periods without supervision.
- **Custom Exception**: `ScrapingError` is raised for predictable issues (e.g., failed requests, missing HTML tables). This allows the main loop to catch these errors, log them, and continue to the next item without crashing.
- **Logging Levels**:
  - `INFO`: Tracks the main progress of the script (e.g., "Starting processing for league...", "Processing season...").
  - `WARNING`: Indicates non-critical issues, such as skipping a table row with an unexpected format or being unable to parse a specific data point.
  - `ERROR`: Reports a failure for a specific task (e.g., processing a single season) that was caught and handled.
  - `CRITICAL`: Reports a major failure that may have stopped a significant part of the process.

## Limitations and Known Issues

-   **Website Changes**: The scraper's logic in `parsers.py` is tightly coupled to the HTML structure of Transfermarkt. Any significant change to the website's layout will likely break the parsers and require code updates.
-   **IP Blocking**: While the scraper is designed to be polite, extremely long and continuous scraping sessions (spanning many hours or days) could still trigger automated blocking from the website. It is recommended to process a few leagues at a time if you encounter connection issues.
-   **Data Accuracy**: The data is a direct reflection of what is publicly available on Transfermarkt. Its accuracy is subject to the source's own data quality.

## Project Structure

```bash
soccer_scraper/
├── .venv/
├── data/
│   └── raw/
├── src/
│   └── scraper/
│       ├── __init__.py
│       ├── config.py
│       ├── exceptions.py
│       ├── network.py
│       ├── parsers.py
│       ├── utils.py
│       └── main.py
├── .gitignore
├── .python-version
├── main.py
├── pyproject.toml
├── README.md
└── uv.lock
```

## Setup and Installation

Follow these steps to set up the project environment.

### Prerequisites

-   [Git](https://git-scm.com/)
-   [Python](https://www.python.org/) (version 3.9+ recommended)
-   [pyenv](https://github.com/pyenv/pyenv) (recommended for managing Python versions; it will use the `.python-version` file automatically)
-   [uv](https://github.com/astral-sh/uv) (an extremely fast Python package installer and resolver)

### Installation Steps

1.  **Clone the repository:**
    * Command:
        ```bash
        git clone [https://github.com/YOUR_USERNAME/soccer_scraper.git](https://github.com/YOUR_USERNAME/soccer_scraper.git)
        cd soccer_scraper
        ```

2.  **Set up the Python environment:**
    * A virtual environment is crucial for isolating project dependencies. Create one using `uv`:
        ```bash
        uv venv
        ```

3.  **Install dependencies:**
    * Activate the virtual environment:
        ```bash
        source .venv/bin/activate
        ```
    * Sync the environment with the exact package versions specified in `uv.lock` for a reproducible setup:
        ```bash
        uv sync
        ```

## How to Run

The scraper is configured and executed from the project's root directory.

1.  **Configure the Scraper**:
    * Open `src/soccer_scraper/config.py`.
    * The `LEAGUES` dictionary is the main control panel. Set the `"processed"` key to `"false"` for any league you wish to process.
    * You can also adjust the `MIN_START_YEAR` in `src/soccer_scraper/main.py` if you wish to limit how far back the scraper goes.

2.  **Run the Script**:
    * Ensure your virtual environment is activated.
    * Execute the main entry point script:
        ```bash
        uv run main.py
        ```
    * The script will log its progress to the console.

## Data Output Schema

All data is saved within the `data/raw/` directory, following a `league_slug/season_year/` structure.

---

### 1. League Standings

A single file containing the final classification table for a given league and season.

-   **Directory:** `data/raw/{league_slug}/{season_year}/final_standings/`
-   **Filename:** `{league_slug}_{season_year}_standings.csv`
-   **Description:** This file represents a snapshot of the complete league table at the end of the season. It is crucial for discovering all participating teams and their respective page URLs for deeper scraping.

| Column            | Type   | Description                                                     | Example         |
| ----------------- | ------ | --------------------------------------------------------------- | --------------- |
| `position`        | string | The team's final rank in the table.                             | 1             |
| `team`            | string | The full name of the team.                                      | Manchester City |
| `played`          | string | Total number of matches played.                                 | 38            |
| `won`             | string | Total number of matches won.                                    | 28            |
| `drawn`           | string | Total number of matches drawn.                                  | 7             |
| `lost`            | string | Total number of matches lost.                                   | 3             |
| `goal_ratio`      | string | Goals for vs. goals against.                                    | 96:34         |
| `goal_difference` | string | The final goal difference.                                      | 62           |
| `points`          | string | Total points accumulated.                                       | 91            |
| `team_url`        | string | The absolute URL to the team's main page on Transfermarkt.      | https://...   |

---

### 2. Team Games

A separate CSV file is created for each team, detailing their full season journey in the league.

-   **Directory:** `data/raw/{league_slug}/{season_year}/team_games/`
-   **Filename:** `{league_slug}_{season_year}_{sanitized_team_name}.csv`
-   **Description:** This file provides a match-by-match breakdown for a single team, including tactical information, results, and attendance, offering a granular view of their performance over the season.

| Column      | Type    | Description                                                     | Example                  |
| ----------- | ------- | --------------------------------------------------------------- | ------------------------ |
| `round`     | string  | The matchday or round number.                                   | 1                      |
| `date`      | string  | The date of the match.                                          | Sun Aug 18, 2024      |
| `time`      | string  | The kickoff time in the local timezone of the site.             | 12:30                  |
| `home_team` | string  | The name of the home team.                                      | Chelsea FC             |
| `away_team` | string  | The name of the away team.                                      | Manchester City        |
| `formation` | string  | The tactical formation used by the team.                        | 4-3-3 Attacking        |
| `coach`     | string  | The name of the team's coach for that match.                    | Pep Guardiola          |
| `audience`  | integer | The official match attendance.                                  | 55017                    |
| `result`    | string  | The final score of the match.                                   | 0:2                    |
| `match_link`| string  | The absolute URL to the detailed match report on Transfermarkt. | https://...            |

## Contributing

Contributions are welcome! If you'd like to improve the scraper or add new features, please follow these steps:

1.  Fork the repository.
2.  Create a new feature branch (`git switch -c feature/your-awesome-feature`).
3.  Commit your changes (`git commit -m "feat: Add your awesome feature"`).
4.  Push to the branch (`git push origin feature/your-awesome-feature`).
5.  Open a Pull Request.

## License

This project is licensed under the MIT License.