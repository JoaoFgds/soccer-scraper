# Soccer Analytics Engine
[![Python Version](https://img.shields.io/badge/python-3.13+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## 1. Overview
This project is a complete and robust data engineering pipeline designed to extract, process, and analyze professional soccer data from the [Transfermarkt](https://www.transfermarkt.com.br) website. Operating as a fully automated system, it transforms raw, unstructured web data into structured, validated datasets and, ultimately, actionable statistical insights. The application architecture is based on the **Medallion Architecture** with three stages (scrape, process, analysis), mirroring the Bronze, Silver, and Gold layers. This approach ensures data quality, traceability, and robustness. The ultimate goal is to analyze the concept of "schedule balance" and determine its statistical significance in team performance, especially final league position.

## 2. Main Features
- **End-to-End Automated Pipeline**: A single command can trigger the entire workflow: extracting raw data, cleaning and validating it, and running a full suite of statistical analyses.
- **Medallion Data Architecture**:
  - **Bronze Layer**: Raw, untouched data directly from the source.
  - **Silver Layer**: Cleaned, validated, enriched, and analysis-ready data.
  - **Gold Layer**: Aggregated business insights and statistical model outputs.
- **Robust and Ethical Scraping**: The scraper is built to be resilient with automatic retries and exponential backoff. It incorporates random delays to ensure ethical, low-impact interaction with the source server.
- **Automated Data Processing and Validation**: The processing stage applies a series of business rules to validate data quality, checking for completeness (e.g., all team files present), correctness (e.g., valid team URLs), and structural integrity (e.g., round-robin format).
- **Club Market Values**: A dedicated Transfermarkt scraper collects squad and club market values by league-season and consolidates them into a Silver dataset.
- **Advanced Statistical Analysis**: The analysis stage automatically calculates:
  - **Spearman’s G Coefficient**: To quantify the strength and balance of each team’s schedule.
  - **Schedule-Group Proportions**: SSB measurement across multiple magnitude and significance classifications.
  - **Cliff’s Delta**: To measure the effect of schedule classification on final league position.
  - **Mann-Whitney U Test**: To assess differences in final-position distributions between unbalanced and balanced schedule groups.
- **Modular and Maintainable Code**: The project is logically structured into distinct modules for scraping, processing, analysis, and utilities, following software engineering best practices.

## 3. Project Architecture and Data Flow
The application is orchestrated by `main.py` at the project root, which executes the three main pipelines in sequence. Each stage consumes data from the previous layer and produces artifacts for the next.

### Main Module (main.py)
This is the application’s entry point. It uses `argparse` to allow the user to run one of the three pipelines (scrape, process, analysis) or all sequentially (`all`). It contains no business logic, only orchestrating the execution of the main modules for each stage.

### Utilities Module (src/utils)
This module provides support functionalities used throughout the project.
- `helpers.py`: Contains critical helper functions, such as `sanitize_filename` for cleaning file names, `generate_id` for creating deterministic SHA-256 hashes for unique identifiers, `extract_metadata_from_filename` for extracting structured information from standardized file names, and `validate_url_year` for checking year consistency in URLs.
- `logger.py`: Configures the application-wide logging system, directing logs to both the console and a rotating file. Includes a customized formatter that displays file paths relatively, making logs more readable.
- `paths.py`: Centralizes all filesystem paths for the project. It defines the complete directory structure (Bronze, Silver, Gold) and creates these folders at startup to ensure the application can save its artifacts correctly.

### Stage 1: Scraper (Bronze Layer)
- **Responsibility**: Extract raw, unaltered data for specified leagues and seasons from Transfermarkt.
- **Main Modules**:
  - `scraper/main.py`: Orchestrates the scraping workflow, iterating through leagues and seasons defined in `scraper/constants.py`.
  - `scraper/constants.py`: Central configuration file defining scraping parameters, such as network settings (retries, delays), request headers, and the list of `LEAGUES` to be extracted.
  - `scraper/network.py`: Manages all HTTP requests with robust error handling, random delays, and retries with exponential backoff.
  - `scraper/parsers.py`: Contains logic to parse HTML content from web pages using BeautifulSoup and extract structured data into pandas DataFrames.
  - `scraper/exceptions.py`: Defines the custom `ScrapingError` exception to handle predictable scraping failures.
- **Output**: Raw CSV files organized by league and season, stored in `data/bronze/`. This layer serves as the single source of truth for all subsequent processing.

#### Club Market-Value Scraper

- **Responsibility**: Extract squad size, average age, foreign-player count, and average and total club market values for the historical league-season scope.
- **Entry point**: `python -m src.market_value.main`.
- **Output**: League-season CSV files in `data/bronze/market_values/`; `python -m src.market_value.process_silver` consolidates them into `data/silver/market_values_silver.csv`.

### Stage 2: Processor (Silver Layer)
- **Responsibility**: Transform raw data from the Bronze layer into clean, validated, analysis-ready datasets.
- **Main Modules**:
  - `processor/main.py`: Orchestrates the entire preprocessing and data validation workflow.
  - `processor/processors.py`: Contains functions to read, consolidate, clean, enrich, and validate raw data. It performs critical data quality checks based on predefined business rules.
  - `processor/mapping.py`: Implements a sophisticated team name standardization process. It uses a combination of a manual mapping file and an optimal assignment algorithm (`linear_sum_assignment` from SciPy) to resolve name variations across different data sources.
- **Output**: Validated, complete master CSV files stored in `data/silver/`. These data are considered reliable and ready for business intelligence and statistical analysis.

### Stage 3: Analysis (Gold Layer)
- **Responsibility**: Read the validated Silver datasets and generate the analysis metrics, tables, effect-size analyses, and figures under `data/gold/analysis/`. The analysis stage reads the Silver outputs.
- **Entry point**: Run `uv run main.py analysis` to call `analysis_pipeline()` in `analysis/main.py`. The pipeline executes the following tasks in order:
  1. Calculate Spearman’s G coefficient for final-standing and market-value schedule-strength proxies.
  2. Aggregate G-type counts into a summary table.
  3. Generate the Tables for the magnitude thresholds `0.200`, `0.300`, and `0.400`, and significance levels `0.10` and `0.05`.
  4. Calculate season-level Cliff’s delta and generate Figure 1.
  5. Calculate league-level significance results and generate Figure 2 for both proxies.
- **Inputs**: `data/silver/team_games_valid.csv` and `data/silver/final_standings_valid_market_ranking.csv`.
- **Main Modules**:
  - `analysis/main.py`: Orchestrates the five analysis tasks.
  - `analysis/spearman_coeff_calculate.py`: Calculates schedule balance, writes the coefficient tables, and stores the individual schedule vectors as JSON.
  - `analysis/spearman_coeff_summary.py`: Aggregates G-type counts across seasons, leagues, and the complete dataset.
  - `analysis/schedule_classifications.py`: Defines the magnitude and significance classifications used.
  - `analysis/ssb_proportions.py`: Generates Tables 2 and 3 for both schedule-strength proxies.
  - `analysis/cliffs_delta.py`: Calculates season-level Cliff’s delta and generates Figure 1.
  - `analysis/significancy_analysis.py`: Calculates league-level Cliff’s delta and Mann-Whitney p-values and generates Figure 2.
- **Outputs**:
  - `data/gold/analysis/spearman_coefficient/metrics/`: Spearman balance results and the G-type summary.
  - `data/gold/analysis/spearman_coefficient/schedules_data/individual/`: Per-league-season schedule vectors in JSON format.
  - `data/gold/analysis/spearman_coefficient/{classification}/`: `table_2.csv` and `table_3.csv` for each `G_type_0.200`, `G_type_0.300`, `G_type_0.400`, `significance_0.10`, and `significance_0.05` classification.
  - `data/gold/analysis/cliffs_delta/{classification}/`: Cliff’s delta CSV results and Figure 1 PNG files.
  - `data/gold/analysis/significancy_analysis/{market-value-proxy,final-ranking-proxy}/{classification}/`: significance CSV results and Figure 2 PNG files for each classification.

## 4. Data Artifacts and Schemas
The project produces structured data artifacts at each pipeline layer.

### Bronze Layer (data/bronze/)

#### League Standings
- **Path**: `data/bronze/scraper/{league_slug}/{season_year}/final_standings/{league_slug}_{season_year}_standings.csv`
- **Description**: The final league standings table for a given season.

| Column          | Type   | Description                                    | Example           |
|-----------------|--------|------------------------------------------------|-------------------|
| position        | string | The team’s final ranking in the table.         | 1                 |
| team            | string | The team’s full name.                          | Manchester City   |
| played          | string | Total number of matches played.                | 38                |
| won             | string | Total number of matches won.                   | 28                |
| drawn           | string | Total number of matches drawn.                 | 7                 |
| lost            | string | Total number of matches lost.                  | 3                 |
| goal_ratio      | string | Goals scored vs. goals conceded.               | 96:34             |
| goal_difference | string | Final goal difference.                         | 62                |
| points          | string | Total points accumulated.                      | 91                |
| team_url        | string | Absolute URL to the team’s main page on Transfermarkt. | https://... |

#### Team Games
- **Path**: `data/bronze/scraper/{league_slug}/{season_year}/team_games/{league_slug}_{season_year}_{sanitized_team_name}.csv`
- **Description**: A match-by-match breakdown for a single team, including tactical information, results, and attendance.

| Column      | Type    | Description                                           | Example             |
|-------------|---------|-------------------------------------------------------|---------------------|
| round       | string  | The round or matchday number.                         | 1                   |
| date        | string  | The match date.                                       | Sun, 08/18/2024     |
| time        | string  | The start time in the website’s local timezone.       | 12:30               |
| home_team   | string  | The home team’s name.                                 | Chelsea FC          |
| away_team   | string  | The away team’s name.                                 | Manchester City     |
| formation   | string  | The tactical formation used by the team.              | 4-3-3 attacking     |
| coach       | string  | The team’s coach name for that match.                 | Pep Guardiola       |
| audience    | integer | The official match attendance.                        | 55017               |
| result      | string  | The final match score.                                | 0:2                 |
| match_link  | string  | Absolute URL to the detailed match report.            | https://...         |

### Silver Layer (data/silver/)

#### Seasons Summary
- **Path**: `data/silver/seasons_summary.csv`
- **Description**: A data quality artifact that summarizes and validates each extracted season based on a set of business rules.

| Column                    | Type    | Description                                                                 |
|---------------------------|---------|-----------------------------------------------------------------------------|
| source_id                 | string  | Unique SHA-256 hash of the source standings file.                           |
| source_csv_file           | string  | Name of the source standings CSV file.                                      |
| league_name               | string  | Sanitized slug of the league name.                                          |
| season_year               | integer | Season start year.                                                          |
| num_total_teams           | integer | Total number of teams in the standings table.                               |
| num_total_games           | integer | Total number of unique games played in the season.                          |
| num_null_attendance_games | integer | Count of games with null or zero attendance data.                           |
| pct_null_attendance_games | float   | Percentage of games with null attendance.                                   |
| is_valid_url              | boolean | Flag indicating if all team URLs in the standings are consistent with the season year. |
| is_double_rounded         | boolean | Flag indicating if the total number of games matches a round-robin tournament (N*(N-1)). |
| is_valid_attendance       | boolean | Flag indicating if the percentage of null attendance is below an acceptable threshold (5%). |
| has_all_teams_files       | boolean | Flag indicating if a games file exists for every team listed in the standings. |

#### Validated Standings
- **Path**: `data/silver/final_standings_valid.csv`
- **Description**: A consolidated master table of all high-quality season standings data, enriched with unique IDs and canonical team names.

| Column           | Type   | Description                                                                 |
|------------------|--------|-----------------------------------------------------------------------------|
| id               | string | Unique SHA-256 hash for the team-season record.                             |
| source_id        | string | Unique SHA-256 hash of the source standings file.                           |
| position         | Int64  | The team’s final numeric ranking.                                           |
| team             | string | The team’s original display name.                                           |
| team_sanitized   | string | Sanitized team name (lowercase, no spaces/accents).                         |
| played           | Int64  | Matches played.                                                            |
| won              | Int64  | Matches won.                                                               |
| drawn            | Int64  | Matches drawn.                                                             |
| lost             | Int64  | Matches lost.                                                              |
| goal_ratio       | string | Goals scored:conceded ratio.                                               |
| goal_difference  | string | Goal difference.                                                           |
| points           | Int64  | Total points.                                                              |
| team_url         | string | URL to the team’s page on Transfermarkt.                                    |
| league_name      | string | Sanitized slug of the league name.                                          |
| season_year      | integer| Season start year.                                                          |
| source_csv_file  | string | Name of the source standings CSV file.                                      |
| team_canonical   | string | Standardized canonical team name after mapping.                             |

#### Validated Games
- **Path**: `data/silver/team_games_valid.csv`
- **Description**: A consolidated master table of all high-quality season games data, enriched with canonical names and imputed attendance values.

| Column                  | Type         | Description                                                                 |
|-------------------------|--------------|-----------------------------------------------------------------------------|
| id                      | string       | Unique SHA-256 hash for the game record.                                    |
| source_id               | string       | Unique SHA-256 hash of the source team games file.                          |
| standings_id            | string       | Foreign key linking to the season’s standings file.                         |
| round                   | string       | Round or matchday number.                                                  |
| date                    | string       | Original match date.                                                       |
| time                    | string       | Original match time.                                                       |
| datetime                | datetime64[ns] | Parsed and unified match date and time.                                  |
| home_team               | string       | Original home team name.                                                   |
| home_team_sanitized     | string       | Sanitized home team name.                                                  |
| home_team_canonical     | string       | Standardized canonical home team name.                                      |
| away_team               | string       | Original away team name.                                                   |
| away_team_sanitized     | string       | Sanitized away team name.                                                  |
| away_team_canonical     | string       | Standardized canonical away team name.                                      |
| formation               | string       | Tactical formation used.                                                   |
| coach                   | string       | Original coach name.                                                       |
| coach_sanitized         | string       | Sanitized coach name.                                                      |
| result                  | string       | Final match score.                                                         |
| audience                | Int64        | Official match attendance.                                                 |
| audience_filled_fb      | Int64        | Attendance with missing values imputed by forward/backward fill.            |
| audience_filled_mean    | Int64        | Attendance with missing values imputed by the team’s season mean.           |
| audience_filled_median  | Int64        | Attendance with missing values imputed by the team’s season median.         |
| league_name             | string       | Sanitized slug of the league name.                                          |
| season_year             | integer      | Season start year.                                                          |
| source_csv_file         | string       | Name of the source games CSV file.                                          |
| standings_csv_file      | string       | Name of the associated standings CSV file.                                  |

### Gold Layer (data/gold/)

#### Name Mappings
- **Path (Combined)**: `data/gold/name_mappings/combined/name_mappings_combined.csv`
- **Path (Individual)**: `data/gold/name_mappings/individual/{league}_{season}_mapping.csv`
- **Description**: The output of the team name standardization process, showing how "other" names (from games data) were mapped to "canonical" names (from standings data).

| Column             | Type    | Description                                                                 |
|--------------------|---------|-----------------------------------------------------------------------------|
| league_name        | string  | League name.                                                                |
| season_year        | integer | Season year.                                                                |
| canonical_name     | string  | The standard (ground truth) name.                                           |
| other_name         | string  | The name that was mapped.                                                   |
| precision_score    | integer | Similarity score (0-100) from thefuzz library. 101 indicates a manual mapping. |
| is_confident_match | boolean | Flag indicating if the similarity score is above a confidence threshold (70). |

#### Schedule Balance Coefficients
- **Paths**:
  - `data/gold/analysis/spearman_coefficient/metrics/strength_schedule_balance.csv`
  - `data/gold/analysis/spearman_coefficient/metrics/strength_schedule_balance_ranking.csv`
  - `data/gold/analysis/spearman_coefficient/metrics/strength_schedule_balance_market.csv`
- **Description**: Schedule-balance results for final-standing and market-value strength proxies, containing one row per team-season.

| Column          | Type    | Description                                                                 |
|-----------------|---------|-----------------------------------------------------------------------------|
| standings_id    | string  | ID linking to the season’s standings record.                                 |
| league_name     | string  | League name.                                                                |
| season_year     | integer | Season year.                                                                |
| team_canonical  | string  | Canonical team name.                                                        |
| final_position  | integer | Team’s final league ranking.                                                 |
| R_array         | string  | String representation of the ideal opponent rankings list.                   |
| S_array         | string  | String representation of the actual opponent rankings list in the order faced. |
| G               | float   | Spearman’s rank correlation coefficient.                                     |
| P_value         | float   | P-value associated with the Spearman correlation.                            |
| is_significant  | boolean | Whether the correlation meets the 0.05 significance level.                   |
| G_type_0.200    | string  | Classification at the 0.200 magnitude threshold.                             |
| G_type_0.250    | string  | Classification at the 0.250 magnitude threshold.                             |
| G_type_0.300    | string  | Classification at the 0.300 magnitude threshold.                             |
| G_type_0.350    | string  | Classification at the 0.350 magnitude threshold.                             |
| G_type_0.400    | string  | Classification at the 0.400 magnitude threshold.                             |

#### G Coefficient Summary
- **Path**: `data/gold/analysis/spearman_coefficient/metrics/strength_schedule_summary.csv`
- **Description**: An aggregated table containing total tests, significant tests, and G-type counts by league-season, league, and the complete dataset.

| Column       | Type    | Description                                                                 |
|--------------|---------|-----------------------------------------------------------------------------|
| league_name  | string  | League name (all_leagues for overall summary).                               |
| season_year  | string  | Season year (all_seasons for league and overall summaries).                  |
| total_tests  | integer | Number of team-season results in the aggregation.                            |
| significant_tests | integer | Number of results classified as significant.                             |
| G_type_*     | integer | Count for each schedule classification and magnitude threshold.              |

#### Schedule Raw Data
- **Path**: `data/gold/analysis/spearman_coefficient/schedules_data/individual/schedule_data_{league}_{season}.json`
- **Description**: Per-league-season JSON files containing the opponent order and rank vectors used to calculate Spearman’s G coefficient.

#### Cliff’s Delta Results
- **Path**: `data/gold/analysis/cliffs_delta/{classification}/cliffs_delta.csv`
- **Description**: Season-level Cliff’s delta results comparing unbalanced schedule groups with the balanced group for each strength proxy.

#### Significance Analysis Results
- **Paths**:
  - `data/gold/analysis/significancy_analysis/market-value-proxy/{classification}/significancy_analysis.csv`
  - `data/gold/analysis/significancy_analysis/final-ranking-proxy/{classification}/significancy_analysis.csv`
- **Description**: League-level Cliff’s delta and two-sided Mann-Whitney U results for final-position comparisons.

#### Visualization Plots
- **Paths**:
  - `data/gold/analysis/cliffs_delta/{classification}/cliffs_delta.png`: Figure 1, the season-level Cliff’s delta distributions.
  - `data/gold/analysis/significancy_analysis/{market-value-proxy,final-ranking-proxy}/{classification}/significancy_analysis.png`: Figure 2, the league-level significance analysis.

## 5. Statistical Analysis Explained
The core of this project is to investigate whether a team’s game schedule has a tangible effect on its performance.

### Spearman’s G Coefficient: Measuring Schedule Balance
To quantify schedule balance, we calculate Spearman’s rank correlation coefficient (**G**) between two vectors built from the first encounter with each opponent:
1. **R Vector (Ideal Schedule)**: The team’s opponents ordered by the selected strength proxy, from strongest to weakest.
2. **S Vector (Actual Schedule)**: The same opponents ordered chronologically by the first match against each one.
The resulting **G** correlation is interpreted at each configured magnitude threshold (**τ**):
- **G > τ (Unbalanced Strong / G+)**: Stronger opponents tend to appear earlier in the schedule, producing a harder or front-loaded start.
- **G < -τ (Unbalanced Weak / G-)**: Weaker opponents tend to appear earlier in the schedule, producing an easier or back-loaded start.
- **-τ <= G <= τ (Balanced / G0)**: The schedule does not meet the selected magnitude threshold for an unbalanced classification.

### Cliff’s Delta: Measuring Effect Size
Cliff’s delta measures the direction and magnitude of rank differences between two groups. For each league-season and strength proxy, the analysis compares final league positions for `G-` or `G+` schedules against `G0` schedules. Values range from `-1` to `1`; a negative value means that the compared unbalanced group tends to have lower, better final-position values than `G0`. The results are written to CSV and visualized in Figure 1.

### Mann-Whitney U Test: Testing Significance
The significance analysis applies a two-sided Mann-Whitney U test to final-position distributions at league level. It compares `G-` with `G0` and `G+` with `G0` for each configured magnitude or significance classification. A low p-value, typically below `0.05`, indicates evidence that the compared distributions differ. The output also includes the season-stratified Cliff’s delta and the `-log10(p-value)` used in Figure 2.

## 6. Project Structure
```
├── assets/
│   └── manual_name_mapping.csv
├── data/
│   ├── bronze/
│   ├── silver/
│   └── gold/
├── logs/
│   └── app.log
├── src/
│   ├── analysis/
│   ├── market_value/
│   ├── processor/
│   ├── scraper/
│   ├── utils/
│   └── consolidate_football_data.py
├── docs/
│   └── REPRODUCIBILITY.md
├── reproducibility/
├── main.py
├── pyproject.toml
└── README.md
```

## 7. Setup and Installation

### Prerequisites
- Python 3.13+
- uv (recommended package manager)

### Installation Steps
1. **Clone the repository:**
   ```bash
   git clone https://github.com/JoaoFgds/soccer-scraper.git
   cd soccer-scraper
   ```
2. **Create and activate a virtual environment:**
   ```bash
   uv venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   ```
3. **Install dependencies from pyproject.toml:**
   ```bash
   uv sync --locked
   ```
   This command installs all dependencies specified in `pyproject.toml`, ensuring the project environment is correctly configured.

## 8. How to Run
The full pipeline is controlled via `main.py` using command-line arguments.
1. **Activate the virtual environment:**
   ```bash
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   ```
2. **Run a specific pipeline:**
   - To run only the scraper:
     ```bash
     uv run main.py scrape
     ```
   - To run only the data processor:
     ```bash
     uv run main.py process
     ```
   - To run only the statistical analysis:
     ```bash
     uv run main.py analysis
     ```
   - To run the entire end-to-end pipeline:
     ```bash
     uv run main.py all
     ```

### Market-value pipeline

```bash
# One league-season; omit the options to run the configured historical scope.
uv run --locked python -m src.market_value.main \
  --league premierleague \
  --season 2024

uv run --locked python -m src.market_value.process_silver
```

### Team-season consolidated dataset

After restoring the frozen inputs documented in
[`docs/REPRODUCIBILITY.md`](docs/REPRODUCIBILITY.md):

```bash
uv run --locked python -m src.consolidate_football_data
```

## 9. Reproducing published results

Live scraping can change as Transfermarkt changes. Exact historical
reproduction therefore uses versioned input snapshots and SHA-256 manifests.
See [`docs/REPRODUCIBILITY.md`](docs/REPRODUCIBILITY.md) for the input schemas,
commands, expected row counts, checksums, and remaining manual data-publication
step.

## 10. License
This project is licensed under the MIT License.
