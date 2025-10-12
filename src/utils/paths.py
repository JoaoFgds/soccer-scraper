from pathlib import Path

# --- 1. Base Project Directory ---
ROOT_DIR = Path(__file__).resolve().parent.parent.parent

# --- 2. Core Application Directories ---
LOG_DIR = ROOT_DIR / "logs"
DATA_DIR = ROOT_DIR / "data"
ASSETS_DIR = ROOT_DIR / "assets"

# --- 3. Data Layer Directories ---

# Raw data from the scraper
BRONZE_DATA_DIR = DATA_DIR / "bronze"

# Processed, cleaned, and validated data
SILVER_DATA_DIR = DATA_DIR / "silver"

# Analysis-ready data, models, and final artifacts
GOLD_DATA_DIR = DATA_DIR / "gold"

# --- 4. Asset Paths ---
# Manually maintained files, like configuration or overrides.
MANUAL_NAME_MAPPING_PATH = ASSETS_DIR / "manual_name_mapping.csv"

# --- 5. Log File Path ---
LOG_FILE = LOG_DIR / "app.log"

# --- 6. Bronze Layer Paths (Scraper Output) ---
SCRAPER_OUTPUT_DIR = BRONZE_DATA_DIR / "scraper"

# --- 7. Silver Layer Paths (Pre-Processor Output) ---

# Intermediate, complete datasets before validation
STANDINGS_COMPLETE_PATH = SILVER_DATA_DIR / "final_standings_complete.csv"
GAMES_COMPLETE_PATH = SILVER_DATA_DIR / "team_games_complete.csv"

# Final, validated datasets ready for analysis
STANDINGS_VALID_PATH = SILVER_DATA_DIR / "final_standings_valid.csv"
GAMES_VALID_PATH = SILVER_DATA_DIR / "team_games_valid.csv"

# Data quality summary artifact
SEASON_SUMMARY_PATH = SILVER_DATA_DIR / "seasons_summary.csv"

# --- 8. Gold Layer Paths (Analysis & Mapping Artifacts) ---

ANALYSIS_OUTPUT_DIR = GOLD_DATA_DIR / "analysis"
NAME_MAPPINGS_DIR = GOLD_DATA_DIR / "name_mappings"
NAME_MAPPINGS_COMBINED_DIR = NAME_MAPPINGS_DIR / "combined"
NAME_MAPPINGS_INDIVIDUAL_DIR = NAME_MAPPINGS_DIR / "individual"
NAME_MAPPINGS_COMBINED_PATH = NAME_MAPPINGS_COMBINED_DIR / "name_mappings_combined.csv"

# --- 9. Directory Initialization ---

# A list of all directories that must exist for the application to run.
# This makes it easy to manage and ensures they are created on startup.

DIRECTORIES_TO_CREATE = [
    LOG_DIR,
    ASSETS_DIR,
    BRONZE_DATA_DIR,
    SILVER_DATA_DIR,
    GOLD_DATA_DIR,
    SCRAPER_OUTPUT_DIR,
    ANALYSIS_OUTPUT_DIR,
    NAME_MAPPINGS_COMBINED_DIR,
    NAME_MAPPINGS_INDIVIDUAL_DIR,
]


def create_project_directories():
    """
    Creates all necessary project directories if they don't already exist.
    """
    for directory in DIRECTORIES_TO_CREATE:
        directory.mkdir(parents=True, exist_ok=True)


# Automatically create directories when this module is imported.
create_project_directories()
