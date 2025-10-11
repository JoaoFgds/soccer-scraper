# src/utils/paths.py

from pathlib import Path

# --- Project Root ---
ROOT_DIR = Path(__file__).resolve().parent.parent.parent

# --- Logging ---
LOG_DIR = ROOT_DIR / "logs"
LOG_FILE = LOG_DIR / "app.log"

# --- Data Tiers ---
DATA_DIR = ROOT_DIR / "data"
BRONZE_DATA_DIR = DATA_DIR / "bronze"  # Raw data from scraper
SILVER_DATA_DIR = DATA_DIR / "silver"  # Processed and cleaned data
GOLD_DATA_DIR = DATA_DIR / "gold"  # Analysis-ready data and artifacts

# --- Assets (Manual files, configs, etc.) ---
ASSETS_DIR = ROOT_DIR / "assets"
MANUAL_NAME_MAPPING_PATH = ASSETS_DIR / "manual_name_mapping.csv"


# --- Bronze Layer (Inputs for Pre-Processor) ---
SCRAPER_OUTPUT_DIR = BRONZE_DATA_DIR / "scraper"

# --- Silver Layer (Outputs from Pre-Processor) ---
PRE_PROCESSOR_OUTPUT_DIR = SILVER_DATA_DIR
STANDINGS_COMPLETE_PATH = PRE_PROCESSOR_OUTPUT_DIR / "standings_complete.csv"
STANDINGS_VALID_PATH = PRE_PROCESSOR_OUTPUT_DIR / "standings_valid.csv"
GAMES_COMPLETE_PATH = PRE_PROCESSOR_OUTPUT_DIR / "games_complete.csv"
GAMES_VALID_PATH = PRE_PROCESSOR_OUTPUT_DIR / "games_valid.csv"
SEASON_SUMMARY_PATH = PRE_PROCESSOR_OUTPUT_DIR / "season_summary.csv"

# --- Gold Layer (Outputs from Name Mapping & Analysis) ---
ANALYSIS_OUTPUT_DIR = GOLD_DATA_DIR / "analysis"
NAME_MAPPINGS_DIR = GOLD_DATA_DIR / "name_mappings"


NAME_MAPPINGS_INDIVIDUAL_DIR = NAME_MAPPINGS_DIR
NAME_MAPPINGS_COMBINED_PATH = NAME_MAPPINGS_DIR / "name_mappings_combined.csv"

# Create directories to ensure they exist when written to
ASSETS_DIR.mkdir(parents=True, exist_ok=True)
PRE_PROCESSOR_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
ANALYSIS_OUTPUT_DIR.mkdir(
    parents=True, exist_ok=True
)  # <--- GARANTIR QUE O DIRETÓRIO DE ANÁLISE EXISTA
NAME_MAPPINGS_INDIVIDUAL_DIR.mkdir(parents=True, exist_ok=True)
