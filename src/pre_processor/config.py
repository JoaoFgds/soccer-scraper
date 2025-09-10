# File: src/pre_processor/config.py
"""
Configuration and logging setup for the pre-processor module.
"""
import logging
from pathlib import Path

# Base directories
BASE_DIR = Path(__file__).parent.parent.parent
DATA_DIR = BASE_DIR / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
PROCESSED_DATA_DIR = DATA_DIR / "processed"

# Ensure processed data directory exists
PROCESSED_DATA_DIR.mkdir(parents=True, exist_ok=True)

# Output file paths
FINAL_STANDINGS_SUMMARY_CSV = PROCESSED_DATA_DIR / "final_standings_summary.csv"
FINAL_STANDINGS_VALID_CSV = PROCESSED_DATA_DIR / "final_standings_valid.csv"
FINAL_STANDINGS_COMPLETE_CSV = PROCESSED_DATA_DIR / "final_standings_complete.csv"
FINAL_STANDINGS_COMPLETE_VALID_CSV = (
    PROCESSED_DATA_DIR / "final_standings_complete_valid.csv"
)


def configure_logging():
    """
    Configures the root logger for the application.
    """
    log_format = "[%(asctime)s] - %(levelname)s - %(name)s - " "%(message)s"
    logging.basicConfig(
        level=logging.INFO,
        format=log_format,
        handlers=[
            logging.StreamHandler(),
        ],
    )
