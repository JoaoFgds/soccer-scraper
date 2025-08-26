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
STANDINGS_CONCAT_CSV = PROCESSED_DATA_DIR / "final_standings_processed.csv"
SCHEDULES_CONCAT_CSV = PROCESSED_DATA_DIR / "team_schedules_processed.csv"
ATTENDANCE_CONCAT_CSV = PROCESSED_DATA_DIR / "attendance_summary_processed.csv"
FINAL_JSON = PROCESSED_DATA_DIR / "standings_summary.json"


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
