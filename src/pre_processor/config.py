"""
Configuration and logging setup for the pre-processor module.
"""

import logging
from pathlib import Path

BASE_DIR = Path(__file__).parent.parent.parent
DATA_DIR = BASE_DIR / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
PROCESSED_DATA_DIR = DATA_DIR / "processed"

PROCESSED_DATA_DIR.mkdir(parents=True, exist_ok=True)

STANDINGS_CONCAT_CSV = PROCESSED_DATA_DIR / "final_standings_concat.csv"
STANDINGS_JSON = PROCESSED_DATA_DIR / "final_standings_concat.json"


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
