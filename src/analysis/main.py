"""
Orchestrates the data analysis workflow.
"""

import logging
from src.utils import logger_setup
from src.analysis import spearman_coeff

logger_setup.setup_logging()
logger = logging.getLogger(__name__)


def analysis_pipeline():
    """
    Main entry point for the analysis workflow.
    """
    logger.info("--- Starting Data Analysis Workflow ---")

    spearman_coeff.calculate_strength_schedule_balance()

    logger.info("--- Data Analysis Workflow Complete ---")


analysis_pipeline()
