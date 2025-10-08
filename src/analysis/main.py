# File: src/analysis/main.py
"""
Orchestrates the data analysis workflow.
"""
import logging
from src.analysis import spearman_coeff
from src.analysis import spearman_coeff_summary
from src.analysis import mann_whitney_seasons
from src.analysis import mann_whitney_attendance
from src.utils.logger_setup import setup_logging

logger = logging.getLogger(__name__)


def analysis_pipeline():
    """
    Main entry point for the analysis workflow.
    """
    logger.info("--- Starting Data Analysis Workflow ---")

    # Passo 1: Calcular o coeficiente de Spearman
    spearman_coeff.calculate_strength_schedule_balance()

    # Passo 2: Gerar a tabela de resumo
    spearman_coeff_summary.create_g_type_summary()

    # Passo 3: Executar a análise de significância estatística
    mann_whitney_seasons.run_statistical_analysis()

    # Passo 4: Executar a análise de ocupação de estádios
    mann_whitney_attendance.run_occupancy_analysis()

    logger.info("--- Data Analysis Workflow Complete ---")
