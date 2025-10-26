import logging


from src.analysis import (
    spearman_coeff_summary,
    spearman_coeff_calculate,
    mann_whitney_seasons,
    mann_whitney_attendance,
    spearman_coeff_plots,
)

from src.utils.logger import setup_logging

logger = logging.getLogger(__name__)


def analysis_pipeline():
    """Orchestrates the entire data analysis workflow.

    This function serves as the main entry point for the analysis phase of the
    project. It executes a sequence of data analysis tasks in a predefined
    order, transforming the validated data from the silver layer into final
    insights and artifacts in the gold layer.

    The pipeline performs the following key steps:
    1.  Calculates Spearman's G coefficient to measure the strength and
        balance of each team's schedule for every valid season.
    2.  Aggregates the G coefficient results into a high-level summary table.
    3.  Conducts Mann-Whitney U tests to determine if a team's schedule
        balance has a statistically significant effect on its final rank.
    4.  Runs a sensitivity analysis using Mann-Whitney U tests to explore the
        relationship between schedule balance and stadium occupancy.
    """

    setup_logging()
    logger.info("--- Starting Data Analysis Workflow ---")

    try:
        print("\n")
        logger.info(
            "Task 1: Calculating Spearman's coefficient for strength of schedule."
        )
        spearman_coeff_calculate.calculate_strength_schedule_balance()

        print("\n")
        logger.info("Task 2: Creating summary table for G-type analysis.")
        spearman_coeff_summary.create_g_type_summary()

        print("\n")
        logger.info("Task 3: Creating plots for G-type analysis.")
        spearman_coeff_plots.generate_all_visualizations()

        # print("\n")
        # logger.info("Task 3: Running Mann-Whitney U tests for season final ranks.")
        # mann_whitney_seasons.run_statistical_analysis()

        # print("\n")
        # logger.info("Task 4: Running Mann-Whitney U tests for stadium occupancy.")
        # mann_whitney_attendance.run_occupancy_analysis()

    except Exception as e:
        logger.critical(
            "An unhandled error occurred during the analysis pipeline: %s",
            e,
            exc_info=True,
        )
    else:
        logger.info("--- Data Analysis Workflow Complete ---")
