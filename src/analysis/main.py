import logging


from src.analysis import (
    cliffs_delta,
    significancy_analysis,
    spearman_coeff_summary,
    spearman_coeff_calculate,
    ssb_proportions,
    mann_whitney_seasons,
    mann_whitney_attendance,
    mann_whitney_sensitivity,
    mann_whitney_sensitivity_plot,
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
    3.  Generates Tables 2 and 3 for each magnitude and significance threshold.
    4.  Calculates and plots season-level Cliff's delta distributions.
    5.  Generates league-level significance results and plots.
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
        logger.info("Task 3: Generating paper Tables 2 and 3.")
        ssb_proportions.generate_ssb_proportion_tables()

        print("\n")
        logger.info("Task 4: Generating Cliff's delta analysis and Figure 1.")
        cliffs_delta.generate_cliffs_delta_analysis()

        print("\n")
        logger.info("Task 5: Generating significance analysis and Figure 2.")
        significancy_analysis.generate_significancy_analysis()

        # print("\n")
        # logger.info("Task 6: Creating plots for G-type analysis.")
        # spearman_coeff_plots.generate_all_visualizations()

        # print("\n")
        # logger.info("Task 7: Running Mann-Whitney U tests for season final ranks.")
        # mann_whitney_seasons.run_seasons_analysis()

        # print("\n")
        # logger.info("Task 8: Running Mann-Whitney U tests for stadium occupancy.")
        # mann_whitney_attendance.run_occupancy_analysis()

        # print("\n")
        # logger.info("Task 9: Running Mann-Whitney sensitivity analysis.")
        # mann_whitney_sensitivity.run_sensitivity_analysis()

        # print("\n")
        # logger.info("Task 10: Running Mann-Whitney sensitivity plots.")
        # mann_whitney_sensitivity_plot.run_plotting()

    except Exception as e:
        logger.critical(
            "An unhandled error occurred during the analysis pipeline: %s",
            e,
            exc_info=True,
        )
    else:
        logger.info("--- Data Analysis Workflow Complete ---")
