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

from src.utils import paths
from src.utils.logger import setup_logging

logger = logging.getLogger(__name__)


def _require_analysis_inputs() -> None:
    """Fail before analysis when the two required Silver inputs are absent."""

    required_inputs = {
        paths.GAMES_VALID_PATH,
        paths.STANDINGS_VALID_MARKET_RANKING_PATH,
    }
    missing_inputs = sorted(
        (path for path in required_inputs if not path.is_file()),
        key=str,
    )
    if missing_inputs:
        missing_list = ", ".join(str(path) for path in missing_inputs)
        raise FileNotFoundError(
            "Missing required analysis inputs: "
            f"{missing_list}. Restore the reproducibility release data and run "
            "the processor before analysis."
        )


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
        _require_analysis_inputs()

        print("\n")
        logger.info(
            "Task 1: Calculating Spearman's coefficient for strength of schedule."
        )
        balance = spearman_coeff_calculate.calculate_strength_schedule_balance()
        if balance.empty:
            raise RuntimeError("Schedule-balance calculation produced no results.")

        print("\n")
        logger.info("Task 2: Creating summary table for G-type analysis.")
        summary = spearman_coeff_summary.create_g_type_summary()
        if summary is None or summary.empty:
            raise RuntimeError("Schedule-balance summary produced no results.")

        print("\n")
        logger.info("Task 3: Generating paper Tables 2 and 3.")
        if not ssb_proportions.generate_ssb_proportion_tables():
            raise RuntimeError("No schedule-proportion tables were generated.")

        print("\n")
        logger.info("Task 4: Generating Cliff's delta analysis and Figure 1.")
        if not cliffs_delta.generate_cliffs_delta_analysis():
            raise RuntimeError("No Cliff's delta outputs were generated.")

        print("\n")
        logger.info("Task 5: Generating significance analysis and Figure 2.")
        if not significancy_analysis.generate_significancy_analysis():
            raise RuntimeError("No significance-analysis outputs were generated.")

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
            "The analysis pipeline failed: %s",
            e,
            exc_info=True,
        )
        raise
    else:
        logger.info("--- Data Analysis Workflow Complete ---")
