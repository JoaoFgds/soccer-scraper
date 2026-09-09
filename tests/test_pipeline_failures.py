"""Tests that pipeline entry points fail clearly on unusable inputs."""

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from src.analysis import main as analysis_main
from src.processor import main as processor_main


class ProcessorFailureTest(unittest.TestCase):
    def test_missing_bronze_data_raises_an_error(self):
        with tempfile.TemporaryDirectory() as directory:
            with (
                patch.object(
                    processor_main.paths,
                    "SCRAPER_OUTPUT_DIR",
                    Path(directory),
                ),
                patch.object(processor_main, "setup_logging"),
            ):
                with self.assertRaisesRegex(
                    FileNotFoundError,
                    "No Bronze standings files",
                ):
                    processor_main.pre_processor_pipeline()


class AnalysisFailureTest(unittest.TestCase):
    def test_missing_analysis_inputs_raise_an_error(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with (
                patch.object(
                    analysis_main.paths,
                    "GAMES_VALID_PATH",
                    root / "games.csv",
                ),
                patch.object(
                    analysis_main.paths,
                    "STANDINGS_VALID_MARKET_RANKING_PATH",
                    root / "standings.csv",
                ),
                patch.object(analysis_main, "setup_logging"),
            ):
                with self.assertRaisesRegex(
                    FileNotFoundError,
                    "Missing required analysis inputs",
                ):
                    analysis_main.analysis_pipeline()

    def test_analysis_task_error_is_propagated(self):
        with (
            patch.object(analysis_main, "_require_analysis_inputs"),
            patch.object(analysis_main, "setup_logging"),
            patch.object(
                analysis_main.spearman_coeff_calculate,
                "calculate_strength_schedule_balance",
                side_effect=RuntimeError("calculation failed"),
            ),
        ):
            with self.assertRaisesRegex(RuntimeError, "calculation failed"):
                analysis_main.analysis_pipeline()


if __name__ == "__main__":
    unittest.main()
