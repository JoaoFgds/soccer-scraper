"""Tests for selecting the documented schedule-strength proxies."""

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from src.analysis import spearman_coeff_calculate


class StrengthProxySelectionTest(unittest.TestCase):
    def test_enriched_standings_use_final_and_market_rank_columns(self):
        games = pd.DataFrame()
        standings = pd.DataFrame(
            [
                {
                    "league_name": "league",
                    "season_year": 2024,
                    "position_old": 2,
                    "position": 1,
                    "total_market_value_euros": 100.0,
                }
            ]
        )
        result = pd.DataFrame([{"G": 0.1}])

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with (
                patch.object(
                    spearman_coeff_calculate.pd,
                    "read_csv",
                    side_effect=[games, standings.copy(), standings.copy()],
                ),
                patch.object(
                    spearman_coeff_calculate,
                    "_calculate_schedule_balance",
                    side_effect=[result.copy(), result.copy()],
                ) as calculate,
                patch.object(
                    spearman_coeff_calculate.paths,
                    "SPEARMAN_BALANCE_RANKING_PATH",
                    root / "ranking.csv",
                ),
                patch.object(
                    spearman_coeff_calculate.paths,
                    "SPEARMAN_BALANCE_PATH",
                    root / "combined.csv",
                ),
                patch.object(
                    spearman_coeff_calculate.paths,
                    "SPEARMAN_BALANCE_MARKET_PATH",
                    root / "market.csv",
                ),
                patch.object(
                    spearman_coeff_calculate.paths,
                    "SCHEDULES_DATA_COMBINED_PATH",
                    root / "schedules.json",
                ),
            ):
                spearman_coeff_calculate.calculate_strength_schedule_balance()

        ranking_call, market_call = calculate.call_args_list
        self.assertEqual(ranking_call.kwargs["strength_column"], "position_old")
        self.assertEqual(
            ranking_call.kwargs["final_position_column"], "position_old"
        )
        self.assertEqual(market_call.kwargs["strength_column"], "market_value_rank")
        self.assertEqual(market_call.kwargs["final_position_column"], "position")


if __name__ == "__main__":
    unittest.main()
