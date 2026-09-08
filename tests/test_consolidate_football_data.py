"""Tests for the team-season consolidation contributed by PedroTask."""

import tempfile
import unittest
from pathlib import Path

import pandas as pd

from src.consolidate_football_data import (
    DataValidationError,
    OUTPUT_COLUMNS,
    build_consolidated_dataset,
)


class ConsolidatedDatasetTest(unittest.TestCase):
    def setUp(self):
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary_directory.name)
        self.reference = self.root / "reference.csv"
        self.occupancy = self.root / "occupancy.csv"
        self.market = self.root / "market.csv"
        self.ranking = self.root / "ranking.csv"

        pd.DataFrame(
            [
                {
                    "league_name": "league",
                    "season_year": 2023,
                    "team_canonical": "club",
                    "position_old": 4,
                    "position": 6,
                    "total_market_value_euros": 100.0,
                },
                {
                    "league_name": "league",
                    "season_year": 2024,
                    "team_canonical": "club",
                    "position_old": 2,
                    "position": 3,
                    "total_market_value_euros": 120.0,
                },
            ]
        ).to_csv(self.reference, index=False)

        pd.DataFrame(
            [
                {
                    "league_name": "league",
                    "season_year": year,
                    "team_canonical": "club",
                    "mean_attendance": 10000.0 + year,
                    "max_attendance": 20000,
                    "average_occupancy": 0.6,
                }
                for year in (2023, 2024)
            ]
        ).to_csv(self.occupancy, index=False)

        for path, coefficient in ((self.market, 0.2), (self.ranking, -0.3)):
            pd.DataFrame(
                [
                    {
                        "league_name": "league",
                        "season_year": year,
                        "team_canonical": "club",
                        "G": coefficient,
                        "P_value": 0.05,
                    }
                    for year in (2023, 2024)
                ]
            ).to_csv(path, index=False)

    def tearDown(self):
        self.temporary_directory.cleanup()

    def build(self):
        return build_consolidated_dataset(
            self.reference,
            self.occupancy,
            self.market,
            self.ranking,
        )

    def test_builds_historical_output_contract(self):
        result = self.build()

        self.assertEqual(result.columns.tolist(), OUTPUT_COLUMNS)
        self.assertEqual(len(result), 2)
        self.assertEqual(result["ranking_last_season"].tolist(), [-1, 4])
        self.assertEqual(result["ranking_market_last_season"].tolist(), [-1, 6])
        self.assertEqual(result["avg_attendance"].tolist(), [0.6, 0.6])

    def test_duplicate_keys_are_rejected(self):
        reference = pd.read_csv(self.reference)
        pd.concat([reference, reference.iloc[[0]]], ignore_index=True).to_csv(
            self.reference, index=False
        )

        with self.assertRaisesRegex(DataValidationError, "duplicate join keys"):
            self.build()


if __name__ == "__main__":
    unittest.main()
