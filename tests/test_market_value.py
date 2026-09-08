"""Focused tests for the imported market-value pipeline."""

import tempfile
import unittest
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from src.market_value.config import generate_url
from src.market_value.parser import TransfermarktParser
from src.market_value import process_silver


class MarketValueParserTest(unittest.TestCase):
    def test_money_units_match_historical_parser(self):
        parser = TransfermarktParser()
        self.assertEqual(parser.parse_money("€ 1.36 bi."), Decimal("1360000000.00"))
        self.assertEqual(parser.parse_money("€ 214.15 mi."), Decimal("214150000.00"))
        self.assertEqual(parser.parse_money("€ 593 mil"), Decimal("593000"))
        self.assertEqual(parser.parse_money("-"), Decimal("0"))

    def test_table_and_rendered_season_are_parsed(self):
        html = """
        <select name="saison_id"><option value="2024" selected>24/25</option></select>
        <table class="items"><tbody><tr>
          <td class="hauptlink">Example FC</td><td>25</td><td>24,7</td><td>10</td>
          <td>€ 2.50 mi.</td><td>€ 62.50 mi.</td>
        </tr></tbody></table>
        """
        result = TransfermarktParser().parse_league_table(
            html, "exampleleague", 2024, expected_saison_id=2024
        )
        self.assertEqual(result.to_dict(orient="records")[0], {
            "league_name": "exampleleague",
            "season_year": 2024,
            "club_name": "Example FC",
            "squad_size": 25,
            "average_age": 24.7,
            "foreigners_number": 10,
            "average_market_value_euros": "2500000.00",
            "total_market_value_euros": "62500000.00",
        })

    def test_calendar_year_league_uses_offset(self):
        url = generate_url("campeonatobrasileiroseriea", 2024)
        self.assertIn("saison_id=2023", url)


class MarketValueSilverTest(unittest.TestCase):
    def test_incomplete_flag_applies_to_entire_league_season_file(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            bronze = root / "bronze"
            bronze.mkdir()
            output = root / "silver.csv"
            pd.DataFrame(
                [
                    {
                        "league_name": "league",
                        "season_year": 2024,
                        "club_name": "A",
                        "total_market_value_euros": 10,
                    },
                    {
                        "league_name": "league",
                        "season_year": 2024,
                        "club_name": "B",
                        "total_market_value_euros": 0,
                    },
                ]
            ).to_csv(bronze / "league_2024.csv", index=False)

            with (
                patch.object(process_silver, "DATA_DIR", bronze),
                patch.object(process_silver, "SILVER_PATH", output),
            ):
                result = process_silver.process_bronze_to_silver()

            self.assertIsNotNone(result)
            self.assertEqual(result["is_complete"].tolist(), [False, False])
            self.assertTrue(output.is_file())


if __name__ == "__main__":
    unittest.main()
