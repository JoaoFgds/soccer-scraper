"""HTML parser for Transfermarkt club market-value tables."""

import logging
import re
from decimal import Decimal
from pathlib import Path

import pandas as pd
from bs4 import BeautifulSoup


LOGGER = logging.getLogger(__name__)


class TransfermarktParser:
    """Parse a competition's club market-value table."""

    @staticmethod
    def parse_money(money_str: str) -> Decimal:
        """Convert a localized Transfermarkt monetary string to euros."""

        if not money_str or money_str.strip() == "-":
            return Decimal("0")

        numbers = re.findall(r"[\d\.]+", money_str)
        if not numbers:
            return Decimal("0")

        value = Decimal(numbers[0])
        words = money_str.lower().replace(".", " ").split()

        if "bi" in words or "bilhões" in words:
            value *= Decimal("1000000000")
        elif "mi" in words:
            value *= Decimal("1000000")
        elif "mil" in words:
            value *= Decimal("1000")

        return value

    @staticmethod
    def parse_float(num_str: str) -> float:
        if not num_str or num_str.strip() in {"-", ""}:
            return 0.0
        try:
            return float(num_str.replace(",", "."))
        except ValueError:
            return 0.0

    @staticmethod
    def parse_int(num_str: str) -> int:
        if not num_str or num_str.strip() in {"-", ""}:
            return 0
        try:
            return int(float(num_str.replace(",", ".")))
        except ValueError:
            return 0

    def parse_league_table(
        self,
        html: str,
        league_slug: str,
        season: int,
        expected_saison_id: int | None = None,
    ) -> pd.DataFrame:
        """Parse the primary club table and validate the rendered season."""

        soup = BeautifulSoup(html, "lxml")

        if expected_saison_id is not None:
            saison_select = soup.find("select", {"name": "saison_id"})
            if saison_select:
                selected_option = saison_select.find("option", selected=True)
                if selected_option and selected_option.get("value"):
                    rendered_saison_id = selected_option.get("value")
                    if str(rendered_saison_id) != str(expected_saison_id):
                        raise ValueError(
                            "Season mismatch: requested "
                            f"{expected_saison_id}, rendered {rendered_saison_id}"
                        )
            else:
                LOGGER.warning("Could not find the season selector in the HTML")

        table = soup.find("table", class_="items")
        if not table:
            raise ValueError("The Transfermarkt 'items' table was not found")

        tbody = table.find("tbody")
        if not tbody:
            raise ValueError("The Transfermarkt table body was not found")

        data = []
        for row in tbody.find_all("tr"):
            cells = row.find_all("td")
            if not cells:
                continue

            club_index = next(
                (
                    index
                    for index, cell in enumerate(cells)
                    if "hauptlink" in cell.get("class", [])
                ),
                -1,
            )
            if club_index == -1 or len(cells) <= club_index + 5:
                LOGGER.debug("Skipping a row whose structure is not recognized")
                continue

            data.append(
                {
                    "league_name": league_slug,
                    "season_year": season,
                    "club_name": cells[club_index].get_text(strip=True),
                    "squad_size": self.parse_int(
                        cells[club_index + 1].get_text(strip=True)
                    ),
                    "average_age": self.parse_float(
                        cells[club_index + 2].get_text(strip=True)
                    ),
                    "foreigners_number": self.parse_int(
                        cells[club_index + 3].get_text(strip=True)
                    ),
                    "average_market_value_euros": (
                        f"{self.parse_money(cells[club_index + 4].get_text(strip=True)):.2f}"
                    ),
                    "total_market_value_euros": (
                        f"{self.parse_money(cells[club_index + 5].get_text(strip=True)):.2f}"
                    ),
                }
            )

        frame = pd.DataFrame(data)
        LOGGER.info("Parsed %s club rows", len(frame))
        return frame

    def save_to_csv(
        self,
        frame: pd.DataFrame,
        league_name: str,
        season: int,
        output_dir: Path,
    ) -> Path:
        output_dir.mkdir(parents=True, exist_ok=True)
        output_path = output_dir / f"{league_name.replace('-', '')}_{season}.csv"
        frame.to_csv(output_path, index=False, encoding="utf-8")
        LOGGER.info("Data saved to %s", output_path)
        return output_path
