"""Command-line entry point for club market-value scraping."""

import argparse
import logging
import sys

import pandas as pd

from .config import DATA_DIR, LEAGUES, generate_url, get_saison_id
from .parser import TransfermarktParser
from .scraper import TransfermarktScraper


LOGGER = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Extract Transfermarkt club market values"
    )
    parser.add_argument(
        "--league",
        choices=sorted(LEAGUES),
        help="Extract only this configured league",
    )
    parser.add_argument(
        "--season",
        type=int,
        help="Extract only this season (requires --league)",
    )
    args = parser.parse_args()
    if args.season is not None and args.league is None:
        parser.error("--season requires --league")
    return args


def main() -> int:
    args = parse_args()
    scraper = TransfermarktScraper()
    transfermarkt_parser = TransfermarktParser()

    if args.league:
        seasons = [args.season] if args.season else LEAGUES[args.league]["seasons"]
        leagues_to_scrape = [(args.league, seasons)]
    else:
        leagues_to_scrape = [
            (league_key, league_info["seasons"])
            for league_key, league_info in LEAGUES.items()
        ]

    statuses = []
    for league_key, seasons in leagues_to_scrape:
        league_info = LEAGUES[league_key]
        league_slug = league_info["slug"]
        output_league_name = league_slug.replace("-", "")

        for season in seasons:
            LOGGER.info("Extracting %s season %s", league_key, season)
            url = ""
            try:
                saison_id = get_saison_id(season, league_info.get("offset", 0))
                url = generate_url(league_key, season)
                html = scraper.fetch_page(url)
                frame = transfermarkt_parser.parse_league_table(
                    html,
                    output_league_name,
                    season,
                    expected_saison_id=saison_id,
                )
                status = "Failed"
                if not frame.empty:
                    transfermarkt_parser.save_to_csv(
                        frame, league_slug, season, DATA_DIR
                    )
                    status = "Success"
                else:
                    LOGGER.warning("No rows extracted for %s %s", league_key, season)
            except Exception as exc:
                LOGGER.error("Failed to extract %s %s: %s", league_key, season, exc)
                status = "Failed"

            statuses.append(
                {
                    "league_name": output_league_name,
                    "season_year": season,
                    "url": url or "URL_ERROR",
                    "status": status,
                }
            )

    if statuses:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        status_path = DATA_DIR / "scraping_status.csv"
        pd.DataFrame(statuses).to_csv(status_path, index=False, encoding="utf-8")
        LOGGER.info("Status report saved to %s", status_path)

    return 0


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )
    raise SystemExit(main())
