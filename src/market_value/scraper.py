"""HTTP client for Transfermarkt club market-value pages."""

import logging
import random
import time

import requests
from requests.exceptions import RequestException

from .config import HEADERS


LOGGER = logging.getLogger(__name__)


class TransfermarktScraper:
    """Fetch Transfermarkt pages with politeness delays and retries."""

    def __init__(self, max_retries=3, min_delay=2.0, max_delay=5.0):
        self.max_retries = max_retries
        self.min_delay = min_delay
        self.max_delay = max_delay
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    def _sleep(self):
        delay = random.uniform(self.min_delay, self.max_delay)
        LOGGER.debug("Waiting %.2f seconds", delay)
        time.sleep(delay)

    def fetch_page(self, url: str) -> str:
        """Fetch one page, retrying request failures with exponential backoff."""

        retries = 0
        backoff_factor = 2

        while retries <= self.max_retries:
            self._sleep()
            LOGGER.info(
                "Fetching URL %s (attempt %s/%s)",
                url,
                retries + 1,
                self.max_retries + 1,
            )
            try:
                response = self.session.get(url, timeout=15)
                response.raise_for_status()
                return response.text
            except RequestException as exc:
                LOGGER.warning("Request failed: %s", exc)
                retries += 1
                if retries <= self.max_retries:
                    sleep_time = random.uniform(
                        self.min_delay, self.max_delay
                    ) * (backoff_factor**retries)
                    LOGGER.info("Retrying in %.2f seconds", sleep_time)
                    time.sleep(sleep_time)
                else:
                    LOGGER.error("Maximum attempts reached for %s", url)
                    raise

        raise RuntimeError("unreachable")
