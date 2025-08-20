import time
import random
import logging
import requests
from bs4 import BeautifulSoup

from . import config
from .exceptions import ScrapingError


def fetch_soup(url: str) -> BeautifulSoup:
    """
    Performs a robust HTTP GET request and returns a parsed BeautifulSoup object.

    This is the central function for all web requests in the scraper. It is
    designed to be resilient and respectful to the target server by incorporating
    several key features:

    1.  **Politeness Delay**: A randomized delay (configured by module-level
        constants) is enforced before each request to mimic human-like
        browsing patterns and avoid overwhelming the server.

    2.  **Automatic Retries with Exponential Backoff**: The function will
        automatically retry requests if it encounters transient issues. The
        waiting time between retries increases exponentially to give the
        server time to recover.

    Retries are attempted for the following conditions:
    -   Rate limiting errors (HTTP 429).
    -   Server availability errors (HTTP 503).
    -   General network connection errors (e.g., DNS failure, connection refused).

    The function will fail immediately for non-transient HTTP errors such as
    'Not Found' (404) or 'Forbidden' (403), as retrying these is futile.

    Args:
        url (str): The URL of the website to fetch.

    Returns:
        BeautifulSoup: A BeautifulSoup object containing the parsed HTML of the
            successfully fetched page.

    Raises:
        ScrapingError: If a non-retriable HTTP error occurs, or if all retry
            attempts for a transient error are exhausted.
    """
    delay = random.uniform(*config.REQUEST_DELAY_RANGE_SECONDS)
    time.sleep(delay)
    last_exception = None

    for attempt in range(config.MAX_RETRIES):
        try:
            response = requests.get(url, headers=config.HEADERS, timeout=20)
            response.raise_for_status()
            return BeautifulSoup(response.text, "html.parser")
        except requests.exceptions.HTTPError as e:
            last_exception = e
            status_code = e.response.status_code
            if status_code in [429, 503]:
                wait_time = config.BACKOFF_FACTOR**attempt
                logging.warning(
                    f"Received status {status_code} for {url}. "
                    f"Attempt {attempt + 1}/{config.MAX_RETRIES}. "
                    f"Waiting {wait_time} seconds."
                )
                time.sleep(wait_time)
            else:
                logging.error(f"Unrecoverable HTTP {status_code} for {url}: {e}")
                raise ScrapingError(f"Unrecoverable HTTP error for {url}") from e
        except requests.exceptions.RequestException as e:
            last_exception = e
            wait_time = config.BACKOFF_FACTOR**attempt
            logging.warning(
                f"Network error for {url}: {e}. Attempt {attempt + 1}/{config.MAX_RETRIES}. "
                f"Waiting {wait_time} seconds."
            )
            time.sleep(wait_time)

    logging.error(f"Failed to fetch URL {url} after {config.MAX_RETRIES} attempts.")
    raise ScrapingError(
        f"Failed to fetch {url} after {config.MAX_RETRIES} attempts"
    ) from last_exception
