import time
import random
import logging
import requests
from bs4 import BeautifulSoup

from . import config
from .exceptions import ScrapingError

logger = logging.getLogger(__name__)


def fetch_soup(url: str) -> BeautifulSoup:
    """
    Performs a robust HTTP GET request and returns a parsed BeautifulSoup object.

    This function is the central entry point for all web requests. It is
    designed to be resilient and respectful to the target server by
    incorporating a random politeness delay and an automatic retry mechanism
    with exponential backoff for transient errors.

    Args:
        url (str): The URL of the page to fetch.

    Returns:
        BeautifulSoup: A BeautifulSoup object containing the parsed HTML of
            the successfully fetched page.

    Raises:
        ScrapingError: If a non-retriable HTTP error occurs (e.g., 404, 403),
            or if all retry attempts for a transient error (e.g., 429, 503,
            network issues) are exhausted.
    """
    delay = random.uniform(*config.REQUEST_DELAY_RANGE_SECONDS)
    logger.debug(f"Waiting for {delay:.2f} seconds before request to {url}")
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
                logger.warning(
                    f"HTTP {status_code} for {url}. Attempt {attempt + 1}/{config.MAX_RETRIES}. "
                    f"Retrying in {wait_time} seconds."
                )
                time.sleep(wait_time)
            else:
                logger.error(f"Unrecoverable HTTP {status_code} for {url}: {e}")
                raise ScrapingError(f"Unrecoverable HTTP error for {url}") from e

        except requests.exceptions.RequestException as e:
            last_exception = e
            wait_time = config.BACKOFF_FACTOR**attempt
            logger.warning(
                f"Network error for {url}: {e}. Attempt {attempt + 1}/{config.MAX_RETRIES}. "
                f"Retrying in {wait_time} seconds."
            )
            time.sleep(wait_time)

    logger.critical(f"Failed to fetch URL after {config.MAX_RETRIES} attempts: {url}")
    raise ScrapingError(
        f"Failed to fetch {url} after {config.MAX_RETRIES} attempts"
    ) from last_exception
