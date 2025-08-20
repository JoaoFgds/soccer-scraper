class ScrapingError(Exception):
    """
    Custom exception for predictable errors during the scraping process.

    This exception is intended to be raised when a scraping operation fails
    for a specific, anticipated reason, such as an HTTP request failure after
    all retries or a required HTML element not being found on the page.

    Using a specific exception type allows for more granular error handling
    in the application's main control flow, distinguishing scraping-related
    failures from other unexpected programming errors.

    Example:
        try:
            standings_df = fetch_league_standings(url)
        except ScrapingError as e:
            logging.error(f"Could not scrape standings: {e}")
            # The application can then handle the failure gracefully.
    """

    pass
