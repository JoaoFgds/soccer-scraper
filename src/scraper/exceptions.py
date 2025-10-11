class ScrapingError(Exception):
    """
    Custom exception for predictable errors during the scraping process.

    This exception should be raised when a scraping operation fails for a
    specific, anticipated reason, such as an HTTP request failure after all
    retries or a required HTML element not being found. This allows for
    granular error handling in the application's main control flow.
    """

    pass
