import re
import unicodedata


def sanitize_filename(text: str) -> str:
    """
    Cleans and sanitizes a string to be used as a single-word, safe filename.

    This function performs the following steps:
    1.  Normalizes Unicode characters to remove accents (e.g., 'ç' -> 'c').
    2.  Converts the string to lowercase.
    3.  Removes any character that is not a letter, number, whitespace, or hyphen.
    4.  Removes all remaining spaces and hyphens, concatenating the string into a single word.

    Args:
        text: The original string to be cleaned.

    Returns:
        A clean, single-word, and safe string for use in filenames.
    """
    text = str(text)
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("utf-8")
    text = text.lower()
    text = re.sub(r"[^\w\s-]", "", text)
    text = re.sub(r"[\s-]+", "", text)

    return text
