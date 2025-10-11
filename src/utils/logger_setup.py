# src/utils/logger_setup.py

import sys
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
from src.utils import paths

LOG_LEVEL = "INFO"
LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
LOG_FORMAT = (
    "[%(asctime)s] [%(levelname)s] [%(name)s:%(funcName)s:%(lineno)d] - %(message)s"
)


class RelativePathFormatter(logging.Formatter):
    """
    Custom log formatter that converts pathlib.Path objects in log arguments
    to relative paths based on the project root.
    """

    def __init__(self, *args, root_dir: Path, **kwargs):
        """Initializes the formatter."""
        super().__init__(*args, **kwargs)
        self.root_dir = root_dir

    def format(self, record: logging.LogRecord) -> str:
        """
        Formats the LogRecord.

        If the record's arguments contain pathlib.Path objects, they are
        converted to strings representing paths relative to the project root.
        This prevents absolute paths from being logged.

        Args:
            record (logging.LogRecord): The log record to format.

        Returns:
            str: The formatted log message.
        """
        if isinstance(record.args, tuple) and record.args:
            new_args = []
            for arg in record.args:
                if isinstance(arg, Path):
                    try:
                        # Attempt to make the path relative to the project root
                        new_args.append(arg.relative_to(self.root_dir))
                    except ValueError:
                        # Path is not within the project, keep it as is
                        new_args.append(arg)
                else:
                    new_args.append(arg)
            record.args = tuple(new_args)
        return super().format(record)


def setup_logging():
    """
    Sets up the root logger for the application.

    This function provides a centralized logging configuration. It is idempotent,
    meaning it can be safely called multiple times without adding duplicate handlers.

    It configures the logger to output to:
    1.  Console (stdout) for real-time monitoring.
    2.  A rotating file in the location specified by `paths.LOG_FILE`.
    """
    root_logger = logging.getLogger()
    if root_logger.handlers:
        # Logger is already configured, do nothing.
        return

    root_logger.setLevel(LOG_LEVEL)
    paths.LOG_DIR.mkdir(parents=True, exist_ok=True)

    # --- Create and set the custom formatter ---
    formatter = RelativePathFormatter(
        fmt=LOG_FORMAT, datefmt=LOG_DATE_FORMAT, root_dir=paths.ROOT_DIR
    )

    # --- Console Handler ---
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)

    # --- Rotating File Handler ---
    file_handler = RotatingFileHandler(
        paths.LOG_FILE, maxBytes=10 * 1024 * 1024, backupCount=5
    )
    file_handler.setFormatter(formatter)
    root_logger.addHandler(file_handler)
