# src/utils/logging_config.py

import sys
import logging
from src.utils import paths
from logging.handlers import RotatingFileHandler


LOG_LEVEL = "INFO"
LOG_FILENAME = "app.log"
LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
LOG_PATH = paths.LOG_DIR / LOG_FILENAME

LOG_FORMAT = (
    "[%(asctime)s] [%(levelname)s] [%(filename)s:%(funcName)s:%(lineno)d] - %(message)s"
)

from pathlib import Path


class RelativePathFormatter(logging.Formatter):
    """
    A custom log formatter that converts absolute paths in log messages
    to paths relative to the project's root directory.
    """

    def __init__(self, *args, root_dir: Path, **kwargs):
        super().__init__(*args, **kwargs)
        self.root_dir = root_dir

    def format(self, record: logging.LogRecord) -> str:
        # Check if there are arguments to format
        if record.args:
            # Create a new list of arguments, converting Paths to relative paths
            new_args = []
            for arg in record.args:
                if isinstance(arg, Path):
                    try:
                        # Attempt to make the path relative
                        new_args.append(arg.relative_to(self.root_dir))
                    except ValueError:
                        # If the path is not within the project, keep it absolute
                        new_args.append(arg)
                else:
                    new_args.append(arg)

            # Replace the original record args with the modified ones
            record.args = tuple(new_args)

        # Call the original format method to produce the final log message
        return super().format(record)


def setup_logging():
    """
    Set up the root logger for the entire application.

    This function provides a centralized logging configuration. It is idempotent,
    meaning it can be safely called multiple times without adding duplicate
    handlers, which would result in repeated log messages.

    It configures the logger to output to:
    1. Console (stdout) for real-time monitoring.
    2. A rotating file (in 'data/logs/app.log') for persistent records.
    """

    root_logger = logging.getLogger()

    if root_logger.handlers:
        return

    root_logger.setLevel(LOG_LEVEL)

    # 1. Console Handler (for stdout)
    console_handler = logging.StreamHandler(sys.stdout)

    # 2. Rotating File Handler (for persistent logs)
    # Ensure the log directory from paths.py exists before creating the handler.
    paths.LOG_DIR.mkdir(parents=True, exist_ok=True)

    # This handler rotates logs, preventing them from growing indefinitely.
    # maxBytes=10MB, keep 5 backup files.
    file_handler = RotatingFileHandler(
        LOG_PATH, maxBytes=10 * 1024 * 1024, backupCount=5
    )

    # --- Create a Formatter and apply it to the handlers ---

    formatter = RelativePathFormatter(
        fmt=LOG_FORMAT, datefmt=LOG_DATE_FORMAT, root_dir=paths.ROOT_DIR
    )

    console_handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)

    # --- Add the configured handlers to the root logger ---
    root_logger.addHandler(console_handler)
    root_logger.addHandler(file_handler)
