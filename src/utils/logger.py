import sys
import logging
from pathlib import Path
from src.utils import paths
from logging.handlers import RotatingFileHandler

LOG_LEVEL = "INFO"
LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
LOG_FORMAT = (
    "[%(asctime)s] [%(levelname)s] [%(name)s:%(funcName)s:%(lineno)d] - %(message)s"
)


class RelativePathFormatter(logging.Formatter):
    """Custom log formatter to render pathlib.Path objects as relative paths.

    This formatter intercepts log records and checks their arguments. If any
    argument is a `pathlib.Path` object, it attempts to convert it into a

    path relative to a specified root directory. This is useful for keeping
    log messages clean and avoiding absolute file paths.
    """

    def __init__(self, *args, root_dir: Path, **kwargs):
        """Initializes the RelativePathFormatter.

        Args:
            *args: Variable length argument list passed to the parent Formatter.
            root_dir (Path): The project's root directory to which paths will be
                made relative.
            **kwargs: Arbitrary keyword arguments passed to the parent Formatter.
        """
        super().__init__(*args, **kwargs)
        self.root_dir = root_dir

    def format(self, record: logging.LogRecord) -> str:
        """Formats the log record, converting any Path objects in the arguments.

        This method scans the log record's arguments. If an argument is a
        `pathlib.Path` object, it is converted to a string representing a path
        relative to the `root_dir` provided during initialization. If a path
        cannot be made relative (e.g., it is on a different drive), it is
        left unchanged.

        Args:
            record (logging.LogRecord): The log record to be formatted.

        Returns:
            str: The formatted log message string.
        """
        if isinstance(record.args, tuple) and record.args:
            new_args = []
            for arg in record.args:
                if isinstance(arg, Path):
                    try:
                        new_args.append(arg.relative_to(self.root_dir))
                    except ValueError:
                        new_args.append(arg)
                else:
                    new_args.append(arg)
            record.args = tuple(new_args)
        return super().format(record)


def setup_logging():
    """Configures the root logger for the entire application.

    Sets up a centralized logging configuration with a specific format and level.
    This function is idempotent; it checks if handlers are already configured
    and will not add duplicates if called multiple times.

    Logging is directed to two destinations:
    1.  Console (stdout) for immediate, real-time monitoring.
    2.  A rotating file, which archives logs when they reach a certain size,
        located at the path specified by `paths.LOG_FILE`.
    """
    root_logger = logging.getLogger()
    if root_logger.handlers:
        return

    root_logger.setLevel(LOG_LEVEL)
    paths.LOG_DIR.mkdir(parents=True, exist_ok=True)

    formatter = RelativePathFormatter(
        fmt=LOG_FORMAT, datefmt=LOG_DATE_FORMAT, root_dir=paths.ROOT_DIR
    )

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)

    file_handler = RotatingFileHandler(
        paths.LOG_FILE, maxBytes=10 * 1024 * 1024, backupCount=5
    )
    file_handler.setFormatter(formatter)
    root_logger.addHandler(file_handler)
