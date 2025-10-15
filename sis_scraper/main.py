import asyncio
import datetime as dt
import logging
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

import sis_scraper


class ColoredFormatter(logging.Formatter):
    """
    Simple wrapper class that adds colors to logging.

    Requires a format, and otherwise accepts any keyword arguments
    that are accepted by logging.Formatter().
    """

    def __init__(self, fmt: str, **kwargs):
        self._fmt = fmt
        self._kwargs = kwargs
        self._reset_color = "\x1b[0m"
        self._COLORS = {
            logging.DEBUG: "\x1b[38;20m",  # Gray
            logging.INFO: "\x1b[38;20m",  # Gray
            logging.WARNING: "\x1b[33;20m",  # Yellow
            logging.ERROR: "\x1b[31;20m",  # Red
            logging.CRITICAL: "\x1b[31;1m",  # Dark red
        }

    def format(self, record: logging.LogRecord) -> str:
        color = self._COLORS[record.levelno]
        formatter = logging.Formatter(
            **self._kwargs, fmt=f"{color}{self._fmt}{self._reset_color}"
        )
        return formatter.format(record)


def logging_init(logs_dir: Path | str, log_level: int = logging.INFO) -> None:
    """
    Initializes logging settings once on startup; these settings determine the behavior
    of all logging calls within this program.
    """
    if logs_dir is None:
        raise ValueError("logs_dir must be specified")
    if isinstance(logs_dir, str):
        logs_dir = Path(logs_dir)

    # Logging format config
    formatter_config = {
        "fmt": "[%(asctime)s %(levelname)s] %(message)s",
        "datefmt": "%H:%M:%S",
    }
    color_formatter = ColoredFormatter(**formatter_config)
    formatter = logging.Formatter(**formatter_config)

    # Root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)

    # Console logging
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)
    console_handler.setFormatter(color_formatter)
    root_logger.addHandler(console_handler)

    # File logging
    if not logs_dir.exists():
        logs_dir.mkdir()
        logging.info("No logs directory detected, creating one for you")
    for log in logs_dir.iterdir():
        create_time = dt.datetime.fromtimestamp(os.path.getctime(log))
        if create_time < dt.datetime.now() - dt.timedelta(days=5):
            log.unlink()
    curr_time = dt.datetime.now().strftime("%Y.%m.%d %H.%M.%S")
    logfile_path = logs_dir / f"{curr_time}.log"
    logfile_path.touch()
    file_handler = logging.FileHandler(filename=logfile_path, encoding="utf-8")
    file_handler.setLevel(log_level)
    # Normal Formatter is used instead of ColoredFormatter for file
    # logging because colors would just render as text.
    file_handler.setFormatter(formatter)
    root_logger.addHandler(file_handler)


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python -m sis_scraper <start_year> <end_year>")
        sys.exit(1)
    start_year = int(sys.argv[1])
    end_year = int(sys.argv[2])

    # Load environment variables from .env file if it exists
    load_dotenv()

    try:
        logs_dir = Path(__file__).parent / os.getenv("SCRAPER_LOGS_DIR")
        output_data_dir = Path(__file__).parent / os.getenv("SCRAPER_OUTPUT_DATA_DIR")
        code_mappings_dir = Path(__file__).parent / os.getenv(
            "SCRAPER_CODE_MAPPINGS_DIR"
        )

        subject_name_code_map_path = code_mappings_dir / os.getenv(
            "SUBJECT_NAME_CODE_MAP_FILENAME"
        )
        known_instructor_rcsids_path = code_mappings_dir / os.getenv(
            "KNOWN_INSTRUCTOR_RCSIDS_FILENAME"
        )
        restriction_name_code_map_path = code_mappings_dir / os.getenv(
            "RESTRICTION_NAME_CODE_MAP_FILENAME"
        )
        attribute_name_code_map_path = code_mappings_dir / os.getenv(
            "ATTRIBUTE_NAME_CODE_MAP_FILENAME"
        )
    except TypeError as e:
        print(
            "ERROR: One or more required environment variables are not set."
            " Ensure that an .env file exists with all required variables."
        )
        import traceback

        traceback.print_exc()
        sys.exit(1)

    logging_init(logs_dir, log_level=logging.INFO)
    asyncio.run(
        sis_scraper.main(
            output_data_dir=output_data_dir,
            start_year=start_year,
            end_year=end_year,
            subject_name_code_map_path=subject_name_code_map_path,
            known_instructor_rcsids_path=known_instructor_rcsids_path,
            restriction_name_code_map_path=restriction_name_code_map_path,
            attribute_name_code_map_path=attribute_name_code_map_path,
        )
    )
