import logging
from pathlib import Path

from configuration import LOGGING_LEVEL
from classes.log_formatter import BaseFormatter, ColorFormatter

LEVELS = {
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARNING": logging.WARNING,
    "ERROR": logging.ERROR,
    "CRITICAL": logging.CRITICAL
}

log_level = LEVELS.get(LOGGING_LEVEL, logging.NOTSET)
if log_level == logging.NOTSET:
    print("Invalid logging level. Defaulting to INFO")
    log_level = logging.INFO

# Create a custom logger
logger = logging.getLogger(__name__)

# Set default level of logger
logger.setLevel(log_level)

# Create handlers
## Create a console handler
c_handler = logging.StreamHandler()
c_handler.setLevel(log_level)

## Create a file handler
if not Path("./logs").is_dir():
    Path("./logs").mkdir()

f_handler = logging.FileHandler('logs/log.log')
f_handler.setLevel(log_level)

# Create formatters and add it to handlers
format = "%(asctime)s [%(custom_group)s]\t[%(thread)d]\t[%(levelname)s]\t%(message)s"
c_handler.setFormatter(ColorFormatter(format))
f_handler.setFormatter(BaseFormatter(format))

# Add handlers to the logger
logger.addHandler(c_handler)
logger.addHandler(f_handler)
