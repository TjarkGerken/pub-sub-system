import logging
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
logger2 = logging.getLogger(__name__)

# Set default level of logger
logger2.setLevel(logging.DEBUG)

# Create handlers
## Create a debug handler
temp_handler = logging.FileHandler("testing.csv")
temp_handler.setLevel(logging.DEBUG)

# Create formatters and add it to handlers
temp_handler.setFormatter(BaseFormatter("%(message)s"))

# Add handlers to the logger
logger2.addHandler(temp_handler)