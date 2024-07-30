"""
This module sets up a logger for the system with console and file handlers, using custom formatters for log messages.
The logging level (severity) is set in the configuration file.
"""

import logging
from pathlib import Path

from configuration import LOGGING_LEVEL
from utils.log_formatter import ColorFormatter, BaseFormatter

# Dictionary to map logging level set in the configuration file to the logging module
LEVELS = {
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARNING": logging.WARNING,
    "ERROR": logging.ERROR,
    "CRITICAL": logging.CRITICAL
}

# Get logging level from configuration, defaulting to NOTSET if invalid value provided
log_level = LEVELS.get(str(LOGGING_LEVEL).upper(), logging.NOTSET)
if log_level == logging.NOTSET:
    print("Invalid logging level. Defaulting to INFO")
    log_level = logging.INFO

# Create a custom logger
logger = logging.getLogger(__name__)

# Set default level of logger
logger.setLevel(log_level)

# Create handlers
# Console handler
c_handler = logging.StreamHandler()
c_handler.setLevel(log_level)

# File handler
# Create logs directory if it does not exist
if not Path("./logs").is_dir():
    Path("./logs").mkdir()
# Specify file to log to and severity level
f_handler = logging.FileHandler('logs/log.log')
f_handler.setLevel(log_level)

# Create formatters and add it to handlers
log_fmt = "%(asctime)s [%(custom_group)s]\t[%(thread)d]\t[%(levelname)s]\t%(message)s"
c_handler.setFormatter(ColorFormatter(log_fmt))
f_handler.setFormatter(BaseFormatter(log_fmt))

# Add handlers to the logger
logger.addHandler(c_handler)
logger.addHandler(f_handler)
