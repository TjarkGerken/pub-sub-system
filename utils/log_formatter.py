import logging

from configuration import LOGGING_PADDING


class BaseFormatter(logging.Formatter):
    """
    Custom logging formatter that extends the standard logging.Formatter class

    Attributes:
        _num_digits : int
            The number of digits used for padding the custom group string
    """
    def __init__(self, fmt):
        """
        Initializes the BaseFormatter with a specified format

        :param fmt: The logging format string.
        :return: None
        """
        super().__init__(fmt)
        self._num_digits = LOGGING_PADDING

    def format(self, record):
        """
        Formats the log record as text with additional information

        :param record: The log record to be formatted
        :return: The formatted log record
        """
        record.custom_group = f"{record.filename}:{record.lineno} - {record.funcName}()".ljust(self._num_digits)
        return super().format(record)


class ColorFormatter(BaseFormatter):
    """
    Custom logging formatter that extends the BaseFormatter class and adds color to the log messages based on their
    severity level

    Attributes:
        formats : dict
            A dictionary that maps the logging levels to their corresponding color codes
    """
    def __init__(self, fmt):
        """
        Initializes the ColorFormatter with a specified format and sets up color codes for different log levels

        :param fmt: The logging format string
        :return: None
        """
        super().__init__(fmt)
        grey = "\x1b[0;37m"
        yellow = "\x1b[33;20m"
        red = "\x1b[31;20m"
        bold_red = "\x1b[31;1m"
        green = "\x1b[32;20m"
        reset = "\x1b[0m"

        self.formats = {
            logging.DEBUG: grey + fmt + reset,
            logging.INFO: green + fmt + reset,
            logging.WARNING: yellow + fmt + reset,
            logging.ERROR: red + fmt + reset,
            logging.CRITICAL: bold_red + fmt + reset
        }

    def format(self, record):
        """
        Formats the specified record as text, applying color codes based on the log level

        :param record: The log record to be formatted
        :return: The formatted log record with color codes applied
        """
        record.custom_group = f"{record.filename}:{record.lineno} - {record.funcName}()".ljust(self._num_digits)
        log_fmt = self.formats.get(record.levelno, self._fmt)
        formatter = logging.Formatter(log_fmt)

        return formatter.format(record)
