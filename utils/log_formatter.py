import logging


class BaseFormatter(logging.Formatter):
    def __init__(self, format):
        super().__init__(format)

    def format(self, record):
        record.custom_group = f"{record.filename}:{record.lineno} - {record.funcName}()".ljust(45)
        return super().format(record)


class ColorFormatter(BaseFormatter):
    def __init__(self, format):
        super().__init__(format)
        grey = "\x1b[38;20m"
        yellow = "\x1b[33;20m"
        red = "\x1b[31;20m"
        bold_red = "\x1b[31;1m"
        green = "\x1b[32;20m"
        reset = "\x1b[0m"

        self.formats = {
            logging.DEBUG: grey + format + reset,
            logging.INFO: green + format + reset,
            logging.WARNING: yellow + format + reset,
            logging.ERROR: red + format + reset,
            logging.CRITICAL: bold_red + format + reset
        }

    def format(self, record):
        record.custom_group = f"{record.filename}:{record.lineno} - {record.funcName}()".ljust(45)
        log_fmt = self.formats.get(record.levelno, self._fmt)
        formatter = logging.Formatter(log_fmt)
        return formatter.format(record)
