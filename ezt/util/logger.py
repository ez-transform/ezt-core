import logging
import os
from datetime import datetime


class EztLogger:
    """Class that handles logging."""

    def __init__(self, logs_destination=None):
        self.logs_destination = logs_destination
        (self.format, self.filename, self.level) = self.setup_logging()

    def setup_logging(self):
        """Specifies the logging parameters for the project."""

        if self.logs_destination:
            # Create logging folder if not exists.
            if not os.path.isdir(self.logs_destination):
                os.makedirs(self.logs_destination)

            format = "[%(asctime)s] %(levelname)s: %(message)s"
            filename = f"{self.logs_destination}/log_ezt_run_{str(datetime.now())}.log"
            level = logging.DEBUG

            logging.basicConfig(
                format=format,
                filename=filename,
                level=level,
                force=True,
            )
            return (format, filename, level)
        else:
            return (None, None, None)

    def log_info(self, message):
        """Logs a message on info-level."""
        if self.logs_destination:
            logging.info(message)
        else:
            pass

    def log_error(self, message):
        """Logs a message on error-level."""
        if self.logs_destination:
            logging.error(message)
        else:
            pass
