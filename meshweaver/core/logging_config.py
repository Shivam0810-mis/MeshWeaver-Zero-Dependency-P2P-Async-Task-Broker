
import json
import logging
import sys
from datetime import datetime, timezone


class JsonFormatter(logging.Formatter):
    """Format log records as structured JSON."""

    def format(self, record):
        log_data = {
            "timestamp": datetime.fromtimestamp(
                record.created, timezone.utc
            ).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_data)


def configure_logging(level=logging.INFO, stream=None):
    """Configure centralized JSON logging for MeshWeaver."""

    logger = logging.getLogger("meshweaver")
    logger.setLevel(level)
    logger.propagate = False

    # Avoid adding duplicate handlers on repeated configuration.
    if not logger.handlers:
        handler = logging.StreamHandler(stream or sys.stderr)
        handler.setFormatter(JsonFormatter())
        logger.addHandler(handler)

    return logger


def get_logger(name):
    """Return a logger under the MeshWeaver namespace."""

    return logging.getLogger(f"meshweaver.{name}")
