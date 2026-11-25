
import logging
from contextlib import contextmanager
from io import StringIO


@contextmanager
def capture_logs(logger_name="meshweaver", level=logging.DEBUG):
    """Capture log output during a test."""

    logger = logging.getLogger(logger_name)
    original_level = logger.level
    stream = StringIO()
    handler = logging.StreamHandler(stream)

    logger.setLevel(level)
    logger.addHandler(handler)

    try:
        yield stream
    finally:
        logger.removeHandler(handler)
        handler.close()
        logger.setLevel(original_level)
