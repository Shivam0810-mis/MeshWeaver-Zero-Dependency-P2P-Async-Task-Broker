
import json
import logging
import unittest
from io import StringIO

from meshweaver.core.logging_config import (
    configure_logging,
    get_logger,
)
from tests.test_helpers import capture_logs


class TestLoggingConfiguration(unittest.TestCase):

    def setUp(self):
        self.logger = logging.getLogger("meshweaver")
        self.original_handlers = self.logger.handlers[:]
        self.original_level = self.logger.level
        self.original_propagate = self.logger.propagate

        for handler in self.logger.handlers[:]:
            self.logger.removeHandler(handler)

    def tearDown(self):
        for handler in self.logger.handlers[:]:
            self.logger.removeHandler(handler)
            handler.close()

        for handler in self.original_handlers:
            self.logger.addHandler(handler)

        self.logger.setLevel(self.original_level)
        self.logger.propagate = self.original_propagate

    def test_configure_logging_outputs_valid_json(self):
        stream = StringIO()
        logger = configure_logging(stream=stream)

        logger.info("MeshWeaver started")

        log_output = stream.getvalue().strip()
        log_data = json.loads(log_output)

        self.assertEqual(log_data["level"], "INFO")
        self.assertEqual(log_data["message"], "MeshWeaver started")
        self.assertEqual(log_data["logger"], "meshweaver")

    def test_get_logger_returns_meshweaver_logger(self):
        logger = get_logger("broker")

        self.assertEqual(logger.name, "meshweaver.broker")

    def test_repeated_configuration_does_not_add_handlers(self):
        configure_logging()
        initial_count = len(self.logger.handlers)

        configure_logging()

        self.assertEqual(len(self.logger.handlers), initial_count)

    def test_capture_logs_helper_captures_message(self):
        logger = get_logger("test")

        with capture_logs("meshweaver.test") as captured:
            logger.warning("Test warning message")

        self.assertIn("Test warning message", captured.getvalue())


if __name__ == "__main__":
    unittest.main()
