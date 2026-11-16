import unittest

from meshweaver.config.loader import load_config
from meshweaver.core.errors import ConfigurationError


class TestConfigLoader(unittest.TestCase):

    def test_load_default_config(self):
        config = load_config()

        self.assertIsInstance(config, dict)
        self.assertIn("node_id", config)
        self.assertIn("host", config)
        self.assertIn("port", config)
        self.assertIn("database_path", config)

    def test_missing_config_file(self):
        with self.assertRaises(ConfigurationError):
            load_config("missing_config.json")


if __name__ == "__main__":
    unittest.main()