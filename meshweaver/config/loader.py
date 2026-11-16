import json
from pathlib import Path

from meshweaver.core.errors import ConfigurationError


DEFAULT_CONFIG_PATH = Path(__file__).with_name("default_config.json")


def load_config(config_path=None):
    """Load application configuration from a JSON file."""

    path = Path(config_path) if config_path else DEFAULT_CONFIG_PATH

    try:
        with path.open("r", encoding="utf-8") as config_file:
            config = json.load(config_file)

    except FileNotFoundError as exc:
        raise ConfigurationError(
            f"Configuration file not found: {path}"
        ) from exc

    except json.JSONDecodeError as exc:
        raise ConfigurationError(
            f"Invalid JSON configuration: {exc}"
        ) from exc

    if not isinstance(config, dict):
        raise ConfigurationError(
            "Configuration must contain a JSON object."
        )

    return config