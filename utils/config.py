import os
import sys
import logging

try:
    import tomllib
except ModuleNotFoundError:
    try:
        import tomli as tomllib
    except ModuleNotFoundError:
        print("Error: Python 3.11+ is required, or install 'tomli' (pip install tomli).")
        sys.exit(1)

logger = logging.getLogger(__name__)
CONFIG_PATH = "config.toml"

def load_config():
    if not os.path.exists(CONFIG_PATH):
        logger.error(f"Configuration file {CONFIG_PATH} not found!")
        sys.exit(1)
    try:
        with open(CONFIG_PATH, "rb") as f:
            return tomllib.load(f)
    except Exception as e:
        logger.error(f"Failed to parse configuration file {CONFIG_PATH}: {e}")
        sys.exit(1)
