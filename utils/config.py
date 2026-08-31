import os
import sys
import logging
import socket

# Force IPv4 DNS resolution globally to bypass broken IPv6 routing on host
orig_getaddrinfo = socket.getaddrinfo
def forced_getaddrinfo(*args, **kwargs):
    lst = list(args)
    if len(lst) > 2:
        lst[2] = socket.AF_INET
    else:
        kwargs['family'] = socket.AF_INET
    return orig_getaddrinfo(*lst, **kwargs)
socket.getaddrinfo = forced_getaddrinfo

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
        with open(CONFIG_PATH, "r", encoding="utf-8-sig") as f:
            return tomllib.loads(f.read())
    except Exception as e:
        logger.error(f"Failed to parse configuration file {CONFIG_PATH}: {e}")
        sys.exit(1)
