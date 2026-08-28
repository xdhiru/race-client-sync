import os
import json
import logging

logger = logging.getLogger(__name__)

MAPPINGS_PATH = "tracker_mappings.json"

def load_json_state(path, default_factory=dict):
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Failed to load state from {path}: {e}. Starting fresh.")
    return default_factory()

def save_json_state(path, state):
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(state, f, indent=2)
    except Exception as e:
        logger.error(f"Failed to save state to {path}: {e}")

def get_tracker_mapping(local_hash):
    if not os.path.exists(MAPPINGS_PATH):
        return None
    try:
        with open(MAPPINGS_PATH, "r", encoding="utf-8") as f:
            mappings = json.load(f)
        val = mappings.get(local_hash)
        if val:
            return val
        double_hex = local_hash.encode('utf-8').hex()
        val = mappings.get(double_hex)
        if val:
            return val
    except Exception as e:
        logger.error(f"Failed to read mappings file: {e}")
    return None

def remove_tracker_mapping(local_hash):
    if not os.path.exists(MAPPINGS_PATH):
        return
    try:
        with open(MAPPINGS_PATH, "r", encoding="utf-8") as f:
            mappings = json.load(f)
        has_changed = False
        if local_hash in mappings:
            mappings.pop(local_hash)
            has_changed = True
        double_hex = local_hash.encode('utf-8').hex()
        if double_hex in mappings:
            mappings.pop(double_hex)
            has_changed = True
        if has_changed:
            with open(MAPPINGS_PATH, "w", encoding="utf-8") as f:
                json.dump(mappings, f, indent=2)
            logger.info(f"Removed mapping for local hash {local_hash}")
    except Exception as e:
        logger.error(f"Failed to remove mapping: {e}")

def register_tracker_mapping(local_hash, racing_hash):
    mappings = {}
    if os.path.exists(MAPPINGS_PATH):
        try:
            with open(MAPPINGS_PATH, "r", encoding="utf-8") as f:
                mappings = json.load(f)
        except Exception as e:
            logger.error(f"Failed to load mappings: {e}")
            
    mappings[local_hash] = racing_hash
    
    try:
        with open(MAPPINGS_PATH, "w", encoding="utf-8") as f:
            json.dump(mappings, f, indent=2)
        logger.info(f"Registered mapping: Local {local_hash} -> Racing {racing_hash}")
    except Exception as e:
        logger.error(f"Failed to save mappings file after registering mapping: {e}")
