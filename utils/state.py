import os
import json
import logging

logger = logging.getLogger(__name__)

MAPPINGS_PATH = "data/tracker_mappings.json"

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
        d = os.path.dirname(path)
        if d: os.makedirs(d, exist_ok=True)
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
        if val is not None:
            return val
        double_hex = local_hash.encode('utf-8').hex()
        val = mappings.get(double_hex)
        if val is not None:
            return val
    except Exception as e:
        logger.error(f"Failed to read mappings file: {e}")
    return None

def remove_tracker_mapping(local_hash, racing_hash=None):
    if not os.path.exists(MAPPINGS_PATH):
        return
    try:
        with open(MAPPINGS_PATH, "r", encoding="utf-8") as f:
            mappings = json.load(f)
        
        has_changed = False
        for key in [local_hash, local_hash.encode('utf-8').hex()]:
            if key in mappings:
                if racing_hash is None:
                    mappings.pop(key)
                    has_changed = True
                else:
                    existing = mappings[key]
                    if isinstance(existing, str):
                        existing = [existing]
                    if not isinstance(existing, list):
                        existing = []
                    if racing_hash in existing:
                        existing.remove(racing_hash)
                        has_changed = True
                    if not existing:
                        mappings.pop(key)
                    else:
                        mappings[key] = existing
                        
        if has_changed:
            d = os.path.dirname(MAPPINGS_PATH)
            if d: os.makedirs(d, exist_ok=True)
            with open(MAPPINGS_PATH, "w", encoding="utf-8") as f:
                json.dump(mappings, f, indent=2)
            logger.info(f"Removed mapping for local hash {local_hash} (racing: {racing_hash})")
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
            
    existing = mappings.get(local_hash, [])
    if isinstance(existing, str):
        existing = [existing]
    elif not isinstance(existing, list):
        existing = []
        
    if racing_hash not in existing:
        existing.append(racing_hash)
        
    mappings[local_hash] = existing
    
    try:
        d = os.path.dirname(MAPPINGS_PATH)
        if d: os.makedirs(d, exist_ok=True)
        with open(MAPPINGS_PATH, "w", encoding="utf-8") as f:
            json.dump(mappings, f, indent=2)
        logger.info(f"Registered mapping: Local {local_hash} -> Racing {existing}")
    except Exception as e:
        logger.error(f"Failed to save mappings file after registering mapping: {e}")
