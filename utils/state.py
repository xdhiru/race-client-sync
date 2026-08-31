import os
import json
import logging
import tempfile

logger = logging.getLogger(__name__)

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
        # Atomic write: write to temp file then rename
        with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', dir=d, delete=False, suffix='.tmp') as tf:
            json.dump(state, tf, indent=2)
            temp_path = tf.name
        os.replace(temp_path, path)
    except Exception as e:
        logger.error(f"Failed to save state to {path}: {e}")
        # Clean up temp file if it exists
        try:
            if 'temp_path' in locals() and os.path.exists(temp_path):
                os.remove(temp_path)
        except Exception:
            pass
