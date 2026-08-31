"""Smoke test for the AddTorrentResult enum and telegram queue behavior."""
import os
import shutil
import sys
import tempfile
import time
from unittest.mock import MagicMock

sys.modules.setdefault("qbittorrentapi", MagicMock())

# Create a temp working dir with a valid config.toml so load_config doesn't sys.exit.
TMPDIR = tempfile.mkdtemp(prefix="race_test_")
SAMPLE_CONFIG = os.path.join(os.path.dirname(__file__), "test_config.toml")
shutil.copy(SAMPLE_CONFIG, os.path.join(TMPDIR, "config.toml"))
os.chdir(TMPDIR)
os.makedirs("data", exist_ok=True)
sys.path.insert(0, '.')

from clients.base import AddTorrentResult
import services.telegram_queue as tg_queue_mod


def test_enum_values():
    assert AddTorrentResult.ADDED.value == "added"
    assert AddTorrentResult.EXISTS_CORRECT.value == "exists_correct"
    assert AddTorrentResult.EXISTS_WRONG.value == "exists_wrong"
    assert AddTorrentResult.FAILED.value == "failed"
    print("test_enum_values: PASS")


def test_telegram_queue_dispatch():
    """Verify a single dispatcher thread sends debounced updates."""
    from services.telegram_queue import start, stop, enqueue_status

    sent = []

    def fake_update_telegram_status(config, job, info_hash, racing_hashes=None):
        sent.append((info_hash, job["state"]))

    tg_queue_mod.update_telegram_status = fake_update_telegram_status
    tg_queue_mod.send_already_seeding_notification = lambda *a, **kw: None

    start()
    try:
        job = {"state": "added_local", "name": "test"}
        enqueue_status(job, "hash1")
        enqueue_status(job, "hash1")
        enqueue_status(job, "hash1")
        # Allow time for the dispatcher to drain.
        deadline = time.time() + 5.0
        while time.time() < deadline and not sent:
            time.sleep(0.2)
        print(f"  debug: sent={sent}")
        assert len(sent) >= 1, f"expected at least 1 send, got {len(sent)}"
        print(f"test_telegram_queue_dispatch: PASS (sent {len(sent)} updates)")
    finally:
        stop()


if __name__ == "__main__":
    test_enum_values()
    test_telegram_queue_dispatch()
    print("All infrastructure smoke tests PASSED")
