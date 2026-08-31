"""Smoke test: verify per-job state machine transitions without qBittorrent."""
import sys
import time
import threading
from unittest.mock import MagicMock

# Stub qbittorrentapi before any import that would pull it in.
sys.modules.setdefault("qbittorrentapi", MagicMock())

sys.path.insert(0, '.')

from clients.base import AddTorrentResult
from state_machine import TorrentJobStateMachine, Transition


def make_mock_client():
    c = MagicMock()
    c.get_torrents_info.return_value = []
    c.get_torrent_files.return_value = []
    c.add_torrent.return_value = AddTorrentResult.ADDED
    c.set_file_priorities.return_value = True
    c.pause_torrent.return_value = True
    c.resume_torrent.return_value = True
    c.set_location.return_value = True
    return c


def test_added_local_completes():
    job = {
        "state": "added_local",
        "name": "test",
        "size": 100,
        "batches": [{"file_ids": [0, 1], "file_paths": ["a", "b"], "size": 100}],
        "current_batch_index": 0,
        "priorities_configured": True,
        "completion_time": None,
        "torrent_file": "/tmp/x.torrent",
    }
    cfg = {"settings": {"wait_time_minutes": 0.05}, "paths": {"local_save_path": "/tmp"}}
    client = make_mock_client()
    client.get_torrent_files.return_value = [
        {"id": 0, "name": "a", "size": 50, "progress": 1.0},
        {"id": 1, "name": "b", "size": 50, "progress": 1.0},
    ]
    torrents_by_hash = {"abc": {"hash": "abc", "state": "uploading", "progress": 1.0}}
    sm = TorrentJobStateMachine("abc", job, cfg, client, {}, {})
    result = sm.tick(torrents_by_hash)
    assert result == Transition.ADVANCED, f"expected ADVANCED, got {result}"
    assert job["state"] == "waiting_5min", f"expected waiting_5min, got {job['state']}"
    print("test_added_local_completes: PASS")


def test_waiting_5min_transitions_to_rclone():
    job = {
        "state": "waiting_5min",
        "name": "test",
        "size": 100,
        "completion_time": time.time() - 600,
        "torrent_file": "/tmp/x.torrent",
    }
    cfg = {"settings": {"wait_time_minutes": 5.0}, "paths": {"local_save_path": "/tmp"}, "rclone": {"remote": "remote:"}}
    client = make_mock_client()
    sm = TorrentJobStateMachine("abc", job, cfg, client, {}, {})
    result = sm.tick({})
    assert result == Transition.ADVANCED
    assert job["state"] == "rclone_moving"
    print("test_waiting_5min_transitions_to_rclone: PASS")


def test_added_remote_self_heals():
    job = {
        "state": "added_remote",
        "name": "test",
        "size": 100,
        "torrent_file": "/tmp/x.torrent",
        "readd_start_time": time.time() - 60,
    }
    cfg = {"settings": {}, "paths": {"remote_save_path": "/mnt/cloud"}}
    client = make_mock_client()
    sm = TorrentJobStateMachine("abc", job, cfg, client, {}, {})
    result = sm.tick({})
    assert result == Transition.WAITING
    assert client.add_torrent.called
    print("test_added_remote_self_heals: PASS")


if __name__ == "__main__":
    test_added_local_completes()
    test_waiting_5min_transitions_to_rclone()
    test_added_remote_self_heals()
    print("All state machine smoke tests PASSED")
