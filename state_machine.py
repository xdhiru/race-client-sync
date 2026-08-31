import os
import re
import time
import shutil
import logging
import threading
from collections import deque

from clients.base import AddTorrentResult
import services.telegram_queue as tg_queue

logger = logging.getLogger(__name__)

RCLONE_LOCK = threading.Lock()
JOBS_LOCK = threading.Lock()


class Transition:
    ADVANCED = "advanced"
    WAITING = "waiting"
    STUCK = "stuck"
    COMPLETED = "completed"
    FAILED = "failed"


class TorrentJobStateMachine:
    """Per-job state machine. Each instance owns one torrent's lifecycle.

    All handlers are methods named `_handle_<state>`. They mutate the
    job dict in place. State transitions call `_set_state`, which marks
    the job dirty and enqueues a Telegram update.
    """

    VALID_STATES = {
        "added_local", "waiting_5min", "rclone_moving",
        "fuse_wait", "added_remote", "completed",
        "rclone_failed", "add_remote_failed",
    }

    def __init__(self, info_hash, job, config, client, rclone_status, rclone_threads):
        self.info_hash = info_hash.lower()
        self.job = job
        self.config = config
        self.client = client
        self.rclone_status = rclone_status
        self.rclone_threads = rclone_threads
        self._last_progress_ts = 0.0
        self._last_tg_state = None

    def _set_state(self, new_state, **extra):
        if new_state not in self.VALID_STATES:
            raise ValueError(f"Invalid state: {new_state}")
        old = self.job.get("state")
        self.job["state"] = new_state
        for k, v in extra.items():
            self.job[k] = v
        logger.info(f"[{self.info_hash[:8]}] {old} -> {new_state} ({self.job.get('name', '?')})")
        tg_queue.enqueue_status(self.job, self.info_hash)
        return Transition.ADVANCED

    def tick(self, torrents_by_hash):
        """Run one transition. Returns one of Transition.*"""
        state = self.job.get("state", "added_local")
        t = torrents_by_hash.get(self.info_hash)
        handler = getattr(self, f"_handle_{state}", None)
        if handler is None:
            logger.error(f"No handler for state {state} on job {self.info_hash}")
            return Transition.STUCK
        try:
            return handler(t)
        except Exception as e:
            logger.error(f"Handler for state {state} threw: {e}", exc_info=True)
            return Transition.STUCK

    def _job_size_for_budget(self):
        batches = self.job.get("batches")
        if batches and "current_batch_index" in self.job:
            idx = self.job["current_batch_index"]
            if 0 <= idx < len(batches):
                return batches[idx]["size"]
        return self.job.get("size", 0)

    def _is_batch_complete(self, t):
        if not t:
            return False
        progress = t.get("progress", 0.0)
        if progress >= 0.999:
            return True
        batches = self.job.get("batches")
        if not batches:
            return progress >= 0.999
        try:
            files = self.client.get_torrent_files(self.info_hash)
        except Exception as e:
            logger.warning(f"get_torrent_files failed for {self.info_hash}: {e}")
            return progress >= 0.999
        if not files:
            return progress >= 0.999
        idx = self.job.get("current_batch_index", 0)
        if idx >= len(batches):
            return progress >= 0.999
        batch_ids = set(batches[idx]["file_ids"])
        completed = 0.0
        total = batches[idx]["size"]
        for f in files:
            if f.get("id") in batch_ids:
                completed += f.get("size", 0) * f.get("progress", 0.0)
        if total <= 0:
            return True
        return (completed / total) >= 0.999

    def _ensure_batch_priorities(self, t):
        if self.job.get("priorities_configured"):
            return True
        batches = self.job.get("batches")
        if not batches:
            return True
        try:
            try:
                self.client.pause_torrent(self.info_hash)
            except Exception:
                pass
            files = self.client.get_torrent_files(self.info_hash)
            if not files:
                return False
            all_ids = [f["id"] for f in files]
            curr_idx = self.job.get("current_batch_index", 0)
            batch_ids = batches[curr_idx]["file_ids"]
            self.client.set_file_priorities(self.info_hash, all_ids, 0)
            self.client.set_file_priorities(self.info_hash, batch_ids, 1)
            try:
                self.client.resume_torrent(self.info_hash)
            except Exception:
                pass
            self.job["priorities_configured"] = True
            return True
        except Exception as e:
            logger.warning(f"Priority config failed for {self.info_hash}: {e}")
            return False

    def _handle_added_local(self, t):
        if not t:
            tf = self.job.get("torrent_file", "")
            if tf and not os.path.exists(tf):
                logger.warning(f"Torrent file {tf} missing. Dropping job {self.info_hash}.")
                with JOBS_LOCK:
                    self.job["_evict"] = True
                return Transition.FAILED
            return Transition.WAITING

        if not self.job.get("priorities_configured") and self.job.get("batches"):
            if not self._ensure_batch_priorities(t):
                return Transition.WAITING

        if self._is_batch_complete(t):
            total_batches = len(self.job.get("batches", []))
            curr = self.job.get("current_batch_index", 0) + 1
            logger.info(f"{self.job.get('name')} batch {curr}/{total_batches} complete. Cooldown.")
            return self._set_state("waiting_5min", rclone_retries=0, completion_time=time.time())
        return Transition.WAITING

    def _handle_waiting_5min(self, t):
        wait_seconds = self.config["settings"].get("wait_time_minutes", 5.0) * 60
        elapsed = time.time() - self.job.get("completion_time", 0)
        if elapsed < wait_seconds:
            return Transition.WAITING
        try:
            self.client.delete_torrent(self.info_hash, delete_files=False)
        except Exception as e:
            logger.error(f"Failed to delete torrent {self.info_hash} from client: {e}")
        return self._set_state(
            "rclone_moving",
            move_start_time=time.time(),
        )

    def _handle_rclone_moving(self, t):
        with RCLONE_LOCK:
            status = self.rclone_status.get(self.info_hash)
            if status is None:
                status = self.rclone_status.get(self.info_hash.lower())

        if status is None:
            return self._maybe_start_rclone()
        if status == "success":
            return self._on_rclone_success()
        if status.startswith("failed") or status.startswith("error"):
            return self._on_rclone_failure(status)
        return Transition.WAITING

    def _maybe_start_rclone(self):
        max_jobs = self.config.get("rclone", {}).get("max_parallel_jobs", 2)
        with RCLONE_LOCK:
            running = sum(1 for v in self.rclone_status.values() if v == "running")
        if running >= max_jobs:
            return Transition.WAITING
        try:
            from torrent_sync import run_rclone_move_async
            local_dir = self.config["paths"]["local_save_path"]
            remote_target, _ = get_job_remote_paths(self.config, self.job["name"])
            rclone_transfers = self.config.get("rclone", {}).get("transfers", 2)
            cmd = _build_rclone_cmd(self.job, local_dir, remote_target, rclone_transfers)
            run_rclone_move_async(self.info_hash, cmd)
        except Exception as e:
            logger.error(f"Failed to launch rclone for {self.info_hash}: {e}")
        return Transition.WAITING

    def _on_rclone_success(self):
        logger.info(f"rclone success for {self.job.get('name')}. Cleaning up local.")
        local_path = os.path.join(self.config["paths"]["local_save_path"], self.job["name"])
        if os.path.exists(local_path):
            try:
                if os.path.isdir(local_path):
                    shutil.rmtree(local_path)
                else:
                    os.remove(local_path)
            except Exception as e:
                logger.error(f"Local cleanup failed: {e}")

        with RCLONE_LOCK:
            self.rclone_status.pop(self.info_hash, None)
            self.rclone_threads.pop(self.info_hash, None)

        batches = self.job.get("batches")
        if batches:
            next_idx = self.job.get("current_batch_index", 0) + 1
            if next_idx < len(batches):
                if self._re_add_for_next_batch(next_idx):
                    return self._set_state(
                        "added_local",
                        current_batch_index=next_idx,
                        added_time=time.time(),
                        completion_time=None,
                        priorities_configured=False,
                    )
                return Transition.WAITING

        return self._set_state("fuse_wait", move_completed_time=time.time())

    def _re_add_for_next_batch(self, next_idx):
        try:
            result = self.client.add_torrent(
                torrent_bytes=self.job["torrent_file"],
                save_path=self.config["paths"]["local_save_path"],
                category=self.config["paths"].get("category"),
                paused=True,
            )
            if result == AddTorrentResult.FAILED:
                logger.error(f"Failed to re-add {self.job.get('name')} for batch {next_idx+1}.")
                return False
            return True
        except Exception as e:
            logger.error(f"Re-add threw: {e}")
            return False

    def _on_rclone_failure(self, status):
        logger.error(f"rclone failed for {self.job.get('name')}: {status}")
        with RCLONE_LOCK:
            self.rclone_status.pop(self.info_hash, None)
            self.rclone_threads.pop(self.info_hash, None)
        max_retries = self.config.get("rclone", {}).get("max_retries", 3)
        retries = self.job.get("rclone_retries", 0) + 1
        if retries <= max_retries:
            wait_seconds = self.config["settings"].get("wait_time_minutes", 5.0) * 60
            return self._set_state(
                "waiting_5min",
                rclone_retries=retries,
                completion_time=time.time() - wait_seconds + 30,
            )
        return self._set_state("rclone_failed", error_msg=status)

    def _handle_fuse_wait(self, t):
        fuse_cooldown = self.config["settings"].get("fuse_cooldown_seconds", 15)
        elapsed = time.time() - self.job.get("move_completed_time", 0)
        if elapsed < fuse_cooldown:
            return Transition.WAITING
        _, remote_save_path = get_job_remote_paths(self.config, self.job["name"])
        result = self.client.add_torrent(
            torrent_bytes=self.job["torrent_file"],
            save_path=remote_save_path,
            category=self.config["paths"].get("remote_category", "remote"),
            is_skip_checking=True,
        )

        if result == AddTorrentResult.EXISTS_CORRECT:
            return self._on_added_remote()

        if result == AddTorrentResult.EXISTS_WRONG:
            logger.info(f"Torrent {self.info_hash} exists on SSD path. Deleting from client and re-adding pointing to FUSE with skip check.")
            self.client.delete_torrent(self.info_hash, delete_files=False)
            local_dir = self.config.get("paths", {}).get("local_save_path")
            if local_dir:
                local_path = os.path.join(local_dir, self.job["name"])
                if os.path.exists(local_path):
                    try:
                        if os.path.isdir(local_path):
                            shutil.rmtree(local_path)
                        else:
                            os.remove(local_path)
                        logger.info(f"Cleaned up local SSD path: {local_path}")
                    except Exception as e:
                        logger.error(f"Failed to clean up local path {local_path}: {e}")
            result = self.client.add_torrent(
                torrent_bytes=self.job["torrent_file"],
                save_path=remote_save_path,
                category=self.config["paths"].get("remote_category", "remote"),
                is_skip_checking=True,
            )
            if result in (AddTorrentResult.ADDED, AddTorrentResult.EXISTS_CORRECT):
                return self._on_added_remote()
            logger.warning(f"EXISTS_WRONG delete & re-add failed for {self.info_hash}; will retry.")
            result = AddTorrentResult.FAILED

        if result == AddTorrentResult.ADDED:
            return self._on_added_remote()

        retries = self.job.get("readd_retries", 0) + 1
        max_retries = self.config.get("settings", {}).get("max_readd_retries", 10)
        if retries <= max_retries:
            logger.warning(
                f"fuse_wait add failed for {self.job.get('name')}. "
                f"Retry {retries}/{max_retries} after {fuse_cooldown}s."
            )
            self.job["readd_retries"] = retries
            self.job["move_completed_time"] = time.time()
            return Transition.WAITING
        return self._set_state(
            "add_remote_failed",
            error_msg=f"Failed to add to remote after {max_retries} retries",
        )

    def _on_added_remote(self):
        return self._set_state(
            "added_remote",
            readd_start_time=time.time(),
        )

    def _handle_added_remote(self, t):
        if not t:
            elapsed = time.time() - self.job.get("readd_start_time", 0)
            if elapsed > 30:
                _, remote_save_path = get_job_remote_paths(self.config, self.job["name"])
                self.client.add_torrent(
                    torrent_bytes=self.job["torrent_file"],
                    save_path=remote_save_path,
                    category=self.config["paths"].get("remote_category", "remote"),
                    is_skip_checking=True,
                )
                self.job["readd_start_time"] = time.time()
            return Transition.WAITING

        is_checking = (t["state"].startswith("checking") or "checking" in t["state"])
        if t.get("progress", 0.0) >= 0.999 and not is_checking:
            return self._complete()
        return Transition.WAITING

    def _complete(self):
        from torrent_sync import _archive_file_safely
        from utils.torrent import get_torrent_details
        from racing_injector import enqueue_injection

        try:
            details = get_torrent_details(self.job["torrent_file"])
            files = details["files"]
            sorted_files = sorted(files, key=lambda x: x["name"])
        except Exception:
            details = self.job
            try:
                files = self.client.get_torrent_files(self.info_hash)
                sorted_files = sorted(files, key=lambda x: x["name"]) if files else []
            except Exception:
                sorted_files = []

        try:
            _archive_file_safely(self.job["torrent_file"], self.config)
        except Exception as e:
            logger.error(f"Archive failed for {self.job.get('torrent_file')}: {e}")

        self.job["seeding_completed_time"] = time.time()
        self.job["state"] = "completed"
        tg_queue.enqueue_status(self.job, self.info_hash)

        _, remote_save_path = get_job_remote_paths(self.config, self.job["name"])
        enqueue_injection(
            info_hash=self.info_hash,
            details=details,
            sorted_files=sorted_files,
            remote_save_path=remote_save_path,
            config=self.config,
            origin_client=self.client,
        )

        with JOBS_LOCK:
            self.job["_evict"] = True
        return Transition.COMPLETED

    def _handle_rclone_failed(self, t):
        return Transition.FAILED

    def _handle_add_remote_failed(self, t):
        return Transition.FAILED

    def _handle_completed(self, t):
        with JOBS_LOCK:
            self.job["_evict"] = True
        return Transition.COMPLETED


def _build_batches_from_files(files, ssd_limit):
    sorted_files = sorted(files, key=lambda x: x.get("id", 0))
    batches = []
    cur_ids, cur_paths, cur_size = [], [], 0
    for f in sorted_files:
        f_size = f.get("size", 0)
        f_id = f.get("id", 0)
        f_name = f.get("name", "")
        if cur_size + f_size > ssd_limit and cur_ids:
            batches.append({"file_ids": cur_ids, "file_paths": cur_paths, "size": cur_size})
            cur_ids, cur_paths, cur_size = [f_id], [f_name], f_size
        else:
            cur_ids.append(f_id)
            cur_paths.append(f_name)
            cur_size += f_size
    if cur_ids:
        batches.append({"file_ids": cur_ids, "file_paths": cur_paths, "size": cur_size})
    return batches


def get_job_remote_paths(config, torrent_name):
    rclone_cfg = config.get("rclone") or {}
    paths_cfg = config.get("paths") or {}
    remote_target = rclone_cfg.get("remote", "")
    remote_save_path = paths_cfg.get("remote_save_path", "")
    if re.search(r'[sS]\d+[\s._-]*[eE]\d+', torrent_name):
        remote_target = f"{remote_target.rstrip('/')}/unsorted/"
        remote_save_path = f"{remote_save_path.rstrip('/')}/unsorted/"
    return remote_target, remote_save_path


def _build_rclone_cmd(job, local_dir, remote_target, rclone_transfers):
    from torrent_sync import escape_rclone_glob
    if "batches" in job and "current_batch_index" in job:
        batch = job["batches"][job["current_batch_index"]]
        cmd = ["rclone", "move", local_dir, remote_target, f"--transfers={rclone_transfers}"]
        for path in batch["file_paths"]:
            cmd.extend(["--include", escape_rclone_glob(path)])
        return cmd
    if job.get("is_multi_file"):
        src = os.path.join(local_dir, job["name"])
        dest = f"{remote_target.rstrip('/')}/{job['name']}"
        return ["rclone", "move", src, dest, f"--transfers={rclone_transfers}"]
    src = os.path.join(local_dir, job["name"])
    dest = f"{remote_target.rstrip('/')}/{job['name']}"
    return ["rclone", "moveto", src, dest, f"--transfers={rclone_transfers}"]
