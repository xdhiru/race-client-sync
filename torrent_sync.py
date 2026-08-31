import os
import sys
import urllib.parse
import time
import json
import glob
import shutil
import logging
import subprocess
import threading
import re

import clients
from clients.base import AddTorrentResult
from utils.config import load_config
from utils.state import load_json_state, save_json_state
import hashlib
from utils.torrent import get_torrent_details, bdecode, bencode
from services.prowlarr import get_prowlarr_indexer_id, search_prowlarr, download_torrent_bytes
from services.telegram import update_telegram_status, send_already_seeding_notification, start_telegram_listener
import services.telegram_queue as tg_queue
import racing_injector
from state_machine import (
    TorrentJobStateMachine,
    RCLONE_LOCK,
    JOBS_LOCK,
    get_job_remote_paths,
    _build_batches_from_files,
    Transition,
)

def normalize_info_hash(h):
    if not h:
        return ""
    h = h.strip().lower()
    if len(h) == 80:
        try:
            h = bytes.fromhex(h).decode('utf-8').lower()
        except Exception:
            pass
    return h

def clean_search_query(name):
    for ext in ['.mkv', '.mp4', '.avi', '.ts', '.mp3', '.flac']:
        if name.lower().endswith(ext):
            name = name[:-len(ext)]
            break
    cleaned = re.sub(r'[\s._-]', ' ', name)
    return ' '.join(cleaned.split())

# Configure Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def setup_logging(config):
    settings = config.get("settings", {})
    file_level_str = settings.get("log_level", "DEBUG").upper()
    console_level_str = settings.get("console_log_level", "INFO").upper()

    level_map = {
        "DEBUG": logging.DEBUG,
        "INFO": logging.INFO,
        "WARNING": logging.WARNING,
        "ERROR": logging.ERROR,
        "CRITICAL": logging.CRITICAL
    }
    file_level = level_map.get(file_level_str, logging.DEBUG)
    console_level = level_map.get(console_level_str, logging.INFO)

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)  # Capture all logs at root

    # Silence verbose third-party loggers
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.getLogger("qbittorrentapi").setLevel(logging.WARNING)

    # Remove all existing handlers
    for handler in list(root_logger.handlers):
        root_logger.removeHandler(handler)

    # File Handler
    log_file = settings.get("torrent_sync_log_file", "data/torrent_sync.log")
    try:
        import os
        # Create parent directory if it doesn't exist
        log_dir = os.path.dirname(log_file)
        if log_dir:
            os.makedirs(log_dir, exist_ok=True)
            
        # Resolve log file relative to config.toml or current directory
        fh = logging.FileHandler(log_file, encoding='utf-8')
        fh.setLevel(file_level)
        fh.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] (%(filename)s:%(lineno)d) %(message)s'))
        root_logger.addHandler(fh)
    except Exception as e:
        sys.stderr.write(f"Failed to initialize file logging: {e}\n")

    # Console Handler
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(console_level)
    ch.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))
    root_logger.addHandler(ch)

CONFIG_PATH = "config.toml"
STATE_PATH = "data/torrent_sync_state.json"

def load_state():
    state = load_json_state(STATE_PATH)
    state.setdefault("active_jobs", {})
    return state

def save_state(state):
    save_json_state(STATE_PATH, state)

def get_qb_client(config):
    try:
        client = clients.get_client(config.get("qbittorrent"))
        if client and client.connect():
            return client
        return None
    except Exception as e:
        logger.error(f"Failed to initialize qBittorrent client: {e}")
        return None

def get_racing_client(config):
    try:
        client = clients.get_client(config.get("racing_client"))
        if client and client.connect():
            return client
        return None
    except Exception as e:
        logger.error(f"Failed to initialize racing client: {e}")
        return None

# ==============================================================================
# Asynchronous rclone executor
# ==============================================================================

rclone_threads = {}
rclone_status = {}
last_dangling_search_times = {}

def run_rclone_move_async(info_hash, cmd):
    from state_machine import RCLONE_LOCK
    def target():
        with RCLONE_LOCK:
            rclone_status[info_hash] = "running"
        logger.info(f"Starting rclone process for {info_hash}: {' '.join(cmd)}")
        try:
            subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)
            logger.info(f"rclone process succeeded for {info_hash}")
            with RCLONE_LOCK:
                rclone_status[info_hash] = "success"
        except subprocess.CalledProcessError as e:
            logger.error(f"rclone process failed for {info_hash}: Exit code {e.returncode}. Error:\n{e.stderr}")
            with RCLONE_LOCK:
                rclone_status[info_hash] = f"failed: {e.stderr}"
        except Exception as e:
            logger.error(f"Error running rclone for {info_hash}: {e}")
            with RCLONE_LOCK:
                rclone_status[info_hash] = f"error: {str(e)}"

    t = threading.Thread(target=target, daemon=True)
    rclone_threads[info_hash] = t
    t.start()

# ==============================================================================
# Helper Functions
# ==============================================================================

def get_current_occupied_space(active_jobs):
    occupied = 0
    for job in active_jobs.values():
        if job["state"] in ["added_local", "waiting_5min", "rclone_moving", "rclone_failed"]:
            if "batches" in job and "current_batch_index" in job:
                idx = job["current_batch_index"]
                if idx < len(job["batches"]):
                    occupied += job["batches"][idx]["size"]
                else:
                    pass  # Batch index OOB, no local space occupied
            else:
                occupied += job["size"]
    return occupied

def is_same_or_parent_path(parent, child):
    if not parent or not child:
        return False
    p = os.path.normpath(parent).lower().rstrip(os.path.sep)
    c = os.path.normpath(child).lower().rstrip(os.path.sep)
    return c == p or c.startswith(p + os.path.sep)

def escape_rclone_glob(path):
    escaped = ""
    for char in path:
        if char in ['\\', '*', '?', '[', ']']:
            escaped += '\\' + char
        else:
            escaped += char
    return escaped

def get_physical_free_space(path):
    try:
        os.makedirs(path, exist_ok=True)
        _, _, free = shutil.disk_usage(path)
        return free
    except Exception as e:
        logger.error(f"Failed to check disk usage for path {path}: {e}")
        return 0


def _archive_file_safely(src_file, config):
    """Archive a file to completed_dir, handling cross-device moves."""
    try:
        completed_dir = config["paths"].get("completed_dir")
        if not completed_dir:
            watch_dir = config["paths"]["watch_dir"]
            completed_dir = os.path.join(watch_dir, "completed")
        os.makedirs(completed_dir, exist_ok=True)
        dest_file = os.path.join(completed_dir, os.path.basename(src_file))
        if os.path.exists(dest_file):
            base, ext = os.path.splitext(dest_file)
            dest_file = f"{base}_{int(time.time())}{ext}"
        # Use copy + unlink for cross-device support
        import shutil
        shutil.copy2(src_file, dest_file)
        os.unlink(src_file)
        logger.info(f"Archived {src_file} to {dest_file}.")
    except Exception as e:
        logger.error(f"Failed to archive file {src_file}: {e}")

# ==============================================================================
# Main Process Loop
# ==============================================================================


def get_torrent_files_sizes(files):
    if not files: return []
    return sorted([f["size"] for f in files])

def inject_racing_torrents(local_info_hash, details, sorted_files, remote_save_path, config, normal_client):
    """Legacy wrapper preserved for backward compat. Now asynchronous."""
    racing_injector.enqueue_injection(
        info_hash=local_info_hash,
        details=details,
        sorted_files=sorted_files,
        remote_save_path=remote_save_path,
        config=config,
        origin_client=normal_client,
    )
    return []


def process_state_machine(config, state, client):
    """Per-job state machine driver. Each job ticks once per main loop.

    Replaces the old nested-if monolithic state machine. Each job delegates
    to its own TorrentJobStateMachine instance. Jobs marked for eviction
    are removed from active_jobs and persisted.
    """
    active_jobs = state.get("active_jobs", {})
    try:
        torrents_list = client.get_torrents_info()
        torrents_by_hash = {t["hash"].lower(): t for t in torrents_list}
    except Exception as e:
        logger.error(f"Failed to fetch torrent list from client: {e}")
        return

    rclone_status = globals()["rclone_status"]
    rclone_threads = globals()["rclone_threads"]

    evicted = []
    for info_hash in list(active_jobs.keys()):
        job = active_jobs[info_hash]
        sm = TorrentJobStateMachine(
            info_hash=info_hash,
            job=job,
            config=config,
            client=client,
            rclone_status=rclone_status,
            rclone_threads=rclone_threads,
        )
        sm.tick(torrents_by_hash)
        if job.get("_evict"):
            evicted.append(info_hash)

    for h in evicted:
        active_jobs.pop(h, None)
        logger.info(f"Evicted completed/failed job {h}")
    if evicted:
        save_state(state)


def main():
    config = load_config()
    setup_logging(config)
    logger.info("Starting sync loop daemon...")
    start_telegram_listener(load_config)
    state = load_state()

    if "qbittorrent" not in config:
        logger.error("Missing qbittorrent configuration keys in config!")
        sys.exit(1)

    tg_queue.start()
    racing_injector.start()

    client = None
    consecutive_connection_failures = 0

    try:
        while True:
            try:
                config = load_config()
                state = load_state()
                poll_interval = config["settings"].get("poll_interval_seconds", 10)
                ssd_limit = config["settings"].get("ssd_limit_gb", 35.0) * 1024 * 1024 * 1024

                if client is None:
                    client = get_qb_client(config)
                    if client is not None:
                        consecutive_connection_failures = 0
                else:
                    if client.check_health():
                        consecutive_connection_failures = 0
                    else:
                        consecutive_connection_failures += 1
                        logger.warning(f"Torrent client connection unresponsive (failure {consecutive_connection_failures}/3)")
                        if consecutive_connection_failures >= 3:
                            logger.warning("Torrent client connection lost permanently. Re-authenticating...")
                            client = get_qb_client(config)
                            if client is not None:
                                consecutive_connection_failures = 0
                        else:
                            logger.info("Reusing existing session for next loop in hope of transient recovery.")

                if client is None:
                    logger.warning("Torrent client connection failed. Retrying next loop.")
                    time.sleep(poll_interval)
                    continue

                process_state_machine(config, state, client)

                occupied_space = get_current_occupied_space(state["active_jobs"])
                max_active_downloads = config["settings"].get("max_active_downloads", 3)
                active_downloads = sum(1 for job in state["active_jobs"].values() if job["state"] == "added_local")

                watch_dir = config["paths"]["watch_dir"]
                if not os.path.exists(watch_dir):
                    os.makedirs(watch_dir, exist_ok=True)

                torrent_files = sorted(glob.glob(os.path.join(watch_dir, "*.torrent")))
                magnet_files = sorted(glob.glob(os.path.join(watch_dir, "*.magnet")))
                active_paths = {job["torrent_file"] for job in state["active_jobs"].values()}

                # Process magnet files
                candidate_magnets = [f for f in magnet_files if f not in active_paths]
                for magnet_file in candidate_magnets:
                    try:
                        with open(magnet_file, "r", encoding="utf-8") as f_mag:
                            magnet_link = f_mag.read().strip()
                        info_hash_match = re.search(r'xt=urn:btih:([a-fA-F0-9]{32,40})', magnet_link)
                        if not info_hash_match:
                            logger.error(f"Failed to parse infohash from magnet file {magnet_file}")
                            continue
                        info_hash = info_hash_match.group(1).lower()
                        name_match = re.search(r'dn=([^&]+)', magnet_link)
                        parsed_name = urllib.parse.unquote(name_match.group(1)) if name_match else os.path.splitext(os.path.basename(magnet_file))[0]
                        if info_hash in state["active_jobs"]:
                            continue
                        logger.info(f"Adding magnet link for {parsed_name} (Hash: {info_hash}) to client.")
                        client.add_torrent(
                            torrent_bytes=magnet_link,
                            save_path=config["paths"]["local_save_path"],
                            category=config["paths"].get("category"),
                            paused=True,
                        )
                        state["active_jobs"][info_hash] = {
                            "torrent_file": magnet_file,
                            "name": parsed_name,
                            "size": 0,
                            "is_multi_file": False,
                            "tracker": "Public",
                            "state": "added_local",
                            "added_time": time.time(),
                            "completion_time": None,
                            "priorities_configured": False,
                            "is_magnet": True,
                        }
                        save_state(state)
                    except Exception as e:
                        logger.error(f"Failed to process magnet file {magnet_file}: {e}")

                candidate_files = [f for f in torrent_files if f not in active_paths]
                if candidate_files:
                    logger.info(f"Found {len(candidate_files)} new candidate torrent(s) in watch directory.")

                for torrent_file in candidate_files:
                    try:
                        details = get_torrent_details(torrent_file)
                    except Exception as e:
                        logger.error(f"Failed to parse torrent file {torrent_file}: {e}")
                        continue

                    info_hash = details["info_hash"]
                    size = details["size"]
                    if info_hash in state["active_jobs"]:
                        continue

                    sorted_files = sorted(details["files"], key=lambda x: x["id"])
                    batches = _build_batches_from_files(sorted_files, ssd_limit)

                    exists_in_qb = False
                    existing_torrent_info = None
                    try:
                        all_qb_torrents = client.get_torrents_info()
                        res = [t for t in all_qb_torrents if t["hash"] == info_hash]
                        if not res:
                            remote_save_path_cfg = config["paths"]["remote_save_path"]
                            res = [t for t in all_qb_torrents if t["name"] == details["name"] and is_same_or_parent_path(remote_save_path_cfg, t.get("save_path", ""))]
                        if res:
                            exists_in_qb = True
                            existing_torrent_info = res[0]
                    except Exception as e:
                        logger.warning(f"Error checking if torrent exists in client: {e}")

                    if exists_in_qb:
                        remote_save_path = config["paths"]["remote_save_path"]
                        save_path = existing_torrent_info.get("save_path", "")
                        if is_same_or_parent_path(remote_save_path, save_path):
                            try:
                                _archive_file_safely(torrent_file, config)
                                logger.info(f"Torrent {details['name']} is already seeding from remote path ({save_path}). Archived.")
                            except Exception as e:
                                logger.error(f"Failed to archive completed torrent {torrent_file}: {e}")

                            _, remote_save_path = get_job_remote_paths(config, details["name"])
                            racing_injector.enqueue_injection(
                                info_hash=info_hash,
                                details=details,
                                sorted_files=sorted_files,
                                remote_save_path=remote_save_path,
                                config=config,
                                origin_client=client,
                            )
                            tg_queue.enqueue_already_seeding(
                                info_hash=info_hash,
                                name=details["name"],
                                size=details["size"],
                                tracker=details.get("tracker", "Unknown"),
                                racing_hashes=None,
                            )
                            continue

                        logger.info(f"Torrent {details['name']} already exists locally in client. Re-associating with state tracking.")
                        priorities_ok = False
                        try:
                            try:
                                client.pause_torrent(info_hash)
                            except Exception:
                                pass
                            try:
                                all_ids = [f["id"] for f in sorted_files]
                                batch_ids = batches[0]["file_ids"]
                                client.set_file_priorities(info_hash, all_ids, 0)
                                client.set_file_priorities(info_hash, batch_ids, 1)
                                priorities_ok = True
                            except Exception as e:
                                logger.warning(f"Failed to set batch priorities during re-association: {e}")
                            try:
                                client.resume_torrent(info_hash)
                            except Exception:
                                pass

                            state["active_jobs"][info_hash] = {
                                "torrent_file": torrent_file,
                                "name": details["name"],
                                "size": size,
                                "is_multi_file": details["is_multi_file"],
                                "tracker": details.get("tracker", "Unknown"),
                                "state": "added_local",
                                "added_time": time.time(),
                                "completion_time": None,
                                "batches": batches,
                                "current_batch_index": 0,
                                "priorities_configured": priorities_ok,
                            }
                            tg_queue.enqueue_status(state["active_jobs"][info_hash], info_hash)
                            save_state(state)
                            occupied_space += batches[0]["size"]
                            active_downloads += 1
                            continue
                        except Exception as e:
                            logger.error(f"Failed to re-associate torrent {details['name']}: {e}")
                            continue

                    _, remote_save_path = get_job_remote_paths(config, details["name"])
                    fuse_target = os.path.join(remote_save_path, details["name"])
                    if os.path.exists(fuse_target):
                        logger.info(f"Torrent {details['name']} already exists on FUSE mount ({fuse_target}). Adding to client in remote seeding state.")
                        add_result = client.add_torrent(
                            torrent_bytes=torrent_file,
                            save_path=remote_save_path,
                            category=config["paths"].get("remote_category", "remote"),
                            is_skip_checking=True,
                            paused=True,
                        )
                        if add_result == AddTorrentResult.FAILED:
                            logger.error(f"Failed to add existing FUSE torrent {details['name']} to client.")
                            continue
                        if add_result == AddTorrentResult.EXISTS_WRONG:
                            if client.set_location(info_hash, remote_save_path):
                                logger.info(f"Relocated existing torrent {info_hash} to {remote_save_path}.")
                            else:
                                logger.warning(f"EXISTS_WRONG and set_location failed for {info_hash}; skipping.")
                                continue

                        try:
                            client.resume_torrent(info_hash)
                        except Exception:
                            pass

                        state["active_jobs"][info_hash] = {
                            "torrent_file": torrent_file,
                            "name": details["name"],
                            "size": size,
                            "is_multi_file": details["is_multi_file"],
                            "tracker": details.get("tracker", "Unknown"),
                            "state": "added_remote",
                            "added_time": time.time(),
                            "completion_time": time.time(),
                            "added_remote_time": time.time(),
                            "priorities_configured": True,
                        }
                        try:
                            _archive_file_safely(torrent_file, config)
                        except Exception as e:
                            logger.error(f"Failed to archive {torrent_file}: {e}")
                        save_state(state)

                        racing_injector.enqueue_injection(
                            info_hash=info_hash,
                            details=details,
                            sorted_files=sorted_files,
                            remote_save_path=remote_save_path,
                            config=config,
                            origin_client=client,
                        )
                        tg_queue.enqueue_already_seeding(
                            info_hash=info_hash,
                            name=details["name"],
                            size=details["size"],
                            tracker=details.get("tracker", "Unknown"),
                            racing_hashes=None,
                        )
                        continue

                    if active_downloads >= max_active_downloads:
                        logger.info(f"Reached max concurrent downloads limit ({active_downloads}/{max_active_downloads}). Waiting to schedule {details['name']}.")
                        continue

                    first_batch_size = batches[0]["size"]
                    if occupied_space + first_batch_size > ssd_limit:
                        logger.info(f"Torrent {details['name']} (First Batch: {first_batch_size / (1024**3):.2f} GB) does not fit in remaining SSD space (Free: {(ssd_limit - occupied_space) / (1024**3):.2f} GB). Waiting.")
                        continue

                    physical_free = get_physical_free_space(config["paths"]["local_save_path"])
                    if physical_free < first_batch_size:
                        logger.warning(f"Torrent {details['name']} fits in budget, but physical disk space is too low! Required: {first_batch_size / (1024**3):.2f} GB, Free: {physical_free / (1024**3):.2f} GB. Waiting.")
                        continue

                    logger.info(f"Scheduling torrent {details['name']} (Total: {size / (1024**3):.2f} GB, Batch 1: {first_batch_size / (1024**3):.2f} GB) under SSD limit ({ssd_limit / (1024**3):.2f} GB)")
                    try:
                        add_result = client.add_torrent(
                            torrent_bytes=torrent_file,
                            save_path=config["paths"]["local_save_path"],
                            category=config["paths"].get("category"),
                            paused=True,
                        )
                        if add_result == AddTorrentResult.FAILED:
                            logger.error(f"Failed to add torrent {details['name']} to client. Will retry on next loop.")
                            continue

                        state["active_jobs"][info_hash] = {
                            "torrent_file": torrent_file,
                            "name": details["name"],
                            "size": size,
                            "is_multi_file": details["is_multi_file"],
                            "tracker": details.get("tracker", "Unknown"),
                            "state": "added_local",
                            "added_time": time.time(),
                            "completion_time": None,
                            "batches": batches,
                            "current_batch_index": 0,
                            "priorities_configured": False,
                        }
                        save_state(state)
                        occupied_space += first_batch_size
                        active_downloads += 1

                        try:
                            try:
                                client.pause_torrent(info_hash)
                            except Exception:
                                pass
                            all_ids = [f["id"] for f in sorted_files]
                            batch_ids = batches[0]["file_ids"]
                            client.set_file_priorities(info_hash, all_ids, 0)
                            client.set_file_priorities(info_hash, batch_ids, 1)
                            client.resume_torrent(info_hash)
                            state["active_jobs"][info_hash]["priorities_configured"] = True
                            save_state(state)
                            logger.info(f"Successfully configured and started torrent {details['name']} (Batch 1)")
                        except Exception as config_err:
                            logger.warning(f"Torrent {details['name']} was added, but failed to configure priorities: {config_err}. Will retry in state machine.")

                        tg_queue.enqueue_status(state["active_jobs"][info_hash], info_hash)
                    except Exception as e:
                        logger.error(f"Failed to add torrent {torrent_file} to client: {e}")

            except Exception as e:
                logger.error(f"Unexpected error in main daemon loop: {e}", exc_info=True)
            time.sleep(poll_interval)
    finally:
        logger.info("Shutting down background workers...")
        try:
            racing_injector.stop()
        except Exception as e:
            logger.error(f"racing_injector.stop failed: {e}")
        try:
            tg_queue.stop()
        except Exception as e:
            logger.error(f"tg_queue.stop failed: {e}")


if __name__ == "__main__":
    main()

