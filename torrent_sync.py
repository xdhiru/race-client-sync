import os
import sys
import time
import json
import glob
import shutil
import logging
import subprocess
import threading
import hashlib
import re
import urllib.request
import urllib.parse

import clients
from utils.config import load_config
from utils.state import load_json_state, save_json_state, get_tracker_mapping, remove_tracker_mapping
from utils.torrent import get_torrent_details, get_tracker_domain, get_torrent_file_structure, bdecode, bencode
from services.prowlarr import get_prowlarr_indexer_id, search_prowlarr, download_torrent_bytes
from services.telegram import update_telegram_status, send_telegram_notification, format_size

# Configure Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

CONFIG_PATH = "config.toml"
STATE_PATH = "torrent_sync_state.json"

def load_state():
    return load_json_state(STATE_PATH)

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

def run_rclone_move_async(info_hash, cmd):
    def target():
        try:
            rclone_status[info_hash] = "running"
            logger.info(f"Starting rclone process for {info_hash}: {' '.join(cmd)}")
            subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)
            logger.info(f"rclone process succeeded for {info_hash}")
            rclone_status[info_hash] = "success"
        except subprocess.CalledProcessError as e:
            logger.error(f"rclone process failed for {info_hash}: Exit code {e.returncode}. Error:\n{e.stderr}")
            rclone_status[info_hash] = f"failed: {e.stderr}"
        except Exception as e:
            logger.error(f"Error running rclone for {info_hash}: {e}")
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
                    occupied += job["size"]
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
        total, used, free = shutil.disk_usage(path)
        return free
    except Exception as e:
        logger.error(f"Failed to check disk usage for path {path}: {e}")
        return 0

def get_job_remote_paths(config, torrent_name):
    remote_target = config["rclone"]["remote"]
    remote_save_path = config["paths"]["remote_save_path"]
    
    if re.search(r'[sS]\d+[\s._-]*[eE]\d+', torrent_name):
        remote_target = f"{remote_target.rstrip('/')}/unsorted/"
        remote_save_path = f"{remote_save_path.rstrip('/')}/unsorted/"
        
    return remote_target, remote_save_path

# ==============================================================================
# Main Process Loop
# ==============================================================================

def process_state_machine(config, state, client):
    active_jobs = state["active_jobs"]
    wait_time_minutes = config["settings"].get("wait_time_minutes", 5.0)
    wait_time_seconds = wait_time_minutes * 60
    
    try:
        torrents_list = client.get_torrents_info()
        torrents_by_hash = {t["hash"]: t for t in torrents_list}
    except Exception as e:
        logger.error(f"Failed to fetch torrent list from client: {e}")
        return

    for info_hash in list(active_jobs.keys()):
        job = active_jobs[info_hash]
        current_state = job["state"]
        
        # --- STATE: added_local ---
        if current_state == "added_local":
            t = torrents_by_hash.get(info_hash)
            if not t:
                if not os.path.exists(job["torrent_file"]):
                    logger.warning(f"Torrent file {job['torrent_file']} missing. Removing from state.")
                    active_jobs.pop(info_hash, None)
                    save_state(state)
                continue
                
            if not job.get("priorities_configured", False):
                logger.info(f"Enforcing/retrying batch priorities configuration for {job['name']}.")
                try:
                    try:
                        client.pause_torrent(info_hash)
                    except Exception:
                        pass
                        
                    files = client.get_torrent_files(info_hash)
                    if files:
                        all_ids = [f["id"] for f in files]
                        curr_idx = job.get("current_batch_index", 0)
                        batch_ids = job["batches"][curr_idx]["file_ids"]
                        client.set_file_priorities(info_hash, all_ids, 0)
                        client.set_file_priorities(info_hash, batch_ids, 1)
                        client.resume_torrent(info_hash)
                        
                        job["priorities_configured"] = True
                        save_state(state)
                        logger.info(f"Successfully configured batch priorities for {job['name']}.")
                except Exception as e:
                    logger.warning(f"Failed to configure batch priorities for {job['name']} in state machine: {e}. Will retry.")
                    
            if "batches" in job and "current_batch_index" in job:
                try:
                    files = client.get_torrent_files(info_hash)
                    if files:
                        curr_idx = job["current_batch_index"]
                        batch_ids = set(job["batches"][curr_idx]["file_ids"])
                        
                        batch_completed_size = 0
                        for f in files:
                            if f["id"] in batch_ids:
                                f_progress = f.get("progress", 1.0)
                                batch_completed_size += int(f["size"] * f_progress)
                                
                        batch_total_size = job["batches"][curr_idx]["size"]
                        batch_progress = batch_completed_size / batch_total_size if batch_total_size > 0 else 1.0
                        
                        if batch_progress >= 1.0:
                            logger.info(f"Batch {curr_idx + 1} completed downloading for {job['name']}. Starting cooldown.")
                            job["state"] = "waiting_5min"
                            job["completion_time"] = time.time()
                            update_telegram_status(config, job, info_hash)
                            save_state(state)
                    else:
                        logger.warning(f"No files returned by client for torrent {job['name']}. Waiting.")
                except Exception as e:
                    logger.warning(f"Failed to check batch progress for {job['name']}: {e}")
            else:
                if t["progress"] >= 1.0:
                    logger.info(f"Torrent completed downloading locally: {job['name']}. Starting cooldown.")
                    job["state"] = "waiting_5min"
                    job["completion_time"] = time.time()
                    update_telegram_status(config, job, info_hash)
                    save_state(state)

        # --- STATE: waiting_5min ---
        elif current_state == "waiting_5min":
            elapsed = time.time() - job["completion_time"]
            if elapsed >= wait_time_seconds:
                logger.info(f"Cooldown completed for {job['name']}. Deleting local torrent from qBittorrent and starting rclone move.")
                try:
                    client.delete_torrent(info_hash, delete_files=False)
                    job["state"] = "rclone_moving"
                    job["rclone_retries"] = 0
                    job["move_start_time"] = time.time()
                    save_state(state)
                except Exception as e:
                    logger.error(f"Failed to delete torrent {info_hash} from client: {e}")

        # --- STATE: rclone_moving ---
        elif current_state == "rclone_moving":
            status = rclone_status.get(info_hash)
            
            if status is None:
                max_jobs = config.get("rclone", {}).get("max_parallel_jobs", 2)
                running_jobs = sum(1 for val in rclone_status.values() if val == "running")
                if running_jobs >= max_jobs:
                    logger.debug(f"rclone move for {job['name']} waiting. Current running: {running_jobs}/{max_jobs}")
                    continue
                    
                local_dir = config["paths"]["local_save_path"]
                remote_target, remote_save_path = get_job_remote_paths(config, job["name"])
                rclone_transfers = config.get("rclone", {}).get("transfers", 2)
                
                if "batches" in job and "current_batch_index" in job:
                    batch = job["batches"][job["current_batch_index"]]
                    cmd = ["rclone", "move", local_dir, remote_target, f"--transfers={rclone_transfers}"]
                    for path in batch["file_paths"]:
                        cmd.extend(["--include", escape_rclone_glob(path)])
                else:
                    if job["is_multi_file"]:
                        src = os.path.join(local_dir, job["name"])
                        dest = f"{remote_target.rstrip('/')}/{job['name']}"
                        cmd = ["rclone", "move", src, dest, f"--transfers={rclone_transfers}"]
                    else:
                        src = os.path.join(local_dir, job["name"])
                        dest = f"{remote_target.rstrip('/')}/{job['name']}"
                        cmd = ["rclone", "moveto", src, dest, f"--transfers={rclone_transfers}"]
                    
                run_rclone_move_async(info_hash, cmd)
                
            elif status == "success":
                logger.info(f"rclone move successful for {job['name']}. Cleaning up local torrent folder/file.")
                
                local_path = os.path.join(config["paths"]["local_save_path"], job["name"])
                if os.path.exists(local_path):
                    try:
                        if os.path.isdir(local_path):
                            shutil.rmtree(local_path)
                            logger.info(f"Cleaned up local folder: {local_path}")
                        else:
                            os.remove(local_path)
                            logger.info(f"Cleaned up local file: {local_path}")
                    except Exception as e:
                        logger.error(f"Failed to clean up local path {local_path}: {e}")
                
                next_idx = job.get("current_batch_index", 0) + 1 if "batches" in job else 1
                total_batches = len(job["batches"]) if "batches" in job else 1
                
                if next_idx < total_batches:
                    logger.info(f"Completed batch {next_idx}/{total_batches} for {job['name']}. Re-adding torrent for batch {next_idx + 1}.")
                    try:
                        client.add_torrent(
                            torrent_bytes=job["torrent_file"],
                            save_path=config["paths"]["local_save_path"],
                            category=config["paths"].get("category"),
                            paused=True
                        )
                    except Exception as e:
                        err_msg_lower = str(e).lower()
                        if "torrent hash" in err_msg_lower or "409" in err_msg_lower or "conflict" in err_msg_lower:
                            logger.info(f"Torrent {job['name']} already exists when re-adding for next batch. Proceeding.")
                        else:
                            raise e
                    try:
                        client.pause_torrent(info_hash)
                    except Exception:
                        pass
                        
                    files_list = []
                    for _ in range(10):
                        try:
                            files_list = client.get_torrent_files(info_hash)
                            if files_list:
                                break
                        except Exception:
                            pass
                        time.sleep(1)
                        
                    if not files_list:
                        raise ValueError("Could not load file list from client after re-adding paused torrent")
                        
                    all_ids = [f["id"] for f in files_list]
                    next_batch_ids = job["batches"][next_idx]["file_ids"]
                    client.set_file_priorities(info_hash, all_ids, 0)
                    client.set_file_priorities(info_hash, next_batch_ids, 1)
                    client.resume_torrent(info_hash)
                    
                    job["current_batch_index"] = next_idx
                    job["state"] = "added_local"
                    job["added_time"] = time.time()
                    job["completion_time"] = None
                    save_state(state)
                else:
                    logger.info(f"All batches completed for {job['name']}. Entering FUSE mount wait state.")
                    job["state"] = "fuse_wait"
                    job["move_completed_time"] = time.time()
                    save_state(state)
                    
                rclone_status.pop(info_hash, None)
                rclone_threads.pop(info_hash, None)
                
            elif status.startswith("failed") or status.startswith("error"):
                logger.error(f"rclone move failed for {job['name']}: {status}")
                rclone_status.pop(info_hash, None)
                rclone_threads.pop(info_hash, None)
                
                max_retries = config.get("rclone", {}).get("max_retries", 3)
                retries = job.get("rclone_retries", 0) + 1
                job["rclone_retries"] = retries
                
                if retries <= max_retries:
                    logger.info(f"Retrying rclone move for {job['name']} (attempt {retries}/{max_retries}) in 30s...")
                    job["state"] = "waiting_5min"
                    job["completion_time"] = time.time() - wait_time_seconds + 30
                    save_state(state)
                else:
                    logger.error(f"Rclone move failed after {max_retries} attempts for {job['name']}. Giving up.")
                    job["state"] = "rclone_failed"
                    job["error_msg"] = status
                    update_telegram_status(config, job, info_hash)
                    save_state(state)

        # --- STATE: fuse_wait ---
        elif current_state == "fuse_wait":
            fuse_cooldown = config["settings"].get("fuse_cooldown_seconds", 15)
            elapsed = time.time() - job["move_completed_time"]
            if elapsed >= fuse_cooldown:
                logger.info(f"FUSE cooldown of {fuse_cooldown}s completed for {job['name']}. Adding to remote seeding path directly.")
                remote_target, remote_save_path = get_job_remote_paths(config, job["name"])
                
                try:
                    try:
                        client.add_torrent(
                            torrent_bytes=job["torrent_file"],
                            save_path=remote_save_path,
                            category=config["paths"].get("remote_category", "remote"),
                            is_skip_checking=True
                        )
                    except Exception as e:
                        err_msg_lower = str(e).lower()
                        if "torrent hash" in err_msg_lower or "409" in err_msg_lower or "conflict" in err_msg_lower:
                            logger.info(f"Torrent {job['name']} already exists on remote path. Proceeding.")
                        else:
                            raise e
                    job["state"] = "added_remote"
                    job["readd_start_time"] = time.time()
                    save_state(state)
                    logger.info(f"Successfully re-added torrent {job['name']} to remote path.")
                except Exception as e:
                    logger.error(f"Failed to re-add torrent {job['name']} to remote path: {e}")

        # --- STATE: added_remote ---
        elif current_state == "added_remote":
            t = torrents_by_hash.get(info_hash)
            if not t:
                continue
                
            is_checking = t["state"].startswith("checking") or "checking" in t["state"]
            if t["progress"] == 1.0 and not is_checking:
                logger.info(f"Torrent successfully seeding from remote path: {job['name']}. Archiving .torrent file.")
                
                completed_dir = config["paths"].get("completed_dir")
                if not completed_dir:
                    watch_dir = config["paths"]["watch_dir"]
                    completed_dir = os.path.join(watch_dir, "completed")
                
                try:
                    os.makedirs(completed_dir, exist_ok=True)
                    dest_file = os.path.join(completed_dir, os.path.basename(job["torrent_file"]))
                    
                    if os.path.exists(dest_file):
                        base, ext = os.path.splitext(dest_file)
                        dest_file = f"{base}_{int(time.time())}{ext}"
                        
                    shutil.move(job["torrent_file"], dest_file)
                    logger.info(f"Moved {job['torrent_file']} to {dest_file}")
                except Exception as e:
                    logger.error(f"Failed to archive torrent file {job['torrent_file']}: {e}")
                
                job["state"] = "completed"
                job["seeding_completed_time"] = time.time()
                update_telegram_status(config, job, info_hash)
                
                racing_hash = get_tracker_mapping(info_hash)
                migration_ok = True
                if racing_hash:
                    job["linked_tracker"] = "Racing"
                    migration_ok = False
                    logger.info(f"Local torrent {info_hash} has mapping to Racing torrent {racing_hash}. Exporting Racing torrent from racing client and adding to normal client pointing to remote FUSE path.")
                    try:
                        racing_client = get_racing_client(config)
                        if racing_client:
                            remote_target, remote_save_path = get_job_remote_paths(config, job["name"])
                            
                            racing_torrent_bytes = None
                            try:
                                racing_torrent_bytes = racing_client.export_torrent(racing_hash)
                            except NotImplementedError:
                                logger.info(f"Export not supported by racing client. Attempting Prowlarr fallback search for racing hash {racing_hash}.")
                            except Exception as export_err:
                                logger.warning(f"Failed to export Racing torrent {racing_hash} from racing client: {export_err}. Retrying with Prowlarr fallback.")

                            if not racing_torrent_bytes:
                                p_config = config.get("prowlarr", {})
                                racing_indexer_name = p_config.get("racing_indexer_name")
                                if racing_indexer_name:
                                    prowlarr_url = p_config["url"]
                                    prowlarr_api_key = p_config["api_key"]
                                    try:
                                        racing_indexer_id = get_prowlarr_indexer_id(prowlarr_url, prowlarr_api_key, racing_indexer_name)
                                        if racing_indexer_id:
                                            sanitized_query = re.sub(r'[\s._-]', ' ', job["name"])
                                            search_results = search_prowlarr(prowlarr_url, prowlarr_api_key, racing_indexer_id, sanitized_query)
                                            for res in search_results:
                                                res_hash = res.get("infoHash", "").lower()
                                                if res_hash == racing_hash.lower():
                                                    download_url = res.get("downloadUrl")
                                                    if download_url:
                                                        logger.info(f"Found matching racing torrent on Prowlarr. Downloading: {res.get('title')}")
                                                        racing_torrent_bytes = download_torrent_bytes(download_url, prowlarr_api_key)
                                                        break
                                        else:
                                            logger.warning(f"Could not resolve indexer ID for racing indexer '{racing_indexer_name}'")
                                    except Exception as p_err:
                                        logger.error(f"Error during Prowlarr fallback search: {p_err}")
                                else:
                                    logger.error(f"Failed to export racing torrent {racing_hash} and no racing_indexer_name configured in config.")

                            if racing_torrent_bytes:
                                try:
                                    client.add_torrent(
                                        torrent_bytes=racing_torrent_bytes,
                                        save_path=remote_save_path,
                                        category=config["paths"].get("remote_category", "remote"),
                                        is_skip_checking=True
                                    )
                                    logger.info(f"Successfully added Racing torrent {racing_hash} to normal client pointing to remote FUSE path: {remote_save_path}")
                                    migration_ok = True
                                    remove_tracker_mapping(info_hash)
                                except Exception as add_err:
                                    err_msg_lower = str(add_err).lower()
                                    if "torrent hash" in err_msg_lower or "409" in err_msg_lower or "conflict" in err_msg_lower:
                                        logger.info(f"Racing torrent {racing_hash} already exists in normal client.")
                                        migration_ok = True
                                        remove_tracker_mapping(info_hash)
                                    else:
                                        logger.error(f"Failed to add Racing torrent to normal client: {add_err}")
                                        
                                if migration_ok:
                                    racing_settings = config.get("racing_settings") or {}
                                    racing_completed_cat = racing_settings.get("completed_category", "processed")
                                    try:
                                        racing_client.set_category(racing_hash, racing_completed_cat)
                                        logger.info(f"Assigned completed category '{racing_completed_cat}' to racing torrent {racing_hash}")
                                    except Exception as cat_err:
                                        logger.warning(f"Failed to set category for racing torrent {racing_hash}: {cat_err}")
                            else:
                                logger.error(f"Could not retrieve racing torrent bytes for {racing_hash}")
                        else:
                            logger.error(f"Failed to connect to racing client to export torrent {racing_hash}.")
                    except Exception as e:
                        logger.error(f"Error handling Racing torrent migration: {e}")

                if migration_ok:
                    active_jobs.pop(info_hash, None)
                    save_state(state)

def sweep_dangling_mappings(config, client):
    if not os.path.exists(MAPPINGS_PATH):
        return
    try:
        with open(MAPPINGS_PATH, "r", encoding="utf-8") as f:
            mappings = json.load(f)
        if not mappings:
            return
    except Exception as e:
        logger.error(f"Failed to read mappings file for sweep: {e}")
        return

    racing_client = None
    try:
        racing_client = get_racing_client(config)
    except Exception as e:
        logger.error(f"Failed to connect to racing client for sweep: {e}")
        return

    if not racing_client:
        return

    try:
        normal_torrents = client.get_torrents_info()
        normal_torrents_by_hash = {t["hash"]: t for t in normal_torrents}
    except Exception as e:
        logger.error(f"Failed to fetch normal client torrents for sweep: {e}")
        return

    for local_hash, racing_hash in list(mappings.items()):
        local_hash_clean = local_hash.lower()
        if len(local_hash_clean) == 80:
            try:
                local_hash_clean = bytes.fromhex(local_hash_clean).decode('utf-8').lower()
            except Exception:
                pass

        t = normal_torrents_by_hash.get(local_hash_clean)
        if t:
            is_checking = t["state"].startswith("checking") or "checking" in t["state"]
            
            remote_save_path = config["paths"]["remote_save_path"]
            t_save_path_normalized = os.path.normpath(t["save_path"]).lower()
            remote_save_path_normalized = os.path.normpath(remote_save_path).lower()
            
            is_on_fuse = (
                t_save_path_normalized == remote_save_path_normalized or
                t_save_path_normalized.startswith(remote_save_path_normalized + os.sep)
            )
            
            if t["progress"] == 1.0 and not is_checking and is_on_fuse:
                logger.info(f"Sweep: Found completed Local torrent {local_hash_clean} in normal client with pending Tracker mapping {racing_hash}. Migrating now.")
                remote_save_path = t["save_path"]
                
                racing_torrent_bytes = None
                try:
                    racing_torrent_bytes = racing_client.export_torrent(racing_hash)
                except NotImplementedError:
                    logger.info(f"Sweep: Export not supported by racing client. Attempting Prowlarr fallback search for racing hash {racing_hash}.")
                except Exception as e:
                    logger.warning(f"Sweep: Failed to export Racing torrent {racing_hash} from racing client: {e}. Retrying with Prowlarr fallback.")

                if not racing_torrent_bytes:
                    p_config = config.get("prowlarr", {})
                    racing_indexer_name = p_config.get("racing_indexer_name")
                    if racing_indexer_name:
                        prowlarr_url = p_config["url"]
                        prowlarr_api_key = p_config["api_key"]
                        try:
                            racing_indexer_id = get_prowlarr_indexer_id(prowlarr_url, prowlarr_api_key, racing_indexer_name)
                            if racing_indexer_id:
                                sanitized_query = re.sub(r'[\s._-]', ' ', t["name"])
                                search_results = search_prowlarr(prowlarr_url, prowlarr_api_key, racing_indexer_id, sanitized_query)
                                for res in search_results:
                                    res_hash = res.get("infoHash", "").lower()
                                    if res_hash == racing_hash.lower():
                                        download_url = res.get("downloadUrl")
                                        if download_url:
                                            logger.info(f"Sweep: Found matching racing torrent on Prowlarr. Downloading: {res.get('title')}")
                                            racing_torrent_bytes = download_torrent_bytes(download_url, prowlarr_api_key)
                                            break
                            else:
                                logger.warning(f"Sweep: Could not resolve indexer ID for racing indexer '{racing_indexer_name}'")
                        except Exception as p_err:
                            logger.error(f"Sweep: Error during Prowlarr fallback search: {p_err}")
                    else:
                        logger.error(f"Sweep: Failed to export racing torrent {racing_hash} and no racing_indexer_name configured in config.")

                if racing_torrent_bytes:
                    try:
                        migration_ok = False
                        try:
                            client.add_torrent(
                                torrent_bytes=racing_torrent_bytes,
                                save_path=remote_save_path,
                                category=config["paths"].get("remote_category", "remote"),
                                is_skip_checking=True
                            )
                            logger.info(f"Sweep: Successfully added Racing torrent {racing_hash} to normal client pointing to FUSE: {remote_save_path}")
                            remove_tracker_mapping(local_hash)
                            migration_ok = True
                        except Exception as add_err:
                            err_msg_lower = str(add_err).lower()
                            if "torrent hash" in err_msg_lower or "409" in err_msg_lower or "conflict" in err_msg_lower:
                                logger.info(f"Sweep: Racing torrent {racing_hash} already exists in normal client.")
                                remove_tracker_mapping(local_hash)
                                migration_ok = True
                            else:
                                logger.error(f"Sweep: Failed to add Racing torrent {racing_hash} to normal client: {add_err}")
                                migration_ok = False
                                
                        if migration_ok:
                            racing_settings = config.get("racing_settings", {})
                            racing_completed_cat = racing_settings.get("completed_category", "processed")
                            try:
                                racing_client.set_category(racing_hash, racing_completed_cat)
                            except Exception as cat_err:
                                logger.warning(f"Sweep: Failed to set category for racing torrent {racing_hash}: {cat_err}")
                    except Exception as e:
                        logger.error(f"Sweep: Failed to migrate racing torrent {racing_hash} for local torrent {local_hash_clean}: {e}")
                else:
                    logger.error(f"Sweep: Could not retrieve racing torrent bytes for {racing_hash}")

def main():
    logger.info("Starting sync loop daemon...")
    config = load_config()
    state = load_state()
    
    if "qbittorrent" not in config:
        logger.error("Missing qbittorrent configuration keys in config!")
        sys.exit(1)
        
    client = None
    consecutive_connection_failures = 0
    
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
            
            try:
                sweep_dangling_mappings(config, client)
            except Exception as e:
                logger.error(f"Error during dangling mappings sweep: {e}")
            
            occupied_space = get_current_occupied_space(state["active_jobs"])
            max_active_downloads = config["settings"].get("max_active_downloads", 3)
            active_downloads = sum(1 for job in state["active_jobs"].values() if job["state"] == "added_local")
            
            watch_dir = config["paths"]["watch_dir"]
            if not os.path.exists(watch_dir):
                os.makedirs(watch_dir, exist_ok=True)
                
            torrent_files = sorted(glob.glob(os.path.join(watch_dir, "*.torrent")))
            active_paths = {job["torrent_file"] for job in state["active_jobs"].values()}
            candidate_files = [f for f in torrent_files if f not in active_paths]
            
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
                batches = []
                current_batch_files = []
                current_batch_paths = []
                current_batch_size = 0
                
                for f in sorted_files:
                    f_size = f["size"]
                    f_id = f["id"]
                    f_name = f["name"]
                    
                    if current_batch_size + f_size > ssd_limit and current_batch_files:
                        batches.append({
                            "file_ids": current_batch_files,
                            "file_paths": current_batch_paths,
                            "size": current_batch_size
                        })
                        current_batch_files = [f_id]
                        current_batch_paths = [f_name]
                        current_batch_size = f_size
                    else:
                        current_batch_files.append(f_id)
                        current_batch_paths.append(f_name)
                        current_batch_size += f_size
                        
                if current_batch_files:
                    batches.append({
                        "file_ids": current_batch_files,
                        "file_paths": current_batch_paths,
                        "size": current_batch_size
                    })

                exists_in_qb = False
                existing_torrent_info = None
                try:
                    res = [t for t in client.get_torrents_info() if t["hash"] == info_hash]
                    if res:
                        exists_in_qb = True
                        existing_torrent_info = res[0]
                except Exception as e:
                    logger.warning(f"Error checking if torrent exists in client: {e}")
                    
                if exists_in_qb:
                    remote_save_path = config["paths"]["remote_save_path"]
                    save_path = existing_torrent_info.get("save_path", "")
                    
                    if is_same_or_parent_path(remote_save_path, save_path):
                        completed_dir = config["paths"].get("completed_dir")
                        if not completed_dir:
                            watch_dir = config["paths"]["watch_dir"]
                            completed_dir = os.path.join(watch_dir, "completed")
                            
                        try:
                            os.makedirs(completed_dir, exist_ok=True)
                            dest_file = os.path.join(completed_dir, os.path.basename(torrent_file))
                            if os.path.exists(dest_file):
                                base, ext = os.path.splitext(dest_file)
                                dest_file = f"{base}_{int(time.time())}{ext}"
                            shutil.move(torrent_file, dest_file)
                            logger.info(f"Torrent {details['name']} is already seeding from remote path ({save_path}). Archived .torrent to {dest_file}.")
                        except Exception as e:
                            logger.error(f"Failed to archive completed torrent {torrent_file}: {e}")
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
                            "priorities_configured": priorities_ok
                        }
                        update_telegram_status(config, state["active_jobs"][info_hash], info_hash)
                        save_state(state)
                        occupied_space += batches[0]["size"]
                        active_downloads += 1
                        continue
                    except Exception as e:
                        logger.error(f"Failed to re-associate torrent {details['name']}: {e}")
                        continue
                
                if active_downloads >= max_active_downloads:
                    logger.debug(f"Reached max concurrent downloads limit ({active_downloads}/{max_active_downloads}). Waiting to schedule {details['name']}.")
                    break
                    
                first_batch_size = batches[0]["size"]
                if occupied_space + first_batch_size <= ssd_limit:
                    physical_free = get_physical_free_space(config["paths"]["local_save_path"])
                    if physical_free < first_batch_size:
                        logger.warning(f"Torrent {details['name']} fits in budget, but physical disk space is too low! Required: {first_batch_size / (1024**3):.2f} GB, Free: {physical_free / (1024**3):.2f} GB. Waiting.")
                        break
                        
                    logger.info(f"Scheduling torrent {details['name']} (Total: {size / (1024**3):.2f} GB, Batch 1: {first_batch_size / (1024**3):.2f} GB) under SSD limit ({ssd_limit / (1024**3):.2f} GB)")
                    try:
                        try:
                            client.add_torrent(
                                torrent_bytes=torrent_file,
                                save_path=config["paths"]["local_save_path"],
                                category=config["paths"].get("category"),
                                paused=True
                            )
                        except Exception as e:
                            err_msg_lower = str(e).lower()
                            if "torrent hash" in err_msg_lower or "409" in err_msg_lower or "conflict" in err_msg_lower:
                                logger.info(f"Torrent {details['name']} already exists in client. Proceeding to track.")
                            else:
                                raise e
                                
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
                            "priorities_configured": False
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
                            
                        update_telegram_status(config, state["active_jobs"][info_hash], info_hash)
                        save_state(state)
                    except Exception as e:
                        logger.error(f"Failed to add torrent {torrent_file} to client: {e}")
                else:
                    logger.debug(f"Torrent {details['name']} (First Batch: {first_batch_size / (1024**3):.2f} GB) does not fit in remaining SSD space (Free: {(ssd_limit - occupied_space) / (1024**3):.2f} GB). Waiting.")
                    break
                    
        except Exception as e:
            logger.error(f"Unexpected error in main daemon loop: {e}", exc_info=True)
            
        time.sleep(poll_interval)

if __name__ == "__main__":
    main()
