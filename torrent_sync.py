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
import qbittorrentapi

# Configure Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("torrent_sync.log", encoding="utf-8")
    ]
)
logger = logging.getLogger("torrent_sync")

CONFIG_PATH = "config.json"
STATE_PATH = "torrent_sync_state.json"
MAPPINGS_PATH = "tracker_mappings.json"

# ==============================================================================
# Pure-Python Bencode Encoder and Decoder
# ==============================================================================

def bdecode(data):
    if not data:
        raise ValueError("Empty data")
    
    def decode_val(pos):
        if pos >= len(data):
            raise ValueError("Unexpected end of data")
        char = data[pos:pos+1]
        if char == b'i':
            end = data.find(b'e', pos)
            if end == -1:
                raise ValueError("Unterminated integer")
            return int(data[pos+1:end]), end + 1
        elif char.isdigit():
            colon = data.find(b':', pos)
            if colon == -1:
                raise ValueError("Invalid string length")
            length = int(data[pos:colon])
            start = colon + 1
            end = start + length
            if end > len(data):
                raise ValueError("String length exceeds data size")
            return data[start:end], end
        elif char == b'l':
            p = pos + 1
            lst = []
            while p < len(data) and data[p:p+1] != b'e':
                val, p = decode_val(p)
                lst.append(val)
            if p >= len(data) or data[p:p+1] != b'e':
                raise ValueError("Unterminated list")
            return lst, p + 1
        elif char == b'd':
            p = pos + 1
            dct = {}
            while p < len(data) and data[p:p+1] != b'e':
                key, p = decode_val(p)
                if not isinstance(key, bytes):
                    raise ValueError("Dictionary key must be bytes")
                val, p = decode_val(p)
                dct[key] = val
            if p >= len(data) or data[p:p+1] != b'e':
                raise ValueError("Unterminated dictionary")
            return dct, p + 1
        else:
            raise ValueError(f"Unknown type prefix: {char}")

    val, _ = decode_val(0)
    return val

def bencode(val):
    if isinstance(val, int):
        return f"i{val}e".encode('ascii')
    elif isinstance(val, bytes):
        return f"{len(val)}:".encode('ascii') + val
    elif isinstance(val, str):
        val_bytes = val.encode('utf-8')
        return f"{len(val_bytes)}:".encode('ascii') + val_bytes
    elif isinstance(val, list):
        return b"l" + b"".join(bencode(item) for item in val) + b"e"
    elif isinstance(val, dict):
        normalized = {}
        for k, v in val.items():
            k_bytes = k if isinstance(k, bytes) else k.encode('utf-8')
            normalized[k_bytes] = v
        sorted_items = sorted(normalized.items())
        parts = []
        for k_bytes, v in sorted_items:
            parts.append(f"{len(k_bytes)}:".encode('ascii') + k_bytes)
            parts.append(bencode(v))
        return b"d" + b"".join(parts) + b"e"
    raise TypeError(f"Unsupported type: {type(val)}")

def get_tracker_domain(announce_url):
    if not announce_url:
        return "Unknown"
    try:
        if isinstance(announce_url, bytes):
            announce_url = announce_url.decode('utf-8', errors='ignore')
        parsed = urllib.parse.urlparse(announce_url)
        hostname = parsed.hostname
        if hostname:
            return hostname
        # Fallback: simple string splitting if urlparse failed or returned empty hostname
        netloc = parsed.netloc or announce_url.split('/')[2]
        if ':' in netloc:
            netloc = netloc.split(':')[0]
        return netloc
    except Exception:
        return "Unknown"

def get_torrent_details(torrent_path):
    with open(torrent_path, "rb") as f:
        data = f.read()
    decoded = bdecode(data)
    info_dict = decoded[b'info']
    
    info_bytes = bencode(info_dict)
    info_hash = hashlib.sha1(info_bytes).hexdigest()
    
    name = info_dict[b'name'].decode('utf-8', errors='ignore')
    
    is_multi_file = b'files' in info_dict
    files_info = []
    if is_multi_file:
        size = 0
        for idx, file_dict in enumerate(info_dict[b'files']):
            f_size = file_dict[b'length']
            size += f_size
            rel_path = "/".join(p.decode('utf-8', errors='ignore') for p in file_dict[b'path'])
            full_rel_path = f"{name}/{rel_path}"
            files_info.append({
                "id": idx,
                "name": full_rel_path,
                "size": f_size
            })
    else:
        size = info_dict[b'length']
        files_info.append({
            "id": 0,
            "name": name,
            "size": size
        })
        
    # Extract tracker announce
    announce = decoded.get(b'announce', b'')
    if not announce and b'announce-list' in decoded:
        announce_list = decoded[b'announce-list']
        if announce_list and isinstance(announce_list, list) and announce_list[0]:
            announce = announce_list[0][0]
            
    tracker = get_tracker_domain(announce)
        
    return {
        "info_hash": info_hash,
        "name": name,
        "size": size,
        "is_multi_file": is_multi_file,
        "tracker": tracker,
        "files": files_info
    }

def get_job_remote_paths(config, torrent_name):
    # Default remote target and remote save path from config
    remote_target = config["rclone"]["remote"]
    remote_save_path = config["paths"]["remote_save_path"]
    
    # Check if it contains both season and episode, e.g. S01E01
    if re.search(r'[sS]\d+[\s._-]*[eE]\d+', torrent_name):
        remote_target = f"{remote_target.rstrip('/')}/unsorted/"
        remote_save_path = f"{remote_save_path.rstrip('/')}/unsorted/"
        
    return remote_target, remote_save_path

def format_size(size_bytes):
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size_bytes < 1024.0:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.2f} PB"

def call_telegram_api(method, payload, token):
    url = f"https://api.telegram.org/bot{token}/{method}"
    headers = {"Content-Type": "application/json"}
    data = json.dumps(payload).encode('utf-8')
    req = urllib.request.Request(url, data=data, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=10) as response:
            res = json.loads(response.read().decode('utf-8'))
            if not res.get("ok"):
                desc = res.get("description", "")
                if "message is not modified" not in desc:
                    logger.error(f"Telegram API error ({method}): {desc}")
            return res
    except Exception as e:
        logger.error(f"Telegram API call failed ({method}): {e}")
        return None

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

def format_time(ts):
    if not ts:
        return "Pending"
    try:
        return time.strftime('%H:%M:%S', time.localtime(ts))
    except Exception:
        return "Error"

def build_telegram_message_text(job, info_hash):
    name = job["name"]
    size = job["size"]
    size_formatted = format_size(size)
    tracker = job.get("tracker", "Unknown")
    state = job["state"]
    
    # Batch info helper
    batch_suffix = ""
    if "batches" in job and len(job["batches"]) > 1:
        curr = job.get("current_batch_index", 0) + 1
        total = len(job["batches"])
        batch_suffix = f" [{curr}/{total}]"
        
    # Determine overall status and icon
    if state == "rclone_failed":
        status_header = "🔴 <b>[SYNC_FAILED]</b>"
    elif state == "completed":
        status_header = "🟢 <b>[SYNC_COMPLETED]</b>"
    else:
        status_header = f"🟡 <b>[SYNC_IN_PROGRESS]{batch_suffix}</b>"
        
    # Stage statuses
    local_dl = "⚪ DL"
    cooldown = "⚪ Cooldown"
    rclone_move = "⚪ Move"
    re_add = "⚪ Seed"
    
    if state == "added_local":
        local_dl = "🔵 DL"
    elif state == "waiting_5min":
        local_dl = "🟢 DL"
        cooldown = "🔵 Cooldown"
    elif state == "rclone_moving":
        local_dl = "🟢 DL"
        cooldown = "🟢 Cooldown"
        rclone_move = "🔵 Move"
    elif state == "rclone_failed":
        local_dl = "🟢 DL"
        cooldown = "🟢 Cooldown"
        rclone_move = "🔴 Move"
    elif state == "fuse_wait":
        local_dl = "🟢 DL"
        cooldown = "🟢 Cooldown"
        rclone_move = "🟢 Move"
        re_add = "🔵 Seed"
    elif state == "added_remote":
        local_dl = "🟢 DL"
        cooldown = "🟢 Cooldown"
        rclone_move = "🟢 Move"
        re_add = "🔵 Seed"
    elif state == "completed":
        local_dl = "🟢 DL"
        cooldown = "🟢 Cooldown"
        rclone_move = "🟢 Move"
        re_add = "🟢 Seed"
        
    text = (
        f"{status_header} {name} ({size_formatted})\n"
        f"Hash: <code>{info_hash}</code> | Tracker: {tracker}\n"
        f"Steps: {local_dl} ➔ {cooldown} ➔ {rclone_move} ➔ {re_add}"
    )
    
    # Format times dynamically
    added_time_str = format_time(job.get("added_time"))
    completion_time_str = format_time(job.get("completion_time"))
    
    # DL Time
    times_parts = [f"DL ({added_time_str} ➔ {completion_time_str})"]
    
    # Move Time
    move_start = job.get("move_start_time")
    move_end = job.get("move_completed_time")
    if move_start or move_end:
        move_start_str = format_time(move_start)
        if state == "rclone_failed":
            move_end_str = "Failed"
        else:
            move_end_str = format_time(move_end)
        times_parts.append(f"Move ({move_start_str} ➔ {move_end_str})")
        
    # Seed Time
    readd_start = job.get("readd_start_time")
    seeding_completed = job.get("seeding_completed_time")
    if readd_start or seeding_completed:
        readd_start_str = format_time(readd_start)
        seeding_completed_str = format_time(seeding_completed)
        times_parts.append(f"Seed ({readd_start_str} ➔ {seeding_completed_str})")
        
    times_line = " | ".join(times_parts)
    text += f"\nTimes: {times_line}"
    
    error_msg = job.get("error_msg")
    if error_msg:
        escaped_error = error_msg.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
        text += f"\nError: <pre>{escaped_error}</pre>"
        
    return text

def update_telegram_status(config, job, info_hash):
    tg_config = config.get("telegram", {})
    if not tg_config.get("enabled", False):
        return
    
    bot_token = tg_config.get("bot_token")
    chat_id = tg_config.get("chat_id")
    if not bot_token or not chat_id:
        logger.warning("Telegram notification enabled but token or chat_id is missing.")
        return
        
    text = build_telegram_message_text(job, info_hash)
    message_id = job.get("telegram_message_id")
    
    if not message_id:
        payload = {
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "HTML"
        }
        res = call_telegram_api("sendMessage", payload, bot_token)
        if res and res.get("ok"):
            job["telegram_message_id"] = res["result"]["message_id"]
    else:
        payload = {
            "chat_id": chat_id,
            "message_id": message_id,
            "text": text,
            "parse_mode": "HTML"
        }
        call_telegram_api("editMessageText", payload, bot_token)


# ==============================================================================
# Helper functions for state configuration
# ==============================================================================

def load_config():
    if not os.path.exists(CONFIG_PATH):
        logger.error(f"Configuration file {CONFIG_PATH} not found!")
        sys.exit(1)
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)

def load_state():
    if os.path.exists(STATE_PATH):
        try:
            with open(STATE_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Failed to load state: {e}. Starting fresh.")
    return {"active_jobs": {}}

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
    except Exception as e:
        logger.error(f"Failed to remove mapping: {e}")

def save_state(state):
    try:
        with open(STATE_PATH, "w", encoding="utf-8") as f:
            json.dump(state, f, indent=2)
    except Exception as e:
        logger.error(f"Failed to save state: {e}")

def get_qb_client(config):
    qb_config = config["qbittorrent"]
    try:
        requests_args = {'timeout': (15, 45)}
        if "requests_args" in qb_config:
            user_args = qb_config["requests_args"]
            if isinstance(user_args, dict):
                requests_args.update(user_args)
                if "auth" in requests_args and isinstance(requests_args["auth"], list):
                    requests_args["auth"] = tuple(requests_args["auth"])
        client = qbittorrentapi.Client(
            host=qb_config["url"],
            username=qb_config["username"],
            password=qb_config["password"],
            REQUESTS_ARGS=requests_args
        )
        client.auth_log_in()
        return client
    except Exception as e:
        logger.error(f"Failed to connect to qBittorrent WebUI: {e}")
        return None

def get_racing_qb_client(config):
    qb_config = config.get("racing_qbittorrent")
    if not qb_config:
        return None
    try:
        requests_args = {'timeout': (15, 45)}
        if "requests_args" in qb_config:
            user_args = qb_config["requests_args"]
            if isinstance(user_args, dict):
                requests_args.update(user_args)
                if "auth" in requests_args and isinstance(requests_args["auth"], list):
                    requests_args["auth"] = tuple(requests_args["auth"])
        client = qbittorrentapi.Client(
            host=qb_config["url"],
            username=qb_config["username"],
            password=qb_config["password"],
            REQUESTS_ARGS=requests_args
        )
        client.auth_log_in()
        return client
    except Exception as e:
        logger.error(f"Failed to connect to racing qBittorrent: {e}")
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
# Main Process Loop
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

def process_state_machine(config, state, client):
    active_jobs = state["active_jobs"]
    wait_time_minutes = config["settings"].get("wait_time_minutes", 5.0)
    wait_time_seconds = wait_time_minutes * 60
    
    # Query qBittorrent once for all torrent info to save API roundtrips
    try:
        torrents_list = client.torrents_info()
        torrents_by_hash = {t.hash: t for t in torrents_list}
    except Exception as e:
        logger.error(f"Failed to fetch torrent list from qBittorrent: {e}")
        return

    # Process each active job
    for info_hash in list(active_jobs.keys()):
        job = active_jobs[info_hash]
        current_state = job["state"]
        
        # --- STATE: added_local ---
        if current_state == "added_local":
            t = torrents_by_hash.get(info_hash)
            if not t:
                # Torrent might have been removed or not added yet, check if file exists
                if not os.path.exists(job["torrent_file"]):
                    logger.warning(f"Torrent file {job['torrent_file']} missing. Removing from state.")
                    active_jobs.pop(info_hash, None)
                    save_state(state)
                continue
                
            # If priorities are not configured (due to connection drop during add), retry configuring them
            if not job.get("priorities_configured", False):
                logger.info(f"Enforcing/retrying batch priorities configuration for {job['name']}.")
                try:
                    # Enforce paused
                    try:
                        client.torrents_pause(torrent_hashes=info_hash)
                    except Exception:
                        pass
                        
                    files = client.torrents_files(torrent_hash=info_hash)
                    if files:
                        all_ids = [f["id"] for f in files]
                        curr_idx = job.get("current_batch_index", 0)
                        batch_ids = job["batches"][curr_idx]["file_ids"]
                        client.torrents_file_priority(torrent_hash=info_hash, file_ids=all_ids, priority=0)
                        client.torrents_file_priority(torrent_hash=info_hash, file_ids=batch_ids, priority=1)
                        client.torrents_resume(torrent_hashes=info_hash)
                        
                        job["priorities_configured"] = True
                        save_state(state)
                        logger.info(f"Successfully configured batch priorities for {job['name']}.")
                except Exception as e:
                    logger.warning(f"Failed to configure batch priorities for {job['name']} in state machine: {e}. Will retry.")
                    
            # If job has batches, check progress of files in the current batch
            if "batches" in job and "current_batch_index" in job:
                try:
                    files = client.torrents_files(torrent_hash=info_hash)
                    batch_file_ids = set(job["batches"][job["current_batch_index"]]["file_ids"])
                    batch_completed = True
                    for f in files:
                        if f["id"] in batch_file_ids:
                            if f["progress"] < 1.0:
                                batch_completed = False
                                break
                except Exception as e:
                    logger.error(f"Failed to check batch progress for {job['name']}: {e}")
                    batch_completed = False
            else:
                batch_completed = (t.progress == 1.0)
                
            if batch_completed:
                batch_idx_str = f" (Batch {job.get('current_batch_index', 0) + 1}/{len(job.get('batches', [1]))})" if "batches" in job else ""
                logger.info(f"Torrent batch completed downloading locally{batch_idx_str}: {job['name']}. Starting {wait_time_minutes}-minute delay.")
                job["state"] = "waiting_5min"
                job["completion_time"] = time.time()
                save_state(state)

        # --- STATE: waiting_5min ---
        elif current_state == "waiting_5min":
            elapsed = time.time() - job["completion_time"]
            if elapsed >= wait_time_seconds:
                logger.info(f"{wait_time_minutes}-minute delay completed for {job['name']}. Removing from qBittorrent (keeping files) and preparing to move.")
                try:
                    # Remove from qBittorrent, keep files
                    client.torrents_delete(delete_files=False, torrent_hashes=info_hash)
                    job["state"] = "rclone_moving"
                    job["rclone_retries"] = 0
                    job["move_start_time"] = time.time()
                    save_state(state)
                except Exception as e:
                    logger.error(f"Failed to delete torrent {info_hash} from qBittorrent: {e}")

        # --- STATE: rclone_moving ---
        elif current_state == "rclone_moving":
            # Check if there is an active background process running
            status = rclone_status.get(info_hash)
            
            if status is None:
                # Check concurrency limit for rclone background processes
                max_jobs = config.get("rclone", {}).get("max_parallel_jobs", 2)
                running_jobs = sum(1 for val in rclone_status.values() if val == "running")
                if running_jobs >= max_jobs:
                    # Skip starting new move command for this loop, wait for a slot to open
                    logger.debug(f"rclone move for {job['name']} waiting. Current running: {running_jobs}/{max_jobs}")
                    continue
                    
                # Need to start the rclone command
                local_dir = config["paths"]["local_save_path"]
                remote_target, remote_save_path = get_job_remote_paths(config, job["name"])
                
                # Get transfers count from config
                rclone_transfers = config.get("rclone", {}).get("transfers", 2)
                
                # Construct targeted command using batch paths if available
                if "batches" in job and "current_batch_index" in job:
                    batch = job["batches"][job["current_batch_index"]]
                    cmd = ["rclone", "move", local_dir, remote_target, f"--transfers={rclone_transfers}"]
                    for path in batch["file_paths"]:
                        cmd.extend(["--include", escape_rclone_glob(path)])
                else:
                    if job["is_multi_file"]:
                        # Directories: rclone move /path/local/name remote:name
                        src = os.path.join(local_dir, job["name"])
                        dest = f"{remote_target.rstrip('/')}/{job['name']}"
                        cmd = ["rclone", "move", src, dest, f"--transfers={rclone_transfers}"]
                    else:
                        # Single Files: rclone moveto /path/local/file remote:file
                        src = os.path.join(local_dir, job["name"])
                        dest = f"{remote_target.rstrip('/')}/{job['name']}"
                        cmd = ["rclone", "moveto", src, dest, f"--transfers={rclone_transfers}"]
                    
                run_rclone_move_async(info_hash, cmd)
                
            elif status == "success":
                logger.info(f"rclone move successful for {job['name']}. Cleaning up local torrent folder/file.")
                
                # Delete local torrent folder/file to clean up SSD space
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
                
                # Check if there are more batches left to process
                next_idx = job.get("current_batch_index", 0) + 1 if "batches" in job else 1
                total_batches = len(job["batches"]) if "batches" in job else 1
                
                if next_idx < total_batches:
                    logger.info(f"Completed batch {next_idx}/{total_batches} for {job['name']}. Re-adding torrent for batch {next_idx + 1}.")
                    try:
                        # Re-add torrent to local qBittorrent in paused state
                        try:
                            client.torrents_add(
                                torrent_files=job["torrent_file"],
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
                        # Fallback guarantee to make sure it's paused
                        try:
                            client.torrents_pause(torrent_hashes=info_hash)
                        except Exception:
                            pass
                            
                        # Wait a few seconds for qBittorrent to load files metadata
                        files_list = []
                        for _ in range(10):
                            try:
                                files_list = client.torrents_files(torrent_hash=info_hash)
                                if files_list:
                                    break
                            except Exception:
                                pass
                            time.sleep(1)
                            
                        if not files_list:
                            raise ValueError("Could not load file list from qBittorrent after re-adding paused torrent")
                            
                        # Set priorities for the next batch: batch files to 1, others to 0
                        all_ids = [f["id"] for f in files_list]
                        next_batch_ids = job["batches"][next_idx]["file_ids"]
                        client.torrents_file_priority(torrent_hash=info_hash, file_ids=all_ids, priority=0)
                        client.torrents_file_priority(torrent_hash=info_hash, file_ids=next_batch_ids, priority=1)
                        
                        # Resume torrent download
                        client.torrents_resume(torrent_hashes=info_hash)
                        
                        # Update job state to loop back to added_local for the next batch
                        job["current_batch_index"] = next_idx
                        job["state"] = "added_local"
                        job["added_time"] = time.time()
                        job["completion_time"] = None
                        save_state(state)
                    except Exception as e:
                        logger.error(f"Failed to re-add torrent {job['name']} for next batch: {e}")
                        job["state"] = "rclone_failed"
                        job["error_msg"] = f"Failed to re-add torrent for next batch: {e}"
                        update_telegram_status(config, job, info_hash)
                        save_state(state)
                else:
                    logger.info(f"All batches completed for {job['name']}. Entering FUSE mount wait state.")
                    job["state"] = "fuse_wait"
                    job["move_completed_time"] = time.time()
                    save_state(state)
                    
                # Clean up thread status cache
                rclone_status.pop(info_hash, None)
                rclone_threads.pop(info_hash, None)
                
            elif status.startswith("failed") or status.startswith("error"):
                retries = job.get("rclone_retries", 0)
                logger.error(f"rclone move failed for {job['name']}: {status}. Retries: {retries}/3")
                # Clear status cache to trigger retry
                rclone_status.pop(info_hash, None)
                rclone_threads.pop(info_hash, None)
                
                if retries < 3:
                    job["rclone_retries"] = retries + 1
                    save_state(state)
                else:
                    logger.error(f"rclone move failed 3 times for {job['name']}. Suspending processing. Intervention required.")
                    job["state"] = "rclone_failed"
                    job["error_msg"] = status
                    update_telegram_status(config, job, info_hash)
                    save_state(state)

        # --- STATE: fuse_wait ---
        elif current_state == "fuse_wait":
            cooldown = config["settings"].get("fuse_cooldown_seconds", 15)
            elapsed = time.time() - job["move_completed_time"]
            
            if elapsed >= cooldown:
                remote_target, remote_save_path = get_job_remote_paths(config, job["name"])
                target_path = os.path.join(remote_save_path, job["name"])
                
                try:
                    exists = os.path.exists(target_path)
                except Exception as e:
                    logger.warning(f"Error checking FUSE path {target_path}: {e}")
                    exists = False
                    
                if exists or elapsed >= 60:
                    if not exists:
                        logger.warning(f"Target path {target_path} not found on mount after 60s cooldown. Proceeding to add anyway.")
                    else:
                        logger.info(f"Target path {target_path} detected on FUSE mount. Re-adding to qBittorrent.")
                    
                    try:
                        try:
                            client.torrents_add(
                                torrent_files=job["torrent_file"],
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
                    except Exception as e:
                        logger.error(f"Failed to re-add torrent {job['name']} to remote path: {e}")

        # --- STATE: added_remote ---
        elif current_state == "added_remote":
            t = torrents_by_hash.get(info_hash)
            if not t:
                # Might take a second to appear in API list after torrents_add
                continue
                
            # Wait until status changes from checking to seeding (progress is 1.0 and state doesn't start with checking)
            is_checking = t.state.lower().startswith("checking") or "checking" in t.state.lower()
            if t.progress == 1.0 and not is_checking:
                logger.info(f"Torrent successfully seeding from remote path: {job['name']}. Archiving .torrent file.")
                
                # Archive the torrent file
                completed_dir = config["paths"].get("completed_dir")
                if not completed_dir:
                    watch_dir = config["paths"]["watch_dir"]
                    completed_dir = os.path.join(watch_dir, "completed")
                
                try:
                    os.makedirs(completed_dir, exist_ok=True)
                    dest_file = os.path.join(completed_dir, os.path.basename(job["torrent_file"]))
                    
                    # Handle duplicate filename in completed folder if any
                    if os.path.exists(dest_file):
                        base, ext = os.path.splitext(dest_file)
                        dest_file = f"{base}_{int(time.time())}{ext}"
                        
                    shutil.move(job["torrent_file"], dest_file)
                    logger.info(f"Moved {job['torrent_file']} to {dest_file}")
                except Exception as e:
                    logger.error(f"Failed to archive torrent file {job['torrent_file']}: {e}")
                
                # Send final Telegram notification before removing from tracking
                job["state"] = "completed"
                job["seeding_completed_time"] = time.time()
                update_telegram_status(config, job, info_hash)
                
                # Check for racing instance mappings
                racing_hash = get_tracker_mapping(info_hash)
                migration_ok = True
                if racing_hash:
                    job["linked_tracker"] = "Racing"
                    migration_ok = False
                    logger.info(f"Local torrent {info_hash} has mapping to Racing torrent {racing_hash}. Exporting Racing torrent from racing client and adding to normal client pointing to remote FUSE path.")
                    try:
                        racing_client = get_racing_qb_client(config)
                        if racing_client:
                            remote_target, remote_save_path = get_job_remote_paths(config, job["name"])
                            
                            # Export torrent bytes from racing client
                            try:
                                racing_torrent_bytes = racing_client.torrents_export(torrent_hash=racing_hash)
                                
                                # Add to normal qBittorrent client pointing to FUSE remote
                                try:
                                    client.torrents_add(
                                        torrent_files=racing_torrent_bytes,
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
                                        logger.info(f"Racing torrent {racing_hash} already exists in normal qBittorrent client.")
                                        migration_ok = True
                                        remove_tracker_mapping(info_hash)
                                    else:
                                        logger.error(f"Failed to add Racing torrent to normal qBittorrent: {add_err}")
                                        
                                if migration_ok:
                                    # Set completed category on racing client for visual deletion identification
                                    racing_settings = config.get("racing_settings") or {}
                                    racing_completed_cat = racing_settings.get("completed_category", "processed")
                                    try:
                                        racing_client.torrents_create_category(name=racing_completed_cat)
                                    except Exception:
                                        pass
                                    try:
                                        racing_client.torrents_set_category(category=racing_completed_cat, torrent_hashes=racing_hash)
                                        logger.info(f"Assigned completed category '{racing_completed_cat}' to racing torrent {racing_hash}")
                                    except Exception as cat_err:
                                        logger.warning(f"Failed to set category for racing torrent {racing_hash}: {cat_err}")
                            except Exception as export_err:
                                logger.error(f"Failed to export Racing torrent {racing_hash} from racing client: {export_err}")
                        else:
                            logger.error(f"Failed to connect to racing client to export torrent {racing_hash}.")
                    except Exception as e:
                        logger.error(f"Error handling Racing torrent migration: {e}")

                if migration_ok:
                    # Done, remove from active tracking
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

    # Establish racing client connection once
    racing_client = None
    try:
        racing_client = get_racing_qb_client(config)
    except Exception as e:
        logger.error(f"Failed to connect to racing client for sweep: {e}")
        return

    if not racing_client:
        return

    # Query normal qBittorrent client once to check if any mapped local torrent is already seeding from FUSE
    try:
        normal_torrents = client.torrents_info()
        normal_torrents_by_hash = {t.hash.lower(): t for t in normal_torrents}
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
            is_checking = t.state.lower().startswith("checking") or "checking" in t.state.lower()
            if t.progress == 1.0 and not is_checking:
                logger.info(f"Sweep: Found completed Local torrent {local_hash_clean} in normal client with pending Tracker mapping {racing_hash}. Migrating now.")
                remote_save_path = t.save_path
                try:
                    # Export from racing client
                    racing_torrent_bytes = racing_client.torrents_export(torrent_hash=racing_hash)
                    
                    # Add to normal client
                    try:
                        client.torrents_add(
                            torrent_files=racing_torrent_bytes,
                            save_path=remote_save_path,
                            category=config["paths"].get("remote_category", "remote"),
                            is_skip_checking=True
                        )
                        logger.info(f"Sweep: Successfully added Racing torrent {racing_hash} to normal client pointing to: {remote_save_path}")
                        remove_tracker_mapping(local_hash)
                    except Exception as add_err:
                        err_msg_lower = str(add_err).lower()
                        if "torrent hash" in err_msg_lower or "409" in err_msg_lower or "conflict" in err_msg_lower:
                            logger.info(f"Sweep: Racing torrent {racing_hash} already exists in normal qBittorrent client.")
                            remove_tracker_mapping(local_hash)
                        else:
                            logger.error(f"Sweep: Failed to add Racing torrent to normal client: {add_err}")
                            continue

                    # Set category on racing client
                    racing_settings = config.get("racing_settings") or {}
                    racing_completed_cat = racing_settings.get("completed_category", "processed")
                    try:
                        racing_client.torrents_create_category(name=racing_completed_cat)
                    except Exception:
                        pass
                    try:
                        racing_client.torrents_set_category(category=racing_completed_cat, torrent_hashes=racing_hash)
                        logger.info(f"Sweep: Assigned completed category '{racing_completed_cat}' to racing torrent {racing_hash}")
                    except Exception as cat_err:
                        logger.warning(f"Sweep: Failed to set category for racing torrent {racing_hash}: {cat_err}")
                except Exception as e:
                    logger.error(f"Sweep: Error migrating Racing torrent {racing_hash}: {e}")

def main():
    logger.info("Initializing qBittorrent and rclone sync automation...")
    config = load_config()
    state = load_state()
    
    client = None
    consecutive_connection_failures = 0
    
    while True:
        try:
            # Reload config dynamically
            config = load_config()
            state = load_state()
            poll_interval = config["settings"].get("poll_interval_seconds", 10)
            ssd_limit = config["settings"].get("ssd_limit_gb", 35.0) * 1024 * 1024 * 1024
            
            # Establish or reuse client connection
            if client is None:
                client = get_qb_client(config)
                if client is not None:
                    consecutive_connection_failures = 0
            else:
                try:
                    # Test session health using lightweight query
                    client.app.version
                    consecutive_connection_failures = 0
                except Exception as e:
                    consecutive_connection_failures += 1
                    logger.warning(f"qBittorrent connection unresponsive (failure {consecutive_connection_failures}/3): {e}")
                    
                    if consecutive_connection_failures >= 3:
                        logger.warning("qBittorrent connection lost permanently. Re-authenticating...")
                        try:
                            client.auth_log_out()
                        except Exception:
                            pass
                        client = get_qb_client(config)
                        if client is not None:
                            consecutive_connection_failures = 0
                    else:
                        logger.info("Reusing existing session for next loop in hope of transient recovery.")
                    
            if client is None:
                logger.warning("qBittorrent client connection failed. Retrying next loop.")
                time.sleep(poll_interval)
                continue
            
            # 1. Process active jobs state machine
            process_state_machine(config, state, client)
            
            # Run background sweep to heal any dangling mappings for already completed torrents
            try:
                sweep_dangling_mappings(config, client)
            except Exception as e:
                logger.error(f"Error during dangling mappings sweep: {e}")
            
            # 2. Check occupied space and see if we can schedule new torrents
            occupied_space = get_current_occupied_space(state["active_jobs"])
            
            # Count currently active downloading jobs
            max_active_downloads = config["settings"].get("max_active_downloads", 3)
            active_downloads = sum(1 for job in state["active_jobs"].values() if job["state"] == "added_local")
            
            # Scan watch folder for torrent files
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
                    
                # Construct batches from torrent files info
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

                # Check if the torrent already exists in qBittorrent
                exists_in_qb = False
                existing_torrent_info = None
                try:
                    res = client.torrents_info(torrent_hashes=info_hash)
                    if res:
                        exists_in_qb = True
                        existing_torrent_info = res[0]
                except Exception as e:
                    logger.warning(f"Error checking if torrent exists in qBittorrent: {e}")
                    
                if exists_in_qb:
                    # Check if it has already been moved to the remote mount (VFS)
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
                            
                        # Skip processing this torrent as it is already remote/done
                        continue
                        
                    # Otherwise, it's on local path: re-associate with state tracking and enforce batch priorities
                    logger.info(f"Torrent {details['name']} already exists locally in qBittorrent. Re-associating with state tracking.")
                    
                    priorities_ok = False
                    try:
                        # Enforce pausing
                        try:
                            client.torrents_pause(torrent_hashes=info_hash)
                        except Exception:
                            pass
                            
                        # Apply batch priorities: batch files to 1, others to 0
                        try:
                            all_ids = [f["id"] for f in sorted_files]
                            batch_ids = batches[0]["file_ids"]
                            client.torrents_file_priority(torrent_hash=info_hash, file_ids=all_ids, priority=0)
                            client.torrents_file_priority(torrent_hash=info_hash, file_ids=batch_ids, priority=1)
                            priorities_ok = True
                        except Exception as e:
                            logger.warning(f"Failed to set batch priorities during re-association: {e}")
                            
                        # Resume torrent download
                        try:
                            client.torrents_resume(torrent_hashes=info_hash)
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
                
                # Gate 0: Check concurrent active downloads limit
                if active_downloads >= max_active_downloads:
                    logger.debug(f"Reached max concurrent downloads limit ({active_downloads}/{max_active_downloads}). Waiting to schedule {details['name']}.")
                    break
                    
                first_batch_size = batches[0]["size"]
                # Gate 1: Check in-memory budget limit
                if occupied_space + first_batch_size <= ssd_limit:
                    # Gate 2: Check physical free space on disk
                    physical_free = get_physical_free_space(config["paths"]["local_save_path"])
                    if physical_free < first_batch_size:
                        logger.warning(f"Torrent {details['name']} fits in budget, but physical disk space is too low! Required: {first_batch_size / (1024**3):.2f} GB, Free: {physical_free / (1024**3):.2f} GB. Waiting.")
                        break
                        
                    logger.info(f"Scheduling torrent {details['name']} (Total: {size / (1024**3):.2f} GB, Batch 1: {first_batch_size / (1024**3):.2f} GB) under SSD limit ({ssd_limit / (1024**3):.2f} GB)")
                    try:
                        # Add torrent paused
                        try:
                            client.torrents_add(
                                torrent_files=torrent_file,
                                save_path=config["paths"]["local_save_path"],
                                category=config["paths"].get("category"),
                                paused=True
                            )
                        except Exception as e:
                            err_msg_lower = str(e).lower()
                            if "torrent hash" in err_msg_lower or "409" in err_msg_lower or "conflict" in err_msg_lower:
                                logger.info(f"Torrent {details['name']} already exists in qBittorrent. Proceeding to track.")
                            else:
                                raise e
                                
                        # Register in tracking state immediately to prevent concurrent scheduling
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
                        
                        # Configure priorities in an isolated try block
                        try:
                            # Ensure it is paused (robustness fallback)
                            try:
                                client.torrents_pause(torrent_hashes=info_hash)
                            except Exception:
                                pass
                                
                            # Set priorities for the first batch: batch files to 1, others to 0
                            all_ids = [f["id"] for f in sorted_files]
                            batch_ids = batches[0]["file_ids"]
                            client.torrents_file_priority(torrent_hash=info_hash, file_ids=all_ids, priority=0)
                            client.torrents_file_priority(torrent_hash=info_hash, file_ids=batch_ids, priority=1)
                            
                            # Resume torrent download
                            client.torrents_resume(torrent_hashes=info_hash)
                            state["active_jobs"][info_hash]["priorities_configured"] = True
                            save_state(state)
                            logger.info(f"Successfully configured and started torrent {details['name']} (Batch 1)")
                        except Exception as config_err:
                            logger.warning(f"Torrent {details['name']} was added, but failed to configure priorities: {config_err}. Will retry in state machine.")
                            
                        update_telegram_status(config, state["active_jobs"][info_hash], info_hash)
                        save_state(state)
                    except Exception as e:
                        logger.error(f"Failed to add torrent {torrent_file} to qBittorrent: {e}")
                else:
                    logger.debug(f"Torrent {details['name']} (First Batch: {first_batch_size / (1024**3):.2f} GB) does not fit in remaining SSD space (Free: {(ssd_limit - occupied_space) / (1024**3):.2f} GB). Waiting.")
                    break
                    
        except Exception as e:
            logger.error(f"Unexpected error in main daemon loop: {e}", exc_info=True)
            
        time.sleep(poll_interval)

if __name__ == "__main__":
    main()
