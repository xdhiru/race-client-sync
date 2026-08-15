import os
import sys
import time
import json
try:
    import tomllib
except ModuleNotFoundError:
    try:
        import tomli as tomllib
    except ModuleNotFoundError:
        print("Error: Python 3.11+ is required, or install 'tomli' (pip install tomli).")
        sys.exit(1)
import logging
import re
import urllib.request
import urllib.parse
import qbittorrentapi
import hashlib

# Configure Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("racing_link.log", encoding="utf-8")
    ]
)
logger = logging.getLogger("racing_link")

CONFIG_PATH = "config.toml"
STATE_PATH = "racing_link_state.json"
MAPPINGS_PATH = "tracker_mappings.json"

def load_config():
    if not os.path.exists(CONFIG_PATH):
        logger.error(f"Configuration file {CONFIG_PATH} not found!")
        sys.exit(1)
    try:
        with open(CONFIG_PATH, "rb") as f:
            return tomllib.load(f)
    except Exception as e:
        logger.error(f"Failed to parse configuration file {CONFIG_PATH}: {e}")
        sys.exit(1)

def load_state():
    if os.path.exists(STATE_PATH):
        try:
            with open(STATE_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Failed to load state: {e}. Starting fresh.")
    return {"processed_hashes": {}, "active_searches": {}}

def save_state(state):
    try:
        with open(STATE_PATH, "w", encoding="utf-8") as f:
            json.dump(state, f, indent=2)
    except Exception as e:
        logger.error(f"Failed to save state: {e}")

def normalize_title(title):
    title = title.lower()
    title = re.sub(r'[\s._-]', ' ', title)
    title = re.sub(r'[^a-z0-9\s]', '', title)
    return " ".join(title.split())

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
            val_len = int(data[pos:colon])
            start = colon + 1
            end = start + val_len
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

def get_torrent_file_structure(torrent_bytes):
    try:
        decoded = bdecode(torrent_bytes)
        info = decoded[b'info']
        
        total_size = 0
        file_sizes = []
        
        if b'files' in info:
            for f in info[b'files']:
                length = f[b'length']
                total_size += length
                file_sizes.append(length)
        else:
            length = info[b'length']
            total_size = length
            file_sizes.append(length)
            
        return {
            "total_size": total_size,
            "file_sizes": sorted(file_sizes),
            "is_single_file": b'files' not in info
        }
    except Exception as e:
        logger.error(f"Error parsing torrent file bytes: {e}")
        return None

def call_telegram_api(method, payload, token):
    url = f"https://api.telegram.org/bot{token}/{method}"
    headers = {"Content-Type": "application/json"}
    data = json.dumps(payload).encode('utf-8')
    req = urllib.request.Request(url, data=data, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=10) as response:
            res = json.loads(response.read().decode('utf-8'))
            return res
    except Exception as e:
        logger.error(f"Telegram API call failed ({method}): {e}")
        return None

def send_telegram_notification(config, chat_id_key, text):
    tg_config = config.get("telegram", {})
    if not tg_config.get("enabled", False):
        return
    bot_token = tg_config.get("bot_token")
    chat_id = tg_config.get(chat_id_key) or tg_config.get("chat_id")
    if not bot_token or not chat_id:
        return
        
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML"
    }
    call_telegram_api("sendMessage", payload, bot_token)

def format_size(size_bytes):
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size_bytes < 1024.0:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.2f} PB"

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

def get_prowlarr_indexer_id(prowlarr_url, api_key, indexer_name):
    url = f"{prowlarr_url.rstrip('/')}/api/v1/indexer"
    req = urllib.request.Request(url, method="GET")
    req.add_header("X-Api-Key", api_key)
    try:
        with urllib.request.urlopen(req, timeout=15) as res:
            data = json.loads(res.read().decode('utf-8'))
            for idx in data:
                if idx.get("name", "").lower() == indexer_name.lower():
                    return idx.get("id")
    except Exception as e:
        logger.error(f"Failed to fetch indexers from Prowlarr: {e}")
    return None

def search_prowlarr(prowlarr_url, api_key, indexer_id, query):
    params = {
        "query": query,
        "indexerIds": indexer_id,
        "type": "search"
    }
    url = f"{prowlarr_url.rstrip('/')}/api/v1/search?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(url, method="GET")
    req.add_header("X-Api-Key", api_key)
    try:
        with urllib.request.urlopen(req, timeout=30) as res:
            return json.loads(res.read().decode('utf-8'))
    except Exception as e:
        logger.error(f"Search query failed in Prowlarr for '{query}': {e}")
    return []

def download_torrent_bytes(download_url, api_key):
    req = urllib.request.Request(download_url, method="GET")
    req.add_header("X-Api-Key", api_key)
    try:
        with urllib.request.urlopen(req, timeout=30) as res:
            return res.read()
    except Exception as e:
        logger.error(f"Failed to download torrent bytes from {download_url}: {e}")
    return None

def get_racing_qb_client(config):
    qb_config = config["racing_qbittorrent"]
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

def main():
    logger.info("Initializing racing instance bridge...")
    config = load_config()
    state = load_state()
    
    if "racing_qbittorrent" not in config or "prowlarr" not in config:
        logger.error("Missing racing_qbittorrent or prowlarr configuration keys!")
        sys.exit(1)
        
    p_config = config["prowlarr"]
    prowlarr_url = p_config["url"]
    prowlarr_api_key = p_config["api_key"]
    indexer_name = p_config["indexer_name"]
    
    watch_dir = config["paths"]["watch_dir"]
    if not os.path.exists(watch_dir):
        os.makedirs(watch_dir, exist_ok=True)
        
    r_settings = config.get("racing_settings", {})
    poll_interval = r_settings.get("poll_interval_seconds", 60)
    tracker_filter = r_settings.get("tracker_filter", "tracker_domain.com").lower()
    max_search_age_hours = r_settings.get("max_search_age_hours", 24)
    search_interval_minutes = r_settings.get("search_interval_minutes", 15)
    ssd_limit_gb = config["settings"].get("ssd_limit_gb", 35.0)
    ssd_limit = ssd_limit_gb * 1024 * 1024 * 1024
    
    indexer_id = None
    client = None
    consecutive_connection_failures = 0
    last_global_search_time = 0
    
    while True:
        try:
            config = load_config()
            state = load_state()
            
            p_config = config["prowlarr"]
            prowlarr_url = p_config["url"]
            prowlarr_api_key = p_config["api_key"]
            indexer_name = p_config["indexer_name"]
            
            if indexer_id is None:
                indexer_id = get_prowlarr_indexer_id(prowlarr_url, prowlarr_api_key, indexer_name)
                if indexer_id is None:
                    logger.warning(f"Could not resolve Prowlarr indexer ID for '{indexer_name}'. Retrying next loop.")
                    time.sleep(poll_interval)
                    continue
                logger.info(f"Using Prowlarr indexer '{indexer_name}' (ID: {indexer_id})")
            
            p_config = config["prowlarr"]
            prowlarr_url = p_config["url"]
            prowlarr_api_key = p_config["api_key"]
            
            r_settings = config.get("racing_settings", {})
            poll_interval = r_settings.get("poll_interval_seconds", 60)
            tracker_filter = r_settings.get("tracker_filter", "tracker_domain.com").lower()
            max_search_age_hours = r_settings.get("max_search_age_hours", 24)
            search_interval_minutes = r_settings.get("search_interval_minutes", 15)
            ssd_limit_gb = config["settings"].get("ssd_limit_gb", 35.0)
            ssd_limit = ssd_limit_gb * 1024 * 1024 * 1024
            
            if client is None:
                client = get_racing_qb_client(config)
                if client is not None:
                    consecutive_connection_failures = 0
            else:
                try:
                    client.app.version
                    consecutive_connection_failures = 0
                except Exception as e:
                    consecutive_connection_failures += 1
                    logger.warning(f"Racing client connection unresponsive (failure {consecutive_connection_failures}/3): {e}")
                    if consecutive_connection_failures >= 3:
                        logger.warning("Racing client connection lost permanently. Reconnecting...")
                        try:
                            client.auth_log_out()
                        except Exception:
                            pass
                        client = get_racing_qb_client(config)
                        if client is not None:
                            consecutive_connection_failures = 0
                    else:
                        logger.info("Reusing existing racing session for next loop.")
                        
            if client is None:
                logger.warning("Racing qBittorrent client connection failed. Retrying next loop.")
                time.sleep(poll_interval)
                continue
                
            try:
                torrents = client.torrents_info()
            except Exception as e:
                logger.error(f"Failed to fetch torrent list from racing client: {e}")
                time.sleep(poll_interval)
                continue
                
            processed_hashes = state.setdefault("processed_hashes", {})
            active_searches = state.setdefault("active_searches", {})
            
            for t in torrents:
                info_hash = t.hash
                name = t.name
                
                if info_hash in processed_hashes:
                    continue
                    
                # Gather trackers
                trackers = []
                if hasattr(t, "tracker") and t.tracker:
                    trackers.append(t.tracker.lower())
                
                is_match = False
                for tracker in trackers:
                    if tracker_filter in tracker:
                        is_match = True
                        break
                        
                if not is_match and hasattr(t, "trackers"):
                    for tr in t.trackers:
                        tr_url = tr.get("url", "").lower()
                        if tracker_filter in tr_url:
                            is_match = True
                            break
                            
                if not is_match:
                    continue
                    
                # Gate: Only search Local once the Racing torrent is 100% completed
                if t.progress < 1.0:
                    logger.debug(f"Racing torrent {name} is still downloading (progress: {t.progress*100:.1f}%). Skipping Prowlarr search.")
                    continue
                    
                # Initialize active search tracking if not present
                if info_hash not in active_searches:
                    active_searches[info_hash] = {
                        "name": name,
                        "added_on": t.added_on,
                        "last_searched": 0
                    }
                    
                search_info = active_searches[info_hash]
                
                # Check timeout limits (24 hours)
                added_on = search_info["added_on"]
                age_hours = (time.time() - added_on) / 3600
                if age_hours > max_search_age_hours:
                    logger.warning(f"Torrent {name} not found on Local within {max_search_age_hours} hours. Timing out.")
                    send_telegram_notification(
                        config, 
                        "not_found_chat_id", 
                        f"❌ <b>[RACING_TIMEOUT]</b> Raced torrent not found on Local within limit:<br>Name: <code>{name}</code><br>Age: {age_hours:.1f} hours"
                    )
                    processed_hashes[info_hash] = "timed_out"
                    active_searches.pop(info_hash, None)
                    save_state(state)
                    continue
                    
                # Enforce query intervals (15 minutes)
                elapsed_since_search = time.time() - search_info["last_searched"]
                if elapsed_since_search < (search_interval_minutes * 60):
                    continue
                    
                # Enforce global search gap between Prowlarr requests
                search_gap_seconds = r_settings.get("search_gap_seconds", 180)
                elapsed_since_global_search = time.time() - last_global_search_time
                if elapsed_since_global_search < search_gap_seconds:
                    logger.debug(f"Skipping Prowlarr search for {name} to respect global search gap ({elapsed_since_global_search:.1f}s / {search_gap_seconds}s).")
                    continue
                    
                # Retrieve files structure from Racing torrent
                try:
                    racing_files = client.torrents_files(torrent_hash=info_hash)
                except Exception as e:
                    logger.error(f"Failed to fetch files list for {name} from racing client: {e}")
                    continue
                    
                if not racing_files:
                    logger.warning(f"Files list is empty for {name} in racing client.")
                    continue
                    
                racing_total_size = sum(f["size"] for f in racing_files)
                racing_sizes = sorted([f["size"] for f in racing_files])
                
                # Run search query on Prowlarr
                logger.info(f"Searching Local for raced torrent: {name} (Total Size: {format_size(racing_total_size)})")
                sanitized_query = re.sub(r'[\s._-]', ' ', name)
                search_results = search_prowlarr(prowlarr_url, prowlarr_api_key, indexer_id, sanitized_query)
                
                # Update search timestamp and global cooldown timestamp
                search_info["last_searched"] = time.time()
                last_global_search_time = time.time()
                save_state(state)
                
                matched_result = None
                matched_local_hash = None
                
                for res in search_results:
                    download_url = res.get("downloadUrl")
                    res_hash = res.get("infoHash")
                    
                    if not download_url:
                        continue
                        
                    # Download .torrent bytes temporarily to verify file structure
                    torrent_bytes = download_torrent_bytes(download_url, prowlarr_api_key)
                    if not torrent_bytes:
                        continue
                        
                    struct = get_torrent_file_structure(torrent_bytes)
                    if not struct:
                        continue
                        
                    # Match sizes
                    if struct["total_size"] == racing_total_size and struct["file_sizes"] == racing_sizes:
                        matched_result = res
                        matched_local_hash = res_hash
                        if not matched_local_hash:
                            try:
                                decoded = bdecode(torrent_bytes)
                                matched_local_hash = hashlib.sha1(bencode(decoded[b'info'])).hexdigest()
                            except Exception:
                                pass
                        
                        # Verify oversized movie limit: single file larger than SSD limit
                        if struct["is_single_file"] and struct["total_size"] > ssd_limit:
                            logger.warning(f"Matched movie {name} is oversized ({format_size(struct['total_size'])}). Notifying and skipping.")
                            send_telegram_notification(
                                config,
                                "oversized_chat_id",
                                f"⚠️ <b>[OVERSIZED_MOVIE]</b> Raced movie exceeds SSD limit and cannot be batched:<br>Name: <code>{name}</code><br>Size: {format_size(struct['total_size'])}<br>SSD Limit: {format_size(ssd_limit)}"
                            )
                            processed_hashes[info_hash] = "oversized_movie"
                            active_searches.pop(info_hash, None)
                            save_state(state)
                            matched_result = None
                            break
                            
                        # Save matching torrent to watch folder
                        torrent_filename = f"{name}.torrent"
                        save_path = os.path.join(watch_dir, torrent_filename)
                        try:
                            with open(save_path, "wb") as f:
                                f.write(torrent_bytes)
                            logger.info(f"Successfully matched and saved torrent file: {torrent_filename}")
                            
                            # Register mapping in main state
                            if matched_local_hash:
                                if len(matched_local_hash) == 80:
                                    try:
                                        matched_local_hash = bytes.fromhex(matched_local_hash).decode('utf-8')
                                    except Exception:
                                        pass
                                register_tracker_mapping(matched_local_hash.lower(), info_hash.lower())
                                
                            processed_hashes[info_hash] = "processed"
                            active_searches.pop(info_hash, None)
                            save_state(state)
                            break
                        except Exception as e:
                            logger.error(f"Failed to write .torrent file to {save_path}: {e}")
                            
                if not matched_result and info_hash in active_searches:
                    logger.info(f"No matching Local upload found yet for: {name}. Retrying next interval.")
                    
            # Prune stale entries to prevent state file growing indefinitely
            current_hashes = {t.hash for t in torrents}
            state_changed = False
            
            for h in list(processed_hashes.keys()):
                if h not in current_hashes:
                    processed_hashes.pop(h)
                    state_changed = True
                    
            for h in list(active_searches.keys()):
                if h not in current_hashes:
                    active_searches.pop(h)
                    state_changed = True
                    
            if state_changed:
                save_state(state)
                
        except Exception as e:
            logger.error(f"Unexpected error in racing link loop: {e}", exc_info=True)
            
        time.sleep(poll_interval)

if __name__ == "__main__":
    main()
