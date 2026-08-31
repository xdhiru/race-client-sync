import os
import sys
import time
import logging
import re
import hashlib
import urllib.parse

def clean_search_query(name):
    for ext in ['.mkv', '.mp4', '.avi', '.ts', '.mp3', '.flac']:
        if name.lower().endswith(ext):
            name = name[:-len(ext)]
            break
    cleaned = re.sub(r'[\s._-]', ' ', name)
    return ' '.join(cleaned.split()).lower()


import clients
from utils.config import load_config
from utils.state import load_json_state, save_json_state
from utils.torrent import get_torrent_file_structure, bdecode, bencode
from services.prowlarr import get_prowlarr_indexer_id, search_prowlarr, download_torrent_bytes
from services.telegram import send_telegram_notification, format_size

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
    log_file = settings.get("racing_link_log_file", "data/racing_link.log")
    try:
        import os
        log_dir = os.path.dirname(log_file)
        if log_dir:
            os.makedirs(log_dir, exist_ok=True)
            
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

STATE_PATH = "data/racing_link_state.json"

def load_state():
    return load_json_state(STATE_PATH)

def save_state(state):
    save_json_state(STATE_PATH, state)

def get_racing_client(config):
    try:
        client = clients.get_client(config.get("racing_client"))
        if client and client.connect():
            return client
        return None
    except Exception as e:
        logger.error(f"Failed to initialize racing client: {e}")
        return None

def main():
    config = load_config()
    setup_logging(config)
    logger.info("Initializing racing instance bridge...")
    state = load_state()
    
    if "racing_client" not in config or "prowlarr" not in config:
        logger.error("Missing racing_client or prowlarr configuration keys!")
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
                client = get_racing_client(config)
                if client is not None:
                    consecutive_connection_failures = 0
            else:
                if client.check_health():
                    consecutive_connection_failures = 0
                else:
                    consecutive_connection_failures += 1
                    logger.warning(f"Racing client connection unresponsive (failure {consecutive_connection_failures}/3)")
                    if consecutive_connection_failures >= 3:
                        logger.warning("Racing client connection lost permanently. Reconnecting...")
                        client = get_racing_client(config)
                        if client is not None:
                            consecutive_connection_failures = 0
                    else:
                        logger.info("Reusing existing racing session for next loop.")
                        
            if client is None:
                logger.warning("Racing client connection failed. Retrying next loop.")
                time.sleep(poll_interval)
                continue
                
            try:
                torrents = client.get_torrents_info()
                # Sort torrents by added time (oldest first) to ensure FIFO priority
                torrents = sorted(torrents, key=lambda x: x.get("added_on", 0))
            except Exception as e:
                logger.error(f"Failed to fetch torrent list from racing client: {e}")
                time.sleep(poll_interval)
                continue
                
            processed_hashes = state.setdefault("processed_hashes", {})
            active_searches = state.setdefault("active_searches", {})
            
            # Auto-prune state entries for torrents no longer active in the racing client
            active_racing_hashes = {t["hash"] for t in torrents}
            pruned_processed = {}
            for h, status in processed_hashes.items():
                if h in active_racing_hashes or status == "timed_out" or status == "oversized_movie":
                    pruned_processed[h] = status
            
            pruned_searches = {}
            for h, search_info in active_searches.items():
                if h in active_racing_hashes:
                    pruned_searches[h] = search_info
                    
            if len(processed_hashes) != len(pruned_processed) or len(active_searches) != len(pruned_searches):
                state["processed_hashes"] = pruned_processed
                state["active_searches"] = pruned_searches
                save_state(state)
                logger.info("Pruned inactive torrent state hashes from state file")
            
            for t in torrents:
                info_hash = t["hash"]
                name = t["name"]
                
                if info_hash in processed_hashes:
                    continue
                    
                trackers = t.get("trackers", [])
                
                # Support multiple racing tracker filters
                tracker_filters = r_settings.get("tracker_filters", [tracker_filter])
                is_match = False
                for tracker in trackers:
                    for filt in tracker_filters:
                        if filt.lower() in tracker.lower():
                            is_match = True
                            break
                    if is_match:
                        break
                            
                if not is_match:
                    continue
                    
                if t["progress"] < 1.0:
                    logger.debug(f"Racing torrent {name} is still downloading (progress: {t['progress']*100:.1f}%). Skipping Prowlarr search.")
                    continue
                    
                # Check if there is a public matching torrent in the racing client before searching Prowlarr
                public_keywords = r_settings.get("public_tracker_keywords", ["nyaa", "opentrackr", "mywaifu"])
                public_match = None
                
                try:
                    target_name_clean = clean_search_query(name)
                    for rt in torrents:
                        rt_name = rt.get("name", "")
                        # Check if name is matching (case-insensitive or cleaned name match)
                        if clean_search_query(rt_name) != target_name_clean and rt_name.lower() != name.lower():
                            continue
                            
                        # Check if it has any public trackers
                        rt_trackers = rt.get("trackers", [])
                        is_public = False
                        for tr in rt_trackers:
                            for kw in public_keywords:
                                if kw.lower() in tr.lower():
                                    is_public = True
                                    break
                            if is_public:
                                break
                        
                        if is_public:
                            public_match = rt
                            break
                except Exception as e:
                    logger.error(f"Error checking for public matching torrent: {e}")

                if public_match:
                    matched_local_hash = public_match["hash"].lower()
                    logger.info(f"Found public matching torrent in racing client: '{public_match['name']}' (Hash: {matched_local_hash})")
                    
                    dest_path = os.path.join(watch_dir, f"{matched_local_hash}.torrent")
                    try:
                        logger.info(f"Exporting public .torrent file from racing client via SCP/API for hash {matched_local_hash}")
                        torrent_bytes = client.export_torrent(matched_local_hash)
                        with open(dest_path, "wb") as f_out:
                            f_out.write(torrent_bytes)
                        logger.info(f"Successfully saved exported .torrent file: {dest_path}")
                    except Exception as export_err:
                        logger.warning(f"Failed to export .torrent from racing client ({export_err}). Falling back to magnet link generation.")
                        import urllib.parse
                        magnet_link = f"magnet:?xt=urn:btih:{matched_local_hash}&dn={urllib.parse.quote(public_match['name'])}"
                        for tr in public_match.get("trackers", []):
                            magnet_link += f"&tr={urllib.parse.quote(tr)}"
                            
                        magnet_path = os.path.join(watch_dir, f"{matched_local_hash}.magnet")
                        try:
                            with open(magnet_path, "w", encoding="utf-8") as f_out:
                                f_out.write(magnet_link)
                            logger.info(f"Successfully saved magnet file fallback: {magnet_path}")
                        except Exception as write_err:
                            logger.error(f"Failed to write magnet file fallback {magnet_path}: {write_err}")
                            
                    processed_hashes[info_hash] = "completed"
                    active_searches.pop(info_hash, None)
                    save_state(state)
                    continue

                if info_hash not in active_searches:
                    active_searches[info_hash] = {
                        "name": name,
                        "added_on": t["added_on"],
                        "last_searched": 0
                    }
                    
                search_info = active_searches[info_hash]
                
                added_on = search_info["added_on"]
                age_hours = (time.time() - added_on) / 3600
                if age_hours > max_search_age_hours:
                    logger.warning(f"Torrent {name} not found on Local within {max_search_age_hours} hours. Timing out.")
                    send_telegram_notification(
                        config, 
                        "not_found_chat_id", 
                        f"⚠️ <b>[RACING_TIMEOUT]</b> Raced torrent not found on Local within limit:\nName: <code>{name}</code>\nAge: {age_hours:.1f} hours"
                    )
                    processed_hashes[info_hash] = "timed_out"
                    active_searches.pop(info_hash, None)
                    save_state(state)
                    continue
                    
                elapsed_since_search = time.time() - search_info["last_searched"]
                if elapsed_since_search < (search_interval_minutes * 60):
                    continue
                    
                search_gap_seconds = r_settings.get("search_gap_seconds", 180)
                elapsed_since_global_search = time.time() - last_global_search_time
                if elapsed_since_global_search < search_gap_seconds:
                    logger.debug(f"Skipping Prowlarr search for {name} to respect global search gap ({elapsed_since_global_search:.1f}s / {search_gap_seconds}s).")
                    continue
                    
                try:
                    racing_files = client.get_torrent_files(info_hash)
                except Exception as e:
                    logger.error(f"Failed to fetch files list for {name} from racing client: {e}")
                    continue
                    
                if not racing_files:
                    logger.warning(f"Files list is empty for {name} in racing client.")
                    continue
                    
                racing_total_size = sum(f["size"] for f in racing_files)
                racing_sizes = sorted([f["size"] for f in racing_files])
                
                logger.info(f"Searching Local for raced torrent: {name} (Total Size: {format_size(racing_total_size)})")
                sanitized_query = re.sub(r'[\s._-]', ' ', name)
                search_results = search_prowlarr(prowlarr_url, prowlarr_api_key, indexer_id, sanitized_query)
                
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
                        
                    torrent_bytes = download_torrent_bytes(download_url, prowlarr_api_key)
                    if not torrent_bytes:
                        continue
                        
                    struct = get_torrent_file_structure(torrent_bytes)
                    if not struct:
                        continue
                        
                    if struct["total_size"] == racing_total_size and struct["file_sizes"] == racing_sizes:
                        matched_result = res
                        matched_local_hash = res_hash
                        if not matched_local_hash:
                            try:
                                decoded = bdecode(torrent_bytes)
                                matched_local_hash = hashlib.sha1(bencode(decoded[b'info'])).hexdigest()
                            except Exception:
                                pass
                        
                        if struct["is_single_file"] and struct["total_size"] > ssd_limit:
                            logger.warning(f"Matched movie {name} is oversized ({format_size(struct['total_size'])}). Notifying and skipping.")
                            send_telegram_notification(
                                config,
                                "oversized_chat_id",
                                f"🚨 <b>[OVERSIZED_MOVIE]</b> Raced movie exceeds SSD limit and cannot be batched:\nName: <code>{name}</code>\nSize: {format_size(struct['total_size'])}\nSSD Limit: {format_size(ssd_limit)}"
                            )
                            processed_hashes[info_hash] = "oversized_movie"
                            active_searches.pop(info_hash, None)
                            save_state(state)
                            matched_result = None
                            break
                            
                        dest_path = os.path.join(watch_dir, f"{matched_local_hash.lower()}.torrent")
                        try:
                            with open(dest_path, "wb") as f_out:
                                f_out.write(torrent_bytes)
                            logger.info(f"Successfully matched and saved torrent file: {dest_path}")
                            
                            
                            # send_telegram_notification(
                            #     config,
                            #     "chat_id",
                            #     f"🔗 <b>[LINKED]</b> Local search matched racing release:\nName: <code>{name}</code>\nSize: {format_size(struct['total_size'])}"
                            # )
                            
                            processed_hashes[info_hash] = "completed"
                            active_searches.pop(info_hash, None)
                            save_state(state)
                        except Exception as write_err:
                            logger.error(f"Failed to write matched torrent file {dest_path}: {write_err}")
                        break
                        
                if matched_result is None and info_hash in active_searches:
                    logger.info(f"No match found on Prowlarr for {name} in this cycle. Will retry.")
                    
        except Exception as e:
            logger.error(f"Unexpected error in racing bridge main loop: {e}", exc_info=True)
            
        time.sleep(poll_interval)

if __name__ == "__main__":
    main()

