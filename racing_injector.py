import os
import re
import time
import threading
import hashlib
import logging
import urllib.parse
from queue import Queue, Empty
from collections import deque

import clients
from services.prowlarr import get_prowlarr_indexer_id, search_prowlarr, download_torrent_bytes
from services.telegram import send_telegram_notification
from utils.torrent import bdecode, bencode, get_torrent_file_structure
import services.telegram_queue as tg_queue

logger = logging.getLogger(__name__)

_inject_queue = None
_workers = []
_stop_event = None
MAX_WORKERS = 2
MAX_RETRIES = 3
RETRY_BACKOFF = 5.0


def _clean_query(name):
    for ext in ['.mkv', '.mp4', '.avi', '.ts', '.mp3', '.flac']:
        if name.lower().endswith(ext):
            name = name[:-len(ext)]
            break
    cleaned = re.sub(r'[\s._-]', ' ', name)
    return ' '.join(cleaned.split())


def _get_torrent_files_sizes(files):
    if not files:
        return []
    return sorted([f["size"] for f in files])


def _do_inject(item, racing_client, normal_client, config, p_config, indexer_ids, target_total_size, target_file_sizes, local_info_hash, remote_save_path, injected):
    name, source, torrent_bytes, res_hash = item
    add_result = normal_client.add_torrent(
        torrent_bytes=torrent_bytes,
        save_path=remote_save_path,
        category=config["paths"].get("remote_category", "remote"),
        is_skip_checking=True,
    )
    if add_result.value == "added" or add_result.value.startswith("exists"):
        if source == "racing":
            try:
                racing_completed_cat = config.get("racing_settings", {}).get("completed_category", "processed")
                racing_client.set_category(res_hash, racing_completed_cat)
            except Exception:
                pass
        logger.info(f"Injected {source} cross-seed {res_hash} for {name}.")
        injected.append(res_hash)
    else:
        logger.error(f"Failed to inject {source} cross-seed {res_hash} for {name}.")


def _inject_for_job(info_hash, details, sorted_files, remote_save_path, config, origin_client):
    """Worker body: find matches across racing client + Prowlarr, inject them."""
    try:
        target_total_size = details.get("size", 0)
        target_file_sizes = _get_torrent_files_sizes(sorted_files)
        name = details.get("name", "?")
        logger.info(f"Injector: scanning for cross-seeds for '{name}'.")

        racing_client = None
        try:
            racing_client = clients.get_client(config.get("racing_client"))
            if racing_client and not racing_client.connect():
                racing_client = None
        except Exception as e:
            logger.error(f"Injector: racing client connect failed: {e}")

        injected = []

        if racing_client:
            try:
                racing_torrents = racing_client.get_torrents_info()
                for rt in racing_torrents:
                    rt_hash = (rt.get("hash") or "").lower()
                    if not rt_hash or rt_hash == info_hash.lower():
                        continue
                    try:
                        rt_files = racing_client.get_torrent_files(rt_hash)
                    except Exception:
                        continue
                    if not rt_files:
                        continue
                    rt_total = sum(f["size"] for f in rt_files)
                    rt_sizes = _get_torrent_files_sizes(rt_files)
                    if rt_total == target_total_size and rt_sizes == target_file_sizes:
                        try:
                            torrent_bytes = racing_client.export_torrent(rt_hash)
                        except Exception as e:
                            logger.warning(f"Export failed for {rt_hash}: {e}")
                            continue
                        _do_inject(
                            (rt.get("name", ""), "racing", torrent_bytes, rt_hash),
                            racing_client, origin_client, config, None, None,
                            target_total_size, target_file_sizes, info_hash,
                            remote_save_path, injected,
                        )
            except Exception as e:
                logger.error(f"Injector: racing client scan failed: {e}")

        p_config = config.get("prowlarr", {})
        racing_indexer_map = p_config.get("racing_indexer_map", {})
        prowlarr_url = p_config.get("url")
        prowlarr_api_key = p_config.get("api_key")

        if prowlarr_url and prowlarr_api_key and racing_indexer_map:
            indexer_ids = {}
            for idx_key, idx_name in racing_indexer_map.items():
                try:
                    iid = get_prowlarr_indexer_id(prowlarr_url, prowlarr_api_key, idx_name)
                    if iid:
                        indexer_ids[idx_name] = iid
                except Exception as e:
                    logger.error(f"Injector: indexer resolve failed for {idx_name}: {e}")

            sanitized_query = _clean_query(details["name"])
            for idx_name, indexer_id in indexer_ids.items():
                try:
                    results = search_prowlarr(prowlarr_url, prowlarr_api_key, indexer_id, sanitized_query)
                except Exception as e:
                    logger.error(f"Injector: Prowlarr search failed for {idx_name}: {e}")
                    continue
                for res in results:
                    res_hash = (res.get("infoHash") or "").lower()
                    if not res_hash or res_hash in injected or res_hash == info_hash.lower():
                        continue
                    download_url = res.get("downloadUrl")
                    if not download_url:
                        continue
                    try:
                        torrent_bytes = download_torrent_bytes(download_url, prowlarr_api_key)
                        if not torrent_bytes:
                            continue
                        struct = get_torrent_file_structure(torrent_bytes)
                        if not struct:
                            continue
                        if struct["total_size"] != target_total_size or sorted(struct["file_sizes"]) != target_file_sizes:
                            continue
                        if not res_hash:
                            try:
                                decoded = bdecode(torrent_bytes)
                                res_hash = hashlib.sha1(bencode(decoded[b'info'])).hexdigest().lower()
                            except Exception:
                                continue
                        _do_inject(
                            (details["name"], "prowlarr", torrent_bytes, res_hash),
                            racing_client, origin_client, config, p_config, indexer_ids,
                            target_total_size, target_file_sizes, info_hash,
                            remote_save_path, injected,
                        )
                    except Exception as e:
                        logger.error(f"Injector: Prowlarr result handling failed: {e}")
    except Exception as e:
        logger.error(f"Injector: unhandled error for {info_hash}: {e}", exc_info=True)


def _worker_loop():
    while not _stop_event.is_set():
        try:
            item = _inject_queue.get(timeout=1.0)
        except Empty:
            continue
        try:
            _inject_for_job(*item)
        except Exception as e:
            logger.error(f"Injector worker crashed: {e}", exc_info=True)
        finally:
            _inject_queue.task_done()


def start():
    global _inject_queue, _workers, _stop_event
    if _workers:
        return
    _inject_queue = Queue()
    _stop_event = threading.Event()
    for i in range(MAX_WORKERS):
        t = threading.Thread(target=_worker_loop, daemon=True, name=f"RacingInjector-{i}")
        t.start()
        _workers.append(t)


def stop(timeout=5.0):
    global _workers, _stop_event, _inject_queue
    if _stop_event is not None:
        _stop_event.set()
    for t in _workers:
        t.join(timeout=timeout)
    _workers = []
    _stop_event = None
    _inject_queue = None


def enqueue_injection(info_hash, details, sorted_files, remote_save_path, config, origin_client):
    if _inject_queue is None:
        return
    _inject_queue.put((
        info_hash, details, sorted_files, remote_save_path, config, origin_client,
    ))
