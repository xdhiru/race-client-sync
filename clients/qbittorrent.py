import logging
import os
import qbittorrentapi
from .base import BaseTorrentClient, AddTorrentResult


def is_same_or_parent_path(parent, child):
    if not parent or not child:
        return False
    p = os.path.normpath(parent).lower().rstrip(os.path.sep)
    c = os.path.normpath(child).lower().rstrip(os.path.sep)
    return c == p or c.startswith(p + os.path.sep)

logger = logging.getLogger(__name__)

class QBittorrentClient(BaseTorrentClient):
    def __init__(self, config_dict):
        super().__init__(config_dict)
        self.client = None

    def connect(self) -> bool:
        try:
            requests_args = {'timeout': (15, 45)}
            if "requests_args" in self.config:
                user_args = self.config["requests_args"]
                if isinstance(user_args, dict):
                    requests_args.update(user_args)
                    if "auth" in requests_args and isinstance(requests_args["auth"], list):
                        requests_args["auth"] = tuple(requests_args["auth"])
            
            self.client = qbittorrentapi.Client(
                host=self.config["url"],
                username=self.config["username"],
                password=self.config["password"],
                REQUESTS_ARGS=requests_args
            )
            self.client.auth_log_in()
            return True
        except Exception as e:
            logger.error(f"Failed to connect to qBittorrent client: {e}")
            self.client = None
            return False

    def check_health(self) -> bool:
        if not self.client:
            return False
        try:
            # Test session health using lightweight query
            self.client.app.version
            return True
        except Exception:
            return False

    def get_torrents_info(self) -> list:
        if not self.client:
            return []
        try:
            torrents = self.client.torrents_info()
            standardized = []
            for t in torrents:
                # Gather trackers list
                trackers = []
                if hasattr(t, "tracker") and t.tracker:
                    trackers.append(t.tracker.lower())
                if hasattr(t, "trackers"):
                    for tr in t.trackers:
                        tr_url = tr.get("url", "").lower()
                        if tr_url and tr_url not in trackers:
                            trackers.append(tr_url)

                standardized.append({
                    "hash": t.hash.lower() if hasattr(t, "hash") else "",
                    "name": t.name if hasattr(t, "name") else "",
                    "progress": t.progress if hasattr(t, "progress") else 0.0,
                    "added_on": t.added_on if hasattr(t, "added_on") else 0,
                    "state": t.state.lower() if hasattr(t, "state") else "",
                    "save_path": t.save_path if hasattr(t, "save_path") else "",
                    "tracker": t.tracker if hasattr(t, "tracker") else "",
                    "trackers": trackers
                })
            return standardized
        except Exception as e:
            logger.error(f"Failed to fetch torrents info from qBittorrent: {e}")
            return []

    def get_torrent_files(self, torrent_hash: str) -> list:
        if not self.client:
            return []
        try:
            files = self.client.torrents_files(torrent_hash=torrent_hash)
            return [{"id": f.get("index", f.get("id", 0)), "name": f["name"], "size": f["size"], "progress": f.get("progress", 0.0)} for f in files]
        except Exception as e:
            logger.error(f"Failed to fetch file list for torrent {torrent_hash} from qBittorrent: {e}")
            return []

    def add_torrent(self, torrent_bytes, save_path: str, category: str = None, is_skip_checking: bool = False, paused: bool = False) -> AddTorrentResult:
        if not self.client:
            return AddTorrentResult.FAILED

        target_hash = None
        if isinstance(torrent_bytes, str) and torrent_bytes.startswith("magnet:"):
            import re as _re
            m = _re.search(r'xt=urn:btih:([a-fA-F0-9]{32,40})', torrent_bytes)
            if m:
                target_hash = m.group(1).lower()
        elif isinstance(torrent_bytes, (bytes, bytearray)):
            try:
                from utils.torrent import bdecode, bencode
                import hashlib
                decoded = bdecode(bytes(torrent_bytes))
                target_hash = hashlib.sha1(bencode(decoded[b'info'])).hexdigest().lower()
            except Exception:
                pass

        if target_hash:
            try:
                existing_path = self.get_torrent_save_path(target_hash)
                if existing_path:
                    if is_same_or_parent_path(save_path, existing_path):
                        logger.info(f"Torrent {target_hash} already exists at expected path {existing_path}.")
                        return AddTorrentResult.EXISTS_CORRECT
                    else:
                        logger.info(f"Torrent {target_hash} exists but at wrong path {existing_path} (expected {save_path}).")
                        return AddTorrentResult.EXISTS_WRONG
            except Exception:
                pass

        try:
            import os
            if isinstance(torrent_bytes, str) and torrent_bytes.endswith(".magnet") and os.path.exists(torrent_bytes):
                try:
                    with open(torrent_bytes, "r", encoding="utf-8") as f:
                        torrent_bytes = f.read().strip()
                except Exception as e:
                    logger.error(f"Failed to read magnet link from file {torrent_bytes}: {e}")

            if isinstance(torrent_bytes, str) and (torrent_bytes.startswith("magnet:") or torrent_bytes.startswith("http")):
                self.client.torrents_add(
                    urls=torrent_bytes,
                    save_path=save_path,
                    category=category,
                    is_skip_checking=is_skip_checking,
                    paused=paused
                )
            else:
                self.client.torrents_add(
                    torrent_files=torrent_bytes,
                    save_path=save_path,
                    category=category,
                    is_skip_checking=is_skip_checking,
                    paused=paused
                )
            return AddTorrentResult.ADDED
        except Exception as e:
            err_msg_lower = str(e).lower()
            if "conflict" in err_msg_lower or "409" in err_msg_lower or "torrent hash" in err_msg_lower or "already exist" in err_msg_lower:
                logger.info(f"Torrent already exists in qBittorrent (race-detected via 409): {e}")
                if target_hash:
                    try:
                        existing_path = self.get_torrent_save_path(target_hash)
                        if existing_path and is_same_or_parent_path(save_path, existing_path):
                            return AddTorrentResult.EXISTS_CORRECT
                        if existing_path:
                            return AddTorrentResult.EXISTS_WRONG
                    except Exception:
                        pass
                return AddTorrentResult.EXISTS_CORRECT
            logger.error(f"Failed to add torrent to qBittorrent: {e}")
            return AddTorrentResult.FAILED

    def get_torrent_save_path(self, torrent_hash: str) -> str:
        if not self.client:
            return ""
        try:
            torrents = self.client.torrents_info(torrent_hashes=torrent_hash)
            if torrents:
                return (torrents[0].save_path or "")
        except Exception as e:
            logger.error(f"Failed to fetch save_path for {torrent_hash}: {e}")
        return ""

    def set_location(self, torrent_hash: str, location: str) -> bool:
        if not self.client:
            return False
        try:
            self.client.torrents_set_location(location=location, torrent_hashes=torrent_hash)
            return True
        except Exception as e:
            logger.error(f"Failed to set location for {torrent_hash} to {location}: {e}")
            return False

    def delete_torrent(self, torrent_hash: str, delete_files: bool = False) -> bool:
        if not self.client:
            return False
        try:
            self.client.torrents_delete(delete_files=delete_files, torrent_hashes=torrent_hash)
            return True
        except Exception as e:
            logger.error(f"Failed to delete torrent {torrent_hash} from qBittorrent: {e}")
            return False

    def export_torrent(self, torrent_hash: str) -> bytes:
        if not self.client:
            raise Exception("qBittorrent client not connected")
        try:
            return self.client.torrents_export(torrent_hash=torrent_hash)
        except Exception as e:
            logger.error(f"Failed to export torrent {torrent_hash} from qBittorrent: {e}")
            raise e

    def set_category(self, torrent_hash: str, category_name: str) -> bool:
        if not self.client:
            return False
        try:
            # Create category if it does not exist
            try:
                self.client.torrents_create_category(name=category_name)
            except Exception:
                pass
            
            self.client.torrents_set_category(category=category_name, torrent_hashes=torrent_hash)
            return True
        except Exception as e:
            logger.error(f"Failed to set category '{category_name}' for torrent {torrent_hash}: {e}")
            return False
    def pause_torrent(self, torrent_hash: str) -> bool:
        if not self.client:
            return False
        try:
            self.client.torrents_pause(torrent_hashes=torrent_hash)
            return True
        except Exception as e:
            logger.error(f"Failed to pause torrent {torrent_hash}: {e}")
            return False

    def resume_torrent(self, torrent_hash: str) -> bool:
        if not self.client:
            return False
        try:
            self.client.torrents_resume(torrent_hashes=torrent_hash)
            return True
        except Exception as e:
            logger.error(f"Failed to resume torrent {torrent_hash}: {e}")
            return False

    def set_file_priorities(self, torrent_hash: str, file_ids: list, priority: int) -> bool:
        if not self.client:
            return False
        try:
            self.client.torrents_file_priority(torrent_hash=torrent_hash, file_ids=file_ids, priority=priority)
            return True
        except Exception as e:
            logger.error(f"Failed to set file priorities for torrent {torrent_hash}: {e}")
            return False

