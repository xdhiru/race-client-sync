import json
import logging
import urllib.request
import urllib.parse
from .base import BaseTorrentClient

logger = logging.getLogger(__name__)

class DelugeClient(BaseTorrentClient):
    """
    Deluge client wrapper interacting with the Deluge Web API (JSON-RPC) on port 8112.
    Does not require any external client libraries.
    """
    def __init__(self, config_dict):
        super().__init__(config_dict)
        self.url = self.config["url"].rstrip('/')
        self.password = self.config["password"]
        self.session_cookie = None

    def _call(self, method, params=None) -> dict:
        if params is None:
            params = []
            
        payload = {
            "method": method,
            "params": params,
            "id": 1
        }
        
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json"
        }
        if self.session_cookie:
            headers["Cookie"] = self.session_cookie

        # Support Nginx Basic Auth if requests_args.auth is present in config
        if "requests_args" in self.config:
            user_args = self.config["requests_args"]
            if isinstance(user_args, dict) and "auth" in user_args:
                auth_list = user_args["auth"]
                if isinstance(auth_list, (list, tuple)) and len(auth_list) == 2:
                    import base64
                    auth_str = f"{auth_list[0]}:{auth_list[1]}"
                    encoded = base64.b64encode(auth_str.encode('utf-8')).decode('utf-8')
                    headers["Authorization"] = f"Basic {encoded}"
            
        req_url = f"{self.url}/json"
        data = json.dumps(payload).encode('utf-8')
        req = urllib.request.Request(req_url, data=data, headers=headers, method="POST")
        
        try:
            with urllib.request.urlopen(req, timeout=15) as response:
                res = json.loads(response.read().decode('utf-8'))
                
                # Capture session cookie if logging in
                if method == "auth.login" and not self.session_cookie:
                    cookies = response.headers.get_all("Set-Cookie")
                    if cookies:
                        for cookie in cookies:
                            if "_session_id=" in cookie:
                                self.session_cookie = cookie.split(';')[0]
                                break
                                
                if res.get("error"):
                    raise Exception(f"Deluge Web API error: {res['error']}")
                return res
        except Exception as e:
            if not method.startswith("label."):
                logger.error(f"Deluge JSON-RPC request failed ({method}): {e}")
            raise e

    def connect(self) -> bool:
        try:
            self.session_cookie = None
            res = self._call("auth.login", [self.password])
            if res.get("result") is True:
                # Verify connection health
                check = self._call("core.get_config")
                if check.get("result"):
                    return True
            return False
        except Exception:
            return False

    def check_health(self) -> bool:
        try:
            res = self._call("core.get_config")
            return res.get("result") is not None
        except Exception:
            # Try to re-connect once
            return self.connect()

    def get_torrents_info(self) -> list:
        try:
            # Query status of all torrents with standard keys
            keys = ["name", "progress", "state", "save_path", "trackers", "time_added"]
            res = self._call("core.get_torrents_status", [{}, keys])
            torrents_dict = res.get("result") or {}
            
            standardized = []
            for info_hash, status in torrents_dict.items():
                trackers = []
                for tr in status.get("trackers", []):
                    tr_url = tr.get("url", "").lower()
                    if tr_url:
                        trackers.append(tr_url)
                        
                standardized.append({
                    "hash": info_hash.lower(),
                    "name": status.get("name", ""),
                    # Deluge progress is 0-100 float, standardize to 0.0-1.0
                    "progress": float(status.get("progress", 0.0)) / 100.0,
                    "added_on": int(status.get("time_added", 0)),
                    "state": status.get("state", "").lower(),
                    "save_path": status.get("save_path", ""),
                    "tracker": trackers[0] if trackers else "",
                    "trackers": trackers
                })
            return standardized
        except Exception as e:
            logger.error(f"Deluge get_torrents_info failed: {e}")
            return []

    def get_torrent_files(self, torrent_hash: str) -> list:
        try:
            res = self._call("core.get_torrent_status", [torrent_hash.lower(), ["files"]])
            status = res.get("result") or {}
            files = status.get("files", [])
            
            standardized = []
            for idx, f in enumerate(files):
                standardized.append({
                    "id": idx,
                    "name": f.get("path", ""),
                    "size": int(f.get("size", 0)),
                    "progress": 1.0
                })
            return standardized
        except Exception as e:
            logger.error(f"Deluge get_torrent_files failed for {torrent_hash}: {e}")
            return []

    def add_torrent(self, torrent_bytes, save_path: str, category: str = None, is_skip_checking: bool = False, paused: bool = False) -> bool:
        logger.warning("DelugeClient: add_torrent is not fully supported for remote file upload in stub client.")
        return False

    def delete_torrent(self, torrent_hash: str, delete_files: bool = False) -> bool:
        try:
            self._call("core.remove_torrent", [torrent_hash.lower(), delete_files])
            return True
        except Exception as e:
            logger.error(f"Deluge delete_torrent failed for {torrent_hash}: {e}")
            return False

    def export_torrent(self, torrent_hash: str) -> bytes:
        state_dir = self.config.get("state_dir")
        if not state_dir:
            raise NotImplementedError("Deluge does not support exporting .torrent files via remote API. Provide 'state_dir' in config to read from filesystem.")
            
        import os
        import tempfile
        
        filename = f"{torrent_hash.lower()}.torrent"
        
        # Check if SSH parameters are provided for remote fetching
        ssh_host = self.config.get("ssh_host")
        if ssh_host:
            try:
                import paramiko
            except ImportError:
                raise ImportError("The 'paramiko' library is required for remote SSH fetching. Please run 'pip install paramiko'")
                
            ssh_user = self.config.get("ssh_user", "root")
            ssh_key_path = self.config.get("ssh_key_path")
            ssh_passphrase = self.config.get("ssh_key_password")
            ssh_port = int(self.config.get("ssh_port", 22))
            
            remote_path = f"{state_dir.rstrip('/')}/{filename}"
            
            try:
                ssh = paramiko.SSHClient()
                ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                
                # Load private key (supports passphrases automatically if provided)
                if ssh_key_path:
                    try:
                        # Attempt to load as RSA first (most common)
                        key = paramiko.RSAKey.from_private_key_file(ssh_key_path, password=ssh_passphrase)
                    except paramiko.ssh_exception.SSHException:
                        try:
                            # Fallback to Ed25519
                            key = paramiko.Ed25519Key.from_private_key_file(ssh_key_path, password=ssh_passphrase)
                        except paramiko.ssh_exception.SSHException:
                            # Fallback to ECDSA
                            key = paramiko.ECDSAKey.from_private_key_file(ssh_key_path, password=ssh_passphrase)
                            
                    ssh.connect(ssh_host, port=ssh_port, username=ssh_user, pkey=key)
                else:
                    # Password auth or ssh-agent fallback
                    ssh.connect(ssh_host, port=ssh_port, username=ssh_user, password=ssh_passphrase)
                    
                sftp = ssh.open_sftp()
                
                with tempfile.NamedTemporaryFile(delete=False, suffix=".torrent") as tmp:
                    temp_local_path = tmp.name
                    
                sftp.get(remote_path, temp_local_path)
                sftp.close()
                ssh.close()
                
                with open(temp_local_path, "rb") as f:
                    data = f.read()
                os.remove(temp_local_path)
                return data
                
            except Exception as e:
                if 'temp_local_path' in locals() and os.path.exists(temp_local_path):
                    os.remove(temp_local_path)
                raise FileNotFoundError(f"Failed to fetch torrent file from remote VPS via SFTP: {e}")
        
        # Fallback to local filesystem access (e.g. Syncthing mount)
        torrent_path = os.path.join(state_dir, filename)
        if not os.path.exists(torrent_path):
            raise FileNotFoundError(f"Deluge torrent file not found at {torrent_path}")
            
        with open(torrent_path, "rb") as f:
            return f.read()

    def set_category(self, torrent_hash: str, category_name: str) -> bool:
        try:
            if not category_name:
                return True
            try:
                self._call("label.add", [category_name])
            except Exception:
                pass
            self._call("label.set_torrent", [torrent_hash.lower(), category_name])
            return True
        except Exception as e:
            logger.warning(f"Deluge set_category (Label) failed: {e}. Ensure Deluge Label plugin is enabled.")
            return False

    def pause_torrent(self, torrent_hash: str) -> bool:
        try:
            self._call("core.pause_torrent", [[torrent_hash.lower()]])
            return True
        except Exception as e:
            logger.error(f"Deluge pause_torrent failed for {torrent_hash}: {e}")
            return False

    def resume_torrent(self, torrent_hash: str) -> bool:
        try:
            self._call("core.resume_torrent", [[torrent_hash.lower()]])
            return True
        except Exception as e:
            logger.error(f"Deluge resume_torrent failed for {torrent_hash}: {e}")
            return False

    def set_file_priorities(self, torrent_hash: str, file_ids: list, priority: int) -> bool:
        return False


