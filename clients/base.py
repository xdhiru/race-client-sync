class BaseTorrentClient:
    """
    Abstract Base Class representing a standardized Torrent Client.
    All custom client wrappers (qBittorrent, Deluge, etc.) must inherit from this class.
    """
    def __init__(self, config_dict):
        self.config = config_dict

    def connect(self) -> bool:
        """
        Establishes connection to the torrent client.
        Returns:
            bool: True if connection succeeded, False otherwise.
        """
        raise NotImplementedError

    def check_health(self) -> bool:
        """
        Checks if the client connection is still responsive.
        Returns:
            bool: True if responsive, False otherwise.
        """
        raise NotImplementedError

    def get_torrents_info(self) -> list:
        """
        Fetches status info for all torrents currently loaded in the client.
        Returns:
            list[dict]: Standardized dictionary metadata representing status of each torrent:
                {
                    "hash": str,         # Standardized 40-character lowercase hex SHA-1 infohash
                    "name": str,         # Torrent display name
                    "progress": float,   # Progress ratio (between 0.0 and 1.0)
                    "added_on": int,     # Unix epoch timestamp when added
                    "state": str,        # Normalized state string (e.g. 'downloading', 'seeding', 'checking', etc.)
                    "save_path": str,    # Destination save path
                    "tracker": str,      # Main tracker URL
                    "trackers": list[str] # List of all tracker URLs
                }
        """
        raise NotImplementedError

    def get_torrent_files(self, torrent_hash: str) -> list:
        """
        Lists file sizes of all files contained inside the torrent payload.
        Args:
            torrent_hash (str): The torrent infohash.
        Returns:
            list[dict]: Standardized dictionaries representing file metadata:
                [
                    {
                        "name": str,
                        "size": int
                    },
                    ...
                ]
        """
        raise NotImplementedError

    def add_torrent(self, torrent_bytes, save_path: str, category: str = None, is_skip_checking: bool = False, paused: bool = False) -> bool:
        """
        Adds a new torrent from raw bencoded .torrent bytes.
        Args:
            torrent_bytes (bytes): Bencoded raw file bytes.
            save_path (str): Save path destination directory.
            category (str, optional): Target category or label.
            is_skip_checking (bool, optional): Skip hash checking on add.
        Returns:
            bool: True if successful, False otherwise.
        """
        raise NotImplementedError

    def delete_torrent(self, torrent_hash: str, delete_files: bool = False) -> bool:
        """
        Deletes a torrent from the client.
        Args:
            torrent_hash (str): The torrent infohash.
            delete_files (bool, optional): Delete files from disk as well.
        Returns:
            bool: True if successful, False otherwise.
        """
        raise NotImplementedError

    def export_torrent(self, torrent_hash: str) -> bytes:
        """
        Exports the original bencoded .torrent file from the client.
        Args:
            torrent_hash (str): The torrent infohash.
        Returns:
            bytes: The raw bencoded .torrent file bytes.
        """
        raise NotImplementedError

    def set_category(self, torrent_hash: str, category_name: str) -> bool:
        """
        Creates (if needed) and assigns a category or tag to a torrent.
        Args:
            torrent_hash (str): The torrent infohash.
            category_name (str): Category/tag to set.
        Returns:
            bool: True if successful, False otherwise.
        """
        raise NotImplementedError
    def pause_torrent(self, torrent_hash: str) -> bool:
        """
        Pauses the downloading/seeding of a torrent.
        Args:
            torrent_hash (str): The torrent infohash.
        Returns:
            bool: True if successful, False otherwise.
        """
        raise NotImplementedError

    def resume_torrent(self, torrent_hash: str) -> bool:
        """
        Resumes the downloading/seeding of a torrent.
        Args:
            torrent_hash (str): The torrent infohash.
        Returns:
            bool: True if successful, False otherwise.
        """
        raise NotImplementedError

    def set_file_priorities(self, torrent_hash: str, file_ids: list, priority: int) -> bool:
        """
        Sets priority for specific files inside the torrent.
        Args:
            torrent_hash (str): The torrent infohash.
            file_ids (list): List of file IDs/indices.
            priority (int): Target priority level (e.g. 0 to skip, 1 to download).
        Returns:
            bool: True if successful, False otherwise.
        """
        raise NotImplementedError

