from .base import BaseTorrentClient
from .qbittorrent import QBittorrentClient
from .deluge import DelugeClient

def get_client(config_dict) -> BaseTorrentClient:
    """
    Factory function to instantiate the correct torrent client based on the config.
    """
    if not config_dict:
        return None
        
    client_type = config_dict.get("type", "qbittorrent").lower()
    
    if client_type == "qbittorrent":
        return QBittorrentClient(config_dict)
    elif client_type == "deluge":
        return DelugeClient(config_dict)
    else:
        raise ValueError(f"Unknown client type: '{client_type}'")
