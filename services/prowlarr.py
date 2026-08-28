import json
import logging
import urllib.request
import urllib.parse
import re

logger = logging.getLogger(__name__)

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
    url_parts = list(urllib.parse.urlparse(prowlarr_url))
    url_parts[2] = f"{url_parts[2].rstrip('/')}/api/v1/search"
    url_parts[4] = urllib.parse.urlencode(params)
    search_url = urllib.parse.urlunparse(url_parts)
    
    req = urllib.request.Request(search_url, method="GET")
    req.add_header("X-Api-Key", api_key)
    try:
        with urllib.request.urlopen(req, timeout=30) as res:
            return json.loads(res.read().decode('utf-8'))
    except Exception as e:
        logger.error(f"Prowlarr search failed: {e}")
        return []

def download_torrent_bytes(download_url, api_key):
    url_parts = urllib.parse.urlparse(download_url)
    qs = urllib.parse.parse_qs(url_parts.query)
    qs["apikey"] = [api_key]
    new_query = urllib.parse.urlencode(qs, doseq=True)
    target_url = urllib.parse.urlunparse((
        url_parts.scheme,
        url_parts.netloc,
        url_parts.path,
        url_parts.params,
        new_query,
        url_parts.fragment
    ))
    
    req = urllib.request.Request(target_url, method="GET")
    try:
        with urllib.request.urlopen(req, timeout=15) as res:
            return res.read()
    except Exception as e:
        logger.error(f"Failed to download torrent bytes from Prowlarr: {e}")
        return None
