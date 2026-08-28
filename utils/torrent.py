import hashlib
import urllib.parse

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
        netloc = parsed.netloc or announce_url.split('/')[2]
        if ':' in netloc:
            netloc = netloc.split(':')[0]
        return netloc
    except Exception:
        return "Unknown"

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
    except Exception:
        return None

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
