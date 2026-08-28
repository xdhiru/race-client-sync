import json
import logging
import urllib.request
import time

logger = logging.getLogger(__name__)

def format_size(size_bytes):
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size_bytes < 1024.0:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.2f} PB"

def format_time(ts):
    if not ts:
        return "Pending"
    try:
        return time.strftime('%H:%M:%S', time.localtime(ts))
    except Exception:
        return "Error"

def call_telegram_api(method, payload, token):
    url = f"https://api.telegram.org/bot{token}/{method}"
    headers = {"Content-Type": "application/json"}
    data = json.dumps(payload).encode('utf-8')
    req = urllib.request.Request(url, data=data, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=10) as response:
            res = json.loads(response.read().decode('utf-8'))
            if not res.get("ok"):
                logger.error(f"Telegram API error response: {res}")
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

def build_telegram_message_text(job, info_hash):
    name = job["name"]
    size = job["size"]
    size_formatted = format_size(size)
    
    tracker = job.get("tracker", "Unknown")
    tracker = tracker.replace("https://", "").replace("http://", "").split("/")[0]
    
    state = job["state"]
    
    batch_suffix = ""
    if "batches" in job and len(job["batches"]) > 1:
        curr = job.get("current_batch_index", 0) + 1
        total = len(job["batches"])
        batch_suffix = f" [{curr}/{total}]"
        
    if state == "rclone_failed":
        status_header = "❌ <b>[SYNC_FAILED]</b>"
    elif state == "completed":
        status_header = "✅ <b>[SYNC_COMPLETED]</b>"
    else:
        status_header = f"🔄 <b>[SYNC_IN_PROGRESS]{batch_suffix}</b>"
        
    text = f"{status_header}\n<code>{name}</code> ({size_formatted})"
    
    if state == "completed":
        added_time = job.get("added_time")
        completed_time = job.get("seeding_completed_time")
        if added_time and completed_time:
            duration = int(completed_time - added_time)
            m, s = divmod(duration, 60)
            h, m = divmod(m, 60)
            if h > 0:
                duration_str = f"{h}h {m}m"
            else:
                duration_str = f"{m}m {s}s"
            text += f"\nTracker: {tracker} | Time: {duration_str}"
        else:
            text += f"\nTracker: {tracker}"
    elif state == "rclone_failed":
        text += f"\nTracker: {tracker}"
        error_msg = job.get("error_msg")
        if error_msg:
            escaped_error = error_msg.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
            text += f"\nError: <pre>{escaped_error}</pre>"
    else:
        if state == "added_local":
            step = "Downloading..."
        elif state == "waiting_5min":
            step = "Cooldown..."
        elif state == "rclone_moving":
            step = "Moving to Remote..."
        elif state == "fuse_wait":
            step = "Waiting for Mount..."
        elif state == "added_remote":
            step = "Seeding on Remote..."
        else:
            step = state
            
        text += f"\nStatus: {step}"
        
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
