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
    state = job["state"]
    
    batch_suffix = ""
    if "batches" in job and len(job["batches"]) > 1:
        curr = job.get("current_batch_index", 0) + 1
        total = len(job["batches"])
        batch_suffix = f" [{curr}/{total}]"
        
    if state == "rclone_failed":
        status_header = "🔴 <b>[SYNC_FAILED]</b>"
    elif state == "completed":
        status_header = "🟢 <b>[SYNC_COMPLETED]</b>"
    else:
        status_header = f"🟡 <b>[SYNC_IN_PROGRESS]{batch_suffix}</b>"
        
    local_dl = "⚪ DL"
    cooldown = "⚪ Cooldown"
    rclone_move = "⚪ Move"
    re_add = "⚪ Seed"
    
    if state == "added_local":
        local_dl = "🟡 DL"
    elif state == "waiting_5min":
        local_dl = "🟢 DL"
        cooldown = "🟡 Cooldown"
    elif state == "rclone_moving":
        local_dl = "🟢 DL"
        cooldown = "🟢 Cooldown"
        rclone_move = "🟡 Move"
    elif state == "rclone_failed":
        local_dl = "🟢 DL"
        cooldown = "🟢 Cooldown"
        rclone_move = "🔴 Move"
    elif state == "fuse_wait":
        local_dl = "🟢 DL"
        cooldown = "🟢 Cooldown"
        rclone_move = "🟢 Move"
        re_add = "🟡 Seed"
    elif state == "added_remote":
        local_dl = "🟢 DL"
        cooldown = "🟢 Cooldown"
        rclone_move = "🟢 Move"
        re_add = "🟡 Seed"
    elif state == "completed":
        local_dl = "🟢 DL"
        cooldown = "🟢 Cooldown"
        rclone_move = "🟢 Move"
        re_add = "🟢 Seed"
        
    text = (
        f"{status_header} {name} ({size_formatted})\n"
        f"Hash: <code>{info_hash}</code> | Tracker: {tracker}\n"
        f"Steps: {local_dl} ➔ {cooldown} ➔ {rclone_move} ➔ {re_add}"
    )
    
    added_time_str = format_time(job.get("added_time"))
    completion_time_str = format_time(job.get("completion_time"))
    
    times_parts = [f"DL ({added_time_str} ➔ {completion_time_str})"]
    
    move_start = job.get("move_start_time")
    move_end = job.get("move_completed_time")
    if move_start or move_end:
        move_start_str = format_time(move_start)
        if state == "rclone_failed":
            move_end_str = "Failed"
        else:
            move_end_str = format_time(move_end)
        times_parts.append(f"Move ({move_start_str} ➔ {move_end_str})")
        
    readd_start = job.get("readd_start_time")
    seeding_completed = job.get("seeding_completed_time")
    if readd_start or seeding_completed:
        readd_start_str = format_time(readd_start)
        seeding_completed_str = format_time(seeding_completed)
        times_parts.append(f"Seed ({readd_start_str} ➔ {seeding_completed_str})")
        
    times_line = " | ".join(times_parts)
    text += f"\nTimes: {times_line}"
    
    error_msg = job.get("error_msg")
    if error_msg:
        escaped_error = error_msg.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
        text += f"\nError: <pre>{escaped_error}</pre>"
        
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
