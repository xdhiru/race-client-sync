import json
import logging
import urllib.request
import time
import threading
import clients

logger = logging.getLogger(__name__)

_listener_started = False

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

def call_telegram_api(method, payload, token, timeout=10):
    url = f"https://api.telegram.org/bot{token}/{method}"
    headers = {"Content-Type": "application/json"}
    data = json.dumps(payload).encode('utf-8')
    req = urllib.request.Request(url, data=data, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as response:
            res = json.loads(response.read().decode('utf-8'))
            if not res.get("ok"):
                logger.error(f"Telegram API error response: {res}")
            return res
    except Exception as e:
        logger.error(f"Telegram API call failed ({method}): {e}")
        return None

def send_telegram_notification(config, chat_id_key, text, reply_markup=None):
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
    if reply_markup:
        payload["reply_markup"] = reply_markup
    call_telegram_api("sendMessage", payload, bot_token)

def send_already_seeding_notification(config, name, size, tracker, racing_hash=None):
    tg_config = config.get("telegram", {})
    if not tg_config.get("enabled", False):
        return
    bot_token = tg_config.get("bot_token")
    chat_id = tg_config.get("chat_id")
    if not bot_token or not chat_id:
        return
        
    size_formatted = format_size(size)
    tracker_clean = tracker.replace("https://", "").replace("http://", "").split("/")[0]
    
    text = f"⚡ <b>[ALREADY_SEEDING]</b>\n<code>{name}</code> ({size_formatted})\nTracker: {tracker_clean}\nStatus: Already seeding from FUSE mount"
    
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML"
    }
    
    if racing_hash:
        if isinstance(racing_hash, str):
            racing_hashes = [racing_hash]
        elif isinstance(racing_hash, list):
            racing_hashes = racing_hash
        else:
            racing_hashes = []
            
        buttons = []
        for rh in racing_hashes:
            buttons.append([
                {
                    "text": f"Delete {rh[:8]} from Race Client",
                    "callback_data": f"del_race:{rh}"
                }
            ])
            
        if buttons:
            payload["reply_markup"] = {
                "inline_keyboard": buttons
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
        status_header = f"📥 <b>[SYNC_IN_PROGRESS]{batch_suffix}</b>"
        
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

def handle_telegram_callback_query(config, cq, bot_token):
    callback_id = cq.get("id")
    data = cq.get("data", "")
    message = cq.get("message", {})
    chat_id = message.get("chat", {}).get("id")
    message_id = message.get("message_id")
    
    if not data or not callback_id:
        return
        
    if data.startswith("del_race:"):
        racing_hash = data.split("del_race:", 1)[1]
        logger.info(f"Telegram callback received: request to delete racing torrent {racing_hash}")
        
        racing_client = None
        try:
            racing_client = clients.get_client(config.get("racing_client"))
            if racing_client and not racing_client.connect():
                racing_client = None
        except Exception as e:
            logger.error(f"Failed to connect to racing client for Telegram deletion: {e}")
            
        if not racing_client:
            call_telegram_api("answerCallbackQuery", {
                "callback_query_id": callback_id,
                "text": "❌ Could not connect to race client.",
                "show_alert": True
            }, bot_token)
            return
            
        try:
            success = racing_client.delete_torrent(racing_hash, delete_files=True)
            if success:
                logger.info(f"Successfully deleted torrent {racing_hash} from race client via Telegram button.")
                call_telegram_api("answerCallbackQuery", {
                    "callback_query_id": callback_id,
                    "text": "✅ Successfully deleted from race client!"
                }, bot_token)
                
                orig_text = message.get("text", "")
                orig_text_escaped = orig_text.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
                new_text = f"{orig_text_escaped}\n\n🗑️ <b>Deleted from Race Client</b>"
                call_telegram_api("editMessageText", {
                    "chat_id": chat_id,
                    "message_id": message_id,
                    "text": new_text,
                    "parse_mode": "HTML",
                    "reply_markup": {"inline_keyboard": []}
                }, bot_token)
            else:
                call_telegram_api("answerCallbackQuery", {
                    "callback_query_id": callback_id,
                    "text": "❌ Failed to delete torrent from race client. Check logs.",
                    "show_alert": True
                }, bot_token)
        except Exception as e:
            logger.error(f"Error executing Telegram deletion of racing torrent {racing_hash}: {e}")
            call_telegram_api("answerCallbackQuery", {
                "callback_query_id": callback_id,
                "text": f"❌ Error: {str(e)[:50]}",
                "show_alert": True
            }, bot_token)

def start_telegram_listener(config_loader):
    global _listener_started
    if _listener_started:
        return
    _listener_started = True

    def listener_worker():
        logger.info("Starting Telegram Bot callback listener thread...")
        offset = 0
        while True:
            try:
                config = config_loader()
                tg_config = config.get("telegram", {})
                if not tg_config.get("enabled", False):
                    time.sleep(10)
                    continue
                    
                bot_token = tg_config.get("bot_token")
                if not bot_token:
                    time.sleep(10)
                    continue
                    
                payload = {
                    "offset": offset,
                    "timeout": 20,
                    "allowed_updates": ["callback_query"]
                }
                
                res = call_telegram_api("getUpdates", payload, bot_token, timeout=30)
                if res and res.get("ok"):
                    updates = res.get("result", [])
                    for update in updates:
                        update_id = update.get("update_id", 0)
                        if update_id >= offset:
                            offset = update_id + 1
                            
                        cq = update.get("callback_query")
                        if cq:
                            handle_telegram_callback_query(config, cq, bot_token)
                else:
                    time.sleep(5)
            except Exception as e:
                logger.error(f"Error in Telegram callback listener: {e}")
                time.sleep(5)

    thread = threading.Thread(target=listener_worker, daemon=True, name="TelegramListener")
    thread.start()
