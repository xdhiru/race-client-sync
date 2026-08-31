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

def build_racing_keyboard(config, racing_hash, public_hash=None):
    racing_hashes = []
    if racing_hash:
        if isinstance(racing_hash, str):
            racing_hashes = [racing_hash]
        elif isinstance(racing_hash, list):
            racing_hashes = list(racing_hash)  # Copy list
            
    if public_hash and public_hash not in racing_hashes:
        racing_hashes.append(public_hash)
        
    if not racing_hashes:
        return None
        
    racing_torrents = []
    try:
        import clients
        racing_client = clients.get_client(config.get("racing_client"))
        if racing_client and racing_client.connect():
            racing_torrents = racing_client.get_torrents_info()
    except Exception as e:
        logger.error(f"Failed to fetch racing client info for Telegram buttons: {e}")
        
    buttons = []
    actual_racing_hashes = []
    for rh in racing_hashes:
        domain = ""
        found = False
        for t in racing_torrents:
            if t["hash"] == rh:
                found = True
                trackers = t.get("trackers", [])
                if trackers:
                    domain = trackers[0].replace("https://", "").replace("http://", "").split("/")[0]
                break
                
        if not found:
            continue
            
        actual_racing_hashes.append(rh)

    # Re-build buttons in 2 columns
    all_individual_buttons = []
    for rh in actual_racing_hashes:
        domain = ""
        for t in racing_torrents:
            if t["hash"] == rh:
                trackers = t.get("trackers", [])
                if trackers:
                    domain = trackers[0].replace("https://", "").replace("http://", "").split("/")[0]
                break
        
        # We need to extract the existing button prefix (which could be the trash can emoji)
        # However, since the emoji was injected previously via a literal '🗑', 
        # let's just safely use the literal!
        button_text = f"🗑 {domain} ({rh[:6]})" if domain else f"🗑 {rh[:6]}"
        all_individual_buttons.append({
            "text": button_text,
            "callback_data": f"del_race:{rh}"
        })
        
    for i in range(0, len(all_individual_buttons), 2):
        buttons.append(all_individual_buttons[i:i+2])
        
    if len(actual_racing_hashes) > 1:
        buttons.append([
            {
                "text": "🗑️ Remove All from Race Client",
                "callback_data": "del_all"
            }
        ])
        
    if buttons:
        return {"inline_keyboard": buttons}
    return None

def send_already_seeding_notification(config, name, size, tracker, racing_hash=None, info_hash=None):
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
    
    if racing_hash or info_hash:
        keyboard = build_racing_keyboard(config, racing_hash, public_hash=info_hash)
        if keyboard:
            payload["reply_markup"] = keyboard
        
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

def update_telegram_status(config, job, info_hash, racing_hash=None):
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
    
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML"
    }
    
    if racing_hash or info_hash:
        keyboard = build_racing_keyboard(config, racing_hash, public_hash=info_hash)
        if keyboard:
            payload["reply_markup"] = keyboard
            
    if not message_id:
        res = call_telegram_api("sendMessage", payload, bot_token)
        if res and res.get("ok"):
            job["telegram_message_id"] = res["result"]["message_id"]
    else:
        payload["message_id"] = message_id
        call_telegram_api("editMessageText", payload, bot_token)

def handle_telegram_callback_query(config, cq, bot_token):
    callback_id = cq.get("id")
    data = cq.get("data", "")
    message = cq.get("message", {})
    chat_id = message.get("chat", {}).get("id")
    message_id = message.get("message_id")
    
    if not data or not callback_id:
        return
        
    if data == "ignore":
        call_telegram_api("answerCallbackQuery", {
            "callback_query_id": callback_id,
            "text": ""
        }, bot_token)
        return
        
    if data.startswith("del_race:") or data == "del_all":
        logger.info(f"Telegram callback received: request to delete racing torrent(s) via {data}")
        
        orig_keyboard = message.get("reply_markup", {}).get("inline_keyboard", [])
        
        hashes_to_delete = []
        if data == "del_all":
            for row in orig_keyboard:
                for button in row:
                    cb = button.get("callback_data", "")
                    if cb.startswith("del_race:"):
                        hashes_to_delete.append(cb.split("del_race:", 1)[1])
        else:
            hashes_to_delete = [data.split("del_race:", 1)[1]]
            
        active_trackers = 0
        for row in orig_keyboard:
            for button in row:
                cb = button.get("callback_data", "")
                if cb.startswith("del_race:"):
                    active_trackers += 1
                    
        should_delete_files = (active_trackers <= 1) or (data == "del_all")
        
        racing_client = None
        try:
            import clients
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
            success_count = 0
            for rh in hashes_to_delete:
                if racing_client.delete_torrent(rh, delete_files=should_delete_files):
                    success_count += 1
                    logger.info(f"Successfully deleted torrent {rh} from race client (delete_files={should_delete_files}).")
                    
            if success_count > 0:
                call_telegram_api("answerCallbackQuery", {
                    "callback_query_id": callback_id,
                    "text": f"✅ Successfully deleted {success_count} torrent(s)!"
                }, bot_token)
                
                orig_text = message.get("text", "")
                orig_text_escaped = orig_text.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
                
                new_keyboard = []
                for row in orig_keyboard:
                    new_row = []
                    for button in row:
                        cb = button.get("callback_data", "")
                        if cb in [f"del_race:{h}" for h in hashes_to_delete]:
                            new_row.append({
                                "text": button.get("text", "").replace("Delete ", "✅ Removed "),
                                "callback_data": "ignore"
                            })
                        elif cb == "del_all":
                            if data == "del_all" or active_trackers <= 1:
                                new_row.append({
                                    "text": "✅ Removed All!",
                                    "callback_data": "ignore"
                                })
                            else:
                                new_row.append(button)
                        else:
                            new_row.append(button)
                    new_keyboard.append(new_row)
                    
                call_telegram_api("editMessageText", {
                    "chat_id": chat_id,
                    "message_id": message_id,
                    "text": orig_text_escaped,
                    "parse_mode": "HTML",
                    "reply_markup": {"inline_keyboard": new_keyboard}
                }, bot_token)
            else:
                call_telegram_api("answerCallbackQuery", {
                    "callback_query_id": callback_id,
                    "text": "❌ Failed to delete torrent from race client. Check logs.",
                    "show_alert": True
                }, bot_token)
        except Exception as e:
            logger.error(f"Error executing Telegram deletion: {e}")
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
