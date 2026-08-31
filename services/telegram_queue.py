import logging
import threading
import time
from queue import Queue, Empty
from collections import defaultdict

from services.telegram import update_telegram_status, send_already_seeding_notification

logger = logging.getLogger(__name__)

_DEBOUNCE_SECONDS = 3.0
_QUEUE_WAIT_TIMEOUT = 0.5

_queue = None
_worker_thread = None
_stop_event = None
_pending_jobs = {}
_pending_lock = threading.Lock()


def _dispatcher_loop():
    from utils.config import load_config
    logger.info("Telegram debounced dispatcher started.")
    last_sent_at = defaultdict(float)

    def _safe_load_config():
        try:
            return load_config()
        except SystemExit:
            return None
        except Exception as e:
            logger.error(f"load_config failed: {e}")
            return None

    while not _stop_event.is_set():
        config = _safe_load_config()
        if config is None:
            time.sleep(1.0)
            continue

        # 1. Process immediate priority events
        try:
            while True:
                item = _queue.get_nowait()
                kind = item[0]
                info_hash = item[1]
                rest = item[2:]
                try:
                    if kind == "status_with_racing":
                        job, racing_hashes = rest
                        with _pending_lock:
                            _pending_jobs.pop(info_hash, None)
                        update_telegram_status(config, job, info_hash, racing_hashes=racing_hashes)
                        last_sent_at[info_hash] = time.time()
                    elif kind == "already_seeding":
                        name, size, tracker, racing_hashes = rest
                        send_already_seeding_notification(
                            config,
                            name=name,
                            size=size,
                            tracker=tracker,
                            racing_hashes=racing_hashes,
                            info_hash=info_hash,
                        )
                    elif kind == "status_immediate":
                        (job,) = rest
                        with _pending_lock:
                            _pending_jobs.pop(info_hash, None)
                        update_telegram_status(config, job, info_hash)
                        last_sent_at[info_hash] = time.time()
                except Exception as e:
                    logger.error(f"Telegram dispatcher failed to send immediate {kind} for {info_hash}: {e}")
        except (Empty, AttributeError):
            pass

        # 2. Process debounced pending status updates
        now = time.time()
        ready = []
        with _pending_lock:
            for h, (job, queued_time) in list(_pending_jobs.items()):
                # Send if debounced or if it's been waiting longer than DEBOUNCE_SECONDS
                if now - last_sent_at.get(h, 0) >= _DEBOUNCE_SECONDS:
                    ready.append((h, dict(job)))
                    _pending_jobs.pop(h, None)

        for h, job in ready:
            try:
                update_telegram_status(config, job, h)
                last_sent_at[h] = time.time()
            except Exception as e:
                logger.error(f"Telegram dispatcher debounced send failed for {h}: {e}")

        _stop_event.wait(_QUEUE_WAIT_TIMEOUT)

    # Flush remaining items on exit
    config = _safe_load_config()
    if config:
        with _pending_lock:
            for h, (job, _) in _pending_jobs.items():
                try:
                    update_telegram_status(config, job, h)
                except Exception:
                    pass
            _pending_jobs.clear()

    logger.info("Telegram debounced dispatcher stopped.")


def start():
    global _queue, _worker_thread, _stop_event
    if _worker_thread is not None and _worker_thread.is_alive():
        return
    _queue = Queue()
    _stop_event = threading.Event()
    _worker_thread = threading.Thread(target=_dispatcher_loop, daemon=True, name="TelegramDispatcher")
    _worker_thread.start()


def stop(timeout=5.0):
    global _worker_thread, _stop_event, _queue
    if _stop_event is not None:
        _stop_event.set()
    if _worker_thread is not None:
        _worker_thread.join(timeout=timeout)
    _worker_thread = None
    _stop_event = None
    _queue = None


def enqueue_status(job, info_hash):
    if _stop_event is None:
        return
    state = job.get("state")
    # If it's a terminal or critical state, send immediately
    if state in ("completed", "rclone_failed", "add_remote_failed"):
        if _queue is not None:
            _queue.put(("status_immediate", info_hash, dict(job)))
        return

    with _pending_lock:
        _pending_jobs[info_hash] = (dict(job), time.time())


def enqueue_status_with_racing(job, info_hash, racing_hashes):
    if _queue is None:
        return
    _queue.put(("status_with_racing", info_hash, dict(job), racing_hashes))


def enqueue_already_seeding(info_hash, name, size, tracker, racing_hashes):
    if _queue is None:
        return
    _queue.put(("already_seeding", info_hash, name, size, tracker, racing_hashes))
