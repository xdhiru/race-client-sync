import logging
import threading
import time
from queue import Queue, Empty
from collections import defaultdict

from services.telegram import update_telegram_status, send_already_seeding_notification

logger = logging.getLogger(__name__)

_DEBOUNCE_SECONDS = 5.0
_QUEUE_WAIT_TIMEOUT = 1.0

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
            time.sleep(2.0)
            continue

        drained_any = False
        try:
            while True:
                item = _queue.get_nowait()
                drained_any = True
                kind = item[0]
                info_hash = item[1]
                rest = item[2:]
                with _pending_lock:
                    _pending_jobs.pop(info_hash, None)
                    last_sent_at.pop(info_hash, None)
                try:
                    if kind == "status":
                        (job,) = rest
                        update_telegram_status(config, job, info_hash)
                    elif kind == "status_with_racing":
                        job, racing_hashes = rest
                        update_telegram_status(config, job, info_hash, racing_hashes=racing_hashes)
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
                except Exception as e:
                    logger.error(f"Telegram dispatcher failed to send {kind} for {info_hash}: {e}")
        except Empty:
            pass

        now = time.time()
        with _pending_lock:
            ready = [(h, job) for h, (job, _) in _pending_jobs.items()
                     if now - last_sent_at.get(h, 0) >= _DEBOUNCE_SECONDS]
        for h, job in ready:
            try:
                update_telegram_status(config, job, h)
                last_sent_at[h] = time.time()
            except Exception as e:
                logger.error(f"Telegram dispatcher debounced send failed for {h}: {e}")

        if drained_any:
            continue
        _stop_event.wait(_QUEUE_WAIT_TIMEOUT)

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
    global _worker_thread, _stop_event
    if _stop_event is not None:
        _stop_event.set()
    if _worker_thread is not None:
        _worker_thread.join(timeout=timeout)
    _worker_thread = None
    _stop_event = None
    _queue = None


def enqueue_status(job, info_hash):
    if _queue is None:
        return
    with _pending_lock:
        _pending_jobs[info_hash] = (job, time.time())
    _queue.put(("status", info_hash, job))


def enqueue_status_with_racing(job, info_hash, racing_hashes):
    if _queue is None:
        return
    _queue.put(("status_with_racing", info_hash, job, racing_hashes))


def enqueue_already_seeding(info_hash, name, size, tracker, racing_hashes):
    if _queue is None:
        return
    _queue.put(("already_seeding", info_hash, name, size, tracker, racing_hashes))
