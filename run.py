import subprocess
import sys
import time
import signal
import os
import argparse
import shutil
from utils.config import load_config
from services.telegram import send_telegram_notification

def main():
    parser = argparse.ArgumentParser(description="Race Client Sync Automation")
    parser.add_argument("--fresh", "--reset", action="store_true", help="Clear the data and watch_dir folders for a fresh start")
    args = parser.parse_args()

    if not os.path.exists("config.toml"):
        print("Error: config.toml not found! Please copy sample-config.toml to config.toml and configure it.")
        sys.exit(1)

    config = load_config()
    
    if args.fresh:
        print("Performing fresh start: clearing state and watch directories...")
        if os.path.exists("data"):
            try:
                shutil.rmtree("data")
                print(" -> Cleared data directory.")
            except Exception as e:
                print(f" -> Failed to clear data directory: {e}")
                
        watch_dir = config.get("paths", {}).get("watch_dir")
        completed_dir = config.get("paths", {}).get("completed_dir")
        if not completed_dir and watch_dir:
            completed_dir = os.path.join(watch_dir, "completed")
            
        if watch_dir and os.path.exists(watch_dir):
            try:
                for filename in os.listdir(watch_dir):
                    file_path = os.path.join(watch_dir, filename)
                    
                    if completed_dir and os.path.abspath(file_path) == os.path.abspath(completed_dir):
                        continue
                        
                    try:
                        if os.path.isfile(file_path) or os.path.islink(file_path):
                            os.unlink(file_path)
                        elif os.path.isdir(file_path):
                            shutil.rmtree(file_path)
                    except Exception as e:
                        print(f" -> Failed to delete {file_path}. Reason: {e}")
                print(f" -> Cleared watch directory ({watch_dir}).")
            except Exception as e:
                print(f" -> Failed to clear watch directory: {e}")
        print("Fresh start complete. Booting up...")
        print("")
        
    command_str = " ".join(sys.argv)
    send_telegram_notification(
        config,
        "chat_id",
        f"🚀 <b>Race Client Sync Started</b>\nCommand: <code>python {command_str}</code>"
    )

    print("Starting qBittorrent FUSE and Racing Sync Automation...")

    # Start both processes (inheriting stdout/stderr so logs stream to the terminal in one place)
    sync_proc = subprocess.Popen([sys.executable, "torrent_sync.py"])
    racing_proc = subprocess.Popen([sys.executable, "racing_link.py"])

    print(f"Started torrent_sync.py (PID: {sync_proc.pid})")
    print(f"Started racing_link.py (PID: {racing_proc.pid})")
    print("Running... Press [Ctrl+C] to stop both daemons.")

    def signal_handler(sig, frame):
        print("\nShutting down daemons...")
        sync_proc.terminate()
        racing_proc.terminate()
        # Wait for them to exit
        sync_proc.wait()
        racing_proc.wait()
        print("Stopped all sync automation daemons.")
        sys.exit(0)

    # Register signals for clean shutdown
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Keep running and monitor processes
    while True:
        sync_code = sync_proc.poll()
        racing_code = racing_proc.poll()

        # If either process died, terminate the other and exit
        if sync_code is not None:
            print(f"Warning: torrent_sync.py exited unexpectedly with code {sync_code}")
            racing_proc.terminate()
            racing_proc.wait()
            sys.exit(sync_code)
            
        if racing_code is not None:
            print(f"Warning: racing_link.py exited unexpectedly with code {racing_code}")
            sync_proc.terminate()
            sync_proc.wait()
            sys.exit(racing_code)

        time.sleep(1)

if __name__ == "__main__":
    main()
