# qBittorrent FUSE & Racing Sync Automation

A professional, resilient, and fully decoupled Python synchronization suite designed for VPS environments with low CPU/IO budgets. It automates the workflow of downloading torrents, migrating them to rclone remotes, and managing racing client migrations.

## System Overview

The system consists of two completely independent, daemonized Python scripts that coordinate via isolated JSON state files:

1. **`torrent_sync.py` (Main Sync Loop)**:
   - Monitors a watch directory for new `.torrent` files.
   - Manages queue additions to qBittorrent under SSD storage limits.
   - Automates FUSE cooldown states, starts idempotent `rclone move` operations, and migrates seeding status to remote FUSE storage once complete.
   - Migrates completed matched racing client torrents to the local FUSE instance to maximize long-term seeding space.

2. **`racing_link.py` (Bridge Daemon)**:
   - Polls a secondary qBittorrent instance (racing client) for tracker matches (e.g., `tracker_domain.com`).
   - Gated on **100% download completion** to avoid premature matching.
   - Queries Prowlarr for matching files and structures on target indexers (e.g., Local) to locate the same torrent content.
   - Employs a global query spacing limit to prevent API rate-limiting or hammering indexers.
   - Automatically downloads matching `.torrent` files into the watch folder and registers mappings for migration.

---

## Architectural Highlights & Resilience

- **Failsafe Mapping Segregation (`tracker_mappings.json`)**:
  Mappings between the racing instance and the main seed pool are segregated in `tracker_mappings.json`. This decouples the processes entirely and prevents read-write race conditions or JSON corruption.
- **qBittorrent Unreachability Resilience**:
  If either the racing instance or the main instance becomes unresponsive or goes offline, both scripts automatically enter a polling-retry state without crashing.
- **Resilient Migration Retries**:
  If the racing client is down when `torrent_sync.py` completes a FUSE upload, the migration helper defers removing the job. It holds the state and retries the `.torrent` export and migration in every subsequent loop cycle until the client is restored and the migration completes successfully.
- **Prowlarr Spacing Spiker Protection**:
  Allows defining a global search gap (e.g., 3 minutes) so that multiple discovered candidates are searched sequentially over multiple loop cycles rather than all at once.

---

## Configuration (`config.json`)

Configure your credentials and folder paths in the `config.json` file:

```json
{
  "qbittorrent": {
    "url": "http://127.0.0.1:10889/",
    "username": "admin",
    "password": "adminadmin"
  },
  "paths": {
    "watch_dir": "/home/kevin/syncthing/torrentsB",
    "completed_dir": "/home/kevin/torrents/completed",
    "local_save_path": "/home/kevin/torrents/qbittorrent/",
    "remote_save_path": "/mnt/remote_name/qbittorrent/",
    "category": "SSD",
    "remote_category": "remote"
  },
  "settings": {
    "ssd_limit_gb": 30.0,
    "wait_time_minutes": 2,
    "poll_interval_seconds": 10,
    "fuse_cooldown_seconds": 15,
    "max_active_downloads": 3
  },
  "rclone": {
    "remote": "remote_name:qbittorrent/",
    "transfers": 2,
    "max_parallel_jobs": 2
  },
  "telegram": {
    "enabled": false,
    "bot_token": "YOUR_TELEGRAM_BOT_TOKEN",
    "chat_id": "YOUR_TELEGRAM_CHAT_ID",
    "oversized_chat_id": "YOUR_OVERSIZED_CHAT_ID",
    "not_found_chat_id": "YOUR_NOT_FOUND_CHAT_ID"
  },
  "racing_qbittorrent": {
    "url": "http://127.0.0.1:10890/",
    "username": "admin",
    "password": "adminadmin"
  },
  "prowlarr": {
    "url": "http://127.0.0.1:9696/",
    "api_key": "YOUR_PROWLARR_API_KEY",
    "indexer_name": "local"
  },
  "racing_settings": {
    "poll_interval_seconds": 60,
    "tracker_filter": "tracker_domain.com",
    "max_search_age_hours": 24,
    "search_interval_minutes": 15,
    "search_gap_seconds": 180,
    "completed_category": "processed"
  }
}
```

### Key Parameters:
- `settings.ssd_limit_gb`: Keeps total active downloads under this limit (GB) to prevent disk exhaustion.
- `racing_settings.search_gap_seconds`: Global delay enforced between Prowlarr API queries.
- `racing_settings.completed_category`: Visual category assigned to processed torrents in the racing client once safely migrated, indicating they are ready for deletion.

---

## Installation & Setup

1. **Install Dependencies**:
   ```bash
   pip install qbittorrent-api
   ```
2. **Launch Sync Loop**:
   Run in a detached tmux pane:
   ```bash
   python3 torrent_sync.py
   ```
3. **Launch Racing Link Bridge**:
   Run in another detached tmux pane:
   ```bash
   python3 racing_link.py
   ```

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE) for details.
