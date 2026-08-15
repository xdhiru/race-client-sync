# qBittorrent Cross-Tracker Seeding & FUSE Migration Suite

A professional, resilient, and fully decoupled Python automation suite designed to link racing seedboxes and home seeding servers while saving VPS upload bandwidth. 

Instead of transferring files from your racing VPS to your home server (which consumes limited VPS upload bandwidth), this suite uses Prowlarr to match the active release on a second tracker. It then downloads the files directly on the home server, migrates them to rclone FUSE storage, and re-adds both torrents so they seed simultaneously from the exact same FUSE-mounted files.

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
   - Queries Prowlarr for matching files and structures on target indexers to locate the same torrent content.
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

## Configuration (`sample-config.toml`)

To configure the automation suite, copy the template `sample-config.toml` to `config.toml` and fill in your actual credentials and folder paths:

```bash
cp sample-config.toml config.toml
```

Modify the settings inside `config.toml` (refer to the heavily commented [sample-config.toml](sample-config.toml) file for a complete reference of all settings and defaults).

### Key Parameters:
- `settings.ssd_limit_gb`: Keeps total active downloads under this limit (GB) to prevent disk exhaustion.
- `racing_settings.search_gap_seconds`: Global delay enforced between Prowlarr API queries.
- `racing_settings.completed_category`: Visual category assigned to processed torrents in the racing client once safely migrated, indicating they are ready for deletion.

---

## Installation & Setup

1. **Install Dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

### Option A: Unified Runner Script (One Command)
You can run both daemons simultaneously in the foreground using the unified runner script:
```bash
python3 run.py
```
- **Unified Log Stream**: Logs from both processes will stream to your terminal in real-time.
- **Graceful Shutdown**: Pressing `Ctrl+C` will automatically send termination signals and cleanly stop both background processes.
- **Fail-Safe**: If either script crashes, the runner will automatically terminate the other to prevent orphaned states.

### Option B: Detached tmux Panes (Manual Backgrounding)
If you prefer to run the daemons independently in the background:
1. **Launch Sync Loop**:
   ```bash
   python3 torrent_sync.py
   ```
2. **Launch Racing Link Bridge**:
   ```bash
   python3 racing_link.py
   ```

### Option C: Systemd Services (Recommended for Production)
For automatic startup on boot and crash recovery, configure the scripts as systemd units:

1. Create a service file for the main sync loop at `/etc/systemd/system/torrent-sync.service`:
   ```ini
   [Unit]
   Description=qBittorrent FUSE Sync Daemon
   After=network.target

   [Service]
   Type=simple
   User=kevin
   WorkingDirectory=/home/kevin/scripts/torrents_workflow2
   ExecStart=/usr/bin/python3 torrent_sync.py
   Restart=always
   RestartSec=10

   [Install]
   WantedBy=multi-user.target
   ```

2. Create a service file for the racing bridge at `/etc/systemd/system/racing-link.service`:
   ```ini
   [Unit]
   Description=qBittorrent Racing Link Daemon
   After=network.target

   [Service]
   Type=simple
   User=kevin
   WorkingDirectory=/home/kevin/scripts/torrents_workflow2
   ExecStart=/usr/bin/python3 racing_link.py
   Restart=always
   RestartSec=10

   [Install]
   WantedBy=multi-user.target
   ```

3. Reload, enable, and start the daemons:
   ```bash
   sudo systemctl daemon-reload
   sudo systemctl enable torrent-sync.service racing-link.service
   sudo systemctl start torrent-sync.service racing-link.service
   ```

4. Monitor execution and inspect logs:
   ```bash
   journalctl -u torrent-sync.service -f
   journalctl -u racing-link.service -f
   ```

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE) for details.
