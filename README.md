# Enchanted Encoder

A self-hosted Telegram bot that re-encodes videos on the fly using FFmpeg. Drop a video in chat — the bot downloads it, compresses or converts it with your chosen profile, and sends the result back. Uses **[pyroblack](https://github.com/eyMarv/pyroblack)** (MTProto) for message handling, status updates, and uploads, and **[TDLib](https://core.telegram.org/tdlib)** via pytdbot for efficient file downloads.

---

## Features

### Multi-Codec Encoding
- **H.264** — best compatibility, 5 presets from `tiny` to `quality`
- **HEVC** (`libx265`) — ~50% smaller files at similar quality
- **AV1** (`libsvtav1`) — state-of-the-art compression, up to 30% smaller than HEVC

### 7 Built-In Profiles
| Profile | Codec  | Resolution | CRF | Preset          | Bitrate |
|---------|--------|------------|-----|-----------------|---------|
| Tiny    | H.264  | 360p       | 28  | superfast       | 64k     |
| Compact | H.264  | 480p       | 26  | veryfast        | 96k     |
| Speed   | H.264  | 540p       | 24  | superfast       | 96k     |
| Balanced| H.264  | 720p       | 22  | veryfast        | 128k    |
| Quality | H.264  | 720p       | 20  | faster          | 160k    |
| HEVC    | H.265  | 720p       | 26  | medium          | 96k     |
| AV1     | AV1    | 720p       | 30  | 10 (preset)     | 96k     |

### Live Progress & System Telemetry
Every status update shows real-time encode/upload progress bars, speed, ETA, plus a footer with:
- CPU usage & load
- RAM consumption
- Disk free space
- Network transfer totals
- Bot uptime

All status messages are per-user — one consolidated message tracks your downloads, encodes, and uploads, then auto-deletes 30 seconds after all jobs finish.

### Intelligent Concurrency
- Separate download and encode job limits per user
- Global queue capacity with fair user slot allocation
- Configurable number of parallel encoders (default: 2)
- Thread pinning: `ceil(CPU cores / max encoders)` threads per FFmpeg process
- Stall detection (10 min timeout) on both download and upload

### Queue Persistence
- Journal file (`queue_journal.json`) saves queued job metadata
- On restart, orphaned jobs are detected, logged, and cleaned up
- Settings are stored per-user in atomic JSON writes

### Cancel & Control
- `/cancel <job_id>` or reply to source video to cancel
- Only the job owner can cancel their tasks
- `/check` verifies encoder availability (libx264, libx265, libsvtav1)
- `/settings <profile>` to switch default profile
- `/settings document on|off` to send results as file instead of streamable video

### Smart Upload
- Videos are uploaded as streamable MP4 with thumbnail, caption, and metadata (duration, dimensions)
- Optional document mode for non-streamable delivery
- Rate-limit handling with automatic retry (up to 3 attempts)
- Live upload speed tracking via TDLib file updates

### Startup Housekeeping
- Stale file cleanup on boot (clears partial downloads and old encoded outputs older than `STALE_FILE_HOURS`)

---

## Commands

| Command | Description |
|---------|-------------|
| `/start` | Bot welcome |
| `/help` | Command list |
| `/ping` | Reachability check |
| `/id` | Show chat & sender IDs |
| `/settings` | Show current profile & all available profiles |
| `/settings <name>` | Set default profile (e.g., `/settings hevc`) |
| `/settings document on\|off` | Toggle sending as file |
| `/queue`, `/status` | Your active & queued jobs |
| `/cancel <id>` or reply to source | Cancel a job |
| `/check` | List installed FFmpeg encoders |

---

## Quick Start

```bash
pip install -r requirements.txt
cp .env.example .env
# edit .env with your Telegram API credentials
python -m telegram_bot
```

Required environment variables:
- `TELEGRAM_API_ID` — Telegram API ID (from my.telegram.org)
- `TELEGRAM_API_HASH` — Telegram API hash
- `TELEGRAM_BOT_TOKEN` — Bot token from @BotFather

Optional environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `MAX_JOBS_PER_USER` | 3 | Max queued + active jobs per user |
| `MAX_CONCURRENT_ENCODERS` | 2 | Number of parallel FFmpeg processes |
| `BOT_OUTPUT_DIR` | encoded | Output directory for encoded files |
| `BOT_SETTINGS_PATH` | bot_settings.json | User settings file path |
| `STALE_FILE_HOURS` | 24 | Auto-cleanup age for temp files |
| `PYTDBOT_FILES_DIR` | .tdlib | TDLib file download directory |
| `PYTDBOT_TD_VERBOSITY` | 2 | TDLib log verbosity (0-5) |

---

## Requirements

- **Python 3.12+**
- **FFmpeg** with `libx264`, `libx265`, and `libsvtav1` (if using those codecs)
- **pytdbot** (Telegram MTProto client)
- **tdjson** (TDLib native bindings)

---

## License

MIT
