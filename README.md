# Pytdbot Telegram Video Encoder

This codebase is a small Telegram bot that receives video messages, downloads them through TDLib, re-encodes them with FFmpeg, and uploads the encoded MP4 back to the chat.

It is built around `pytdbot`, `tdjson`, and local FFmpeg binaries. The bot is designed for simple VPS-style deployments where only one FFmpeg encode should run at a time to avoid CPU and memory contention.

## Features

- Handles Telegram video, animation, video note, and `video/*` document messages.
- Downloads media with progress updates.
- Queues encode jobs and runs a single encode worker.
- Re-encodes to H.264/AAC MP4 with selectable profiles.
- Uploads the encoded video with thumbnail, streaming support, and progress updates.
- Shows queue, CPU, RAM, disk, network, and uptime status in bot messages.
- Lets each user persist a default encode profile.
- Supports cancellation by job id or by replying to the original task.
- Cleans temporary downloaded, encoded, thumbnail, and stale incomplete files.

## Repository layout

```text
.
├── README.md
├── requirements.txt
└── telegram_bot/
    ├── __main__.py        # python -m telegram_bot entrypoint
    ├── bot.py             # Telegram client setup, commands, and message routing
    ├── config.py          # environment and .env configuration loading
    ├── encoder.py         # ffprobe/ffmpeg encoding and thumbnail generation
    ├── queue_manager.py   # encode queue, cancellation, cleanup, upload handoff
    ├── runtime.py         # resource status, size/time formatting, stale cleanup
    ├── settings.py        # encode profiles and per-user settings persistence
    └── transfer.py        # Telegram download/upload progress helpers
```

## Requirements

- Python 3.10+
- FFmpeg and FFprobe available on `PATH`
- Telegram API credentials from <https://my.telegram.org/apps>
- Telegram bot token from BotFather

Python dependencies are pinned in `requirements.txt`:

```text
pytdbot==0.9.8.post1
tdjson==1.8.56.post3
```

`tdjson` is installed only on supported 64-bit x86 or ARM platforms according to the environment marker in `requirements.txt`.

## Setup

Create and activate a virtual environment:

```bash
python -m venv .venv
source .venv/bin/activate
```

Install Python dependencies:

```bash
pip install -r requirements.txt
```

Check FFmpeg:

```bash
ffmpeg -version
ffprobe -version
```

Create a local `.env` file in the project root:

```env
TELEGRAM_API_ID=123456
TELEGRAM_API_HASH=your_api_hash
TELEGRAM_BOT_TOKEN=123456:your_bot_token
```

Then start the bot:

```bash
python -m telegram_bot
```

## Configuration

The bot reads `.env` first and then environment variables. Existing environment variables take precedence over `.env` values.

| Variable | Required | Default | Description |
| --- | --- | --- | --- |
| `TELEGRAM_API_ID` or `TELEGRAM_API` | yes | none | Telegram application API id. Must be an integer. |
| `TELEGRAM_API_HASH` or `TELEGRAM_HASH` | yes | none | Telegram application API hash. |
| `TELEGRAM_BOT_TOKEN` or `BOT_TOKEN` | yes | none | Bot token from BotFather. |
| `PYTDBOT_FILES_DIR` | no | `.tdlib` | TDLib files and download directory. |
| `PYTDBOT_DB_KEY` | no | `pytdbot-local-db-key` | TDLib local database encryption key. |
| `PYTDBOT_TD_VERBOSITY` | no | `2` | TDLib logging verbosity. |
| `PYTDBOT_TD_LOG` | no | `tdlib.log` | TDLib log file path. Set empty to disable file logging. |
| `PYTDBOT_TDJSON_LIB` | no | none | Explicit path to a `tdjson` shared library. |
| `BOT_SETTINGS_PATH` | no | `bot_settings.json` | JSON file storing each user's selected encode profile. |
| `BOT_OUTPUT_DIR` | no | `encoded` | Directory for encoded outputs and generated thumbnails. |
| `MAX_JOBS_PER_USER` | no | `1` | Maximum active, queued, or downloading jobs per user. Minimum is `1`. |
| `STALE_FILE_HOURS` | no | `24` | Age threshold for startup cleanup. Minimum is `1`. |

Keep credentials in `.env` or deployment secrets. Do not hard-code Telegram secrets in source files.

## Bot commands

| Command | Description |
| --- | --- |
| `/start` | Confirm the bot is running. |
| `/help` | Show command help. |
| `/ping` | Check responsiveness. |
| `/id` | Show current chat id and sender id. |
| `/settings` | Show available encode profiles and the current user's selected default. |
| `/settings <profile>` | Persist the current user's default profile. |
| `/queue` | Show queue and active job status. |
| `/status` | Alias for `/queue`. |
| `/cancel <job_id>` | Cancel the user's queued or active job by id. |
| `/cancel` as a reply | Cancel the task associated with the replied video message. |

Unknown commands return a help hint. In private chats, non-command text is echoed back.

## Encoding profiles

The default profile is `balanced`.

| Profile | Output | Video settings | Audio |
| --- | --- | --- | --- |
| `balanced` | 720p | H.264, CRF 22, `veryfast` | AAC 128k |
| `speed` | 540p | H.264, CRF 24, `superfast` | AAC 96k |
| `compact` | 480p | H.264, CRF 26, `veryfast` | AAC 96k |
| `quality` | 720p | H.264, CRF 20, `faster` | AAC 160k |
| `tiny` | 360p | H.264, CRF 28, `superfast` | AAC 64k |

The FFmpeg command shape is:

```bash
ffmpeg \
  -i <source> \
  -map 0:v:0 -map 0:a? \
  -vf scale=-2:<profile-resolution> \
  -c:v libx264 \
  -preset <profile-x264-preset> \
  -threads 0 \
  -crf <profile-crf> \
  -c:a aac \
  -b:a <profile-audio-bitrate> \
  -movflags +faststart \
  <output>
```

The bot uses `ffprobe` to read duration and dimensions before upload, and it generates a thumbnail from roughly one quarter into the encoded video when possible.

## Queue and runtime behavior

The workflow for a video is:

1. Validate that the message contains video media.
2. Reserve a per-user job slot.
3. Check that free disk space is at least `2.5x` the source file size.
4. Download the Telegram media through TDLib.
5. Add the downloaded file to the encode queue.
6. Encode one queued job at a time.
7. Upload the encoded MP4 back to the original chat.
8. Delete runtime files after the job finishes, fails, or is cancelled.

The queue can hold up to 20 waiting jobs. `MAX_JOBS_PER_USER` controls how many jobs a single user can have across downloading, queued, and active states.

Cancellation is cooperative:

- queued jobs are removed from the queue and their downloaded source file is deleted;
- active encode jobs signal FFmpeg to terminate, then remove partial output;
- active upload jobs request task cancellation, but Telegram may finish an already-started transfer.

## Runtime files

Common generated files and directories:

| Path | Purpose |
| --- | --- |
| `.tdlib/` | TDLib database and downloaded Telegram files by default. |
| `encoded/` | Temporary encoded MP4 files and thumbnails by default. |
| `bot_settings.json` | Per-user selected encode profiles. |
| `tdlib.log` | TDLib log file by default. |

At startup, the bot removes stale generated files from `BOT_OUTPUT_DIR` and old incomplete TDLib artifacts with suffixes like `.part`, `.partial`, and `.tmp` under `PYTDBOT_FILES_DIR`.

After each job, the source download, encoded MP4, and generated thumbnail are removed. User profile settings are persisted in `bot_settings.json`.

## Development notes

Run the bot locally with:

```bash
python -m telegram_bot
```

There is currently no dedicated test suite in this directory. For a quick syntax check:

```bash
python -m compileall telegram_bot
```

The code uses asynchronous handlers from `pytdbot`, `asyncio` subprocesses for FFmpeg/FFprobe, and direct Telegram message edits for progress reporting.

## Troubleshooting

- `Missing required environment variable`: set the required Telegram values in `.env` or your shell.
- `TELEGRAM_API_ID/TELEGRAM_API must be an integer`: use the numeric API id from Telegram, not the hash.
- FFmpeg or FFprobe errors: confirm both binaries are installed and available on `PATH`.
- Disk space errors: free server storage, reduce input size, or choose a smaller profile such as `compact` or `tiny`.
- Unsupported video errors: verify the input has a usable video stream and that the server FFmpeg build supports the codec/container.
- Upload size errors: Telegram or server limits may reject the encoded output; choose a smaller profile.
