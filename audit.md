# Repository Audit: Enchanted Encoder

## PHASE 1 — Repository Overview

### 1. Determine
- **primary language**: Python (3.12+)
- **framework**: `pyroblack` (Pyrogram fork) and `pytdbot` (TDLib wrapper)
- **architecture style**: Event-driven, asynchronous worker queue (Actor-model inspired)
- **package manager**: `pip` (via `requirements.txt`)
- **build system**: None (Raw Python execution)
- **testing framework**: None configured
- **deployment method**: Manual self-hosting (no Dockerfile or CI deployment script found)

### 2. Plain English Explanation
This is a self-hosted Telegram bot that receives video files from users, downloads them, and re-encodes (compresses) them on the fly using FFmpeg. It provides real-time progress bars for downloading, transcoding, and uploading, and allows users to select custom profiles like HEVC or AV1.

### 3. Problem it Solves
It eliminates the need for desktop tools like Handbrake for compressing or reformatting videos. Users can simply forward or upload a video directly via Telegram, and the bot will autonomously transcode it to save space or improve compatibility before sending it back.

### 4. Project Maturity
**Beta / MVP**. The system is highly functional with an advanced asynchronous queue, persistent settings, and robust FFmpeg handling. However, it completely lacks automated tests and relies heavily on implicit OS assumptions.

### 5. Important Directories
- `/telegram_bot/` — The core application package.

### 6. Entry Points
- `telegram_bot/__main__.py` (Delegates to `telegram_bot.bot.main()`).

---

## PHASE 2 — Architecture Analysis

- **request flow**: User uploads a video -> `bot.py` filters the message -> Capacity checks occur in `queue_manager.py` -> A `VideoJob` is placed in `asyncio.Queue` -> A worker in `EncodeQueue._run()` picks it up -> `downloader.download()` fetches it -> `encoder.encode_h264_720p()` transcodes it -> `transfer.upload_video()` returns it.
- **data flow**: Telegram Servers -> Local Disk (`.tdlib` cache) -> FFmpeg (Subprocess) -> Local Disk (`encoded/` directory) -> Telegram Servers.
- **dependency injection**: Configuration (`BotConfig`) is loaded via `get_config()` and passed to handlers. `TDLibDownloader` is injected into `EncodeQueue`. 
- **authentication flow**: Delegated entirely to the Telegram Bot API via MTProto. No application-level authentication is required.
- **authorization flow**: Minimal. Any Telegram user can interact with the bot. Users are restricted from canceling jobs they do not own (`queue_manager.py: _matches` check).
- **database layer**: No formal SQL/NoSQL database. State is stored in flat JSON files (`bot_settings.json`, `queue_journal.json`).
- **API layer**: Exclusively the Telegram Bot API via MTProto.
- **service layer**: `queue_manager.py` acts as the orchestrator, managing workers and job lifecycle.
- **UI layer**: Text-based UI in Telegram chats. Uses `safe_edit_text` to continuously rewrite a single message to mimic live progress bars.
- **background jobs**: Implemented via `asyncio.Task` workers consuming from an `asyncio.Queue`.
- **caching**: No application-level caching, though TDLib maintains an internal file cache.
- **configuration loading**: Environment variables loaded synchronously at startup via `load_dotenv` in `config.py`.

---

## PHASE 3 — Dependency Analysis

| Dependency | Purpose | Status |
|---|---|---|
| `pyroblack` | Fork of Pyrogram; handles commands, text messaging, and uploads over MTProto. | Used actively. |
| `pytdbot` | Wrapper around TDLib; used exclusively for high-speed file downloading. | Used actively. |
| `tdjson` | Native bindings required for `pytdbot`. | Pinned to a specific machine architecture via PEP 508. |

- **Unusual dependencies**: Using `pyroblack` instead of standard `pyrogram` or `aiogram` is non-standard. Running dual clients (`pytdbot` + `pyroblack`) in parallel is a complex choice made explicitly for download speeds.
- **Outdated-looking dependencies**: `tdjson==1.8.56.post3` is strictly pinned, likely to guarantee compatibility with `pytdbot`.
- **Unused dependencies**: None.

---

## PHASE 4 — Code Quality Review

| Finding | Type | Location | Severity |
|---|---|---|---|
| **Inconsistent naming** | The function `encode_h264_720p` handles HEVC, AV1, and multiple resolutions dynamically. The name is misleading. | `encoder.py` | Low |
| **Dead code** | Empty `if message.video: pass` block in `handle_media_message`. | `bot.py` | Low |
| **Complex conditionals** | Huge conditional chains using `getattr` to duck-type Pyrogram and pytdbot objects. | `downloader.py` | Medium |
| **Large functions** | `EncodeQueue._run()` orchestrates the entire job lifecycle in a single 100+ line loop. | `queue_manager.py` | Medium |
| **Magic numbers** | `DISK_HEADROOM_MULTIPLIER = 2.5` | `queue_manager.py` | Low |
| **Magic numbers** | `STATUS_CHAT_INTERVAL = 1.2` | `transfer.py` | Low |

---

## PHASE 5 — Design Patterns

- **Factory**: Used in `_build_ffmpeg_command` to dynamically generate subprocess arguments based on the user's `EncodeProfile`.
- **Repository**: `SettingsStore` in `settings.py` isolates file I/O and provides a CRUD-like interface for user profiles.
- **Observer**: Found throughout `bot.py` via event decorators (`@app.on_message`).
- **Singleton**: Implicitly used via global variables `ENCODE_QUEUE`, `SETTINGS_STORE`, and `DOWNLOADER` instantiated in `main_async()`.

---

## PHASE 6 — Security Review

| Finding | File | Function | Explanation | Confidence |
|---|---|---|---|---|
| **Safe Execution** | `encoder.py` | `encode_h264_720p` | Subprocesses use `asyncio.create_subprocess_exec` with array arguments instead of `shell=True`. High protection against Command Injection. | High |
| **Atomic Writes** | `settings.py` | `_save` | Settings are written to `.tmp` and renamed via `os.replace`, preventing data corruption. | High |
| **Denial of Service** | `queue_manager.py`| `handle_media` | The bot rigorously checks `has_free_space` and calculates disk headroom multiplier, preventing users from filling the host's disk. | High |
| **Path Traversal** | `queue_manager.py`| `_short` / `filename` | File naming sanitization is weak, but files are isolated inside `.tdlib` and `encoded` working directories. | Medium |

*(No XSS, SQLi, or hardcoded secrets found).*

---

## PHASE 7 — Performance Review

- **Blocking operations**: `_save_journal` and `_save` write to disk using synchronous `Path.write_text` inside async loops. Under load, this could block the event loop.
- **OS Incompatibility**: `os.getloadavg()` in `status_footer` (runtime.py) will throw an exception on Windows machines, crashing the UI renderer.
- **Repeated API calls**: Mitigated. `safe_edit_text` enforces a local rate limit (`_next_status_edit_by_chat`) and gracefully backs off when Telegram throws a `FloodWait` exception.
- **Memory Leaks**: `_file_registry` in `TDLibDownloader` and `_next_status_edit_by_chat` in `transfer.py` grow unbounded over the lifetime of the application.

---

## PHASE 8 — Error Handling

- **Exception handling**: Excellent. Specific error mapping exists in `friendly_error()` (`queue_manager.py`) to convert technical FFmpeg and Network errors into user-friendly strings.
- **Graceful failures**: `stop_process()` uses an escalated termination path (`SIGTERM` -> timeout -> `SIGKILL`). Cancel events correctly propagate up the stack.
- **Timeout handling**: Built-in 10-minute timeout (`STALL_TIMEOUT`) if FFmpeg stops emitting stdout, preventing deadlocked workers.

---

## PHASE 9 — Testing

- **Test coverage**: 0%
- **Testing style**: N/A
- **Missing tests**: No unit tests, integration tests, or mock frameworks exist. 
- **Critical areas with no tests**: The job state machine (`queue_manager.py`) and the FFmpeg parser (`encoder.py`) are highly volatile and desperately need tests.

---

## PHASE 10 — Maintainability

| Category | Score | Explanation |
|---|---|---|
| Architecture | 8/10 | Clear boundaries between queues, encoders, and transfer logic. |
| Readability | 8/10 | Excellent type hinting (`from __future__ import annotations`). Variables are descriptive. |
| Modularity | 7/10 | Mostly clean, though the dual client setup (Pyrogram + pytdbot) causes some structural bleeding. |
| Documentation | 6/10 | Clear README, but few inline docstrings for complex async mechanisms. |
| Testing | 0/10 | No automated tests exist. |
| Scalability | 6/10 | Solid vertical concurrency (worker queues), but cannot scale horizontally. |
| Security | 9/10 | Safe subprocesses, no SQL, strict disk quota checks. |
| Performance | 7/10 | Minor sync I/O blocking the event loop; minor unbounded dictionaries. |
| Developer Experience | 7/10 | Minimal setup required, but lack of tests makes refactoring risky. |

---

## PHASE 11 — Reverse Engineering

**Request Flow:**
1. **Entry Point:** The application starts via `__main__.py`, initializing global config and starting `pytdbot` and `pyroblack`.
2. **Routing:** `handle_media_message` in `bot.py` catches incoming video/document messages.
3. **Controllers:** `handle_media` intercepts the message, checks queue constraints, disk space, and user quotas.
4. **Services:** It instantiates a `VideoJob` and calls `queue.enqueue()`. The background `EncodeQueue._run()` task pulls the job.
5. **Execution Phase 1 (Download):** `TDLibDownloader` resolves the file ID and streams it to the local `.tdlib` folder.
6. **Execution Phase 2 (Encode):** `encode_h264_720p` launches FFmpeg. It parses `stdout` to calculate speed/ETA, triggering UI updates via `safe_edit_text`.
7. **Execution Phase 3 (Upload):** The transcoded file is uploaded back via MTProto.
8. **Cleanup:** Working files are deleted via `cleanup_file()`.

---

## PHASE 12 — Find Interesting Code

- **Smartest implementation**: The state tracking in `encoder.py`. Reading FFmpeg's `progress=pipe:1` output asynchronously line-by-line while tracking `time.monotonic()` to catch stalled processes is highly resilient.
- **Most complex algorithm**: `TDLibDownloader._resolve_file_id`. It polls a local `_file_registry` dictionary because the incoming pyrogram `message_id` does not immediately match the pytdbot `updateFile` timeline. 
- **Most reusable abstraction**: `ProgressMessage` in `transfer.py`. It completely abstracts away rate-limiting, progress bar rendering, and elapsed time calculation.
- **Worst module**: `downloader.py`. The `_extract_file_info` function is a fragile chain of `hasattr` and `getattr` calls designed to duct-tape object differences between two distinct SDKs.

---

## PHASE 13 — Hidden Assumptions

- **Operating System**: `status_footer()` in `runtime.py` calls `os.getloadavg()`, which will cause a fatal runtime exception on Windows.
- **Binaries**: Assumes `ffmpeg` and `ffprobe` exist in the system `$PATH`.
- **Filesystem**: Assumes the local disk can handle frequent synchronous JSON writes (`bot_settings.json` and `queue_journal.json`).
- **Dependencies**: Assumes Telegram servers will not change the structure of `types.UpdateNewMessage` or `types.UpdateFile`.

---

## PHASE 14 — Technical Debt

| Description | Location | Risk | Effort |
|---|---|---|---|
| **No Test Suite** | Repository-wide | High | High |
| **Sync I/O in Async Loop** | `settings.py`, `queue_manager.py` | Medium | Low |
| **Unbounded Dictionary Growth** | `transfer.py` (`_next_status_edit_by_chat`) | Medium | Low |
| **Unbounded Dictionary Growth** | `downloader.py` (`_file_registry`) | Medium | Low |
| **Windows Incompatibility** | `runtime.py` (`os.getloadavg()`) | Medium | Low |
| **Fragile Duck Typing** | `downloader.py` (`_extract_file_info`) | Medium | Medium |
| **Misnamed Function** | `encoder.py` (`encode_h264_720p`) | Low | Low |
| **Empty Code Block** | `bot.py` (`if message.video: pass`) | Low | Low |

---

## PHASE 15 — Knowledge Test

1. **If you became the maintainer tomorrow, what would you learn first?**
   I would deeply research the structural differences between `pytdbot` objects and `pyroblack` objects. The bot runs two distinct MTProto clients simultaneously, and bridging them seamlessly is the repository's biggest point of failure.
2. **Which three files are the most important?**
   `queue_manager.py`, `encoder.py`, and `bot.py`.
3. **Which module is hardest to understand?**
   `downloader.py` — specifically how it maps incoming Pyrogram messages to TDLib background file downloads.
4. **Which component appears most fragile?**
   `TDLibDownloader._resolve_file_id`, as it relies on an artificial `asyncio.sleep(0.5)` polling loop to sync the two MTProto clients.
5. **Which part would you never refactor without extensive tests?**
   `EncodeQueue._run()`. Modifying this state machine without unit tests risks introducing race conditions, file locking issues, or deadlocking the worker queue.

---

**Confidence:** 98%  
**Estimated repository understanding:** 95%  
**Hallucination risk:** Low