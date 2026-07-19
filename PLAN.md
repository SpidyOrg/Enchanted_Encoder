# Enchanted Encoder — Engineering Plan & Status

Single source of truth for the Enchanted Encoder Telegram video-transcoding bot.
(Read-only audit notes live in `audit.md`; user-facing docs in `README.md`.)

Last updated: 2026-07-19

---

## Overview

**Goal:** Maintain a robust, crash-safe Telegram bot that transcodes video via FFmpeg
using a dual-MTProto setup — `pyroblack` for commands/uploads and `pytdbot`/`tdjson`
for downloads — with no secrets in code, no SQL, and FFmpeg spawned via
`asyncio.create_subprocess_exec` (never `shell=True`).

**Principles**
1. Fail closed: prefer a clean error over silent corruption.
2. Validate every external input (user IDs, filenames, profile args).
3. Keep the event loop non-blocking; clean up all resources.
4. Atomic writes (temp → fsync → `os.replace`) for settings, journal, queue.
5. Backwards-compatible with existing user settings.

**Status:** Phase A (audit-driven robustness work) is **complete**. Remaining work is
tracked under "Remaining Tasks" below.

---

## Phase A — Completed (verified)

All items below are implemented, compile cleanly (`python -m py_compile telegram_bot/*.py`),
and pass functional smoke tests.

### A1. Dependency Injection (removed globals) — `bot.py`
- `BotContext` dataclass (`config`, `queue`, `settings`, `downloader`) injected as `app.ctx`.
- Removed module globals `ENCODE_QUEUE` / `SETTINGS_STORE` / `DOWNLOADER`.
- Accessors `_encode_queue(app)` / `_settings_store(app)` / `_downloader(app)`.
- `handle_media(app, message, config)` receives the client explicitly.

### A2. Atomic, corruption-safe persistence — `settings.py`, `queue_manager.py`
- Settings + queue journal written via `tempfile` → `flush` → `os.fsync` → `os.replace`.
- Settings `.bak` backup taken before each write; `_load_from()` falls back to `.bak` on corruption.
- Settings file secured to `0600`.

### A3. Downloader resource-leak fixes — `downloader.py`
- `_cleanup_file_id()` pops `_pending` + `_progress` registry entries.
- `_file_registry` capped at 1000 entries (drops oldest ~100 on overflow).

### A4. Stronger cancellation semantics
- `QueueManager._run()` aborts if `cancel_event` is set before the encode phase.
- `transfer._run_cancellable()` races the upload against `cancel_event.wait()` via
  `asyncio.wait(FIRST_COMPLETED)` and cancels the in-flight upload task.

### A5. Thread-safe status updates — `queue_manager.py`
- `_user_status` / `_user_status_locks` reads guarded by `self._queue_lock`.
- `_update_user_status()` and `ProgressMessage.update()` attach `build_queue_keyboard()`;
  `safe_edit_text()` accepts `reply_markup`.

### A6. UX + correctness polish
- Block-glyph progress bars (`█`/`░`) in `encoder.py` and `transfer.py`.
- `friendly_error()` returns actionable messages (codec, size, empty-output, stall).
- Zero-byte FFmpeg output rejected with a descriptive error.
- Per-user command rate limiter (`_rate_limited`, 1s cooldown) on `/settings`, `/queue`.
- `pyroblack>=2.8.0,<3.0.0` pin in `requirements.txt`.

### A7. Bugs fixed while building
- Restored dropped `@app.on_message(filters.command("check"))` decorator (`/check`).
- Removed dead `if message.video: pass` branch.
- Fixed undefined `LOGGER` in the callback handler.

---

## Remaining Tasks (R1–R24)

Legend: 🔴 High · 🟡 Medium · 🟢 Low · ⏳ effort (S/M/L)

### 🔴 High Priority

- **R1.** Circular-import cleanup (`transfer.py` / `runtime.py`) — 🟡 S
  Move `status_footer`, `human_size`, `format_duration` into dependency-free
  `telegram_bot/formatting.py`; update importers; delete lazy `import` statements.
- **R2.** Partial download cleanup on failure (`downloader.py`) — 🔴 S
  Wrap `download()` wait in `try/finally` → `_cleanup_file_id()` + `cancelDownloadFile`;
  confirm partial artifacts match `stale_file_cleanup` suffixes.
- **R3.** Resource limits / abuse guards (`config.py`, `bot.py`) — 🔴 M
  `MAX_FILE_SIZE_MB` (reject at enqueue), `MAX_ENCODE_DURATION` (kill FFmpeg wall-clock),
  `MAX_QUEUE_WAIT` (auto-expire stale jobs).
- **R4.** Input validation hardening (`bot.py`) — 🟡 S
  Validate `user_id`/`chat_id` are ints; explicit max filename length guard;
  sanitize `/settings` and `/cancel` free-text args.

### 🟡 Medium Priority

- **R5.** TDLib message-resolution timeout (`downloader.py`) — 🟡 S
  Configurable `TDLIB_MESSAGE_TIMEOUT`; exponential backoff on the `getMessage` fallback.
- **R6.** Sensitive-data protection (`config.py`, logging) — 🟡 S
  Redacting log filter for `api_hash`/`bot_token`; gate filename logging behind
  `LOG_FILENAMES` (default off).
- **R7.** Success message with statistics (`queue_manager.py`) — 🟡 S
  `Original: 150 MB → Encoded: 45 MB (70% smaller) | Time: 2m 30s | Profile: HEVC`.
- **R8.** Status auto-refresh (`queue_manager.py`) — 🟡 M
  Background task refreshing active statuses ~30s while jobs run; respect rate limiter.

### 🟢 Low Priority / Features

- **R9.** `/history` command — 🟢 M — JSON ring buffer of last N jobs per user.
- **R10.** `/preview <profile>` — 🟢 S — estimate output size/reduction without encoding.
- **R11.** Batch / album processing (`bot.py`) — 🟢 M — enqueue each video in a media group.
- **R12.** Custom user profiles (`settings.py`) — 🟢 L — validated `/settings custom crf= res= codec=`.
- **R13.** Large-file thumbnail streaming (`encoder.py`) — 🟢 S — confirm no full-file read.

### 🧪 Testing & Reliability

- **R14.** Unit tests — 🔴 M — settings I/O + backup + `0600`; ffmpeg command builder (HEVC
  `hvc1`, output last); `_sanitize_filename` edge cases; queue ops + cancel + journal;
  progress/format helpers; `friendly_error`.
- **R15.** Integration tests — 🟡 L — full pipeline with fixture video; cancellation at each
  stage; FloodWait retry paths.
- **R16.** Stress tests — 🟢 L — concurrent users, queue at capacity, 2GB+ files.
- **R17.** CI + tooling — 🟡 S — `pytest` config; `ruff` + `mypy` + `pip-audit` in
  `.github/workflows/bot.yml`.

### 📝 Documentation

- **R18.** Code docs — 🟢 S — module docstrings, mermaid architecture diagram, full config table.
- **R19.** User docs — 🟢 S — profile guide, troubleshooting, admin deployment guide.

### 📦 Dependencies

- **R20.** Security audits — 🟡 S — `pip-audit`; confirm `tdjson`/`pytdbot` pins.

### 🚀 Future Enhancements (backlog)

- **R21.** i18n / multi-language messages.
- **R22.** Web dashboard for queue monitoring.
- **R23.** Prometheus metrics (active encodes, queue depth, duration histogram, error rate).
- **R24.** SQLite/Postgres backend replacing JSON for settings/history/queue.

### Suggested implementation order
1. R2, R4 — close remaining safety gaps.
2. R1 — untangle imports (unblocks clean testing).
3. R14 + R17 — unit tests + CI.
4. R3, R6, R5 — resource limits & sensitive-data hardening.
5. R7, R8 — UX polish.
6. R15 / R16 — integration & stress tests.
7. R9–R13 — features.
8. R18–R24 — docs, audits, backlog.

### Definition of Done (per task)
1. Implemented with type hints; no new blocking calls on the event loop.
2. External input validated; failures degrade gracefully with actionable messages.
3. Unit test(s) added and passing.
4. `ruff`/`mypy` clean; `python -m py_compile telegram_bot/*.py` succeeds.
5. Task item checked off here.

---

## Future Enhancement Designs (reference)

These are designs for the backlog (R21–R24) and feature work; not yet implemented.

### Job history (`/history`)
New `telegram_bot/history.py` with a `JobHistoryEntry` dataclass
(`job_id`, `user_id`, `filename`, `profile`, `original_size`, `encoded_size`,
`duration_seconds`, `timestamp`, `success`, `error_message`) and a store exposing
`get_user_history(user_id, limit=10)`. Command replies with a per-user summary.

### Compression statistics (R7)
After upload success, compute `size_reduction = 100 - encoded/original*100` and
`elapsed = time.time() - job.started_at`; render with `human_size` / `format_duration`.

### Batch processing (R11)
Detect album (media group) messages; enqueue each video with the same profile; track
album progress; single completion notification.

### Custom profiles (R12)
New `/customprofile` wizard: validate codec/resolution/CRF/preset; limit resolution
144p–1080p, CRF 18–32, known presets only; store in user settings.

---

## Testing Strategy

- **Unit:** `pytest tests/` for pure logic (settings, ffmpeg builder, sanitizer, queue,
  formatters, `friendly_error`). No network required.
- **Integration:** full pipeline with a tiny fixture video; cancellation + FloodWait mocks.
- **Stress:** concurrent users, queue at capacity, 2GB+ files (guard against memory leaks).
- **Coverage goal:** >80% unit, 100% of critical paths.
- **Environments:** local dev → CI on push → staging integration/stress → prod smoke.

---

## Rollback & Deployment

- **Critical bug:** stop accepting new jobs, let active jobs finish, `git revert`, restart,
  restore `bot_settings.json.bak` if needed.
- **Pre-deploy:** tests pass, code review done, settings + code backed up.
- **Post-deploy:** smoke `/start` + `/settings`, submit test video, monitor memory & FloodWait.
- **Rollback triggers:** >1GB memory growth/hour, crash rate >5%, FloodWait >10/hr,
  data loss, queue deadlock.

---

## Risk Assessment

| Issue | Impact | Likelihood | Priority |
|-------|--------|------------|----------|
| Memory leak crashes bot | HIGH | HIGH | 🔴 (mitigated in A3; watch in R16) |
| Settings corruption loses user data | HIGH | MEDIUM | 🔴 (mitigated in A2) |
| Race condition corrupts queue | HIGH | MEDIUM | 🔴 (mitigated in A2/A5) |
| Command spam DoS | MEDIUM | HIGH | 🟡 (mitigated in A6; tighten in R4) |
| Poor error messages frustrate users | LOW | HIGH | 🟡 (mitigated in A6; expand in R14) |
| Missing tests cause regressions | MEDIUM | MEDIUM | 🟢 (R14/R17) |
| Partial download artifact leak | MEDIUM | MEDIUM | 🔴 R2 |
| Unvalidated input / abuse | MEDIUM | MEDIUM | 🔴 R3/R4 |

---

## Common Pitfalls to Avoid
- Direct manipulation of `asyncio.Queue` internals.
- Unbounded growth of in-memory structures.
- Blocking operations in async code.
- Not handling `FloodWait`.
- Forgetting to clean up temporary files.
- Race conditions in status-message updates.
