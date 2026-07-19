from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import re
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from pytdbot import Client, types

from telegram_bot.runtime import status_footer


BAR_WIDTH = 12
STATUS_EDIT_INTERVAL = 5.0
STATUS_EDIT_TIMEOUT = 12.0
STATUS_CHAT_INTERVAL = 1.2
MAX_RATE_LIMIT_RETRIES = 3
STALL_TIMEOUT = 600
LOGGER = logging.getLogger(__name__)
_next_status_edit_by_chat: dict[str, float] = {}


async def safe_edit_text(message: types.Message, text: str) -> bool:
    """Status messages are optional telemetry and must never stop media work."""
    chat_key = str(getattr(message, "chat_id", "global"))
    now = time.monotonic()
    if now < _next_status_edit_by_chat.get(chat_key, 0.0):
        return False
    _next_status_edit_by_chat[chat_key] = now + STATUS_CHAT_INTERVAL
    try:
        result = await asyncio.wait_for(message.edit_text(text), timeout=STATUS_EDIT_TIMEOUT)
        if getattr(result, "is_error", False):
            raise RuntimeError(_result_error(result, "Failed to update status"))
        return True
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        if is_rate_limited(exc):
            _next_status_edit_by_chat[chat_key] = max(
                _next_status_edit_by_chat[chat_key], time.monotonic() + retry_after_seconds(exc)
            )
        LOGGER.warning("Status message update failed; it will be retried: %s", exc)
        return False


@dataclass
class ProgressMessage:
    message: types.Message
    label: str
    total: int
    started_at: float = field(default_factory=time.monotonic)
    last_edit_at: float = 0.0
    last_text: str = ""
    extra: str = ""
    next_edit_at: float = field(init=False)
    queue: Any = None
    user_id: str = ""

    def __post_init__(self) -> None:
        message_id = int(getattr(self.message, "id", id(self.message)) or id(self.message))
        self.next_edit_at = self.started_at + (message_id % 5) * STATUS_CHAT_INTERVAL

    async def update(self, current: int, force: bool = False) -> None:
        now = time.monotonic()
        if not force and now < self.next_edit_at:
            return

        progress = render_progress(
            self.label,
            current,
            self.total,
            now - self.started_at,
            self.extra,
        )
        if self.queue and self.user_id:
            text = self.queue._render_user_status(self.user_id, progress)
        else:
            text = progress + status_footer()
        if text == self.last_text:
            return

        self.last_edit_at = now
        self.next_edit_at = now + STATUS_EDIT_INTERVAL
        if await safe_edit_text(self.message, text):
            self.last_text = text


class UploadProgress:
    def __init__(self) -> None:
        self._uploaded_by_path: dict[str, int] = {}

    def watch(self, path: str) -> None:
        self._uploaded_by_path[os.path.abspath(path)] = 0

    def unwatch(self, path: str) -> None:
        self._uploaded_by_path.pop(os.path.abspath(path), None)

    def get(self, path: str) -> int:
        return self._uploaded_by_path.get(os.path.abspath(path), 0)

    def update_from_file(self, file: types.File) -> None:
        local = getattr(file, "local", None)
        remote = getattr(file, "remote", None)
        path = os.path.abspath(getattr(local, "path", "") or "")
        if not path or path not in self._uploaded_by_path:
            return
        self._uploaded_by_path[path] = int(getattr(remote, "uploaded_size", 0) or 0)


def render_progress(
    label: str,
    current: int,
    total: int,
    elapsed: float,
    extra: str = "",
) -> str:
    current = max(0, current)
    total = max(0, total)
    ratio = min(current / total, 1.0) if total else 0.0
    filled = int(ratio * BAR_WIDTH)
    bar = "#" * filled + "-" * (BAR_WIDTH - filled)
    speed = current / elapsed if elapsed > 0 else 0
    percent = ratio * 100
    total_text = human_size(total) if total else "unknown"
    eta = eta_text(current, total, speed)

    return (
        f"{label}\n"
        f"{extra}"
        f"[{bar}] {percent:.1f}%\n"
        f"{human_size(current)} / {total_text}\n"
        f"⚡ Speed: {human_size(speed)}/s\n"
        f"⏳ ETA: {eta}"
    )


def human_size(size: float) -> str:
    units = ("B", "KB", "MB", "GB", "TB")
    value = float(size)
    for unit in units:
        if abs(value) < 1024 or unit == units[-1]:
            return f"{value:.1f} {unit}" if unit != "B" else f"{value:.0f} {unit}"
        value /= 1024
    return f"{value:.1f} TB"


def eta_text(current: float, total: float, speed: float) -> str:
    if total <= 0 or speed <= 0 or current >= total:
        return "done" if total > 0 and current >= total else "calculating"
    remaining = max(total - current, 0) / speed
    from telegram_bot.runtime import format_duration

    return format_duration(remaining)


def extract_media_file(message: types.Message) -> types.File | None:
    content = getattr(message, "content", None)
    if content is None:
        return None

    files = list(_walk_files(content))
    if not files:
        return None

    return max(files, key=lambda item: int(getattr(item, "size", 0) or 0))


async def download_media(
    client: Client,
    media_file: types.File,
    progress: ProgressMessage,
) -> Path:
    file_id = int(media_file.id)
    total = int(getattr(media_file, "size", 0) or getattr(media_file, "expected_size", 0) or 0)

    started = await _start_download(client, file_id)

    while True:
        current = await client.getFile(file_id)
        if getattr(current, "is_error", False):
            error = RuntimeError(_result_error(current, "Failed to read download status"))
            if is_rate_limited(error):
                await asyncio.sleep(retry_after_seconds(error))
                continue
            raise error

        local = current.local
        downloaded = int(getattr(local, "downloaded_size", 0) or 0)
        total = total or int(getattr(current, "size", 0) or getattr(current, "expected_size", 0) or 0)
        progress.total = total
        await progress.update(downloaded)

        if getattr(local, "is_downloading_completed", False):
            await progress.update(total or downloaded, force=True)
            return Path(local.path)

        await asyncio.sleep(1)


async def upload_document(
    message: types.Message,
    path: Path,
    tracker: UploadProgress,
    progress: ProgressMessage,
) -> types.Message:
    total = path.stat().st_size
    progress.total = total
    tracker.watch(str(path))

    send_task = asyncio.create_task(
        message.reply_document(
            types.InputFileLocal(str(path)),
            caption="Downloaded and uploaded back.",
            disable_content_type_detection=False,
        )
    )

    try:
        last_upload_progress = time.monotonic()
        last_uploaded = 0
        while not send_task.done():
            uploaded = tracker.get(str(path))
            if uploaded != last_uploaded:
                last_uploaded = uploaded
                last_upload_progress = time.monotonic()
            elif time.monotonic() - last_upload_progress > STALL_TIMEOUT:
                send_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await send_task
                raise RuntimeError("Upload stalled: no progress for 10 minutes")
            await progress.update(uploaded)
            await asyncio.sleep(1)

        result = await send_task
        await progress.update(total, force=True)
        if getattr(result, "is_error", False):
            raise RuntimeError(getattr(result, "message", "Failed to upload file"))
        return result
    finally:
        tracker.unwatch(str(path))


async def upload_video(
    message: types.Message,
    path: Path,
    tracker: UploadProgress,
    progress: ProgressMessage,
    cancel_event: asyncio.Event | None = None,
    thumbnail: Path | None = None,
    duration: int = 0,
    width: int = 0,
    height: int = 0,
    caption: str = "Encoded video.",
) -> types.Message:
    total = path.stat().st_size
    progress.total = total
    tracker.watch(str(path))

    try:
        for attempt in range(MAX_RATE_LIMIT_RETRIES + 1):
            send_task = asyncio.create_task(
                message.reply_video(
                    types.InputFileLocal(str(path)),
                    thumbnail=(
                        types.InputThumbnail(types.InputFileLocal(str(thumbnail)), width, height)
                        if thumbnail
                        else None
                    ),
                    caption=caption,
                    supports_streaming=True,
                    duration=duration,
                    width=width,
                    height=height,
                )
            )
            last_upload_progress = time.monotonic()
            last_uploaded = 0
            while not send_task.done():
                if cancel_event and cancel_event.is_set():
                    await safe_edit_text(
                        progress.message,
                        "🛑 Cancellation requested. Stopping the upload; Telegram may still finish an already-started transfer."
                        f"{status_footer()}",
                    )
                    send_task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await send_task
                    raise asyncio.CancelledError("Upload cancelled")
                uploaded = tracker.get(str(path))
                if uploaded != last_uploaded:
                    last_uploaded = uploaded
                    last_upload_progress = time.monotonic()
                elif time.monotonic() - last_upload_progress > STALL_TIMEOUT:
                    send_task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await send_task
                    raise RuntimeError("Upload stalled: no progress for 10 minutes")
                await progress.update(uploaded)
                await asyncio.sleep(1)

            result = await send_task
            if not getattr(result, "is_error", False):
                await progress.update(total, force=True)
                return result
            error = RuntimeError(_result_error(result, "Failed to upload video"))
            if not is_rate_limited(error) or attempt == MAX_RATE_LIMIT_RETRIES:
                raise error
            await safe_edit_text(
                progress.message,
                f"⏳ Telegram rate limit. Retrying upload in {retry_after_seconds(error)}s.{status_footer()}",
            )
            await asyncio.sleep(retry_after_seconds(error))
    finally:
        tracker.unwatch(str(path))


async def _start_download(client: Client, file_id: int) -> types.File:
    for attempt in range(MAX_RATE_LIMIT_RETRIES + 1):
        result = await client.downloadFile(
            file_id=file_id, priority=32, offset=0, limit=0, synchronous=False,
        )
        if not getattr(result, "is_error", False):
            return result
        error = RuntimeError(_result_error(result, "Failed to start download"))
        if not is_rate_limited(error) or attempt == MAX_RATE_LIMIT_RETRIES:
            raise error
        await asyncio.sleep(retry_after_seconds(error))
    raise RuntimeError("Failed to start download")


def is_rate_limited(error: BaseException | object) -> bool:
    detail = str(error).lower()
    return "too many requests" in detail or "flood_wait" in detail or "429" in detail


def retry_after_seconds(error: BaseException | object, default: int = 5) -> int:
    match = re.search(r"(?:retry after|flood_wait[_ ]?)(\d+)", str(error).lower())
    return max(1, int(match.group(1))) if match else default


def _result_error(result: object, default: str) -> str:
    return str(getattr(result, "message", "") or getattr(result, "error", "") or default)


def is_video_message(message: types.Message) -> bool:
    content = getattr(message, "content", None)
    if content is None:
        return False

    content_type = type(content).__name__
    if content_type in {"MessageVideo", "MessageAnimation", "MessageVideoNote"}:
        return True

    document = getattr(content, "document", None)
    mime_type = getattr(document, "mime_type", "") if document is not None else ""
    return mime_type.startswith("video/")


def _walk_files(value: Any, seen: set[int] | None = None):
    if seen is None:
        seen = set()

    obj_id = id(value)
    if obj_id in seen:
        return
    seen.add(obj_id)

    if isinstance(value, types.File):
        yield value
        return

    if isinstance(value, (str, bytes, int, float, bool, type(None))):
        return

    if isinstance(value, dict):
        iterable = value.values()
    elif isinstance(value, (list, tuple, set)):
        iterable = value
    else:
        iterable = getattr(value, "__dict__", {}).values()

    for child in iterable:
        yield from _walk_files(child, seen)
