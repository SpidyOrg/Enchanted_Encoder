from __future__ import annotations

import asyncio
import contextlib
import os
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from pytdbot import Client, types

from telegram_bot.runtime import status_footer


BAR_WIDTH = 12


@dataclass
class ProgressMessage:
    message: types.Message
    label: str
    total: int
    started_at: float = field(default_factory=time.monotonic)
    last_edit_at: float = 0.0
    last_text: str = ""
    extra: str = ""

    async def update(self, current: int, force: bool = False) -> None:
        now = time.monotonic()
        if not force and now - self.last_edit_at < 1.5:
            return

        text = render_progress(
            self.label,
            current,
            self.total,
            now - self.started_at,
            self.extra,
        )
        if text == self.last_text:
            return

        self.last_text = text
        self.last_edit_at = now
        await self.message.edit_text(text)


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
        f"{status_footer()}"
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

    started = await client.downloadFile(
        file_id=file_id,
        priority=32,
        offset=0,
        limit=0,
        synchronous=False,
    )
    if getattr(started, "is_error", False):
        raise RuntimeError(getattr(started, "message", "Failed to start download"))

    while True:
        current = await client.getFile(file_id)
        if getattr(current, "is_error", False):
            raise RuntimeError(getattr(current, "message", "Failed to read download status"))

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
        while not send_task.done():
            await progress.update(tracker.get(str(path)))
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

    try:
        while not send_task.done():
            if cancel_event and cancel_event.is_set():
                await progress.message.edit_text(
                    "🛑 Cancellation requested. Stopping the upload; Telegram may still finish an already-started transfer."
                    f"{status_footer()}"
                )
                send_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await send_task
                raise asyncio.CancelledError("Upload cancelled")
            await progress.update(tracker.get(str(path)))
            await asyncio.sleep(1)

        result = await send_task
        await progress.update(total, force=True)
        if getattr(result, "is_error", False):
            raise RuntimeError(getattr(result, "message", "Failed to upload video"))
        return result
    finally:
        tracker.unwatch(str(path))


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
