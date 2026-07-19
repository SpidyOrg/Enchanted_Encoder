from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

from pyrogram.errors import FloodWait
from pyrogram.types import Message


BAR_WIDTH = 12
STATUS_EDIT_INTERVAL = 5.0
STATUS_EDIT_TIMEOUT = 15.0
STATUS_CHAT_INTERVAL = 1.2
LOGGER = logging.getLogger(__name__)
_next_status_edit_by_chat: dict[str, float] = {}


async def safe_edit_text(message: Message, text: str) -> bool:
    chat_key = str(getattr(message, "chat", None) or getattr(message, "chat_id", "global"))
    now = time.monotonic()
    if now < _next_status_edit_by_chat.get(chat_key, 0.0):
        return False
    _next_status_edit_by_chat[chat_key] = now + STATUS_CHAT_INTERVAL
    try:
        await asyncio.wait_for(message.edit_text(text), timeout=STATUS_EDIT_TIMEOUT)
        return True
    except FloodWait as e:
        _next_status_edit_by_chat[chat_key] = max(
            _next_status_edit_by_chat[chat_key], time.monotonic() + e.value
        )
        return False
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        LOGGER.warning("Status message update failed: %s", exc)
        return False


@dataclass
class ProgressMessage:
    message: Message
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
        mid = getattr(self.message, "id", id(self.message)) or id(self.message)
        self.next_edit_at = self.started_at + (mid % 5) * STATUS_CHAT_INTERVAL

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
            text = progress + _status_footer()
        if text == self.last_text:
            return

        self.last_edit_at = now
        self.next_edit_at = now + STATUS_EDIT_INTERVAL
        if await safe_edit_text(self.message, text):
            self.last_text = text


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


async def upload_video(
    message: Message,
    path: Path,
    progress_callback: Callable[[int, int], Any] | None = None,
    cancel_event: asyncio.Event | None = None,
    thumbnail: Path | None = None,
    duration: int = 0,
    width: int = 0,
    height: int = 0,
    caption: str = "",
) -> Message:
    if cancel_event and cancel_event.is_set():
        raise asyncio.CancelledError("Upload cancelled")

    sent = await message.reply_video(
        video=str(path),
        caption=caption,
        duration=duration,
        width=width,
        height=height,
        thumb=str(thumbnail) if thumbnail else None,
        supports_streaming=True,
        progress=progress_callback,
    )
    return sent


async def upload_document(
    message: Message,
    path: Path,
    progress_callback: Callable[[int, int], Any] | None = None,
) -> Message:
    sent = await message.reply_document(
        document=str(path),
        caption="Encoded file.",
        progress=progress_callback,
    )
    return sent


def _status_footer() -> str:
    from telegram_bot.runtime import status_footer
    return status_footer()
