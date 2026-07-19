"""Message editing, progress tracking, and file upload helpers.

All operations use the pytdbot ``types.Message`` bound methods or the
pytdbot ``Client`` directly — no pyrogram / pyroblack dependency.
"""
from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

from pytdbot import types

from telegram_bot.formatting import format_duration, human_size


LOGGER = logging.getLogger(__name__)

BAR_WIDTH = 20
STATUS_CHAT_INTERVAL = 1.0   # minimum seconds between edits per chat
STATUS_EDIT_INTERVAL = 3.0   # minimum seconds between progress updates
STATUS_EDIT_TIMEOUT = 10.0   # asyncio timeout for a single edit call

# Per-chat monotonic timestamp of the next allowed edit
_next_status_edit_by_chat: dict[str, float] = {}


# ---------------------------------------------------------------------------
# Core edit helper
# ---------------------------------------------------------------------------

async def safe_edit_text(
    message: types.Message,
    text: str,
    reply_markup: types.ReplyMarkup | None = None,
) -> bool:
    """Edit *message* in-place, rate-limited and swallowing benign errors.

    Returns ``True`` if the edit was sent, ``False`` if skipped or failed.
    """
    chat_key = str(getattr(message, "chat_id", "global") or "global")
    now = time.monotonic()
    if now < _next_status_edit_by_chat.get(chat_key, 0.0):
        return False
    _next_status_edit_by_chat[chat_key] = now + STATUS_CHAT_INTERVAL

    try:
        result = await asyncio.wait_for(
            message.edit_text(text=text, reply_markup=reply_markup),
            timeout=STATUS_EDIT_TIMEOUT,
        )
        if isinstance(result, types.Error):
            err_msg = getattr(result, "message", str(result))
            # "Message is not modified" and "message to edit not found" are
            # normal — log at debug level only.
            if any(x in err_msg.lower() for x in ("not modified", "not found", "chat not found")):
                LOGGER.debug("Status edit skipped: %s", err_msg)
            else:
                LOGGER.warning("Status edit failed: %s", err_msg)
            return False
        return True
    except asyncio.TimeoutError:
        LOGGER.debug("Status edit timed out for chat %s", chat_key)
        return False
    except asyncio.CancelledError:
        raise
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("Status message update failed: %s", exc)
        return False


# ---------------------------------------------------------------------------
# Progress message tracker
# ---------------------------------------------------------------------------

@dataclass
class ProgressMessage:
    """Tracks and periodically updates a single Telegram status message."""

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
        # Stagger initial edits across concurrent jobs to avoid edit bursts.
        mid = getattr(self.message, "id", id(self.message)) or id(self.message)
        self.next_edit_at = self.started_at + (mid % 5) * STATUS_CHAT_INTERVAL

    async def update(self, current: int, force: bool = False) -> None:
        """Update the status message if enough time has passed."""
        now = time.monotonic()
        if not force and now < self.next_edit_at:
            return

        progress = render_progress(
            self.label, current, self.total, now - self.started_at, self.extra
        )

        reply_markup: types.ReplyMarkup | None = None
        if self.queue and self.user_id:
            text = self.queue._render_user_status(self.user_id, progress)
            try:
                from telegram_bot.keyboards import build_queue_keyboard

                job_ids = [
                    j.id for j in self.queue._active_jobs.values()
                    if j.owner_id == self.user_id
                ]
                job_ids.extend(
                    j.id for j in list(self.queue._queue._queue)
                    if j.owner_id == self.user_id
                )
                reply_markup = build_queue_keyboard(job_ids)
            except Exception as exc:  # noqa: BLE001
                LOGGER.warning("Could not build keyboard in ProgressMessage: %s", exc)
        else:
            from telegram_bot.formatting import status_footer
            text = progress + status_footer()

        if text == self.last_text:
            return

        self.last_edit_at = now
        self.next_edit_at = now + STATUS_EDIT_INTERVAL
        if await safe_edit_text(self.message, text, reply_markup=reply_markup):
            self.last_text = text


# ---------------------------------------------------------------------------
# Progress rendering
# ---------------------------------------------------------------------------

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
    bar = "█" * filled + "░" * (BAR_WIDTH - filled)
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


def eta_text(current: float, total: float, speed: float) -> str:
    if total <= 0 or speed <= 0 or current >= total:
        return "done" if total > 0 and current >= total else "calculating"
    remaining = max(total - current, 0) / speed
    return format_duration(remaining)


# ---------------------------------------------------------------------------
# Upload helpers
# ---------------------------------------------------------------------------

async def upload_video(
    source_message: types.Message,
    path: Path,
    progress_callback: Callable[[int, int], Any] | None = None,
    cancel_event: asyncio.Event | None = None,
    thumbnail: Path | None = None,
    duration: int = 0,
    width: int = 0,
    height: int = 0,
    caption: str = "",
    max_retries: int = 3,
) -> types.Message:
    """Reply to *source_message* with an encoded video file.

    Uses ``message.reply_video`` (pytdbot bound method) so we never need to
    manage chat_id / reply_to_message_id manually.

    Args:
        source_message: The original user message to reply to.
        path: Local path of the encoded video file.
        progress_callback: Optional ``(current, total) -> None`` callback.
        cancel_event: If set before the upload completes, raises CancelledError.
        thumbnail: Optional local thumbnail path.
        duration: Video duration in seconds.
        width: Video width in pixels.
        height: Video height in pixels.
        caption: Caption text for the video message.
        max_retries: Maximum upload attempts on transient errors.

    Returns:
        The sent ``types.Message``.

    Raises:
        asyncio.CancelledError: If *cancel_event* is set.
        RuntimeError: If the upload fails after all retries.
    """
    if cancel_event is not None and cancel_event.is_set():
        raise asyncio.CancelledError("Upload cancelled before start")

    video_file = types.InputFileLocal(path=str(path))
    thumb_file = types.InputFileLocal(path=str(thumbnail)) if thumbnail and thumbnail.exists() else None

    last_error: Exception | None = None
    for attempt in range(max_retries):
        if cancel_event is not None and cancel_event.is_set():
            raise asyncio.CancelledError("Upload cancelled")
        try:
            coro = source_message.reply_video(
                video=video_file,
                thumbnail=thumb_file,
                caption=caption or None,
                duration=duration or None,
                width=width or None,
                height=height or None,
                supports_streaming=True,
            )
            sent = await _run_cancellable(coro, cancel_event)
            if isinstance(sent, types.Error):
                raise RuntimeError(f"Upload failed: {getattr(sent, 'message', sent)}")
            return sent  # type: ignore[return-value]

        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            if attempt < max_retries - 1:
                wait = min(2 ** attempt, 30)
                LOGGER.warning(
                    "Video upload error (attempt %d/%d), retrying in %ds: %s",
                    attempt + 1, max_retries, wait, exc,
                )
                await asyncio.sleep(wait)
            else:
                raise RuntimeError(
                    f"Video upload failed after {max_retries} retries: {exc}"
                ) from exc

    raise RuntimeError(f"Video upload failed: {last_error}")


async def upload_document(
    source_message: types.Message,
    path: Path,
    progress_callback: Callable[[int, int], Any] | None = None,
    max_retries: int = 3,
) -> types.Message:
    """Reply to *source_message* with the encoded file as a document.

    Args:
        source_message: The original user message to reply to.
        path: Local path of the encoded file.
        progress_callback: Optional ``(current, total) -> None`` callback.
        max_retries: Maximum upload attempts on transient errors.

    Returns:
        The sent ``types.Message``.

    Raises:
        RuntimeError: If the upload fails after all retries.
    """
    doc_file = types.InputFileLocal(path=str(path))

    last_error: Exception | None = None
    for attempt in range(max_retries):
        try:
            coro = source_message.reply_document(
                document=doc_file,
                caption="Encoded file.",
                disable_content_type_detection=True,
            )
            sent = await _run_cancellable(coro, None)
            if isinstance(sent, types.Error):
                raise RuntimeError(f"Upload failed: {getattr(sent, 'message', sent)}")
            return sent  # type: ignore[return-value]

        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            if attempt < max_retries - 1:
                wait = min(2 ** attempt, 30)
                LOGGER.warning(
                    "Document upload error (attempt %d/%d), retrying in %ds: %s",
                    attempt + 1, max_retries, wait, exc,
                )
                await asyncio.sleep(wait)
            else:
                raise RuntimeError(
                    f"Document upload failed after {max_retries} retries: {exc}"
                ) from exc

    raise RuntimeError(f"Document upload failed: {last_error}")


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

async def _run_cancellable(coro: Any, cancel_event: asyncio.Event | None) -> Any:
    """Await *coro*, aborting it early if *cancel_event* fires."""
    upload_task = asyncio.ensure_future(coro)
    if cancel_event is None:
        return await upload_task

    cancel_task = asyncio.ensure_future(cancel_event.wait())
    done, _ = await asyncio.wait(
        {upload_task, cancel_task},
        return_when=asyncio.FIRST_COMPLETED,
    )
    cancel_task.cancel()

    if cancel_event.is_set() and not upload_task.done():
        upload_task.cancel()
        try:
            await upload_task
        except (asyncio.CancelledError, Exception):  # noqa: BLE001
            pass
        raise asyncio.CancelledError("Upload cancelled")

    return upload_task.result()


def status_footer() -> str:
    """Thin shim — callers that imported from here keep working."""
    from telegram_bot.formatting import status_footer as _f
    return _f()
