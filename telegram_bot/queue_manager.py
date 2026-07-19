from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import time
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from pytdbot import types

from telegram_bot.downloader import TDLibDownloader
from telegram_bot.encoder import (
    STALL_TIMEOUT,
    encode_h264_720p,
    make_thumbnail,
    probe_video_metadata,
)
from telegram_bot.keyboards import build_queue_keyboard
from telegram_bot.runtime import format_duration, has_free_space, status_footer
from telegram_bot.settings import EncodeProfile
from telegram_bot.transfer import (
    ProgressMessage,
    safe_edit_text,
    upload_document,
    upload_video,
)


LOGGER = logging.getLogger(__name__)
DISK_HEADROOM_MULTIPLIER = 2.5
MAX_STATUS_QUEUE_ITEMS = 10
MAX_STATUS_LENGTH = 3950
AUTO_DELETE_STATUS_DELAY = 30


@dataclass
class VideoJob:
    id: int
    message: types.Message
    status: types.Message
    profile: EncodeProfile
    queued_at: float
    owner_id: str
    owner_label: str
    filename: str
    file_size: int
    source_message_id: int
    chat_id: int
    state: str = "queued"
    started_at: float | None = None
    source_path: Path | None = None
    reserved_space: int = 0
    cancel_event: asyncio.Event = field(default_factory=asyncio.Event)
    send_as_document: bool = False


class EncodeQueue:
    """Thread-safe encode queue with per-user job limits and status tracking.
    
    Attributes:
        encode_threads: Number of threads per encoder
        max_jobs_per_user: Maximum concurrent jobs per user
        active: Whether there are active jobs
        waiting: Number of jobs waiting in queue
        capacity: Maximum queue capacity
    """
    
    def __init__(
        self,
        output_dir: Path = Path("encoded"),
        downloader: TDLibDownloader | None = None,
        max_size: int = 20,
        max_jobs_per_user: int = 3,
        max_concurrent_encoders: int = 2,
        journal_path: Path | None = None,
    ) -> None:
        """Initialize encode queue.
        
        Args:
            output_dir: Directory for encoded output files
            downloader: TDLib downloader instance
            max_size: Maximum queue size
            max_jobs_per_user: Maximum jobs per user
            max_concurrent_encoders: Maximum concurrent encoding processes
            journal_path: Path to queue journal file
        """
        self._queue: asyncio.Queue[VideoJob] = asyncio.Queue(maxsize=max_size)
        self._output_dir = output_dir
        self._downloader = downloader
        self._max_jobs_per_user = max_jobs_per_user
        self._max_concurrent_encoders = max_concurrent_encoders
        self._encode_threads = max(1, math.ceil((os.cpu_count() or 1) / max_concurrent_encoders))
        self._workers: set[asyncio.Task[None]] = set()
        self._active_jobs: dict[int, VideoJob] = {}
        self._reserved_encode_space = 0
        self._next_id = 1
        self._user_jobs: dict[str, int] = {}
        self._user_status: dict[str, types.Message] = {}
        self._user_status_locks: dict[str, asyncio.Lock] = {}
        self._journal_path = journal_path
        
        # Global lock for queue operations
        self._queue_lock = asyncio.Lock()
        
        self._load_journal()

    @property
    def encode_threads(self) -> int:
        return self._encode_threads

    @property
    def max_jobs_per_user(self) -> int:
        return self._max_jobs_per_user

    @property
    def active(self) -> bool:
        return bool(self._active_jobs)

    @property
    def waiting(self) -> int:
        return self._queue.qsize()

    @property
    def capacity(self) -> int:
        return self._queue.maxsize

    def user_job_count(self, user_id: str) -> int:
        return self._user_jobs.get(user_id, 0)

    def reserve_user_slot(self, user_id: str) -> bool:
        count = self._user_jobs.get(user_id, 0)
        if count >= self._max_jobs_per_user:
            return False
        self._user_jobs[user_id] = count + 1
        return True

    def release_user(self, user_id: str) -> None:
        count = self._user_jobs.get(user_id, 0)
        if count <= 1:
            self._user_jobs.pop(user_id, None)
        else:
            self._user_jobs[user_id] = count - 1

    def user_summary(self, user_id: str) -> str:
        """Generate user-specific queue summary with better formatting.
        
        Args:
            user_id: User ID to generate summary for
            
        Returns:
            Formatted queue status string
        """
        active = [j for j in self._active_jobs.values() if j.owner_id == user_id]
        queued = [j for j in list(self._queue._queue) if j.owner_id == user_id]
        total = len(active) + len(queued)
        
        lines = [f"📋 <b>Your Queue</b> — {total} job{'s' if total != 1 else ''}"]
        
        if active:
            lines.append("\n<b>🔄 Active:</b>")
            for j in active:
                elapsed = format_duration(time.time() - (j.started_at or time.time()))
                if j.state == "uploading":
                    icon, state = "📤", "Uploading"
                elif j.state == "downloading":
                    icon, state = "📥", "Downloading"
                else:
                    icon, state = "🎬", "Encoding"
                lines.append(f"{icon} <b>#{j.id}</b> {state}: {_short(j.filename)} [{elapsed}]")
        
        if queued:
            lines.append("\n<b>⏳ Queued:</b>")
            for pos, j in enumerate(queued, 1):
                lines.append(f"  {pos}. #{j.id} {_short(j.filename)} — {j.profile.name}")
        
        if not total:
            lines.append("\n✅ No active or queued jobs.")
        
        lines.append(f"\n📊 Slots: {total}/{self._max_jobs_per_user} used")
        return "\n".join(lines)

    def _journal_path_resolved(self) -> Path | None:
        return self._journal_path

    def _save_journal(self) -> None:
        journal = self._journal_path_resolved()
        if journal is None:
            return
        queued = list(self._queue._queue)
        entries = []
        for j in queued:
            entries.append({
                "id": j.id,
                "profile_key": j.profile.key,
                "owner_id": j.owner_id,
                "owner_label": j.owner_label,
                "filename": j.filename,
                "file_size": j.file_size,
                "queued_at": j.queued_at,
                "source_message_id": j.source_message_id,
                "chat_id": j.chat_id,
                "send_as_document": j.send_as_document,
            })
        for j in self._active_jobs.values():
            if j.state == "queued":
                entries.append({
                    "id": j.id,
                    "profile_key": j.profile.key,
                    "owner_id": j.owner_id,
                    "owner_label": j.owner_label,
                    "filename": j.filename,
                    "file_size": j.file_size,
                    "queued_at": j.queued_at,
                    "source_message_id": j.source_message_id,
                    "chat_id": j.chat_id,
                    "send_as_document": j.send_as_document,
                })
        try:
            import tempfile
            journal.parent.mkdir(parents=True, exist_ok=True)
            payload = json.dumps({"version": 1, "next_id": self._next_id, "jobs": entries}, indent=2)
            
            with tempfile.NamedTemporaryFile("w", dir=journal.parent, delete=False, encoding="utf-8") as temp_file:
                temp_file.write(payload)
                temp_file.flush()
                os.fsync(temp_file.fileno())
                temp_path = Path(temp_file.name)
            os.replace(temp_path, journal)
        except OSError as exc:
            try:
                if 'temp_path' in locals() and temp_path.exists():
                    temp_path.unlink()
            except OSError:
                pass
            LOGGER.warning("Could not save queue journal: %s", exc)

    def _load_journal(self) -> None:
        journal = self._journal_path_resolved()
        if journal is None or not journal.exists():
            return
        try:
            data = json.loads(journal.read_text(encoding="utf-8"))
            next_id = data.get("next_id", 1)
            if next_id > self._next_id:
                self._next_id = next_id
            jobs = data.get("jobs", [])
            orphaned = len(jobs)
            if orphaned:
                LOGGER.warning(
                    "Discarded %d orphaned job(s) from queue journal. "
                    "Please re-submit your videos.", orphaned
                )
        except (OSError, json.JSONDecodeError, KeyError) as exc:
            LOGGER.warning("Could not load queue journal: %s", exc)
        finally:
            try:
                journal.unlink(missing_ok=True)
            except OSError:
                pass

    def ensure_workers(self) -> None:
        self._workers = {worker for worker in self._workers if not worker.done()}
        while len(self._workers) < self._max_concurrent_encoders:
            worker = asyncio.create_task(self._run(), name=f"encode-worker-{len(self._workers) + 1}")
            self._workers.add(worker)

    async def enqueue(self, job: VideoJob) -> int | None:
        try:
            self._queue.put_nowait(job)
        except asyncio.QueueFull:
            return None
        self.ensure_workers()
        await self.refresh_waiting_positions()
        self._save_journal()
        return self._queue.qsize()

    def next_id(self) -> int:
        job_id = self._next_id
        self._next_id += 1
        return job_id

    async def cancel(
        self,
        requester_id: str,
        job_id: int | None = None,
        message_id: int | None = None,
    ) -> str:
        """Cancel a job by ID or source message ID.
        
        Args:
            requester_id: User ID requesting cancellation
            job_id: Job ID to cancel
            message_id: Source message ID to match against
            
        Returns:
            Human-readable result message
        """
        # Check active jobs first
        for active in self._active_jobs.values():
            if not _matches(active, job_id, message_id):
                continue
            if active.owner_id != requester_id:
                return "❌ You can only cancel your own task."
            active.cancel_event.set()
            if active.state == "uploading":
                return f"⚠️ Cancellation requested for upload of job {active.id}. Telegram may finish the already-started upload."
            return f"🛑 Cancelling active job {active.id}..."

        # Check queued jobs - use thread-safe queue operations
        # Create a new queue with all items except the one to cancel
        cancelled_job: VideoJob | None = None
        remaining_jobs: list[VideoJob] = []
        
        # Drain the queue to find and remove the job
        temp_jobs: list[VideoJob] = []
        while not self._queue.empty():
            try:
                job = self._queue.get_nowait()
                if _matches(job, job_id, message_id) and job.owner_id == requester_id:
                    cancelled_job = job
                    job.cancel_event.set()
                else:
                    temp_jobs.append(job)
            except asyncio.QueueEmpty:
                break
        
        # Put remaining jobs back
        for job in temp_jobs:
            try:
                self._queue.put_nowait(job)
            except asyncio.QueueFull:
                LOGGER.error("Queue full when restoring jobs during cancel")
        
        if cancelled_job:
            # Clean up downloaded file if it exists
            if cancelled_job.source_path:
                cleanup_file(cancelled_job.source_path)
            self.release_user(cancelled_job.owner_id)
            await self._update_user_status(cancelled_job.owner_id)
            self._save_journal()
            return f"🗑 Cancelled queued job {cancelled_job.id}."
        
        return "❌ No matching task found."

    async def get_or_create_user_status(self, user_id: str, source_message: types.Message) -> types.Message | None:
        """Get or create a status message for user queue tracking."""
        async with self._queue_lock:
            existing = self._user_status.get(user_id)
            if existing is not None:
                return existing

            keyboard = build_queue_keyboard([])
            result = await source_message.reply_text(
                text="📋 Preparing your queue..." + status_footer(),
                reply_markup=keyboard,
            )
            if result is None or isinstance(result, types.Error):
                LOGGER.warning(
                    "Could not create status message for user %s: %s", user_id, result
                )
                return None

            self._user_status[user_id] = result
            self._user_status_locks[user_id] = asyncio.Lock()
            return result

    async def _update_user_status(self, user_id: str, progress_block: str = "") -> None:
        async with self._queue_lock:
            status_msg = self._user_status.get(user_id)
            lock = self._user_status_locks.get(user_id)
        
        if status_msg is None or lock is None:
            return

        async with lock:
            text = self._render_user_status(user_id, progress_block)
            
            # Generate keyboard for current jobs
            job_ids = [j.id for j in self._active_jobs.values() if j.owner_id == user_id]
            job_ids.extend(j.id for j in list(self._queue._queue) if j.owner_id == user_id)
            keyboard = build_queue_keyboard(job_ids)
            
            await safe_edit_text(status_msg, text, reply_markup=keyboard)

    def _render_user_status(self, user_id: str, progress_block: str = "") -> str:
        active = [j for j in self._active_jobs.values() if j.owner_id == user_id]
        queued = [j for j in list(self._queue._queue) if j.owner_id == user_id]
        total = len(active) + len(queued)
        lines = [f"📋 Your Queue — {total} job{'s' if total != 1 else ''}"]
        if progress_block:
            lines.append("")
            lines.append(progress_block)
        if active:
            if not progress_block:
                lines.append("")
            for j in active:
                elapsed = format_duration(time.time() - (j.started_at or time.time()))
                icon = "📤" if j.state == "uploading" else ("📥" if j.state == "downloading" else "🎬")
                lines.append(f"{icon} #{j.id} {j.state.title()}: {_short(j.filename)} [{elapsed}]")
        if queued:
            lines.append("")
            shown = 0
            for pos, j in enumerate(queued, 1):
                trial = "\n".join(lines + [f"⏳ #{j.id} Queued ({pos}): {_short(j.filename)} — {j.profile.label}"])
                if len(trial) + 200 > MAX_STATUS_LENGTH:
                    lines.append(f"… and {len(queued) - shown} more queued")
                    break
                lines.append(f"⏳ #{j.id} Queued ({pos}): {_short(j.filename)} — {j.profile.label}")
                shown += 1
        if not total:
            lines.append("\n✅ All jobs complete!")
        return "\n".join(lines) + status_footer()

    async def refresh_waiting_positions(self) -> None:
        seen = set()
        for job in list(self._queue._queue):
            if not job.cancel_event.is_set() and job.owner_id not in seen:
                seen.add(job.owner_id)
                await self._update_user_status(job.owner_id)

    def summary(self) -> str:
        lines = [
            "📚 Queue status",
            f"Workers: {len(self._active_jobs)}/{self._max_concurrent_encoders}",
            f"Waiting: {self.waiting}/{self.capacity}",
        ]
        if self._active_jobs:
            lines.append("\nActive jobs:")
            for job in self._active_jobs.values():
                elapsed = format_duration(time.time() - (job.started_at or time.time()))
                lines.append(
                    f"#{job.id} ({job.state}) | {_short(job.filename)} | {job.owner_label} | "
                    f"{job.profile.key} | {elapsed}"
                )
        else:
            lines.append("Active: none")
        if self.waiting:
            lines.append("\nWaiting jobs:")
            queued = list(self._queue._queue)
            lines.extend(
                f"{position}. #{job.id} | {_short(job.filename)} | {job.owner_label} | {job.profile.key}"
                for position, job in enumerate(queued[:MAX_STATUS_QUEUE_ITEMS], start=1)
            )
            if len(queued) > MAX_STATUS_QUEUE_ITEMS:
                lines.append(f"… and {len(queued) - MAX_STATUS_QUEUE_ITEMS} more")
        return "\n".join(lines) + status_footer()

    async def _run(self) -> None:
        while True:
            job = await self._queue.get()
            self._active_jobs[job.id] = job
            job.started_at = time.time()
            encoded_path: Path | None = None
            thumbnail_path: Path | None = None
            downloaded_path: Path | None = None
            failed: bool = False
            try:
                # ── Download phase ──
                job.state = "downloading"
                await self._update_user_status(
                    job.owner_id,
                    f"📥 Downloading: {_short(job.filename)}",
                )
                if self._downloader:
                    dl_progress = ProgressMessage(
                        job.status, "📥 Downloading", job.file_size,
                        queue=self, user_id=job.owner_id,
                    )
                    async def _dl_callback(current: int, total: int) -> None:
                        await dl_progress.update(current)
                    downloaded_path = await self._downloader.download(
                        chat_id=job.chat_id,
                        message_id=job.source_message_id,
                        progress_callback=_dl_callback,
                        cancel_event=job.cancel_event,
                    )
                else:
                    raise RuntimeError("No downloader configured")
                job.source_path = downloaded_path

                # ── Encode phase ──
                if job.cancel_event.is_set():
                    raise asyncio.CancelledError("Job cancelled before encoding")
                
                job.state = "encoding"
                await self._update_user_status(job.owner_id)
                required_space = int(downloaded_path.stat().st_size * DISK_HEADROOM_MULTIPLIER)
                if not has_free_space(self._output_dir, required_space + self._reserved_encode_space):
                    raise RuntimeError("Insufficient free disk space for concurrent encoding")
                self._reserved_encode_space += required_space
                job.reserved_space = required_space

                encode_progress = ProgressMessage(
                    job.status, "🎬 Encoding", 0,
                    extra=f"🆔 Job: {job.id}\n🎛 Profile: {job.profile.label}\n",
                    queue=self, user_id=job.owner_id,
                )
                encoded_path = await encode_h264_720p(
                    downloaded_path, self._output_dir, encode_progress,
                    profile=job.profile, cancel_event=job.cancel_event, job_id=job.id,
                    queue=self, user_id=job.owner_id,
                    encode_threads=self._encode_threads,
                )
                metadata = await probe_video_metadata(encoded_path)
                thumbnail_path = await make_thumbnail(encoded_path, self._output_dir, metadata.duration)

                # ── Upload phase ──
                job.state = "uploading"
                await self._update_user_status(job.owner_id)
                up_progress = ProgressMessage(
                    job.status, "📤 Uploading", encoded_path.stat().st_size,
                    extra=f"🆔 Job: {job.id}\n🎛 Profile: {job.profile.label}\n",
                    queue=self, user_id=job.owner_id,
                )
                async def _up_callback(current: int, total: int) -> None:
                    if job.cancel_event.is_set():
                        raise asyncio.CancelledError("Upload cancelled")
                    await up_progress.update(current)

                if job.send_as_document:
                    await upload_document(
                        job.message,
                        encoded_path,
                        progress_callback=_up_callback,
                    )
                else:
                    await upload_video(
                        job.message,
                        encoded_path,
                        progress_callback=_up_callback,
                        cancel_event=job.cancel_event,
                        thumbnail=thumbnail_path,
                        duration=int(metadata.duration),
                        width=metadata.width,
                        height=metadata.height,
                        caption=(
                            f"Encoded with {job.profile.name}: "
                            f"{job.profile.codec.upper()} {job.profile.resolution}p "
                            f"CRF {job.profile.crf}."
                        ),
                    )

                # Done notification
                try:
                    await job.message.reply_text(
                        text=(
                            f"✅ Done: {job.filename}\n"
                            f"Job #{job.id} | {job.profile.label}"
                            f"{status_footer()}"
                        ),
                    )
                except Exception:
                    pass

            except asyncio.CancelledError:
                await self._update_user_status(job.owner_id)
                if not job.cancel_event.is_set():
                    raise
            except Exception as exc:
                LOGGER.exception("Queued job failed")
                failed = True
                error_msg = friendly_error(exc)
                # Reply the failure directly to the user's message so it is not
                # overwritten by the subsequent status re-render.
                try:
                    await job.message.reply_text(text=f"❌ {error_msg}{status_footer()}")
                except Exception:
                    pass
                await self._update_user_status(job.owner_id, f"❌ {error_msg}")
            finally:
                if downloaded_path:
                    cleanup_file(downloaded_path)
                if encoded_path:
                    cleanup_file(encoded_path)
                if thumbnail_path:
                    cleanup_file(thumbnail_path)
                self._reserved_encode_space = max(0, self._reserved_encode_space - job.reserved_space)
                self._active_jobs.pop(job.id, None)
                self.release_user(job.owner_id)
                self._queue.task_done()
                self._save_journal()
                if not failed:
                    await self._update_user_status(job.owner_id)
                remaining = [j for j in self._active_jobs.values() if j.owner_id == job.owner_id] + \
                            [j for j in list(self._queue._queue) if j.owner_id == job.owner_id]
                if not remaining:
                    status_msg = self._user_status.pop(job.owner_id, None)
                    self._user_status_locks.pop(job.owner_id, None)
                    if status_msg is not None:
                        try:
                            await safe_edit_text(
                                status_msg,
                                "✅ All jobs complete!" + status_footer(),
                                reply_markup=build_queue_keyboard([]),
                            )
                            _schedule_delete(status_msg)
                        except Exception:  # noqa: BLE001
                            pass


def friendly_error(exc: Exception) -> str:
    detail = str(exc).lower()
    if isinstance(exc, asyncio.CancelledError) or "cancel" in detail:
        return "Cancelled."
    if "too many requests" in detail or "flood_wait" in detail or "429" in detail:
        return "Telegram temporarily rate-limited this task. It will retry automatically when possible."
    if "no space left" in detail or "insufficient free disk" in detail or "disk full" in detail:
        return "Not enough free disk space. Try a smaller video or free server storage."
    if any(marker in detail for marker in ("stream map", "matches no streams", "no video", "no usable video")):
        return "This file does not contain a usable video stream."
    if any(marker in detail for marker in ("invalid data", "moov atom", "decoder", "unsupported codec", "unknown decoder")):
        return "Video codec not supported. Try converting to MP4 (H.264) first."
    if any(marker in detail for marker in ("file too large", "file_too_big", "request_entity_too_large")):
        return "File too large for Telegram. Try the 'tiny' profile for smaller output."
    if "empty" in detail or "not created" in detail:
        return "Encoding produced no output. The source video may be corrupted or unsupported."
    if "stalled" in detail:
        return "Encoding stalled and was aborted. Try a faster profile or a smaller video."
    return "Encoding or transfer failed. Check that the video is valid and try again."


def _schedule_delete(msg: types.Message) -> None:
    """Schedule a status message for deletion after AUTO_DELETE_STATUS_DELAY seconds."""
    async def _del() -> None:
        await asyncio.sleep(AUTO_DELETE_STATUS_DELAY)
        try:
            result = await msg.delete(revoke=True)
            if isinstance(result, types.Error):
                LOGGER.debug("Auto-delete failed: %s", getattr(result, "message", result))
        except Exception as exc:  # noqa: BLE001
            LOGGER.debug("Auto-delete exception: %s", exc)
    asyncio.create_task(_del())


def cleanup_file(path: Path | None) -> None:
    if path is None:
        return
    try:
        if path.exists():
            path.unlink()
    except OSError:
        LOGGER.debug("Could not delete runtime file: %s", path)


def _matches(job: VideoJob, job_id: int | None, message_id: int | None) -> bool:
    return (job_id is not None and job.id == job_id) or (
        message_id is not None and job.source_message_id == message_id
    )


def _short(value: str, limit: int = 48) -> str:
    return value if len(value) <= limit else f"{value[:limit - 1]}…"
