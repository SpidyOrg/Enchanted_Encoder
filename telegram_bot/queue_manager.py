from __future__ import annotations

import asyncio
import logging
import time
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path

from pytdbot import types

from telegram_bot.encoder import encode_h264_720p, make_thumbnail, probe_video_metadata
from telegram_bot.runtime import format_duration, has_free_space, status_footer
from telegram_bot.settings import EncodeProfile
from telegram_bot.transfer import ProgressMessage, UploadProgress, upload_video


LOGGER = logging.getLogger(__name__)
DISK_HEADROOM_MULTIPLIER = 2.5


@dataclass
class VideoJob:
    id: int
    message: types.Message
    status: types.Message
    source_path: Path
    profile: EncodeProfile
    queued_at: float
    source_message_id: int
    owner_id: str
    owner_label: str
    filename: str
    state: str = "queued"
    started_at: float | None = None
    cancel_event: asyncio.Event = field(default_factory=asyncio.Event)


class EncodeQueue:
    def __init__(
        self,
        upload_progress: UploadProgress,
        output_dir: Path = Path("encoded"),
        max_size: int = 20,
        max_jobs_per_user: int = 1,
    ) -> None:
        self._queue: asyncio.Queue[VideoJob] = asyncio.Queue(maxsize=max_size)
        self._upload_progress = upload_progress
        self._output_dir = output_dir
        self._max_jobs_per_user = max_jobs_per_user
        self._worker: asyncio.Task | None = None
        self._active_job: VideoJob | None = None
        self._next_id = 1
        self._user_jobs: dict[str, int] = {}

    @property
    def active(self) -> bool:
        return self._active_job is not None

    @property
    def waiting(self) -> int:
        return self._queue.qsize()

    @property
    def capacity(self) -> int:
        return self._queue.maxsize

    def reserve_user(self, user_id: str) -> bool:
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

    def ensure_worker(self) -> None:
        if self._worker is None or self._worker.done():
            self._worker = asyncio.create_task(self._run())

    async def enqueue(self, job: VideoJob) -> int | None:
        try:
            self._queue.put_nowait(job)
        except asyncio.QueueFull:
            return None
        self.ensure_worker()
        await self.refresh_waiting_positions()
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
        active = self._active_job
        if active and _matches(active, job_id, message_id):
            if active.owner_id != requester_id:
                return "You can only cancel your own task."
            active.cancel_event.set()
            if active.state == "uploading":
                return f"🛑 Cancellation requested for upload of job {active.id}. Telegram may finish the already-started upload."
            return f"🛑 Cancelling active job {active.id}."

        queued = list(self._queue._queue)
        for job in queued:
            if not _matches(job, job_id, message_id):
                continue
            if job.owner_id != requester_id:
                return "You can only cancel your own task."
            self._queue._queue = deque(item for item in queued if item is not job)
            self._queue.task_done()
            job.cancel_event.set()
            cleanup_file(job.source_path)
            self.release_user(job.owner_id)
            await self.refresh_waiting_positions()
            return f"🗑 Cancelled queued job {job.id}."

        return "No matching task found."

    async def refresh_waiting_positions(self) -> None:
        for position, job in enumerate(list(self._queue._queue), start=1):
            if job.cancel_event.is_set():
                continue
            try:
                await job.status.edit_text(self._queued_text(job, position))
            except Exception:
                LOGGER.debug("Could not refresh queue position for job %s", job.id, exc_info=True)

    def summary(self) -> str:
        active = self._active_job
        lines = ["📚 Queue status", f"Waiting: {self.waiting}/{self.capacity}"]
        if active is None:
            lines.append("Active: none")
        else:
            elapsed = format_duration(time.time() - (active.started_at or time.time()))
            lines.extend(
                (
                    f"Active: #{active.id} ({active.state})",
                    f"File: {active.filename}",
                    f"Owner: {active.owner_label}",
                    f"Profile: {active.profile.label}",
                    f"Elapsed: {elapsed}",
                )
            )
        if self.waiting:
            lines.append("\nWaiting jobs:")
            lines.extend(
                f"{position}. #{job.id} | {job.filename} | {job.owner_label} | {job.profile.key}"
                for position, job in enumerate(list(self._queue._queue), start=1)
            )
        return "\n".join(lines) + status_footer()

    def _queued_text(self, job: VideoJob, position: int) -> str:
        return (
            "📚 Queued for encoding\n"
            f"Job: {job.id}\n"
            f"Position: {position}\n"
            f"Profile: {job.profile.label}"
            f"{status_footer()}"
        )

    async def _run(self) -> None:
        while True:
            job = await self._queue.get()
            self._active_job = job
            job.state = "encoding"
            job.started_at = time.time()
            encoded_path: Path | None = None
            thumbnail_path: Path | None = None
            try:
                await self.refresh_waiting_positions()
                required_space = int(job.source_path.stat().st_size * DISK_HEADROOM_MULTIPLIER)
                if not has_free_space(self._output_dir, required_space):
                    raise RuntimeError("Insufficient free disk space for this video")
                waited = format_duration(time.time() - job.queued_at)
                await job.status.edit_text(
                    "🎬 Encoding started\n"
                    f"Job: {job.id}\n"
                    f"Profile: {job.profile.label}\n"
                    f"Waited: {waited}"
                    f"{status_footer()}"
                )
                encode_progress = ProgressMessage(
                    job.status, "🎬 Encoding", 0,
                    extra=f"🆔 Job: {job.id}\n🎛 Profile: {job.profile.label}\n",
                )
                encoded_path = await encode_h264_720p(
                    job.source_path, self._output_dir, encode_progress,
                    profile=job.profile, cancel_event=job.cancel_event, job_id=job.id,
                )
                metadata = await probe_video_metadata(encoded_path)
                thumbnail_path = await make_thumbnail(encoded_path, self._output_dir, metadata.duration)
                job.state = "uploading"
                upload_progress = ProgressMessage(
                    job.status, "📤 Uploading", encoded_path.stat().st_size,
                    extra=f"🆔 Job: {job.id}\n🎛 Profile: {job.profile.label}\n",
                )
                await upload_video(
                    job.message, encoded_path, self._upload_progress, upload_progress,
                    cancel_event=job.cancel_event, thumbnail=thumbnail_path,
                    duration=int(metadata.duration), width=metadata.width, height=metadata.height,
                    caption=f"Encoded with {job.profile.name}: H.264 {job.profile.resolution}p CRF {job.profile.crf}.",
                )
                await job.status.edit_text(
                    "✅ Done\n"
                    f"Job: {job.id}\nProfile: {job.profile.label}\n"
                    f"Output: H.264 {job.profile.resolution}p CRF {job.profile.crf}"
                    f"{status_footer()}"
                )
            except asyncio.CancelledError:
                await _safe_edit(job.status, f"🛑 Cancelled\nJob: {job.id}\nProfile: {job.profile.label}{status_footer()}")
            except Exception as exc:
                LOGGER.exception("Queued media transfer failed")
                await _safe_edit(job.status, f"❌ {friendly_error(exc)}{status_footer()}")
            finally:
                cleanup_file(job.source_path)
                if encoded_path is not None:
                    cleanup_file(encoded_path)
                if thumbnail_path is not None:
                    cleanup_file(thumbnail_path)
                self._active_job = None
                self.release_user(job.owner_id)
                self._queue.task_done()
                await self.refresh_waiting_positions()


async def _safe_edit(message: types.Message, text: str) -> None:
    try:
        await message.edit_text(text)
    except Exception:
        LOGGER.debug("Could not update status message", exc_info=True)


def friendly_error(exc: Exception) -> str:
    detail = str(exc).lower()
    if isinstance(exc, asyncio.CancelledError) or "cancel" in detail:
        return "Cancelled."
    if "no space left" in detail or "insufficient free disk" in detail or "disk full" in detail:
        return "Not enough free disk space. Try a smaller video or free server storage."
    if (
        "stream map" in detail
        or "matches no streams" in detail
        or "no video" in detail
        or "no usable video" in detail
    ):
        return "This file does not contain a usable video stream."
    if any(marker in detail for marker in ("invalid data", "moov atom", "decoder", "unsupported codec", "unknown decoder")):
        return "This video codec or container is not supported by FFmpeg on this server."
    if any(marker in detail for marker in ("file too large", "file_too_big", "request_entity_too_large")):
        return "This file is too large for Telegram or the configured server limits."
    return "Encoding or transfer failed. Check that the video is valid and try again."


def cleanup_file(path: Path) -> None:
    try:
        if path.exists():
            path.unlink()
    except OSError:
        LOGGER.debug("Could not delete runtime file: %s", path)


def _matches(job: VideoJob, job_id: int | None, message_id: int | None) -> bool:
    return (job_id is not None and job.id == job_id) or (
        message_id is not None and job.source_message_id == message_id
    )
