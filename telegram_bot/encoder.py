from __future__ import annotations

import asyncio
import json
import os
import signal
import time
from pathlib import Path

from telegram_bot.runtime import status_footer
from telegram_bot.settings import EncodeProfile
from telegram_bot.transfer import BAR_WIDTH, ProgressMessage, safe_edit_text


async def encode_h264_720p(
    source: Path,
    output_dir: Path,
    progress: ProgressMessage,
    profile: EncodeProfile,
    cancel_event: asyncio.Event | None = None,
    job_id: int | None = None,
) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    job_suffix = f".job-{job_id}" if job_id is not None else ""
    target = output_dir / f"{source.stem}{job_suffix}.encoded.{profile.key}.mp4"
    metadata = await probe_video_metadata(source)
    duration = metadata.duration

    command = [
        "ffmpeg",
        "-hide_banner",
        "-y",
        "-i",
        str(source),
        "-map",
        "0:v:0",
        "-map",
        "0:a?",
        "-vf",
        f"scale=-2:{profile.resolution}",
        "-c:v",
        "libx264",
        "-preset",
        profile.x264_preset,
        "-threads",
        "0",
        "-crf",
        str(profile.crf),
        "-c:a",
        "aac",
        "-b:a",
        profile.audio_bitrate,
        "-movflags",
        "+faststart",
        "-progress",
        "pipe:1",
        "-nostats",
        str(target),
    ]

    process = await asyncio.create_subprocess_exec(
        *command,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )

    started_at = time.monotonic()
    last_edit_at = 0.0
    out_time = 0.0
    ffmpeg_speed = 0.0

    assert process.stdout is not None
    try:
        while True:
            if cancel_event and cancel_event.is_set():
                await stop_process(process)
                raise asyncio.CancelledError("Encoding cancelled")

            line = await process.stdout.readline()
            if not line:
                break

            key, _, value = line.decode("utf-8", errors="replace").strip().partition("=")
            if key in {"out_time_us", "out_time_ms"}:
                out_time = max(out_time, _progress_time(value))
            elif key == "out_time":
                out_time = max(out_time, _timestamp_to_seconds(value))
            elif key == "speed":
                ffmpeg_speed = _speed_to_float(value)

            now = time.monotonic()
            if now - last_edit_at >= 5:
                last_edit_at = now
                await safe_edit_text(
                    progress.message,
                    render_encode_progress(
                        out_time,
                        duration,
                        now - started_at,
                        ffmpeg_speed,
                        profile=profile,
                        job_id=job_id,
                    )
                )
    except asyncio.CancelledError:
        await stop_process(process)
        if target.exists():
            target.unlink()
        raise

    stderr = b""
    if process.stderr is not None:
        stderr = await process.stderr.read()

    return_code = await process.wait()
    if return_code != 0:
        error = stderr.decode("utf-8", errors="replace").strip().splitlines()
        detail = error[-1] if error else "ffmpeg failed"
        raise RuntimeError(detail)

    if not target.exists() or target.stat().st_size == 0:
        if target.exists():
            target.unlink()
        raise RuntimeError("Encoded output was not created")

    await safe_edit_text(
        progress.message,
        render_encode_progress(
            duration,
            duration,
            time.monotonic() - started_at,
            ffmpeg_speed,
            profile=profile,
            job_id=job_id,
        )
    )
    return target


class VideoMetadata:
    def __init__(self, duration: float, width: int, height: int) -> None:
        self.duration = duration
        self.width = width
        self.height = height


async def probe_video_metadata(source: Path) -> VideoMetadata:
    process = await asyncio.create_subprocess_exec(
        "ffprobe",
        "-v",
        "error",
        "-show_entries",
        "format=duration:stream=width,height",
        "-of",
        "json",
        str(source),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await process.communicate()
    if process.returncode != 0:
        detail = stderr.decode("utf-8", errors="replace").strip() or "ffprobe failed"
        raise RuntimeError(detail)

    try:
        payload = json.loads(stdout.decode())
        duration = max(float(payload.get("format", {}).get("duration", 0.0)), 0.0)
        video_stream = next(
            (
                stream
                for stream in payload.get("streams", [])
                if stream.get("width") and stream.get("height")
            ),
            {},
        )
        if not video_stream:
            raise RuntimeError("No usable video stream found")
        return VideoMetadata(
            duration=duration,
            width=int(video_stream.get("width", 0) or 0),
            height=int(video_stream.get("height", 0) or 0),
        )
    except (ValueError, TypeError, StopIteration) as exc:
        raise RuntimeError("Could not determine video metadata") from exc


async def make_thumbnail(source: Path, output_dir: Path, duration: float) -> Path | None:
    output_dir.mkdir(parents=True, exist_ok=True)
    target = output_dir / f"{source.stem}.jpg"
    timestamp = max(duration / 4, 0)
    process = await asyncio.create_subprocess_exec(
        "ffmpeg",
        "-hide_banner",
        "-loglevel",
        "error",
        "-ss",
        str(timestamp),
        "-i",
        str(source),
        "-vframes",
        "1",
        "-y",
        str(target),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    await process.communicate()
    if process.returncode == 0 and target.exists() and target.stat().st_size > 0:
        return target
    return None


def render_encode_progress(
    current: float,
    total: float,
    elapsed: float,
    ffmpeg_speed: float = 0.0,
    profile: EncodeProfile | None = None,
    job_id: int | None = None,
) -> str:
    ratio = min(current / total, 1.0) if total > 0 else 0.0
    filled = int(ratio * BAR_WIDTH)
    bar = "#" * filled + "-" * (BAR_WIDTH - filled)
    speed = ffmpeg_speed or (current / elapsed if elapsed > 0 else 0.0)
    eta = encode_eta(current, total, speed)

    job_line = f"🆔 Job: {job_id}\n" if job_id is not None else ""
    profile_line = f"🎛 Profile: {profile.label}\n" if profile else ""
    return (
        "🎬 Encoding\n"
        f"{job_line}"
        f"{profile_line}"
        f"[{bar}] {ratio * 100:.1f}%\n"
        f"{format_time(current)} / {format_time(total)}\n"
        f"⚡ Speed: {speed:.2f}x\n"
        f"⏳ ETA: {eta}"
        f"{status_footer()}"
    )


def format_time(seconds: float) -> str:
    seconds = max(0, int(seconds))
    hours, remainder = divmod(seconds, 3600)
    minutes, secs = divmod(remainder, 60)
    if hours:
        return f"{hours:d}:{minutes:02d}:{secs:02d}"
    return f"{minutes:d}:{secs:02d}"


def _progress_time(value: str) -> float:
    try:
        return max(float(value) / 1_000_000, 0.0)
    except ValueError:
        return 0.0


def _timestamp_to_seconds(value: str) -> float:
    try:
        hours, minutes, seconds = value.split(":")
        return int(hours) * 3600 + int(minutes) * 60 + float(seconds)
    except ValueError:
        return 0.0


def _speed_to_float(value: str) -> float:
    try:
        return max(float(value.rstrip("x")), 0.0)
    except ValueError:
        return 0.0


def encode_eta(current: float, total: float, speed: float) -> str:
    if total <= 0 or speed <= 0 or current >= total:
        return "done" if total > 0 and current >= total else "calculating"
    return format_time((total - current) / speed)


async def stop_process(process: asyncio.subprocess.Process) -> None:
    if process.returncode is not None:
        return

    try:
        process.send_signal(signal.SIGTERM)
    except ProcessLookupError:
        return

    try:
        await asyncio.wait_for(process.wait(), timeout=5)
        return
    except asyncio.TimeoutError:
        pass

    try:
        process.kill()
    except ProcessLookupError:
        return
    await process.wait()
