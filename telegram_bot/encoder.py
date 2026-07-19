from __future__ import annotations

import asyncio
import json
import os
import signal
import time
from pathlib import Path
from typing import Any

from telegram_bot.runtime import status_footer
from telegram_bot.settings import EncodeProfile
from telegram_bot.transfer import BAR_WIDTH, ProgressMessage, safe_edit_text


STALL_TIMEOUT = 600  # 10 minutes
LINE_READ_TIMEOUT = 60  # 1 minute per line read
FFMPEG_LOG_LINES = 50  # Keep last N log lines for error reporting


async def encode_h264_720p(
    source: Path,
    output_dir: Path,
    progress: ProgressMessage,
    profile: EncodeProfile,
    cancel_event: asyncio.Event | None = None,
    job_id: int | None = None,
    queue: Any = None,
    user_id: str = "",
    encode_threads: int = 0,
) -> Path:
    """Encode video file with progress tracking and error handling.
    
    Args:
        source: Source video file path
        output_dir: Output directory for encoded file
        progress: Progress message tracker
        profile: Encode profile with codec settings
        cancel_event: Optional cancellation event
        job_id: Optional job ID for logging
        queue: Optional queue reference for status updates
        user_id: Optional user ID for status updates
        encode_threads: Number of threads to use for encoding
        
    Returns:
        Path to encoded output file
        
    Raises:
        asyncio.CancelledError: If encoding is cancelled
        RuntimeError: If encoding fails
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    job_suffix = f".job-{job_id}" if job_id is not None else ""
    target = output_dir / f"{source.stem}{job_suffix}.encoded.{profile.codec}.mp4"
    
    # Probe video metadata for progress tracking
    try:
        metadata = await probe_video_metadata(source)
        duration = metadata.duration
    except Exception as e:
        raise RuntimeError(f"Failed to probe video: {e}") from e

    command = _build_ffmpeg_command(source, target, profile, encode_threads)

    process = await asyncio.create_subprocess_exec(
        *command,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )

    started_at = time.monotonic()
    last_edit_at = 0.0
    out_time = 0.0
    ffmpeg_speed = 0.0
    last_progress = time.monotonic()
    stderr_lines: list[str] = []  # Collect stderr for error reporting

    assert process.stdout is not None
    assert process.stderr is not None
    
    try:
        # Create tasks for reading stdout and stderr concurrently
        async def read_stdout() -> None:
            nonlocal out_time, ffmpeg_speed, last_progress, last_edit_at
            while True:
                try:
                    line = await asyncio.wait_for(
                        process.stdout.readline(), timeout=LINE_READ_TIMEOUT
                    )
                except asyncio.TimeoutError:
                    continue
                
                if not line:
                    break
                
                key, _, value = line.decode("utf-8", errors="replace").strip().partition("=")
                if key in {"out_time_us", "out_time_ms"}:
                    out_time = max(out_time, _progress_time(value))
                    last_progress = time.monotonic()
                elif key == "out_time":
                    out_time = max(out_time, _timestamp_to_seconds(value))
                    last_progress = time.monotonic()
                elif key == "speed":
                    ffmpeg_speed = _speed_to_float(value)
                    last_progress = time.monotonic()
                
                # Update progress periodically
                now = time.monotonic()
                if now - last_edit_at >= 5:
                    last_edit_at = now
                    encode_text = render_encode_progress(
                        out_time,
                        duration,
                        now - started_at,
                        ffmpeg_speed,
                        profile=profile,
                        job_id=job_id,
                    )
                    if queue and user_id:
                        text = queue._render_user_status(user_id, encode_text)
                    else:
                        text = encode_text + status_footer()
                    await safe_edit_text(progress.message, text)
        
        async def read_stderr() -> None:
            while True:
                try:
                    line = await process.stderr.readline()
                    if not line:
                        break
                    decoded = line.decode("utf-8", errors="replace").strip()
                    if decoded:
                        stderr_lines.append(decoded)
                        # Keep only last N lines
                        if len(stderr_lines) > FFMPEG_LOG_LINES:
                            stderr_lines.pop(0)
                except Exception:
                    break
        
        # Run both readers concurrently
        stdout_task = asyncio.create_task(read_stdout())
        stderr_task = asyncio.create_task(read_stderr())
        
        # Monitor for cancellation and stalls
        while True:
            if cancel_event and cancel_event.is_set():
                await stop_process(process)
                stdout_task.cancel()
                stderr_task.cancel()
                raise asyncio.CancelledError("Encoding cancelled")
            
            # Check for stall
            if time.monotonic() - last_progress > STALL_TIMEOUT:
                await stop_process(process)
                stdout_task.cancel()
                stderr_task.cancel()
                raise RuntimeError(
                    f"Encoding stalled: no progress for {STALL_TIMEOUT // 60} minutes"
                )
            
            # Check if process finished
            if process.returncode is not None:
                break
            
            await asyncio.sleep(1)
        
        # Wait for readers to finish
        try:
            await asyncio.wait_for(stdout_task, timeout=5)
        except asyncio.TimeoutError:
            stdout_task.cancel()
        
        try:
            await asyncio.wait_for(stderr_task, timeout=5)
        except asyncio.TimeoutError:
            stderr_task.cancel()
        
    except asyncio.CancelledError:
        await stop_process(process)
        if target.exists():
            target.unlink()
        raise
    except Exception as e:
        await stop_process(process)
        if target.exists():
            target.unlink()
        raise RuntimeError(f"Encoding process error: {e}") from e

    return_code = await process.wait()
    if return_code != 0:
        error_detail = stderr_lines[-1] if stderr_lines else "ffmpeg failed with no output"
        # Include last few lines of stderr for debugging
        context = "\n".join(stderr_lines[-5:]) if len(stderr_lines) > 1 else error_detail
        raise RuntimeError(f"Encoding failed (code {return_code}): {context}")

    if not target.exists() or target.stat().st_size == 0:
        if target.exists():
            target.unlink()
        raise RuntimeError("Encoded output was not created or is empty")

    # Final progress update
    encode_text = render_encode_progress(
        duration,
        duration,
        time.monotonic() - started_at,
        ffmpeg_speed,
        profile=profile,
        job_id=job_id,
    )
    if queue and user_id:
        text = queue._render_user_status(user_id, encode_text)
    else:
        text = encode_text + status_footer()
    await safe_edit_text(progress.message, text)
    
    return target


def _build_ffmpeg_command(source: Path, target: Path, profile: EncodeProfile, encode_threads: int = 0) -> list[str]:
    """Build FFmpeg command with proper codec-specific arguments.
    
    Args:
        source: Source video path
        target: Output video path
        profile: Encode profile with codec settings
        encode_threads: Number of threads for encoding
        
    Returns:
        List of command arguments for subprocess execution
    """
    # Base command with common arguments
    cmd = [
        "ffmpeg",
        "-hide_banner", "-y",
        "-i", str(source),
        "-map", "0:v:0", "-map", "0:a?",
        "-vf", f"scale=-2:{profile.resolution}",
        "-c:v", profile.encoder_name,
        "-preset", profile.preset,
        "-threads", str(max(encode_threads, 1)),
        "-crf", str(profile.crf),
        "-c:a", "aac",
        "-b:a", profile.audio_bitrate,
    ]
    
    # Add codec-specific arguments BEFORE output path
    if profile.codec == "hevc":
        # HEVC requires hvc1 tag for better compatibility
        cmd.extend(["-tag:v", "hvc1"])
    
    # Output formatting and path (must be last)
    cmd.extend([
        "-movflags", "+faststart",
        "-progress", "pipe:1",
        "-nostats",
        str(target),
    ])
    
    return cmd


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
    bar = "█" * filled + "░" * (BAR_WIDTH - filled)
    speed = ffmpeg_speed or (current / elapsed if elapsed > 0 else 0.0)
    eta = encode_eta(current, total, speed)

    job_line = f"🆔 Job: {job_id}\n" if job_id is not None else ""
    profile_line = f"🎛 Profile: {profile.label}\n" if profile else ""
    codec_label = (profile.codec.upper() if profile else "H.264")
    return (
        f"🎬 Encoding ({codec_label})\n"
        f"{job_line}"
        f"{profile_line}"
        f"[{bar}] {ratio * 100:.1f}%\n"
        f"{format_time(current)} / {format_time(total)}\n"
        f"⚡ Speed: {speed:.2f}x\n"
        f"⏳ ETA: {eta}"
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
