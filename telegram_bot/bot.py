from __future__ import annotations

import logging
import os
import time
from pathlib import Path
from typing import Any

from pytdbot import Client, types

from telegram_bot.config import BotConfig, get_config
from telegram_bot.queue_manager import EncodeQueue, VideoJob, cleanup_file, friendly_error
from telegram_bot.runtime import has_free_space, stale_file_cleanup, status_footer
from telegram_bot.settings import SettingsStore
from telegram_bot.transfer import (
    ProgressMessage,
    UploadProgress,
    download_media,
    extract_media_file,
    is_video_message,
    safe_edit_text,
)


LOG_FORMAT = "%(asctime)s %(levelname)s [%(name)s] %(message)s"
DISK_HEADROOM_MULTIPLIER = 2.5
HELP_TEXT = "\n".join((
    "Available commands:",
    "/start - start the bot",
    "/help - show this message",
    "/ping - check bot responsiveness",
    "/id - show chat and sender ids",
    "/settings - show your encode profiles",
    "/settings balanced - set your default encode profile",
    "/queue or /status - show queue status",
    "/cancel <job_id> - cancel your queued or active job",
    "/cancel as a reply - cancel the replied video task",
    "/check - check installed encoders (H.264/HEVC/AV1)",
))
UPLOAD_PROGRESS = UploadProgress()
ENCODE_QUEUE: EncodeQueue | None = None
SETTINGS_STORE: SettingsStore | None = None


def _encode_queue() -> EncodeQueue:
    if ENCODE_QUEUE is None:
        raise RuntimeError("Encode queue has not been initialized")
    return ENCODE_QUEUE


def _settings_store() -> SettingsStore:
    if SETTINGS_STORE is None:
        raise RuntimeError("Settings store has not been initialized")
    return SETTINGS_STORE


def _message_text(message: Any) -> str:
    text = getattr(message, "text", None)
    if isinstance(text, str):
        return text.strip()
    content = getattr(message, "content", None)
    nested_text = getattr(content, "text", None)
    if isinstance(nested_text, str):
        return nested_text.strip()
    text_obj = getattr(nested_text, "text", None)
    return text_obj.strip() if isinstance(text_obj, str) else ""


def _sender_id(message: Any) -> str:
    for attr in ("from_id", "sender_id"):
        value = getattr(message, attr, None)
        if value is not None:
            return str(value)
    return "unknown"


def _sender_label(message: Any) -> str:
    sender_id = _sender_id(message)
    sender = getattr(message, "sender", None)
    name = getattr(sender, "first_name", None) or getattr(sender, "username", None)
    return f"{name} ({sender_id})" if name else sender_id


def _media_filename(media_file: types.File) -> str:
    local = getattr(media_file, "local", None)
    path = getattr(local, "path", "") if local else ""
    return Path(path).name or f"video-{media_file.id}"


def create_client(config: BotConfig) -> Client:
    kwargs: dict[str, Any] = {
        "token": config.bot_token,
        "api_id": config.api_id,
        "api_hash": config.api_hash,
        "files_directory": str(config.files_dir),
        "database_encryption_key": config.database_encryption_key,
        "td_verbosity": config.td_verbosity,
    }
    if config.tdjson_lib_path:
        kwargs["lib_path"] = config.tdjson_lib_path
    if config.td_log:
        kwargs["td_log"] = types.LogStreamFile(str(config.td_log), 104_857_600)
    client = Client(**kwargs)
    register_handlers(client, config)
    return client


def register_handlers(client: Client, config: BotConfig) -> None:
    @client.on_updateFile()
    async def track_file_update(_: Client, update: types.UpdateFile) -> None:
        UPLOAD_PROGRESS.update_from_file(update.file)

    @client.on_message()
    async def handle_message(_: Client, message: types.Message) -> None:
        text = _message_text(message)
        if text.startswith("/"):
            command = text.split(maxsplit=1)[0].split("@", 1)[0].lower()
            if command == "/start":
                await message.reply_text("✅ Bot is running. Send a video to encode it." + status_footer())
            elif command == "/help":
                await message.reply_text(HELP_TEXT + status_footer())
            elif command == "/ping":
                await message.reply_text("🏓 pong" + status_footer())
            elif command == "/id":
                await message.reply_text(f"chat_id: {message.chat_id}\nfrom_id: {_sender_id(message)}" + status_footer())
            elif command == "/settings":
                parts = text.split(maxsplit=1)
                user_id = _sender_id(message)
                if len(parts) == 1:
                    await message.reply_text(_settings_store().get(user_id).describe() + status_footer())
                else:
                    action = parts[1].strip().lower()
                    if action == "document":
                        current = _settings_store().get(user_id).send_as_document
                        settings = _settings_store().set_document(user_id, not current)
                        await message.reply_text(f"✅ Send as document is now {'on' if settings.send_as_document else 'off'}" + status_footer())
                    elif action.startswith("document "):
                        val = action.split(None, 1)[1]
                        is_on = val in ("on", "yes", "true", "1")
                        settings = _settings_store().set_document(user_id, is_on)
                        await message.reply_text(f"✅ Send as document is now {'on' if settings.send_as_document else 'off'}" + status_footer())
                    else:
                        settings = _settings_store().set_profile(user_id, parts[1])
                        if settings is None:
                            await message.reply_text("❌ Unknown profile.\n\n" + _settings_store().get(user_id).describe() + status_footer())
                        else:
                            await message.reply_text(f"✅ Your default profile is now {settings.profile.label}" + status_footer())
            elif command in {"/queue", "/status"}:
                await message.reply_text(_encode_queue().user_summary(_sender_id(message)))
            elif command == "/check":
                await handle_check(message)
            elif command == "/cancel":
                await handle_cancel(message, text)
            else:
                await message.reply_text("❌ Unknown command. Send /help." + status_footer())
            return

        media_file = extract_media_file(message)
        if media_file is not None:
            if not is_video_message(message):
                await message.reply_text("Send me a video file to encode.")
                return
            await handle_media(client, message, media_file, config)
            return
        if text and getattr(message, "is_private", False):
            await message.reply_text(text)


async def handle_media(client: Client, message: types.Message, media_file: types.File, config: BotConfig) -> None:
    queue = _encode_queue()
    owner_id = _sender_id(message)
    file_size = int(getattr(media_file, "size", 0) or getattr(media_file, "expected_size", 0) or 0)
    if queue.waiting >= queue.capacity:
        await message.reply_text("🚦 Queue is full. Try again later." + status_footer())
        return
    if not queue.reserve_download(owner_id):
        await message.reply_text(
            "⏳ A download is already in progress for you. "
            "Please wait for it to finish before sending another file."
            + status_footer()
        )
        return
    status = await queue.get_or_create_user_status(owner_id, message)
    if status is None:
        queue.release_download(owner_id)
        return
    downloaded_path: Path | None = None
    try:
        required_space = int(file_size * DISK_HEADROOM_MULTIPLIER)
        if file_size and not has_free_space(config.files_dir, required_space):
            raise RuntimeError("Insufficient free disk space for this video")
        text = queue._render_user_status(owner_id, f"📥 Downloading: {_media_filename(media_file)}")
        await safe_edit_text(status, text)
        downloaded_path = await download_media(client, media_file, ProgressMessage(status, "📥 Downloading", file_size, queue=queue, user_id=owner_id))
    except Exception as exc:
        logging.getLogger(__name__).exception("Media transfer failed")
        if downloaded_path is not None:
            cleanup_file(downloaded_path)
        queue.release_download(owner_id)
        await safe_edit_text(status, "❌ " + friendly_error(exc) + status_footer())
        return

    queue.release_download(owner_id)

    if not queue.reserve_user_slot(owner_id):
        cleanup_file(downloaded_path)
        count = queue._user_jobs.get(owner_id, 0)
        await safe_edit_text(
            status,
            f"🚦 You already have {count} job{'s' if count != 1 else ''} "
            f"queued or active (max {queue._max_jobs_per_user}). "
            f"Wait for one to finish or use /cancel."
            + status_footer()
        )
        return

    settings = _settings_store().get(owner_id)
    job = VideoJob(
        id=queue.next_id(), message=message, status=status, source_path=downloaded_path,
        profile=settings.profile, queued_at=time.time(), source_message_id=message.id,
        owner_id=owner_id, owner_label=_sender_label(message), filename=_media_filename(media_file),
        send_as_document=settings.send_as_document,
    )
    if await queue.enqueue(job) is None:
        cleanup_file(downloaded_path)
        queue.release_user(owner_id)
        await safe_edit_text(status, "🚦 Queue is full. Try again later." + status_footer())
        return

    await queue._update_user_status(owner_id)


async def handle_check(message: types.Message) -> None:
    import subprocess

    target = ("libx264", "libx265", "libsvtav1")
    try:
        result = subprocess.run(
            ["ffmpeg", "-hide_banner", "-encoders"],
            capture_output=True, text=True, timeout=30,
        )
        encoders = result.stdout
    except FileNotFoundError:
        await message.reply_text("❌ ffmpeg is not installed on this server." + status_footer())
        return
    except Exception as exc:
        await message.reply_text(f"❌ Could not list ffmpeg encoders: {exc}" + status_footer())
        return

    lines = ["🎛 Encoder availability:\n"]
    for name in target:
        if name in encoders:
            lines.append(f"✅ {name} — available")
        else:
            lines.append(f"❌ {name} — NOT installed")
    lines.append(f"\n🖥 Cores: {os.cpu_count() or 'unknown'}")
    lines.append(f"🧵 Encode threads: {_encode_queue()._encode_threads}")
    await message.reply_text("\n".join(lines) + status_footer())


async def handle_cancel(message: types.Message, text: str) -> None:
    parts = text.split(maxsplit=1)
    job_id: int | None = None
    reply_message_id: int | None = None
    if len(parts) > 1:
        try:
            job_id = int(parts[1].strip())
        except ValueError:
            await message.reply_text("❌ Usage: /cancel <job_id>" + status_footer())
            return
    else:
        reply_to = getattr(message, "reply_to_message_id", None)
        if reply_to:
            reply_message_id = int(reply_to)
    if job_id is None and reply_message_id is None:
        await message.reply_text("Reply to the video task or use /cancel <job_id>." + status_footer())
        return
    result = await _encode_queue().cancel(_sender_id(message), job_id, reply_message_id)
    await message.reply_text(result + status_footer())


def main() -> None:
    global ENCODE_QUEUE, SETTINGS_STORE
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
    config = get_config()
    config.files_dir.mkdir(parents=True, exist_ok=True)
    config.output_dir.mkdir(parents=True, exist_ok=True)
    removed = stale_file_cleanup(config.output_dir, config.files_dir, config.stale_file_hours)
    if removed:
        logging.getLogger(__name__).info("Removed %s stale runtime files", removed)
    SETTINGS_STORE = SettingsStore(config.settings_path)
    ENCODE_QUEUE = EncodeQueue(
        UPLOAD_PROGRESS,
        config.output_dir,
        max_jobs_per_user=config.max_jobs_per_user,
        max_concurrent_encoders=config.max_concurrent_encoders,
        journal_path=config.settings_path.parent / "queue_journal.json",
    )
    logging.getLogger(__name__).info("Starting Telegram bot")
    create_client(config).run()
