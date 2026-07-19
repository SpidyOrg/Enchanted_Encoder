from __future__ import annotations

import asyncio
import logging
import os
import subprocess
import time
from pathlib import Path
from typing import Any

from pyrogram import Client, filters
from pyrogram.types import Message

from telegram_bot.config import BotConfig, get_config
from telegram_bot.downloader import TDLibDownloader
from telegram_bot.queue_manager import EncodeQueue, VideoJob, cleanup_file, friendly_error
from telegram_bot.runtime import has_free_space, stale_file_cleanup, status_footer
from telegram_bot.settings import SettingsStore
from telegram_bot.transfer import ProgressMessage, safe_edit_text


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

ENCODE_QUEUE: EncodeQueue | None = None
SETTINGS_STORE: SettingsStore | None = None
DOWNLOADER: TDLibDownloader | None = None


def _encode_queue() -> EncodeQueue:
    if ENCODE_QUEUE is None:
        raise RuntimeError("Encode queue not initialized")
    return ENCODE_QUEUE


def _settings_store() -> SettingsStore:
    if SETTINGS_STORE is None:
        raise RuntimeError("Settings store not initialized")
    return SETTINGS_STORE


def _downloader() -> TDLibDownloader:
    if DOWNLOADER is None:
        raise RuntimeError("Downloader not initialized")
    return DOWNLOADER


def _sender_id(message: Message) -> str:
    user = getattr(message, "from_user", None)
    if user:
        return str(user.id)
    chat = getattr(message, "sender_chat", None)
    if chat:
        return str(chat.id)
    return str(message.chat.id)


def _sender_label(message: Message) -> str:
    user = getattr(message, "from_user", None)
    if user:
        name = user.first_name or user.username or ""
        return f"{name} ({user.id})" if name else str(user.id)
    chat = getattr(message, "sender_chat", None)
    if chat:
        return f"{chat.title} ({chat.id})" if chat.title else str(chat.id)
    return _sender_id(message)


def _filename_from_message(message: Message) -> str:
    video = getattr(message, "video", None)
    if video:
        return video.file_name or f"video-{message.id}.mp4"
    doc = getattr(message, "document", None)
    if doc:
        return doc.file_name or f"doc-{message.id}"
    return f"media-{message.id}"


def _video_file_size(message: Message) -> int:
    video = getattr(message, "video", None)
    if video:
        return video.file_size or 0
    doc = getattr(message, "document", None)
    if doc:
        return doc.file_size or 0
    return 0


def register_handlers(app: Client, config: BotConfig) -> None:
    @app.on_message(filters.command("start"))
    async def cmd_start(_: Client, message: Message) -> None:
        await message.reply_text("✅ Bot is running. Send a video to encode it." + status_footer())

    @app.on_message(filters.command("help"))
    async def cmd_help(_: Client, message: Message) -> None:
        await message.reply_text(HELP_TEXT + status_footer())

    @app.on_message(filters.command("ping"))
    async def cmd_ping(_: Client, message: Message) -> None:
        await message.reply_text("🏓 pong" + status_footer())

    @app.on_message(filters.command("id"))
    async def cmd_id(_: Client, message: Message) -> None:
        await message.reply_text(
            f"chat_id: {message.chat.id}\nfrom_id: {_sender_id(message)}" + status_footer()
        )

    @app.on_message(filters.command("settings"))
    async def cmd_settings(_: Client, message: Message) -> None:
        parts = message.text.strip().split(maxsplit=1)
        user_id = _sender_id(message)
        if len(parts) == 1:
            await message.reply_text(_settings_store().get(user_id).describe() + status_footer())
        else:
            action = parts[1].strip().lower()
            if action == "document":
                current = _settings_store().get(user_id).send_as_document
                settings = _settings_store().set_document(user_id, not current)
                await message.reply_text(
                    f"✅ Send as document is now {'on' if settings.send_as_document else 'off'}"
                    + status_footer()
                )
            elif action.startswith("document "):
                val = action.split(None, 1)[1]
                is_on = val in ("on", "yes", "true", "1")
                settings = _settings_store().set_document(user_id, is_on)
                await message.reply_text(
                    f"✅ Send as document is now {'on' if settings.send_as_document else 'off'}"
                    + status_footer()
                )
            else:
                settings = _settings_store().set_profile(user_id, parts[1])
                if settings is None:
                    await message.reply_text(
                        "❌ Unknown profile.\n\n" + _settings_store().get(user_id).describe()
                        + status_footer()
                    )
                else:
                    await message.reply_text(
                        f"✅ Your default profile is now {settings.profile.label}" + status_footer()
                    )

    @app.on_message(filters.command("queue") | filters.command("status"))
    async def cmd_queue(_: Client, message: Message) -> None:
        await message.reply_text(_encode_queue().user_summary(_sender_id(message)))

    @app.on_message(filters.command("check"))
    async def cmd_check(_: Client, message: Message) -> None:
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
            lines.append(f"{'✅' if name in encoders else '❌'} {name}")
        lines.append(f"\n🖥 Cores: {os.cpu_count() or 'unknown'}")
        lines.append(f"🧵 Encode threads: {_encode_queue().encode_threads}")
        await message.reply_text("\n".join(lines) + status_footer())

    @app.on_message(filters.command("cancel"))
    async def cmd_cancel(_: Client, message: Message) -> None:
        parts = message.text.strip().split(maxsplit=1)
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
            await message.reply_text(
                "Reply to the video task or use /cancel <job_id>." + status_footer()
            )
            return
        result = await _encode_queue().cancel(_sender_id(message), job_id, reply_message_id)
        await message.reply_text(result + status_footer())

    @app.on_message(filters.video | filters.document)
    async def handle_media_message(_: Client, message: Message) -> None:
        if message.video:
            pass
        elif message.document:
            mime = getattr(message.document, "mime_type", "") or ""
            if not mime.startswith("video/"):
                await message.reply_text("Send me a video file to encode.")
                return
        else:
            return

        await handle_media(message, config)

    @app.on_message(filters.private & filters.text)
    async def echo(_: Client, message: Message) -> None:
        text = message.text.strip()
        if text.startswith("/"):
            return
        await message.reply_text(text)


async def handle_media(message: Message, config: BotConfig) -> None:
    queue = _encode_queue()
    owner_id = _sender_id(message)
    file_size = _video_file_size(message)

    if queue.waiting >= queue.capacity:
        await message.reply_text("🚦 Queue is full. Try again later." + status_footer())
        return

    if not queue.reserve_user_slot(owner_id):
        count = queue.user_job_count(owner_id)
        await message.reply_text(
            f"🚦 You already have {count} job{'s' if count != 1 else ''} "
            f"queued or active (max {queue.max_jobs_per_user}). "
            f"Wait for one to finish or use /cancel."
            + status_footer()
        )
        return

    status = await queue.get_or_create_user_status(owner_id, message)
    if status is None:
        queue.release_user(owner_id)
        return

    required_space = int(file_size * DISK_HEADROOM_MULTIPLIER)
    if file_size and not has_free_space(config.files_dir, required_space):
        queue.release_user(owner_id)
        await message.reply_text("❌ Insufficient free disk space for this video." + status_footer())
        return

    settings = _settings_store().get(owner_id)
    job = VideoJob(
        id=queue.next_id(),
        message=message,
        status=status,
        profile=settings.profile,
        queued_at=time.time(),
        owner_id=owner_id,
        owner_label=_sender_label(message),
        filename=_filename_from_message(message),
        file_size=file_size,
        source_message_id=message.id,
        chat_id=message.chat.id,
        send_as_document=settings.send_as_document,
    )

    if await queue.enqueue(job) is None:
        queue.release_user(owner_id)
        await safe_edit_text(status, "🚦 Queue is full. Try again later." + status_footer())
        return

    await queue._update_user_status(owner_id)


async def main_async() -> None:
    global ENCODE_QUEUE, SETTINGS_STORE, DOWNLOADER
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
    config = get_config()
    config.files_dir.mkdir(parents=True, exist_ok=True)
    config.output_dir.mkdir(parents=True, exist_ok=True)

    removed = stale_file_cleanup(config.output_dir, config.files_dir, config.stale_file_hours)
    if removed:
        logging.getLogger(__name__).info("Removed %s stale runtime files", removed)

    SETTINGS_STORE = SettingsStore(config.settings_path)

    downloader = TDLibDownloader(config)
    await downloader.start()
    DOWNLOADER = downloader

    ENCODE_QUEUE = EncodeQueue(
        config.output_dir,
        downloader=downloader,
        max_jobs_per_user=config.max_jobs_per_user,
        max_concurrent_encoders=config.max_concurrent_encoders,
        journal_path=config.settings_path.parent / "queue_journal.json",
    )

    app = Client(
        "encoder-bot",
        api_id=config.api_id,
        api_hash=config.api_hash,
        bot_token=config.bot_token,
        in_memory=True,
    )
    register_handlers(app, config)

    logging.getLogger(__name__).info("Starting pyroblack client")
    await app.start()

    logging.getLogger(__name__).info("Both clients running — waiting for updates")
    try:
        await asyncio.Event().wait()
    finally:
        await downloader.stop()
        await app.stop()


def main() -> None:
    asyncio.run(main_async())
