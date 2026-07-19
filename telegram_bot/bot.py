from __future__ import annotations

import asyncio
import logging
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from pytdbot import Client, types

from telegram_bot.config import BotConfig, get_config
from telegram_bot.downloader import TDLibDownloader
from telegram_bot.keyboards import (
    ACTION_CANCEL,
    ACTION_CLOSE,
    ACTION_DOCUMENT,
    ACTION_PROFILE,
    ACTION_REFRESH,
    ACTION_SETTINGS,
    build_main_menu_keyboard,
    build_queue_keyboard,
    build_settings_keyboard,
    parse_callback_payload,
)
from telegram_bot.queue_manager import EncodeQueue, VideoJob, cleanup_file, friendly_error
from telegram_bot.runtime import has_free_space, stale_file_cleanup
from telegram_bot.settings import PROFILES, SettingsStore
from telegram_bot.transfer import ProgressMessage, safe_edit_text


LOGGER = logging.getLogger(__name__)
LOG_FORMAT = "%(asctime)s %(levelname)s [%(name)s] %(message)s"
DISK_HEADROOM_MULTIPLIER = 2.5
COMMAND_COOLDOWN_SECONDS = 1.0
_last_command_at: dict[str, float] = {}


@dataclass
class BotContext:
    config: BotConfig
    queue: EncodeQueue
    settings: SettingsStore
    downloader: TDLibDownloader


def _ctx(client: Client) -> BotContext:
    return client.ctx  # type: ignore[attr-defined]


def _encode_queue(client: Client) -> EncodeQueue:
    return _ctx(client).queue


def _settings_store(client: Client) -> SettingsStore:
    return _ctx(client).settings


def _downloader(client: Client) -> TDLibDownloader:
    return _ctx(client).downloader


def _rate_limited(user_id: str) -> bool:
    now = time.monotonic()
    last = _last_command_at.get(user_id, 0.0)
    if now - last < COMMAND_COOLDOWN_SECONDS:
        return True
    _last_command_at[user_id] = now
    return False


def _sender_id(message: types.Message) -> str:
    sender = getattr(message, "sender_id", None)
    if isinstance(sender, dict):
        if sender.get("@type") == "messageSenderChat":
            return str(sender.get("chat_id", "0"))
        return str(sender.get("user_id", "0"))
    user = getattr(message, "from_user", None) or getattr(message, "sender_user", None)
    if user is not None:
        return str(getattr(user, "id", "0"))
    chat = getattr(message, "chat", None)
    if chat is not None:
        return str(getattr(chat, "id", "0"))
    return "0"


def _sender_label(message: types.Message) -> str:
    sender = getattr(message, "sender_id", None)
    if isinstance(sender, dict):
        return str(sender.get("user_id") or sender.get("chat_id") or "user")
    return "user"


def _text_of(message: types.Message) -> str:
    content = getattr(message, "content", None)
    if not isinstance(content, dict):
        return ""
    text = content.get("text")
    if isinstance(text, str):
        return text
    if isinstance(text, dict):
        return text.get("text", "") or ""
    return ""


def _filename_from_message(message: types.Message) -> str:
    content = getattr(message, "content", None)
    if not isinstance(content, dict):
        return "video.mp4"
    for key in ("video", "document", "animation"):
        node = content.get(key)
        if isinstance(node, dict):
            f = node.get(key) or node
            name = f.get("file_name") if isinstance(f, dict) else None
            if name:
                return name
    return "video.mp4"


def _video_file_size(message: types.Message) -> int:
    content = getattr(message, "content", None)
    if not isinstance(content, dict):
        return 0
    for key in ("video", "document", "animation"):
        node = content.get(key)
        if isinstance(node, dict):
            f = node.get(key) or node
            if isinstance(f, dict):
                return int(f.get("size", 0) or 0)
    return 0


def _is_video_message(message: types.Message) -> bool:
    content = getattr(message, "content", None)
    if not isinstance(content, dict):
        return False
    if "video" in content:
        return True
    doc = content.get("document")
    if isinstance(doc, dict):
        mime = doc.get("mime_type", "") or ""
        return mime.startswith("video/")
    return False


async def _safe_reply(message: types.Message, text: str, **kwargs: Any) -> None:
    try:
        result = await message.reply_text(text=text, **kwargs)
        if isinstance(result, types.Error):
            LOGGER.warning("reply failed: %s", getattr(result, "message", result))
        else:
            LOGGER.info("reply ok: msg_id=%s", getattr(result, "id", "?"))
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("reply_text failed: %s", exc)


async def _safe_edit(message: types.Message, text: str, **kwargs: Any) -> bool:
    try:
        result = await message.edit_text(text=text, **kwargs)
        if isinstance(result, types.Error):
            LOGGER.warning("edit failed: %s", getattr(result, "message", result))
            return False
        return True
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("edit failed: %s", exc)
        return False


async def handle_media(client: Client, message: types.Message, config: BotConfig) -> None:
    LOGGER.info("handle_media called for msg %s", getattr(message, "id", "?"))
    queue = _encode_queue(client)
    settings_store = _settings_store(client)
    owner_id = _sender_id(message)
    file_size = _video_file_size(message)

    if queue.waiting >= queue.capacity:
        await _safe_reply(message, "🚦 Queue is full. Try again later." + _footer())
        return

    # Serial processing: only one video at a time. While a job is active or
    # already queued, tell the user to wait instead of accepting more media.
    if queue.active or queue.waiting > 0:
        await _safe_reply(
            message,
            "⏳ I'm already working on a video. "
            "I'll pick up your next one as soon as this finishes — please wait." + _footer(),
        )
        return

    status = await queue.get_or_create_user_status(owner_id, message)
    if status is None:
        return

    required_space = int(file_size * DISK_HEADROOM_MULTIPLIER)
    if file_size and not has_free_space(config.files_dir, required_space):
        await _safe_reply(message, "❌ Insufficient free disk space for this video." + _footer())
        return

    settings = settings_store.get(owner_id)
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
        source_message_id=getattr(message, "id", 0),
        chat_id=getattr(message, "chat_id", 0),
        send_as_document=settings.send_as_document,
    )

    if await queue.enqueue(job) is None:
        await _safe_edit(status, "🚦 Queue is full. Try again later." + _footer())
        return

    await queue._update_user_status(owner_id)


def _footer() -> str:
    from telegram_bot.formatting import status_footer

    return status_footer()


async def handle_callback_query(client: Client, query: types.UpdateNewCallbackQuery) -> None:
    user_id = _sender_id_from_query(query)
    action, value = parse_callback_payload(getattr(query, "payload", None))
    queue = _encode_queue(client)
    settings_store = _settings_store(client)

    try:
        if action == ACTION_PROFILE:
            settings = settings_store.set_profile(user_id, value)
            if settings is not None:
                keyboard = build_settings_keyboard(
                    settings.profile_key, settings.send_as_document, PROFILES
                )
                await _edit_query_message(query, settings.describe(), keyboard)
                await _answer(query, f"✅ Profile changed to {settings.profile.name}")
            else:
                await _answer(query, "❌ Invalid profile", show_alert=True)

        elif action == ACTION_DOCUMENT:
            is_on = value == "on"
            settings = settings_store.set_document(user_id, is_on)
            keyboard = build_settings_keyboard(
                settings.profile_key, settings.send_as_document, PROFILES
            )
            await _edit_query_message(query, settings.describe(), keyboard)
            await _answer(query, f"✅ Document mode {'enabled' if is_on else 'disabled'}")

        elif action == ACTION_REFRESH:
            keyboard = build_queue_keyboard(_user_job_ids(queue, user_id))
            await _edit_query_message(query, queue.user_summary(user_id), keyboard)
            await _answer(query, "🔄 Refreshed")

        elif action == ACTION_SETTINGS:
            settings = settings_store.get(user_id)
            keyboard = build_settings_keyboard(
                settings.profile_key, settings.send_as_document, PROFILES
            )
            await _edit_query_message(query, settings.describe(), keyboard)
            await _answer(query, "⚙️ Settings")

        elif action == ACTION_CANCEL:
            try:
                job_id = int(value) if value else None
            except ValueError:
                await _answer(query, "❌ Invalid job ID", show_alert=True)
                return
            result = await queue.cancel(user_id, job_id=job_id)
            keyboard = build_queue_keyboard(_user_job_ids(queue, user_id))
            await _edit_query_message(query, queue.user_summary(user_id), keyboard)
            await _answer(query, result, show_alert=True)

        elif action == ACTION_CLOSE:
            await _answer(query, "👋 Closed")
            await _delete_query_message(query)

        else:
            await _answer(query, "❓ Unknown action", show_alert=True)
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("Callback query error: %s", exc)
        await _answer(query, "⚠️ Something went wrong", show_alert=True)


def _user_job_ids(queue: EncodeQueue, user_id: str) -> list[int]:
    ids = [j.id for j in queue._active_jobs.values() if j.owner_id == user_id]
    ids.extend(j.id for j in list(queue._queue._queue) if j.owner_id == user_id)
    return ids


def _sender_id_from_query(query: types.UpdateNewCallbackQuery) -> str:
    sender = getattr(query, "sender_user_id", None)
    if sender:
        return str(sender)
    return "0"


async def _edit_query_message(
    query: types.UpdateNewCallbackQuery, text: str, keyboard: Any
) -> None:
    chat_id = getattr(query, "chat_id", 0)
    message_id = getattr(query, "message_id", 0)
    client = query._client if hasattr(query, "_client") else None
    if client is None:
        # Fall back to editing via the bound message if available.
        msg = getattr(query, "message", None)
        if msg is not None:
            await _safe_edit(msg, text, reply_markup=keyboard)
        return
    try:
        result = await client.editMessageText(
            chat_id=chat_id, message_id=message_id, text=text, reply_markup=keyboard
        )
        if isinstance(result, types.Error):
            LOGGER.warning("editMessageText failed: %s", getattr(result, "message", result))
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("editMessageText error: %s", exc)


async def _answer(
    query: types.UpdateNewCallbackQuery, text: str, show_alert: bool = False
) -> None:
    try:
        await query.answer(text=text, show_alert=show_alert)
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("answerCallbackQuery failed: %s", exc)


async def _delete_query_message(query: types.UpdateNewCallbackQuery) -> None:
    client = query._client if hasattr(query, "_client") else None
    if client is None:
        return
    try:
        await client.deleteMessages(
            chat_id=getattr(query, "chat_id", 0),
            message_ids=[getattr(query, "message_id", 0)],
            revoke=True,
        )
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("delete query message failed: %s", exc)


async def register_handlers(client: Client, config: BotConfig) -> None:
    @client.on_updateNewMessage()
    async def on_new_message(_: Client, update: types.UpdateNewMessage) -> None:
        message = getattr(update, "message", None)
        if message is None:
            return
        LOGGER.info(
            "on_new_message chat=%s id=%s content=%s",
            getattr(message, "chat_id", "?"),
            getattr(message, "id", "?"),
            type(getattr(message, "content", None)).__name__,
        )
        # Ignore messages sent by the bot itself.
        if _is_outgoing(message):
            return
        if _is_video_message(message):
            await handle_media(client, message, config)
            return
        text = _text_of(message).strip()
        if not text:
            return
        if not text.startswith("/"):
            await _safe_reply(
                message,
                "👋 Send me a video and I'll encode it. Use /settings to choose a profile."
                + _footer(),
            )
            return
        await _handle_command(client, message, text)

    @client.on_updateNewCallbackQuery()
    async def on_callback_query(_: Client, query: types.UpdateNewCallbackQuery) -> None:
        await handle_callback_query(client, query)


def _is_outgoing(message: types.Message) -> bool:
    return bool(getattr(message, "is_outgoing", False))


async def _handle_command(client: Client, message: types.Message, text: str) -> None:
    parts = text.split()
    command = parts[0].lstrip("/").lower().split("@")[0]
    user_id = _sender_id(message)

    if _rate_limited(user_id):
        return

    if command in ("start", "help"):
        await _safe_reply(
            message,
            "🎬 <b>Enchanted Encoder</b>\n\n"
            "Send me a video and I'll re-encode it with your chosen profile.\n"
            "Commands:\n"
            "/settings — choose encode profile\n"
            "/queue — show status\n"
            "/cancel — cancel a job\n"
            "/id — show your chat id\n"
            "/ping — bot health" + _footer(),
        )
    elif command == "ping":
        await _safe_reply(message, "🏓 Pong! Bot is alive." + _footer())
    elif command == "id":
        await _safe_reply(
            message,
            f"Chat ID: <code>{getattr(message, 'chat_id', '?')}</code>\n"
            f"User ID: <code>{user_id}</code>",
        )
    elif command == "settings":
        settings = _settings_store(client).get(user_id)
        keyboard = build_settings_keyboard(
            settings.profile_key, settings.send_as_document, PROFILES
        )
        await _safe_reply(message, settings.describe(), reply_markup=keyboard)
    elif command in ("queue", "status"):
        queue = _encode_queue(client)
        keyboard = build_queue_keyboard(_user_job_ids(queue, user_id))
        await _safe_reply(message, queue.user_summary(user_id), reply_markup=keyboard)
    elif command == "check":
        queue = _encode_queue(client)
        status = await queue.get_or_create_user_status(user_id, message)
        if status is not None:
            await queue._update_user_status(user_id)
    elif command == "cancel":
        reply_to = getattr(message, "reply_to_message_id", 0) or 0
        queue = _encode_queue(client)
        result = await queue.cancel(user_id, message_id=reply_to)
        await _safe_reply(message, result + _footer())
    else:
        await _safe_reply(message, "❓ Unknown command. Try /help." + _footer())


async def main_async() -> None:
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
    config = get_config()
    config.files_dir.mkdir(parents=True, exist_ok=True)
    config.output_dir.mkdir(parents=True, exist_ok=True)

    removed = stale_file_cleanup(config.output_dir, config.files_dir, config.stale_file_hours)
    if removed:
        LOGGER.info("Removed %s stale runtime files", removed)

    settings_store = SettingsStore(config.settings_path)
    downloader = TDLibDownloader(config)
    await downloader.start()

    queue = EncodeQueue(
        config.output_dir,
        downloader=downloader,
        max_jobs_per_user=config.max_jobs_per_user,
        max_concurrent_encoders=config.max_concurrent_encoders,
        journal_path=config.settings_path.parent / "queue_journal.json",
    )

    client = downloader._client
    if client is None:
        raise RuntimeError("TDLib client failed to start")

    client.ctx = BotContext(
        config=config, queue=queue, settings=settings_store, downloader=downloader
    )
    await register_handlers(client, config)

    LOGGER.info("Bot running on TDLib — waiting for updates")
    try:
        await asyncio.Event().wait()
    finally:
        await downloader.stop()


def main() -> None:
    asyncio.run(main_async())
