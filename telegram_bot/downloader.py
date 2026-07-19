from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from typing import Any, Callable

from pytdbot import Client, types

from telegram_bot.config import BotConfig


LOGGER = logging.getLogger(__name__)

# How long to wait for TDLib to receive a message before giving up
_MESSAGE_WAIT_TIMEOUT = 30.0


class TDLibDownloader:
    def __init__(self, config: BotConfig) -> None:
        self._config = config
        self._client: Client | None = None
        self._pending: dict[int, asyncio.Future[Path]] = {}
        self._progress: dict[int, Callable[[int, int], Any]] = {}

        # Track file IDs from incoming messages: {(chat_id, message_id): file_id}
        self._file_registry: dict[tuple[int, int], tuple[int, int]] = {}
        self._registry_event: asyncio.Event = asyncio.Event()

    async def start(self) -> None:
        kwargs: dict[str, Any] = {
            "token": self._config.bot_token,
            "api_id": self._config.api_id,
            "api_hash": self._config.api_hash,
            "files_directory": str(self._config.files_dir),
            "database_encryption_key": self._config.database_encryption_key,
            "td_verbosity": self._config.td_verbosity,
        }
        if self._config.tdjson_lib_path:
            kwargs["lib_path"] = self._config.tdjson_lib_path
        if self._config.td_log:
            kwargs["td_log"] = types.LogStreamFile(str(self._config.td_log), 104_857_600)

        self._client = Client(**kwargs)

        @self._client.on_updateFile()
        async def handle_file_update(_: Client, update: types.UpdateFile) -> None:
            await self._on_file_update(update.file)

        @self._client.on_updateNewMessage()
        async def handle_new_message(_: Client, update: types.UpdateNewMessage) -> None:
            message = getattr(update, "message", None)
            if message is None:
                return
            cid = getattr(message, "chat_id", 0)
            mid = getattr(message, "id", 0)
            content = getattr(message, "content", None)
            ctype = type(content).__name__ if content else "None"
            LOGGER.debug("TDLib updateNewMessage: chat=%s msg=%s content=%s", cid, mid, ctype)
            self._register_file(message)

        await self._client.start()
        LOGGER.info("TDLib downloader client started")

    async def stop(self) -> None:
        if self._client:
            await self._client.stop()

    def _register_file(self, message: types.Message) -> None:
        content = getattr(message, "content", None)
        cid = getattr(message, "chat_id", 0)
        mid = getattr(message, "id", 0)
        content_type = type(content).__name__ if content else "None"
        LOGGER.debug("_register_file called: chat=%s msg=%s content=%s has_video=%s has_doc=%s",
                      cid, mid, content_type,
                      hasattr(content, "video") if content else False,
                      hasattr(content, "document") if content else False)
        if content is None:
            # Try to use message directly
            file_id, file_size = self._extract_file_info(message)
            if file_id is not None:
                self._file_registry[(cid, mid)] = (file_id, file_size or 0)
                self._registry_event.set()
                LOGGER.debug("Registered file from message directly: chat=%s msg=%s file_id=%s", cid, mid, file_id)
            return
        chat_id = cid
        msg_id = mid
        if not chat_id or not msg_id:
            return

        file_id, file_size = self._extract_file_info(message)
        if file_id is not None:
            self._file_registry[(chat_id, msg_id)] = (file_id, file_size or 0)
            self._registry_event.set()
            LOGGER.debug("Registered file: chat=%s msg=%s file_id=%s", chat_id, msg_id, file_id)

    async def download(
        self,
        chat_id: int,
        message_id: int,
        progress_callback: Callable[[int, int], Any] | None = None,
        cancel_event: asyncio.Event | None = None,
    ) -> Path:
        if self._client is None:
            raise RuntimeError("TDLib downloader not started")

        file_id, file_size = await self._resolve_file_id(chat_id, message_id)
        if file_id is None:
            raise ValueError("No downloadable video file found in the message")

        await self._client.downloadFile(
            file_id=file_id, priority=32, offset=0, limit=0, synchronous=False,
        )

        loop = asyncio.get_event_loop()
        future: asyncio.Future[Path] = loop.create_future()
        self._pending[file_id] = future
        if progress_callback:
            self._progress[file_id] = progress_callback

        wait_task = asyncio.ensure_future(self._wait_completion(future))

        if cancel_event:
            cancel_task = asyncio.ensure_future(cancel_event.wait())
            done, _ = await asyncio.wait(
                [wait_task, cancel_task],
                return_when=asyncio.FIRST_COMPLETED,
            )
            cancel_task.cancel()
            if cancel_event.is_set():
                self._pending.pop(file_id, None)
                self._progress.pop(file_id, None)
                if not future.done():
                    future.cancel()
                raise asyncio.CancelledError("Download cancelled")
        else:
            await wait_task

        return future.result()

    async def _resolve_file_id(self, chat_id: int, message_id: int) -> tuple[int | None, int]:
        key = (chat_id, message_id)
        entry = self._file_registry.get(key)
        if entry is not None:
            return entry

        # Poll for the message to arrive in TDLib (check every 0.5s)
        for _ in range(int(_MESSAGE_WAIT_TIMEOUT / 0.5)):
            await asyncio.sleep(0.5)
            entry = self._file_registry.get(key)
            if entry is not None:
                return entry
            # Also try getMessage as fallback (only after a few seconds)
            if _ >= 4:  # After ~2 seconds, try getMessage
                try:
                    msg = await self._client.call_method(
                        "getMessage", chat_id=chat_id, message_id=message_id,
                    )
                    if not getattr(msg, "is_error", False):
                        fid, fsz = self._extract_file_info(msg)
                        if fid is not None:
                            self._file_registry[key] = (fid, fsz or 0)
                            return (fid, fsz or 0)
                except Exception:
                    pass

        LOGGER.warning("Timed out waiting for TDLib message (%s, %s)", chat_id, message_id)
        return None, 0

    async def _wait_completion(self, future: asyncio.Future[Path]) -> None:
        try:
            await future
        except (asyncio.CancelledError, Exception):
            pass

    async def _on_file_update(self, file: types.File) -> None:
        file_id = file.id
        if file_id not in self._pending:
            return

        local = file.local
        downloaded = getattr(local, "downloaded_size", 0) or 0
        total = file.size or 0

        cb = self._progress.get(file_id)
        if cb:
            res = cb(downloaded, total)
            if asyncio.iscoroutine(res):
                await res

        if getattr(local, "is_downloading_completed", False):
            path = getattr(local, "path", "")
            future = self._pending.pop(file_id, None)
            self._progress.pop(file_id, None)
            if future and not future.done():
                if path:
                    future.set_result(Path(path))
                else:
                    future.set_exception(RuntimeError("Download completed but no path"))

    def _extract_file_info(self, msg: object) -> tuple[int | None, int | None]:
        content = getattr(msg, "content", None) or msg
        video = getattr(content, "video", None)
        if video:
            f = getattr(video, "file", None)
            if f:
                return (f.id, f.size)
            return (getattr(video, "file_id", None), None)

        document = getattr(content, "document", None)
        if document:
            mime = getattr(document, "mime_type", "") or ""
            if mime.startswith("video/"):
                f = getattr(document, "file", None)
                if f:
                    return (f.id, f.size)
                return (getattr(document, "file_id", None), None)

        animation = getattr(content, "animation", None)
        if animation:
            f = getattr(animation, "file", None)
            if f:
                return (f.id, f.size)
            return (getattr(animation, "file_id", None), None)

        if hasattr(msg, "video"):
            video = getattr(msg, "video", None)
            if video:
                return (getattr(video, "file_id", None), getattr(video, "file_size", None))

        if hasattr(msg, "document"):
            doc = getattr(msg, "document", None)
            if doc:
                return (getattr(doc, "file_id", None), getattr(doc, "file_size", None))

        return (None, None)
