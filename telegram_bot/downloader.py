from __future__ import annotations

import asyncio
import logging
import time
from pathlib import Path
from typing import Any, Callable

from pytdbot import Client, types

from telegram_bot.config import BotConfig


LOGGER = logging.getLogger(__name__)

# How long to wait for TDLib to receive/resolve a message before giving up.
_MESSAGE_WAIT_TIMEOUT = 60.0
_POLL_INTERVAL = 0.5


class TDLibDownloader:
    """Download files using the bot's single TDLib (pytdbot) client.

    Because the same client both receives incoming messages and downloads
    files, message identifiers are always consistent — there is no cross-client
    bridge to fail.
    """

    def __init__(self, config: BotConfig) -> None:
        self._config = config
        self._client: Client | None = None
        self._pending: dict[int, asyncio.Future[Path]] = {}
        self._progress: dict[int, Callable[[int, int], Any]] = {}

        # (chat_id, message_id) -> (file_id, file_size)
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
            if message is not None:
                self._register_file(message)

        await self._client.start()
        LOGGER.info("TDLib downloader client started")

    async def stop(self) -> None:
        if self._client is not None:
            await self._client.stop()
            self._client = None

    def _register_file(self, message: types.Message) -> None:
        content = getattr(message, "content", None)
        if not isinstance(content, dict):
            return
        chat_id = getattr(message, "chat_id", 0) or 0
        message_id = getattr(message, "id", 0) or 0
        if not chat_id or not message_id:
            return

        file_id, file_size = self._extract_file_info(content)
        if file_id is not None:
            self._file_registry[(chat_id, message_id)] = (file_id, file_size or 0)
            _enforce_registry_limit(self._file_registry)
            self._registry_event.set()
            LOGGER.debug("Registered file: chat=%s msg=%s file_id=%s", chat_id, message_id, file_id)

    async def download(
        self,
        chat_id: int,
        message_id: int,
        progress_callback: Callable[[int, int], Any] | None = None,
        cancel_event: asyncio.Event | None = None,
    ) -> Path:
        if self._client is None:
            raise RuntimeError("TDLib downloader not started")

        file_id, _ = await self._resolve_file_id(chat_id, message_id)
        if file_id is None:
            raise ValueError("No downloadable video file found in the message")

        await self._client.downloadFile(
            file_id=file_id, priority=32, offset=0, limit=0, synchronous=False
        )

        loop = asyncio.get_event_loop()
        future: asyncio.Future[Path] = loop.create_future()
        self._pending[file_id] = future
        if progress_callback is not None:
            self._progress[file_id] = progress_callback

        wait_task = asyncio.ensure_future(self._wait_completion(future))
        try:
            if cancel_event is not None:
                cancel_task = asyncio.ensure_future(cancel_event.wait())
                try:
                    done, _ = await asyncio.wait(
                        {wait_task, cancel_task},
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                    if cancel_event.is_set():
                        self._cleanup_file_id(file_id)
                        if not future.done():
                            future.cancel()
                        raise asyncio.CancelledError("Download cancelled")
                finally:
                    cancel_task.cancel()
            else:
                await wait_task
        finally:
            wait_task.cancel()

        return future.result()

    def _cleanup_file_id(self, file_id: int) -> None:
        self._pending.pop(file_id, None)
        self._progress.pop(file_id, None)
        if self._client is not None:
            try:
                asyncio.ensure_future(self._client.cancelDownloadFile(file_id))
            except Exception:  # noqa: BLE001
                pass

    async def _resolve_file_id(
        self, chat_id: int, message_id: int
    ) -> tuple[int | None, int]:
        key = (chat_id, message_id)
        entry = self._file_registry.get(key)
        if entry is not None:
            return entry

        # The incoming-message update may not have been processed yet. Poll the
        # in-memory registry briefly, falling back to TDLib's own getMessage.
        for attempt in range(int(_MESSAGE_WAIT_TIMEOUT / _POLL_INTERVAL)):
            await asyncio.sleep(_POLL_INTERVAL)
            entry = self._file_registry.get(key)
            if entry is not None:
                return entry
            if attempt >= 4 and self._client is not None:
                try:
                    msg = await self._client.getMessage(chat_id=chat_id, message_id=message_id)
                    content = getattr(msg, "content", None)
                    if isinstance(content, dict):
                        fid, fsz = self._extract_file_info(content)
                        if fid is not None:
                            self._file_registry[key] = (fid, fsz or 0)
                            return (fid, fsz or 0)
                except Exception:  # noqa: BLE001
                    pass

        LOGGER.warning("Timed out waiting for TDLib message (%s, %s)", chat_id, message_id)
        return None, 0

    async def _wait_completion(self, future: asyncio.Future[Path]) -> None:
        try:
            await future
        except (asyncio.CancelledError, Exception):  # noqa: BLE001
            pass

    async def _on_file_update(self, file: types.File) -> None:
        file_id = getattr(file, "id", None)
        if file_id is None or file_id not in self._pending:
            return
        local = getattr(file, "local", None)
        downloaded = int(getattr(local, "downloaded_size", 0) or 0)
        total = int(getattr(file, "size", 0) or 0)

        cb = self._progress.get(file_id)
        if cb is not None:
            res = cb(downloaded, total)
            if asyncio.iscoroutine(res):
                await res

        if getattr(local, "is_downloading_completed", False):
            path = getattr(local, "path", "") or ""
            future = self._pending.pop(file_id, None)
            self._progress.pop(file_id, None)
            if future is not None and not future.done():
                if path:
                    future.set_result(Path(path))
                else:
                    future.set_exception(RuntimeError("Download completed but no path"))

    def _extract_file_info(self, content: dict) -> tuple[int | None, int | None]:
        video = content.get("video")
        if isinstance(video, dict):
            f = video.get("video") or video
            fid = f.get("id") if isinstance(f, dict) else None
            return fid, (f.get("size") if isinstance(f, dict) else None)

        document = content.get("document")
        if isinstance(document, dict):
            mime = document.get("mime_type", "") or ""
            if mime.startswith("video/"):
                f = document.get("document") or document
                fid = f.get("id") if isinstance(f, dict) else None
                return fid, (f.get("size") if isinstance(f, dict) else None)

        animation = content.get("animation")
        if isinstance(animation, dict):
            f = animation.get("animation") or animation
            fid = f.get("id") if isinstance(f, dict) else None
            return fid, (f.get("size") if isinstance(f, dict) else None)

        return None, None


def _enforce_registry_limit(
    registry: dict[tuple[int, int], tuple[int, int]], limit: int = 1000
) -> None:
    if len(registry) <= limit:
        return
    for key in list(registry.keys())[: max(1, len(registry) // 10)]:
        registry.pop(key, None)
