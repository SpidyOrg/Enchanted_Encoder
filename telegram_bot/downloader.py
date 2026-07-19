from __future__ import annotations

import asyncio
import logging
from pathlib import Path

from pytdbot import Client, types

from telegram_bot.config import BotConfig


LOGGER = logging.getLogger(__name__)


class TDLibDownloader:
    def __init__(self, config: BotConfig) -> None:
        self._config = config
        self._client: Client | None = None
        self._pending: dict[int, asyncio.Future[Path]] = {}
        self._progress: dict[int, callable] = {}

    async def start(self) -> None:
        kwargs: dict = {
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
        def handle_file(_: Client, update: types.UpdateFile) -> None:
            self._on_file_update(update.file)

        await self._client.start()
        LOGGER.info("TDLib downloader client started")

    async def stop(self) -> None:
        if self._client:
            await self._client.stop()

    async def download(
        self,
        chat_id: int,
        message_id: int,
        progress_callback: callable | None = None,
        cancel_event: asyncio.Event | None = None,
    ) -> Path:
        if self._client is None:
            raise RuntimeError("TDLib downloader not started")

        msg = await self._client.call_method("getMessage", {
            "chat_id": chat_id,
            "message_id": message_id,
        })
        file_id, file_size = self._extract_file_info(msg)
        if file_id is None:
            raise ValueError("No downloadable video file found in the message")

        file_size = file_size or 0

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

    async def _wait_completion(self, future: asyncio.Future[Path]) -> None:
        try:
            await future
        except (asyncio.CancelledError, Exception):
            pass

    def _on_file_update(self, file: types.File) -> None:
        file_id = file.id
        if file_id not in self._pending:
            return

        local = file.local
        downloaded = getattr(local, "downloaded_size", 0) or 0
        total = file.size or 0

        cb = self._progress.get(file_id)
        if cb:
            cb(downloaded, total)

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
        content = getattr(msg, "content", None) or {}
        video = getattr(content, "video", None)
        if video:
            f = getattr(video, "file", None)
            if f:
                return (f.id, f.size)
            return (video.file_id, None)

        document = getattr(content, "document", None)
        if document:
            mime = getattr(document, "mime_type", "") or ""
            if mime.startswith("video/"):
                f = getattr(document, "file", None)
                if f:
                    return (f.id, f.size)
                return (document.file_id, None)

        animation = getattr(content, "animation", None)
        if animation:
            f = getattr(animation, "file", None)
            if f:
                return (f.id, f.size)
            return (animation.file_id, None)

        return (None, None)
