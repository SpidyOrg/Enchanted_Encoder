from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class BotConfig:
    api_id: int
    api_hash: str
    bot_token: str
    files_dir: Path
    database_encryption_key: str
    td_verbosity: int
    td_log: Path | None
    tdjson_lib_path: str | None
    settings_path: Path
    output_dir: Path
    max_jobs_per_user: int
    max_concurrent_encoders: int
    stale_file_hours: int


def load_dotenv(path: Path = Path(".env")) -> None:
    if not path.exists():
        return

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        os.environ.setdefault(key, value)


def _required(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def _required_any(*names: str) -> str:
    value = _first_value(*names)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {' or '.join(names)}")
    return value


def _first_value(*names: str, default: str = "") -> str:
    for name in names:
        value = os.environ.get(name, "").strip()
        if value:
            return value
    return default


def get_config() -> BotConfig:
    load_dotenv()

    try:
        api_id = int(_required_any("TELEGRAM_API_ID", "TELEGRAM_API"))
    except ValueError as exc:
        raise RuntimeError("TELEGRAM_API_ID/TELEGRAM_API must be an integer") from exc

    td_log = os.environ.get("PYTDBOT_TD_LOG", "tdlib.log").strip()

    return BotConfig(
        api_id=api_id,
        api_hash=_required_any("TELEGRAM_API_HASH", "TELEGRAM_HASH"),
        bot_token=_required_any("TELEGRAM_BOT_TOKEN", "BOT_TOKEN"),
        files_dir=Path(os.environ.get("PYTDBOT_FILES_DIR", ".tdlib")),
        database_encryption_key=os.environ.get("PYTDBOT_DB_KEY", "pytdbot-local-db-key"),
        td_verbosity=int(os.environ.get("PYTDBOT_TD_VERBOSITY", "2")),
        td_log=Path(td_log) if td_log else None,
        tdjson_lib_path=os.environ.get("PYTDBOT_TDJSON_LIB") or None,
        settings_path=Path(os.environ.get("BOT_SETTINGS_PATH", "bot_settings.json")),
        output_dir=Path(os.environ.get("BOT_OUTPUT_DIR", "encoded")),
        max_jobs_per_user=max(1, int(os.environ.get("MAX_JOBS_PER_USER", "3"))),
        max_concurrent_encoders=min(3, max(1, int(os.environ.get("MAX_CONCURRENT_ENCODERS", "2")))),
        stale_file_hours=max(1, int(os.environ.get("STALE_FILE_HOURS", "24"))),
    )
