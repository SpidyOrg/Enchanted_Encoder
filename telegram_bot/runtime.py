from __future__ import annotations

import os
import time
from pathlib import Path

from telegram_bot.formatting import (
    STARTED_AT,
    format_duration,
    human_size,
    status_footer,
)


def disk_usage(path: Path) -> tuple[int, int]:
    try:
        usage = os.statvfs(path)
    except OSError:
        return 0, 0
    block_size = usage.f_frsize
    total = usage.f_blocks * block_size
    free = usage.f_bavail * block_size
    return free, total


def has_free_space(path: Path, required_bytes: int) -> bool:
    free, _ = disk_usage(path)
    return free >= max(required_bytes, 0)

def stale_file_cleanup(output_dir: Path, files_dir: Path, max_age_hours: int) -> int:
    """Delete only generated outputs and unmistakably incomplete TDLib artifacts."""
    cutoff = time.time() - max_age_hours * 3600
    removed = 0
    targets: list[Path] = []
    if output_dir.exists():
        targets.extend(path for path in output_dir.rglob("*") if path.is_file())
    if files_dir.exists():
        incomplete_suffixes = {".part", ".partial", ".tmp"}
        targets.extend(
            path
            for path in files_dir.rglob("*")
            if path.is_file() and path.suffix.lower() in incomplete_suffixes
        )

    for path in targets:
        try:
            if path.stat().st_mtime < cutoff:
                path.unlink()
                removed += 1
        except OSError:
            continue
    return removed
