from __future__ import annotations

import os
import time
from pathlib import Path


STARTED_AT = time.time()


def human_size(size: float) -> str:
    units = ("B", "KB", "MB", "GB", "TB")
    value = float(size)
    for unit in units:
        if abs(value) < 1024 or unit == units[-1]:
            return f"{value:.1f} {unit}" if unit != "B" else f"{value:.0f} {unit}"
        value /= 1024
    return f"{value:.1f} TB"


def format_duration(seconds: float) -> str:
    total = max(0, int(seconds))
    hours, remainder = divmod(total, 3600)
    minutes, secs = divmod(remainder, 60)
    if hours:
        return f"{hours}h {minutes}m {secs}s"
    if minutes:
        return f"{minutes}m {secs}s"
    return f"{secs}s"


def _memory_usage() -> tuple[int, int]:
    values: dict[str, int] = {}
    try:
        with open("/proc/meminfo", "r", encoding="utf-8") as meminfo:
            for line in meminfo:
                key, value = line.split(":", 1)
                amount_kb = int(value.strip().split()[0])
                values[key] = amount_kb * 1024
    except (OSError, ValueError, IndexError):
        return 0, 0

    total = values.get("MemTotal", 0)
    available = values.get("MemAvailable", 0)
    return max(total - available, 0), total


def _disk_usage(path: Path) -> tuple[int, int]:
    try:
        usage = os.statvfs(path)
    except OSError:
        return 0, 0
    block_size = usage.f_frsize
    total = usage.f_blocks * block_size
    free = usage.f_bavail * block_size
    return free, total


def _network_totals() -> tuple[int, int]:
    rx_total = 0
    tx_total = 0
    try:
        with open("/proc/net/dev", "r", encoding="utf-8") as netdev:
            for line in netdev.readlines()[2:]:
                _, values = line.split(":", 1)
                parts = values.split()
                rx_total += int(parts[0])
                tx_total += int(parts[8])
    except (OSError, ValueError, IndexError):
        return 0, 0
    return rx_total, tx_total


def status_footer() -> str:
    cores = os.cpu_count() or 1
    try:
        load_1m = os.getloadavg()[0]
    except OSError:
        load_1m = 0.0

    cpu_percent = min((load_1m / cores) * 100, 999.0)
    used_ram, total_ram = _memory_usage()
    ram_percent = (used_ram / total_ram) * 100 if total_ram else 0.0
    disk_free, disk_total = _disk_usage(Path("."))
    net_rx, net_tx = _network_totals()

    return (
        f"\n\n🖥 CPU: {cpu_percent:.0f}% ({load_1m:.2f}/{cores})"
        f"\n🧠 RAM: {human_size(used_ram)} / {human_size(total_ram)} ({ram_percent:.0f}%)"
        f"\n💾 Free: {human_size(disk_free)} / {human_size(disk_total)}"
        f"\n🌐 Net: ↓ {human_size(net_rx)} ↑ {human_size(net_tx)}"
        f"\n⏱ Uptime: {format_duration(time.time() - STARTED_AT)}"
    )
