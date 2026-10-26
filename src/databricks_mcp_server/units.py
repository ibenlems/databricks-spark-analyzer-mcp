from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any, Sequence

KIB = 1024
MIB = 1024**2
GIB = 1024**3


def percentile(values: Sequence[float], pct: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    if len(ordered) == 1:
        return float(ordered[0])
    rank = (len(ordered) - 1) * pct / 100.0
    low = int(rank)
    high = min(low + 1, len(ordered) - 1)
    return float(ordered[low] + (ordered[high] - ordered[low]) * (rank - low))


def fmt_bytes(value: float | int | None) -> str:
    if value is None:
        return "n/a"
    size = float(value)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if abs(size) < 1024 or unit == "TiB":
            return f"{size:.0f} {unit}" if unit == "B" else f"{size:.2f} {unit}"
        size /= 1024
    return f"{size:.2f} TiB"


def fmt_ms(value: float | int | None) -> str:
    if value is None:
        return "n/a"
    ms = float(value)
    if ms < 1000:
        return f"{ms:.0f} ms"
    seconds = ms / 1000
    if seconds < 120:
        return f"{seconds:.1f} s"
    minutes = seconds / 60
    if minutes < 120:
        return f"{minutes:.1f} min"
    return f"{minutes / 60:.1f} h"


def fmt_ratio(value: float | None) -> str:
    return "n/a" if value is None else f"{value * 100:.1f}%"


_ISO = re.compile(r"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?:\.(\d+))?")


def parse_spark_time(value: Any) -> int | None:
    """Convert Spark UI REST timestamps (e.g. 2024-05-01T10:00:00.123GMT) or epoch values to epoch ms."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return int(value)
    match = _ISO.match(str(value))
    if not match:
        return None
    base = datetime.strptime(match.group(1), "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
    millis = int((match.group(2) or "0").ljust(3, "0")[:3])
    return int(base.timestamp() * 1000) + millis
