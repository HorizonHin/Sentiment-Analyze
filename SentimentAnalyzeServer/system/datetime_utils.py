from __future__ import annotations

from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import Any, Optional

DEFAULT_DATETIME_FORMATS = (
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d %H:%M",
    "%Y-%m-%d",
)


def datetime_to_timestamp(value: Optional[datetime]) -> Optional[int]:
    if value is None:
        return None
    dt = value
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    else:
        dt = dt.astimezone(UTC)
    return int(dt.timestamp())


def timestamp_to_datetime(value: Any) -> Optional[datetime]:
    if value is None or value == "":
        return None

    try:
        ts = float(value)
    except (TypeError, ValueError):
        return None

    # Heuristic: treat values >= 1e12 as milliseconds.
    if abs(ts) >= 1_000_000_000_000:
        ts = ts / 1000.0

    return datetime.fromtimestamp(ts, UTC).replace(tzinfo=None)


def parse_datetime_value(
    value: Any,
    formats: tuple[str, ...] = DEFAULT_DATETIME_FORMATS,
) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value

    if isinstance(value, (int, float)):
        return timestamp_to_datetime(value)

    text = str(value).strip()
    if not text:
        return None

    if text.isdigit() or (text.startswith("-") and text[1:].isdigit()):
        ts_dt = timestamp_to_datetime(text)
        if ts_dt is not None:
            return ts_dt

    try:
        rfc_dt = parsedate_to_datetime(text)
        if rfc_dt is not None:
            if rfc_dt.tzinfo is not None:
                return rfc_dt.astimezone(UTC).replace(tzinfo=None)
            return rfc_dt
    except (TypeError, ValueError, IndexError):
        pass

    for fmt in formats:
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue

    try:
        iso_text = text[:-1] + "+00:00" if text.endswith("Z") else text
        iso_dt = datetime.fromisoformat(iso_text)
        if iso_dt.tzinfo is not None:
            return iso_dt.astimezone(UTC).replace(tzinfo=None)
        return iso_dt
    except ValueError:
        return None


def format_datetime_value(value: Optional[datetime], fmt: str = "%Y-%m-%d %H:%M:%S") -> str:
    return value.strftime(fmt) if value else ""


def format_timestamp_value(value: Optional[datetime]) -> Optional[int]:
    return datetime_to_timestamp(value)
