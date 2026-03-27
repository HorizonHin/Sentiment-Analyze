from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, Optional

DEFAULT_DATETIME_FORMATS = (
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d %H:%M",
    "%Y-%m-%d",
)


def parse_int_timestamp(value: Any, *, allow_none: bool = True) -> Optional[int]:
    """Parse second-level int timestamp from request/input values."""
    if value is None or value == "":
        if allow_none:
            return None
        raise ValueError("timestamp must be int")

    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError("timestamp must be int") from exc


def datetime_to_timestamp(value: Optional[datetime]) -> Optional[int]:
    return datetime_to_int_timestamp(value)


def datetime_to_int_timestamp(value: Optional[datetime]) -> Optional[int]:
    """Convert datetime to second-level int timestamp in UTC."""
    if value is None:
        return None
    dt = value
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    else:
        dt = dt.astimezone(UTC)
    return int(dt.timestamp())


def timestamp_to_datetime(value: Any) -> Optional[datetime]:
    return int_timestamp_to_datetime(value)


def int_timestamp_to_datetime(value: Optional[int]) -> Optional[datetime]:
    """Convert second-level int timestamp to naive UTC datetime."""
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if not isinstance(value, int):
        return None
    return datetime.fromtimestamp(value, UTC).replace(tzinfo=None)


def parse_datetime_value(
    value: Any,
    formats: tuple[str, ...] = DEFAULT_DATETIME_FORMATS,
) -> Optional[datetime]:
    # Keep this API for external input parsing, but internal code should use
    # int_timestamp_to_datetime / datetime_to_int_timestamp directly.
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
