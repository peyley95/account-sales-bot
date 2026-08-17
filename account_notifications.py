"""Pure account-expiry classification shared by the background monitor tests."""

from __future__ import annotations

from datetime import datetime, timezone


WARNING_REMAINING_BYTES = 1024 ** 3
WARNING_REMAINING_SECONDS = 86_400


def _code(value, allowed: set[int]):
    if value is None or isinstance(value, bool):
        return None
    try:
        result = int(str(value).strip())
    except (TypeError, ValueError):
        return None
    return result if result in allowed else None


def _openvpn_state_code(info: dict):
    code = _code(info.get("um_profile_state"), {0, 1, 2, 3})
    if code is not None:
        return code
    normalized = str(info.get("profile_state") or "").strip().lower()
    normalized = normalized.replace("_", "-").replace(" ", "-")
    return {
        "waiting": 0,
        "running": 1,
        "running-active": 2,
        "used": 3,
    }.get(normalized)


def classify_openvpn_status(
    info: dict, *, quota_bytes: int = 0, now: datetime | None = None
) -> dict | None:
    """Return warning/expired only when User Manager supplied enough evidence."""
    if not isinstance(info, dict) or not info.get("found"):
        return None
    state_code = _openvpn_state_code(info)
    starts_at_code = _code(info.get("um_profile_starts_at"), {0, 1})

    # A First-Use profile deliberately has no deadline before its first login.
    # Its zero counters are not low-volume usage and must not trigger a warning.
    if state_code == 0 and starts_at_code != 1:
        return None

    current = now or datetime.now(timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)

    expiry = info.get("expiry")
    seconds_left = None
    if isinstance(expiry, datetime):
        if expiry.tzinfo is None:
            expiry = expiry.replace(tzinfo=timezone.utc)
        seconds_left = (expiry - current).total_seconds()

    remaining_bytes = None
    if int(quota_bytes or 0) > 0 and bool(info.get("usage_available", False)):
        used = int(info.get("total_download") or 0) + int(info.get("total_upload") or 0)
        remaining_bytes = max(int(quota_bytes) - used, 0)

    terminal_state = state_code in {1, 3}
    volume_expired = remaining_bytes is not None and remaining_bytes <= 0
    time_expired = seconds_left is not None and seconds_left <= 0
    if terminal_state or (state_code != 0 and (volume_expired or time_expired)):
        return {"kind": "expired", "low_volume": volume_expired, "low_time": time_expired}

    low_volume = remaining_bytes is not None and 0 < remaining_bytes <= WARNING_REMAINING_BYTES
    low_time = seconds_left is not None and 0 < seconds_left <= WARNING_REMAINING_SECONDS
    if low_volume or low_time:
        return {"kind": "warning", "low_volume": low_volume, "low_time": low_time}
    return None


def classify_v2ray_status(status: dict) -> dict | None:
    """Classify native 3x-ui quota/deadline state without mutating the panel."""
    if not isinstance(status, dict):
        return None
    if bool(status.get("waiting_first_use", False)):
        return None

    enabled = bool(status.get("enabled", True))
    total = max(int(status.get("total_bytes") or 0), 0)
    remaining = max(int(status.get("remaining_bytes") or 0), 0)
    expiry_ms = int(status.get("expiry_ms") or 0)
    remaining_days = max(float(status.get("remaining_days_float") or 0), 0.0)

    volume_expired = total > 0 and remaining <= 0
    time_expired = expiry_ms > 0 and remaining_days <= 0
    if not enabled or volume_expired or time_expired:
        return {"kind": "expired", "low_volume": volume_expired, "low_time": time_expired}

    low_volume = total > 0 and 0 < remaining <= WARNING_REMAINING_BYTES
    low_time = expiry_ms > 0 and 0 < remaining_days <= 1.0
    if low_volume or low_time:
        return {"kind": "warning", "low_volume": low_volume, "low_time": low_time}
    return None
