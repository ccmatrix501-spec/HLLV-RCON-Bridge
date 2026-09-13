from __future__ import annotations

from datetime import datetime
from typing import Any

from fastapi import Query
from fastapi.encoders import jsonable_encoder

from access_management_api import app
from app import _call, _client


# The hllrcon response model declares admin-log entries using the common base type.
# Pydantic therefore serializes only the common timestamp field when the whole
# response is encoded. Replace the original endpoint and serialize each concrete
# runtime log entry ourselves so HLL:V-specific fields and the raw message survive.
app.router.routes = [
    route
    for route in app.router.routes
    if not (
        getattr(route, "path", None) == "/api/v2/logs"
        and "GET" in (getattr(route, "methods", set()) or set())
    )
]


def _event_type(entry: Any) -> str:
    name = type(entry).__name__.lower()
    checks = (
        ("teamkill", "TEAM KILL"),
        ("playerkill", "KILL"),
        ("disconnect", "DISCONNECT"),
        ("connect", "CONNECT"),
        ("sendmessage", "CHAT"),
        ("receivemessage", "MESSAGE"),
        ("teamswitch", "TEAM SWITCH"),
        ("enteradmincamera", "ADMIN CAMERA"),
        ("leaveadmincamera", "ADMIN CAMERA"),
        ("playerkick", "KICK"),
        ("playerban", "BAN"),
        ("matchstart", "MATCH START"),
        ("matchend", "MATCH END"),
        ("votekickstart", "VOTE KICK"),
        ("votekickvote", "VOTE KICK"),
        ("votekickcomplete", "VOTE KICK"),
        ("votekickexpire", "VOTE KICK"),
        ("votekickpass", "VOTE KICK"),
    )
    for needle, label in checks:
        if needle in name:
            return label
    if "unrecognized" in name:
        return "OTHER"
    return type(entry).__name__.replace("HLLV", "").replace("AdminLog", "").upper() or "OTHER"


def _serialize_entry(entry: Any) -> dict[str, Any]:
    # Calling model_dump() on the concrete entry preserves subclass-specific fields
    # such as player_name, victim_name, weapon_id, channel, reason, chat message, etc.
    if hasattr(entry, "model_dump"):
        try:
            data = entry.model_dump(mode="json", exclude_none=True)
        except TypeError:
            data = entry.model_dump(exclude_none=True)
    else:
        data = dict(getattr(entry, "__dict__", {}) or {})

    data = jsonable_encoder(data)
    if not isinstance(data, dict):
        data = {"data": data}

    timestamp = getattr(entry, "timestamp", data.get("timestamp"))
    if isinstance(timestamp, datetime):
        timestamp = timestamp.isoformat().replace("+00:00", "Z")
    if timestamp is not None:
        data["timestamp"] = timestamp

    # raw_message is deliberately marked exclude=True by hllrcon. Keep it in a
    # separate field so parsed CHAT/MESSAGE events can retain their own `message`.
    raw = str(getattr(entry, "raw_message", "") or "").strip()
    data["type"] = _event_type(entry)
    data["raw_message"] = raw
    data["log_class"] = type(entry).__name__
    return data


@app.get("/api/v2/logs")
async def admin_logs(
    seconds: int = Query(default=3600, ge=0, le=604800),
    filter: str | None = Query(default=None),
) -> dict[str, Any]:
    response = await _call(_client().get_admin_log(seconds_span=seconds, filter_=filter or None))
    entries = list(getattr(response, "entries", []) or [])
    return {
        "entries": [_serialize_entry(entry) for entry in entries],
        "count": len(entries),
        "seconds": seconds,
    }
