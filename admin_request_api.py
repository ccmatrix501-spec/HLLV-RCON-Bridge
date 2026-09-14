from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import sqlite3
import urllib.error
import urllib.request
from datetime import UTC, datetime, timedelta
from typing import Any

from leaderboard_api import app
from app import _call, _client, state
from stats_tracker_api import _connect_db, _init_db

try:
    from hllrcon.admin_logs import HLLVPlayerSendMessageAdminLog
except ImportError:  # pragma: no cover
    HLLVPlayerSendMessageAdminLog = ()  # type: ignore[assignment,misc]

logger = logging.getLogger("hllv-rcon-bridge.admin-requests")

ADMIN_REQUESTS_ENABLED = os.getenv("HLLV_ADMIN_REQUESTS_ENABLED", "true").strip().lower() not in {
    "0",
    "false",
    "no",
    "off",
}
BOT_BASE_URL = os.getenv("HLLV_ADMIN_ALERT_URL", "").strip().rstrip("/")
SHARED_SECRET = os.getenv("HLLV_ADMIN_ALERT_SECRET", "").strip()
POLL_SECONDS = max(1.0, float(os.getenv("HLLV_ADMIN_REQUEST_POLL_SECONDS", "2")))
LOG_WINDOW_SECONDS = max(10, int(os.getenv("HLLV_ADMIN_REQUEST_LOG_WINDOW_SECONDS", "30")))
COOLDOWN_SECONDS = max(10, int(os.getenv("HLLV_ADMIN_REQUEST_COOLDOWN_SECONDS", "60")))
HTTP_TIMEOUT_SECONDS = max(3, int(os.getenv("HLLV_ADMIN_ALERT_HTTP_TIMEOUT_SECONDS", "10")))


class Runtime:
    def __init__(self) -> None:
        self.task: asyncio.Task[Any] | None = None
        self.started_at = datetime.now(UTC)
        self.last_poll_at: datetime | None = None
        self.last_error: str | None = None
        self.cooldowns: dict[str, datetime] = {}


runtime = Runtime()


def _utcnow() -> datetime:
    return datetime.now(UTC)


def _iso(value: datetime | None = None) -> str:
    return (value or _utcnow()).isoformat().replace("+00:00", "Z")


def _entry_time(entry: Any) -> datetime | None:
    value = getattr(entry, "timestamp", None)
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)
    return None


def _event_key(entry: Any) -> str:
    timestamp = getattr(entry, "timestamp", None)
    raw = str(getattr(entry, "raw_message", "") or repr(entry))
    return hashlib.sha256(f"{timestamp!s}|{raw}".encode("utf-8", errors="replace")).hexdigest()


def _init_request_db() -> None:
    _init_db()
    with _connect_db() as db:
        db.executescript(
            """
            CREATE TABLE IF NOT EXISTS admin_request_events (
                event_key TEXT PRIMARY KEY,
                seen_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_admin_request_events_seen
            ON admin_request_events(seen_at);
            """
        )


def _claim_event(entry: Any) -> bool:
    with _connect_db() as db:
        try:
            db.execute(
                "INSERT INTO admin_request_events(event_key, seen_at) VALUES(?, ?)",
                (_event_key(entry), _iso()),
            )
            return True
        except sqlite3.IntegrityError:
            return False


def _is_player_chat(entry: Any) -> bool:
    try:
        if bool(HLLVPlayerSendMessageAdminLog) and isinstance(entry, HLLVPlayerSendMessageAdminLog):
            return True
    except TypeError:
        pass
    return "playersendmessageadminlog" in type(entry).__name__.lower()


def _side_from_entry(entry: Any) -> str:
    team = str(getattr(entry, "player_team_name", "") or "").strip().lower()
    if team == "allies":
        return "US"
    if team == "axis":
        return "NVA"
    return "UNKNOWN"


def _faction_from_player(player: Any) -> str | None:
    faction_id = getattr(player, "faction_id", None)
    try:
        value = int(faction_id)
    except (TypeError, ValueError):
        value = None
    if value == 1:
        return "US"
    if value == 6:
        return "NVA"
    if value == 8:
        return "UNASSIGNED"
    return None


def _role_name(player: Any) -> str:
    try:
        role = getattr(player, "role", None)
        if role is not None:
            for attr in ("display_name", "name", "label"):
                value = getattr(role, attr, None)
                if value:
                    return str(value).replace("_", " ").title()[:96]
    except Exception:
        pass
    value = getattr(player, "role_id", None)
    return str(value or "Unknown")[:96]


async def _player_context(player_id: str) -> dict[str, str]:
    context = {"side": "UNKNOWN", "unit": "UNASSIGNED", "role": "UNKNOWN"}
    try:
        response = await _call(_client().get_players())
        for player in list(getattr(response, "players", []) or []):
            if str(getattr(player, "id", "") or "").strip() != player_id:
                continue
            context["side"] = _faction_from_player(player) or context["side"]
            context["unit"] = str(getattr(player, "platoon", "") or "UNASSIGNED")[:64]
            context["role"] = _role_name(player)
            break
    except Exception as exc:
        logger.debug("Could not load player context for %s: %s", player_id, exc)
    return context


async def _session_context() -> dict[str, str]:
    context = {"map": "UNKNOWN", "game_mode": "UNKNOWN"}
    try:
        session = await _call(_client().get_server_session())
        context["map"] = str(getattr(session, "map_name", "") or getattr(session, "map_id", "") or "UNKNOWN")[:128]
        context["game_mode"] = str(getattr(session, "game_mode_id", "") or "UNKNOWN")[:96]
    except Exception as exc:
        logger.debug("Could not load session context: %s", exc)
    return context


def _post_to_bot_sync(payload: dict[str, Any]) -> dict[str, Any]:
    if not BOT_BASE_URL:
        raise RuntimeError("HLLV_ADMIN_ALERT_URL is not configured")
    if not SHARED_SECRET:
        raise RuntimeError("HLLV_ADMIN_ALERT_SECRET is not configured")

    body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    request = urllib.request.Request(
        f"{BOT_BASE_URL}/api/integrations/hllv/admin-request",
        data=body,
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json",
            "X-HLLV-Secret": SHARED_SECRET,
            "User-Agent": "1st-MI-HLLV-RCON-Bridge/1.0",
        },
        method="POST",
    )

    try:
        with urllib.request.urlopen(request, timeout=HTTP_TIMEOUT_SECONDS) as response:  # noqa: S310
            raw = response.read().decode("utf-8", errors="replace")
            return json.loads(raw) if raw else {"ok": True}
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Discord bot returned HTTP {exc.code}: {raw[:300]}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"Could not reach Discord bot: {exc.reason}") from exc


async def _post_to_bot(payload: dict[str, Any]) -> dict[str, Any]:
    return await asyncio.to_thread(_post_to_bot_sync, payload)


async def _send_private(player_id: str, message: str) -> None:
    await _call(_client().message_player(player_id, message[:500]))


async def _handle_admin_request(entry: Any, reason: str) -> None:
    player_id = str(getattr(entry, "player_id", "") or "").strip()
    player_name = str(getattr(entry, "player_name", "") or player_id or "Unknown player").strip()
    if not player_id:
        return

    now = _utcnow()
    previous = runtime.cooldowns.get(player_id)
    if previous:
        remaining = COOLDOWN_SECONDS - int((now - previous).total_seconds())
        if remaining > 0:
            await _send_private(
                player_id,
                "[ 1ST M.I. ADMIN ]\n\n"
                f"PLEASE WAIT {remaining} SECONDS BEFORE SENDING ANOTHER ADMIN REQUEST.",
            )
            return

    runtime.cooldowns[player_id] = now

    player_context = await _player_context(player_id)
    if player_context["side"] == "UNKNOWN":
        player_context["side"] = _side_from_entry(entry)
    session_context = await _session_context()

    request_id = _event_key(entry)[:16]
    payload = {
        "request_id": request_id,
        "reporter_name": player_name,
        "reporter_id": player_id,
        "reason": reason[:1000],
        "side": player_context["side"],
        "unit": player_context["unit"],
        "role": player_context["role"],
        "map": session_context["map"],
        "game_mode": session_context["game_mode"],
        "chat_channel": str(getattr(entry, "channel", "") or "UNKNOWN"),
        "timestamp": _iso(_entry_time(entry) or now),
    }

    try:
        result = await _post_to_bot(payload)
        await _send_private(
            player_id,
            "[ 1ST M.I. ADMIN ]\n\n"
            "YOUR ADMIN REQUEST HAS BEEN SENT TO STAFF.\n\n"
            "PLEASE CONTINUE PLAYING WHILE IT IS REVIEWED.",
        )
        logger.info(
            "Admin request %s sent for %s (%s): %s",
            request_id,
            player_name,
            player_id,
            result,
        )
    except Exception as exc:
        runtime.last_error = str(exc)
        logger.warning("Admin request delivery failed for %s (%s): %s", player_name, player_id, exc)
        try:
            await _send_private(
                player_id,
                "[ 1ST M.I. ADMIN ]\n\n"
                "THE ADMIN ALERT SYSTEM IS TEMPORARILY UNAVAILABLE.\n"
                "PLEASE TRY AGAIN SHORTLY.",
            )
        except Exception:
            pass


async def _process_logs() -> None:
    response = await _call(_client().get_admin_log(seconds_span=LOG_WINDOW_SECONDS))
    entries = list(getattr(response, "entries", []) or [])
    entries.sort(key=lambda entry: _entry_time(entry) or datetime.min.replace(tzinfo=UTC))

    for entry in entries:
        if not _is_player_chat(entry):
            continue

        raw_message = str(getattr(entry, "message", "") or "").strip()
        lower = raw_message.lower()
        if lower != "!admin" and not lower.startswith("!admin "):
            continue

        entry_time = _entry_time(entry)
        if entry_time and entry_time < runtime.started_at - timedelta(seconds=2):
            _claim_event(entry)
            continue

        if not _claim_event(entry):
            continue

        reason = raw_message[6:].strip()
        player_id = str(getattr(entry, "player_id", "") or "").strip()
        if not reason:
            if player_id:
                await _send_private(
                    player_id,
                    "[ 1ST M.I. ADMIN ]\n\n"
                    "USAGE: !admin <REASON>\n\n"
                    "EXAMPLE: !admin PLAYER123 IS INTENTIONALLY TEAMKILLING AT HQ",
                )
            continue

        await _handle_admin_request(entry, reason)

    runtime.last_poll_at = _utcnow()


def _cleanup_seen() -> None:
    cutoff = _iso(_utcnow() - timedelta(days=2))
    with _connect_db() as db:
        db.execute("DELETE FROM admin_request_events WHERE seen_at < ?", (cutoff,))


async def _worker() -> None:
    logger.info(
        "HLL:V admin request worker started: poll=%ss cooldown=%ss bot_url=%s",
        POLL_SECONDS,
        COOLDOWN_SECONDS,
        BOT_BASE_URL or "NOT CONFIGURED",
    )
    cleanup_counter = 0
    while True:
        try:
            if state.client is not None and state.client.is_connected():
                await _process_logs()
                runtime.last_error = None
                cleanup_counter += 1
                if cleanup_counter >= max(1, int(3600 / POLL_SECONDS)):
                    _cleanup_seen()
                    cleanup_counter = 0
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            runtime.last_error = str(exc)
            logger.warning("Admin request worker failed: %s", exc)
        await asyncio.sleep(POLL_SECONDS)


@app.on_event("startup")
async def start_admin_request_worker() -> None:
    if not ADMIN_REQUESTS_ENABLED:
        logger.info("HLL:V admin requests disabled by HLLV_ADMIN_REQUESTS_ENABLED")
        return
    _init_request_db()
    runtime.task = asyncio.create_task(_worker(), name="hllv-admin-request-worker")


@app.on_event("shutdown")
async def stop_admin_request_worker() -> None:
    if runtime.task:
        runtime.task.cancel()
        try:
            await runtime.task
        except asyncio.CancelledError:
            pass
        runtime.task = None


@app.get("/api/v2/admin-requests/status")
async def admin_request_status() -> dict[str, Any]:
    return {
        "enabled": ADMIN_REQUESTS_ENABLED,
        "command": "!admin <reason>",
        "bot_url_configured": bool(BOT_BASE_URL),
        "secret_configured": bool(SHARED_SECRET),
        "poll_seconds": POLL_SECONDS,
        "cooldown_seconds": COOLDOWN_SECONDS,
        "last_poll_at": runtime.last_poll_at.isoformat() if runtime.last_poll_at else None,
        "last_error": runtime.last_error,
    }
