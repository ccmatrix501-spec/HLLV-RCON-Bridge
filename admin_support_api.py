from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
import sqlite3
import urllib.error
import urllib.request
from datetime import UTC, datetime, timedelta
from typing import Any

import admin_request_api as admin_api
from connection_keeper import app
from stats_tracker_api import _connect_db

logger = logging.getLogger("hllv-rcon-bridge.admin-support")

REPLY_POLL_SECONDS = max(1.0, float(__import__("os").getenv("HLLV_ADMIN_REPLY_POLL_SECONDS", "2")))
REPLY_LOG_WINDOW_SECONDS = max(10, int(__import__("os").getenv("HLLV_ADMIN_REPLY_LOG_WINDOW_SECONDS", "30")))
REPLY_COOLDOWN_SECONDS = max(3, int(__import__("os").getenv("HLLV_ADMIN_REPLY_COOLDOWN_SECONDS", "5")))


class SupportRuntime:
    def __init__(self) -> None:
        self.task: asyncio.Task[Any] | None = None
        self.started_at = datetime.now(UTC)
        self.last_poll_at: datetime | None = None
        self.last_error: str | None = None
        self.cooldowns: dict[str, datetime] = {}


runtime = SupportRuntime()


def _utcnow() -> datetime:
    return datetime.now(UTC)


def _iso(value: datetime | None = None) -> str:
    return (value or _utcnow()).isoformat().replace("+00:00", "Z")


def _init_support_db() -> None:
    admin_api._init_request_db()
    with _connect_db() as db:
        db.executescript(
            """
            CREATE TABLE IF NOT EXISTS admin_support_requests (
                request_id TEXT PRIMARY KEY,
                reporter_id TEXT NOT NULL,
                reporter_name TEXT NOT NULL,
                reason TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'open',
                admin_name TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_admin_support_reporter_status
            ON admin_support_requests(reporter_id, status, updated_at);

            CREATE TABLE IF NOT EXISTS admin_support_reply_events (
                event_key TEXT PRIMARY KEY,
                seen_at TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_admin_support_reply_seen
            ON admin_support_reply_events(seen_at);
            """
        )


def _event_key(entry: Any) -> str:
    timestamp = getattr(entry, "timestamp", None)
    raw = str(getattr(entry, "raw_message", "") or repr(entry))
    return hashlib.sha256(f"{timestamp!s}|{raw}".encode("utf-8", errors="replace")).hexdigest()


def _claim_reply_event(entry: Any) -> bool:
    with _connect_db() as db:
        try:
            db.execute(
                "INSERT INTO admin_support_reply_events(event_key, seen_at) VALUES(?, ?)",
                (_event_key(entry), _iso()),
            )
            return True
        except sqlite3.IntegrityError:
            return False


def _active_request(player_id: str) -> dict[str, Any] | None:
    with _connect_db() as db:
        row = db.execute(
            """
            SELECT request_id, reporter_id, reporter_name, reason, status, admin_name, created_at, updated_at
            FROM admin_support_requests
            WHERE reporter_id = ? AND status != 'resolved'
            ORDER BY updated_at DESC
            LIMIT 1
            """,
            (player_id,),
        ).fetchone()
    return dict(row) if row else None


def _request_by_id(request_id: str) -> dict[str, Any] | None:
    with _connect_db() as db:
        row = db.execute(
            """
            SELECT request_id, reporter_id, reporter_name, reason, status, admin_name, created_at, updated_at
            FROM admin_support_requests
            WHERE request_id = ?
            """,
            (request_id,),
        ).fetchone()
    return dict(row) if row else None


def _save_request(request_id: str, player_id: str, player_name: str, reason: str) -> None:
    now = _iso()
    with _connect_db() as db:
        db.execute(
            """
            INSERT INTO admin_support_requests(
                request_id, reporter_id, reporter_name, reason, status, admin_name, created_at, updated_at
            ) VALUES(?, ?, ?, ?, 'open', NULL, ?, ?)
            ON CONFLICT(request_id) DO UPDATE SET
                reporter_id = excluded.reporter_id,
                reporter_name = excluded.reporter_name,
                reason = excluded.reason,
                updated_at = excluded.updated_at
            """,
            (request_id, player_id, player_name, reason[:1000], now, now),
        )


def _update_request(request_id: str, status: str, admin_name: str | None = None) -> None:
    with _connect_db() as db:
        if admin_name:
            db.execute(
                "UPDATE admin_support_requests SET status = ?, admin_name = ?, updated_at = ? WHERE request_id = ?",
                (status, admin_name[:128], _iso(), request_id),
            )
        else:
            db.execute(
                "UPDATE admin_support_requests SET status = ?, updated_at = ? WHERE request_id = ?",
                (status, _iso(), request_id),
            )


def _post_bot_path_sync(path: str, payload: dict[str, Any]) -> dict[str, Any]:
    if not admin_api.BOT_BASE_URL:
        raise RuntimeError("HLLV_ADMIN_ALERT_URL is not configured")
    if not admin_api.SHARED_SECRET:
        raise RuntimeError("HLLV_ADMIN_ALERT_SECRET is not configured")

    body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    request = urllib.request.Request(
        f"{admin_api.BOT_BASE_URL}{path}",
        data=body,
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json",
            "X-HLLV-Secret": admin_api.SHARED_SECRET,
            "User-Agent": "1st-MI-HLLV-RCON-Bridge/1.1",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=admin_api.HTTP_TIMEOUT_SECONDS) as response:  # noqa: S310
            raw = response.read().decode("utf-8", errors="replace")
            return json.loads(raw) if raw else {"ok": True}
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Discord bot returned HTTP {exc.code}: {raw[:300]}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"Could not reach Discord bot: {exc.reason}") from exc


async def _post_bot_path(path: str, payload: dict[str, Any]) -> dict[str, Any]:
    return await asyncio.to_thread(_post_bot_path_sync, path, payload)


# Wrap the existing !admin handler so every successfully processed request becomes
# a persistent support conversation and a second !admin is redirected to !reply.
_original_handle_admin_request = admin_api._handle_admin_request


async def _handle_admin_request_with_support(entry: Any, reason: str) -> None:
    player_id = str(getattr(entry, "player_id", "") or "").strip()
    player_name = str(getattr(entry, "player_name", "") or player_id or "Unknown player").strip()
    if not player_id:
        return

    existing = _active_request(player_id)
    if existing:
        await admin_api._send_private(
            player_id,
            "[ 1ST M.I. ADMIN ]\n\n"
            "YOU ALREADY HAVE AN OPEN ADMIN REQUEST.\n\n"
            "USE !reply <MESSAGE> TO CONTINUE TALKING TO THE ADMIN.",
        )
        return

    request_id = admin_api._event_key(entry)[:16]
    await _original_handle_admin_request(entry, reason)
    _save_request(request_id, player_id, player_name, reason)


admin_api._handle_admin_request = _handle_admin_request_with_support


def _authorized(request: Any) -> bool:
    supplied = str(request.headers.get("x-hllv-secret", "") or "").strip()
    configured = str(admin_api.SHARED_SECRET or "").strip()
    return bool(configured and supplied and hmac.compare_digest(supplied, configured))


@app.post("/api/v2/admin-support/action")
async def admin_support_action(request: Any) -> dict[str, Any]:
    if not _authorized(request):
        from fastapi import HTTPException
        raise HTTPException(status_code=401, detail="Invalid HLL:V integration secret")

    body = await request.json()
    action = str(body.get("action", "") or "").strip().lower()
    request_id = str(body.get("request_id", "") or "").strip()
    player_id = str(body.get("player_id", "") or "").strip()
    admin_name = str(body.get("admin_name", "") or "Admin").strip()[:128]
    message = str(body.get("message", "") or "").strip()

    if action not in {"claim", "cant_join", "reply", "resolve"}:
        from fastapi import HTTPException
        raise HTTPException(status_code=400, detail="Unsupported admin support action")
    if not request_id or not player_id:
        from fastapi import HTTPException
        raise HTTPException(status_code=400, detail="request_id and player_id are required")

    support_request = _request_by_id(request_id)
    if not support_request or str(support_request.get("reporter_id")) != player_id:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail="Admin request is no longer available")
    if support_request.get("status") == "resolved" and action != "resolve":
        from fastapi import HTTPException
        raise HTTPException(status_code=409, detail="Admin request has already been resolved")

    notification: str | None = None
    new_status = str(support_request.get("status") or "open")

    if action == "claim":
        new_status = "claimed"
        notification = (
            "[ 1ST M.I. ADMIN ]\n\n"
            f"YOUR REQUEST HAS BEEN CLAIMED BY {admin_name.upper()}.\n\n"
            "YOU CAN CONTINUE THE CONVERSATION WITH !reply <MESSAGE>."
        )
    elif action == "cant_join":
        new_status = "remote"
        notification = (
            "[ 1ST M.I. ADMIN ]\n\n"
            f"ADMIN {admin_name.upper()} HAS YOUR REQUEST BUT CANNOT CURRENTLY JOIN THE SERVER.\n\n"
            "YOU CAN TALK TO THEM USING !reply <MESSAGE>."
        )
    elif action == "reply":
        if not message:
            from fastapi import HTTPException
            raise HTTPException(status_code=400, detail="message is required")
        notification = (
            "[ 1ST M.I. ADMIN ]\n\n"
            f"{admin_name.upper()}:\n{message[:380]}\n\n"
            "REPLY WITH !reply <MESSAGE>"
        )
    elif action == "resolve":
        new_status = "resolved"
        notification = (
            "[ 1ST M.I. ADMIN ]\n\n"
            "YOUR ADMIN REQUEST HAS BEEN MARKED AS RESOLVED.\n\n"
            "THANK YOU FOR YOUR REPORT."
        )

    if action in {"claim", "cant_join", "resolve"}:
        _update_request(request_id, new_status, admin_name)

    notified = True
    notify_error = None
    if notification:
        try:
            await admin_api._send_private(player_id, notification)
        except Exception as exc:
            notified = False
            notify_error = str(exc)
            logger.warning("Could not notify player %s for support action %s: %s", player_id, action, exc)
            if action == "reply":
                from fastapi import HTTPException
                raise HTTPException(status_code=409, detail="Player is no longer reachable in-game") from exc

    logger.info(
        "Admin support action=%s request=%s player=%s admin=%s notified=%s",
        action,
        request_id,
        player_id,
        admin_name,
        notified,
    )
    return {
        "ok": True,
        "action": action,
        "request_id": request_id,
        "player_id": player_id,
        "status": new_status,
        "notified": notified,
        "notify_error": notify_error,
    }


async def _handle_player_reply(entry: Any, message: str) -> None:
    player_id = str(getattr(entry, "player_id", "") or "").strip()
    player_name = str(getattr(entry, "player_name", "") or player_id or "Unknown player").strip()
    if not player_id:
        return

    support_request = _active_request(player_id)
    if not support_request:
        await admin_api._send_private(
            player_id,
            "[ 1ST M.I. ADMIN ]\n\n"
            "YOU DO NOT HAVE AN OPEN ADMIN REQUEST.\n\n"
            "START ONE WITH !admin <REASON>.",
        )
        return

    now = _utcnow()
    previous = runtime.cooldowns.get(player_id)
    if previous:
        remaining = REPLY_COOLDOWN_SECONDS - int((now - previous).total_seconds())
        if remaining > 0:
            await admin_api._send_private(
                player_id,
                "[ 1ST M.I. ADMIN ]\n\n"
                f"PLEASE WAIT {remaining} SECONDS BEFORE SENDING ANOTHER REPLY.",
            )
            return
    runtime.cooldowns[player_id] = now

    payload = {
        "request_id": support_request["request_id"],
        "reporter_name": player_name,
        "reporter_id": player_id,
        "message": message[:1000],
        "status": support_request.get("status") or "open",
        "admin_name": support_request.get("admin_name") or "",
        "timestamp": _iso(getattr(entry, "timestamp", None) if isinstance(getattr(entry, "timestamp", None), datetime) else now),
    }
    try:
        result = await _post_bot_path("/api/integrations/hllv/admin-reply", payload)
        with _connect_db() as db:
            db.execute(
                "UPDATE admin_support_requests SET updated_at = ? WHERE request_id = ?",
                (_iso(), support_request["request_id"]),
            )
        await admin_api._send_private(
            player_id,
            "[ 1ST M.I. ADMIN ]\n\nYOUR MESSAGE HAS BEEN SENT TO THE ADMIN.",
        )
        logger.info(
            "Player reply sent to Discord request=%s player=%s result=%s",
            support_request["request_id"],
            player_id,
            result,
        )
    except Exception as exc:
        runtime.last_error = str(exc)
        logger.warning("Player reply delivery failed for %s: %s", player_id, exc)
        try:
            await admin_api._send_private(
                player_id,
                "[ 1ST M.I. ADMIN ]\n\nYOUR MESSAGE COULD NOT BE SENT. PLEASE TRY AGAIN SHORTLY.",
            )
        except Exception:
            pass


async def _process_reply_logs() -> None:
    response = await admin_api._call(admin_api._client().get_admin_log(seconds_span=REPLY_LOG_WINDOW_SECONDS))
    entries = list(getattr(response, "entries", []) or [])
    entries.sort(key=lambda entry: admin_api._entry_time(entry) or datetime.min.replace(tzinfo=UTC))

    for entry in entries:
        if not admin_api._is_player_chat(entry):
            continue
        raw_message = str(getattr(entry, "message", "") or "").strip()
        lower = raw_message.lower()
        if lower != "!reply" and not lower.startswith("!reply "):
            continue

        entry_time = admin_api._entry_time(entry)
        if entry_time and entry_time < runtime.started_at - timedelta(seconds=2):
            _claim_reply_event(entry)
            continue
        if not _claim_reply_event(entry):
            continue

        message = raw_message[6:].strip()
        player_id = str(getattr(entry, "player_id", "") or "").strip()
        if not message:
            if player_id:
                await admin_api._send_private(
                    player_id,
                    "[ 1ST M.I. ADMIN ]\n\nUSAGE: !reply <MESSAGE>",
                )
            continue
        await _handle_player_reply(entry, message)

    runtime.last_poll_at = _utcnow()


def _cleanup_reply_events() -> None:
    cutoff = _iso(_utcnow() - timedelta(days=2))
    with _connect_db() as db:
        db.execute("DELETE FROM admin_support_reply_events WHERE seen_at < ?", (cutoff,))


async def _reply_worker() -> None:
    logger.info(
        "HLL:V two-way admin support started: !reply poll=%.1fs cooldown=%ss",
        REPLY_POLL_SECONDS,
        REPLY_COOLDOWN_SECONDS,
    )
    cleanup_counter = 0
    while True:
        try:
            if admin_api.state.client is not None and admin_api.state.client.is_connected():
                await _process_reply_logs()
                runtime.last_error = None
                cleanup_counter += 1
                if cleanup_counter >= max(1, int(3600 / REPLY_POLL_SECONDS)):
                    _cleanup_reply_events()
                    cleanup_counter = 0
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            runtime.last_error = str(exc)
            logger.warning("Admin support reply worker failed: %s", exc)
        await asyncio.sleep(REPLY_POLL_SECONDS)


@app.on_event("startup")
async def start_admin_support_worker() -> None:
    _init_support_db()
    if runtime.task is None:
        runtime.task = asyncio.create_task(_reply_worker(), name="hllv-admin-support-replies")


@app.on_event("shutdown")
async def stop_admin_support_worker() -> None:
    if runtime.task:
        runtime.task.cancel()
        try:
            await runtime.task
        except asyncio.CancelledError:
            pass
        runtime.task = None


@app.get("/api/v2/admin-support/status")
async def admin_support_status() -> dict[str, Any]:
    with _connect_db() as db:
        open_count = db.execute(
            "SELECT COUNT(*) FROM admin_support_requests WHERE status != 'resolved'"
        ).fetchone()[0]
    return {
        "ok": True,
        "enabled": admin_api.ADMIN_REQUESTS_ENABLED,
        "reply_command": "!reply <message>",
        "open_requests": int(open_count or 0),
        "last_poll_at": runtime.last_poll_at.isoformat() if runtime.last_poll_at else None,
        "last_error": runtime.last_error,
    }
