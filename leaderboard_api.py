from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import sqlite3
from datetime import UTC, datetime, timedelta
from typing import Any

from fastapi import Query

from stats_commands_api import app
from app import _call, _client, state
from stats_tracker_api import _connect_db, _favorite_rows, _init_db

try:
    from hllrcon.admin_logs import HLLVPlayerSendMessageAdminLog
except ImportError:  # pragma: no cover
    HLLVPlayerSendMessageAdminLog = ()  # type: ignore[assignment,misc]

logger = logging.getLogger("hllv-rcon-bridge.leaderboard")

LEADERBOARD_COMMANDS_ENABLED = os.getenv("PLAYER_LEADERBOARD_CHAT_COMMANDS", "true").strip().lower() not in {"0", "false", "no", "off"}
LEADERBOARD_POLL_SECONDS = max(1.0, float(os.getenv("PLAYER_LEADERBOARD_POLL_SECONDS", "2")))
LEADERBOARD_LOG_WINDOW_SECONDS = max(10, int(os.getenv("PLAYER_LEADERBOARD_LOG_WINDOW_SECONDS", "30")))
LEADERBOARD_COOLDOWN_SECONDS = max(5, int(os.getenv("PLAYER_LEADERBOARD_COOLDOWN_SECONDS", "30")))
MIN_KD_KILLS = max(1, int(os.getenv("PLAYER_LEADERBOARD_MIN_KD_KILLS", "10")))

COMMANDS = {
    "!topstats": "summary",
    "!leaderboard": "summary",
    "!topkills": "kills",
    "!toprevives": "revives",
    "!topkd": "kd",
}


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


def _init_leaderboard_db() -> None:
    _init_db()
    with _connect_db() as db:
        db.executescript(
            """
            CREATE TABLE IF NOT EXISTS leaderboard_command_events (
                event_key TEXT PRIMARY KEY,
                seen_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_leaderboard_command_events_seen
            ON leaderboard_command_events(seen_at);
            """
        )


def _claim_event(entry: Any) -> bool:
    with _connect_db() as db:
        try:
            db.execute(
                "INSERT INTO leaderboard_command_events(event_key, seen_at) VALUES(?, ?)",
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


def _clean_name(value: Any, limit: int = 24) -> str:
    text = " ".join(str(value or "Unknown").split())
    return text[:limit]


def _kd_value(kills: int, deaths: int) -> float:
    kills = max(0, int(kills or 0))
    deaths = max(0, int(deaths or 0))
    return float(kills) if deaths == 0 else kills / deaths


def _kd_text(kills: int, deaths: int) -> str:
    kills = max(0, int(kills or 0))
    deaths = max(0, int(deaths or 0))
    if deaths == 0:
        return "INF" if kills > 0 else "0.00"
    return f"{kills / deaths:.2f}"


def _all_rows() -> list[dict[str, Any]]:
    _init_db()
    with _connect_db() as db:
        rows = db.execute(
            "SELECT player_id, player_name, kills, deaths, revives, first_seen, last_seen FROM players"
        ).fetchall()
    return [dict(row) for row in rows]


def _rankings(limit: int = 10) -> dict[str, Any]:
    rows = _all_rows()
    for row in rows:
        row["kills"] = max(0, int(row.get("kills") or 0))
        row["deaths"] = max(0, int(row.get("deaths") or 0))
        row["revives"] = max(0, int(row.get("revives") or 0))
        row["kd"] = _kd_value(row["kills"], row["deaths"])
        row["kd_display"] = _kd_text(row["kills"], row["deaths"])

    top_kills = sorted(rows, key=lambda r: (-r["kills"], r["deaths"], str(r["player_name"]).casefold()))[:limit]
    top_revives = sorted(rows, key=lambda r: (-r["revives"], -r["kills"], str(r["player_name"]).casefold()))[:limit]
    kd_pool = [row for row in rows if row["kills"] >= MIN_KD_KILLS]
    top_kd = sorted(kd_pool, key=lambda r: (-r["kd"], -r["kills"], r["deaths"], str(r["player_name"]).casefold()))[:limit]
    return {
        "top_kills": top_kills,
        "top_revives": top_revives,
        "top_kd": top_kd,
        "tracked_players": len(rows),
        "min_kd_kills": MIN_KD_KILLS,
    }


def _with_favorites(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        weapon, vehicle = _favorite_rows(str(item.get("player_id") or ""))
        item["favorite_weapon"] = weapon
        item["favorite_vehicle"] = vehicle
        result.append(item)
    return result


def _format_list(title: str, rows: list[dict[str, Any]], field: str, count: int) -> list[str]:
    lines = [title]
    if not rows:
        return lines + ["NO DATA YET"]
    for index, row in enumerate(rows[:count], start=1):
        name = _clean_name(row.get("player_name"))
        if field == "kd":
            value = row.get("kd_display") or _kd_text(row.get("kills", 0), row.get("deaths", 0))
        else:
            value = str(max(0, int(row.get(field) or 0)))
        lines.append(f"{index}. {name} - {value}")
    return lines


def _build_message(mode: str) -> str:
    board = _rankings(5)
    if mode == "kills":
        lines = ["[ 1ST M.I. TOP KILLS ]", ""] + _format_list("SERVER TOTAL KILLS", board["top_kills"], "kills", 5)
    elif mode == "revives":
        lines = ["[ 1ST M.I. TOP REVIVES ]", ""] + _format_list("SERVER TOTAL REVIVES", board["top_revives"], "revives", 5)
    elif mode == "kd":
        lines = ["[ 1ST M.I. TOP K/D ]", "", f"MINIMUM {MIN_KD_KILLS} KILLS"] + _format_list("", board["top_kd"], "kd", 5)[1:]
    else:
        lines = ["[ 1ST M.I. SERVER LEADERS ]", ""]
        lines += _format_list("TOP KILLS", board["top_kills"], "kills", 3)
        lines += [""] + _format_list("TOP REVIVES", board["top_revives"], "revives", 3)
        lines += ["", f"TOP K/D - MIN {MIN_KD_KILLS} KILLS"] + _format_list("", board["top_kd"], "kd", 3)[1:]
    return "\n".join(lines).strip()[:500]


async def _send_private(player_id: str, message: str) -> None:
    await _call(_client().message_player(player_id, message[:500]))


async def _handle_command(entry: Any, mode: str) -> None:
    player_id = str(getattr(entry, "player_id", "") or "").strip()
    if not player_id:
        return
    now = _utcnow()
    previous = runtime.cooldowns.get(player_id)
    if previous:
        remaining = LEADERBOARD_COOLDOWN_SECONDS - int((now - previous).total_seconds())
        if remaining > 0:
            try:
                await _send_private(player_id, f"[ 1ST M.I. SERVER LEADERS ]\n\nPLEASE WAIT {remaining} SECONDS BEFORE REQUESTING THE LEADERBOARD AGAIN.")
            except Exception:
                pass
            return
    runtime.cooldowns[player_id] = now
    try:
        await _send_private(player_id, _build_message(mode))
    except Exception as exc:
        runtime.last_error = str(exc)
        logger.warning("Could not answer leaderboard command for %s: %s", player_id, exc)


async def _process_logs() -> None:
    response = await _call(_client().get_admin_log(seconds_span=LEADERBOARD_LOG_WINDOW_SECONDS))
    entries = list(getattr(response, "entries", []) or [])
    entries.sort(key=lambda entry: _entry_time(entry) or datetime.min.replace(tzinfo=UTC))
    for entry in entries:
        if not _is_player_chat(entry):
            continue
        message = str(getattr(entry, "message", "") or "").strip().lower()
        mode = COMMANDS.get(message)
        if not mode:
            continue
        entry_time = _entry_time(entry)
        if entry_time and entry_time < runtime.started_at - timedelta(seconds=2):
            _claim_event(entry)
            continue
        if not _claim_event(entry):
            continue
        await _handle_command(entry, mode)
    runtime.last_poll_at = _utcnow()


def _cleanup_seen() -> None:
    cutoff = _iso(_utcnow() - timedelta(days=2))
    with _connect_db() as db:
        db.execute("DELETE FROM leaderboard_command_events WHERE seen_at < ?", (cutoff,))


async def _worker() -> None:
    logger.info("Leaderboard chat commands started: %s", ", ".join(COMMANDS))
    cleanup_counter = 0
    while True:
        try:
            if state.client is not None and state.client.is_connected():
                await _process_logs()
                runtime.last_error = None
                cleanup_counter += 1
                if cleanup_counter >= max(1, int(3600 / LEADERBOARD_POLL_SECONDS)):
                    _cleanup_seen()
                    cleanup_counter = 0
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            runtime.last_error = str(exc)
            logger.warning("Leaderboard command worker failed: %s", exc)
        await asyncio.sleep(LEADERBOARD_POLL_SECONDS)


@app.on_event("startup")
async def start_leaderboard_commands() -> None:
    if not LEADERBOARD_COMMANDS_ENABLED:
        logger.info("Leaderboard chat commands disabled")
        return
    _init_leaderboard_db()
    runtime.task = asyncio.create_task(_worker(), name="hllv-player-leaderboard-commands")


@app.on_event("shutdown")
async def stop_leaderboard_commands() -> None:
    if runtime.task:
        runtime.task.cancel()
        try:
            await runtime.task
        except asyncio.CancelledError:
            pass
        runtime.task = None


@app.get("/api/v2/player-stats/leaderboard")
async def leaderboard(limit: int = Query(default=10, ge=1, le=100)) -> dict[str, Any]:
    board = _rankings(limit)
    return {
        "top_kills": _with_favorites(board["top_kills"]),
        "top_revives": _with_favorites(board["top_revives"]),
        "top_kd": _with_favorites(board["top_kd"]),
        "tracked_players": board["tracked_players"],
        "min_kd_kills": board["min_kd_kills"],
    }


@app.get("/api/v2/player-stats/leaderboard/status")
async def leaderboard_status() -> dict[str, Any]:
    return {
        "enabled": LEADERBOARD_COMMANDS_ENABLED,
        "commands": COMMANDS,
        "cooldown_seconds": LEADERBOARD_COOLDOWN_SECONDS,
        "poll_seconds": LEADERBOARD_POLL_SECONDS,
        "min_kd_kills": MIN_KD_KILLS,
        "last_poll_at": runtime.last_poll_at.isoformat() if runtime.last_poll_at else None,
        "last_error": runtime.last_error,
    }
