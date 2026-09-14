from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import sqlite3
from datetime import UTC, datetime, timedelta
from typing import Any

from voting_api import app
from app import _call, _client, state
from stats_tracker_api import (
    DB_PATH,
    _connect_db,
    _current_match_stats,
    _init_db,
    _player_row,
    _revive_actor,
)

try:
    from hllrcon.admin_logs import HLLVMatchStartAdminLog, HLLVPlayerSendMessageAdminLog
except ImportError:  # pragma: no cover - compatibility guard
    HLLVMatchStartAdminLog = ()  # type: ignore[assignment,misc]
    HLLVPlayerSendMessageAdminLog = ()  # type: ignore[assignment,misc]

logger = logging.getLogger("hllv-rcon-bridge.stats-commands")

COMMANDS_ENABLED = os.getenv("PLAYER_STATS_CHAT_COMMANDS", "true").strip().lower() not in {"0", "false", "no", "off"}
COMMAND_POLL_SECONDS = max(1.0, float(os.getenv("PLAYER_STATS_COMMAND_POLL_SECONDS", "2")))
COMMAND_LOG_WINDOW_SECONDS = max(10, int(os.getenv("PLAYER_STATS_COMMAND_LOG_WINDOW_SECONDS", "30")))
COMMAND_COOLDOWN_SECONDS = max(5, int(os.getenv("PLAYER_STATS_COMMAND_COOLDOWN_SECONDS", "30")))
BOOTSTRAP_LOG_SECONDS = max(300, int(os.getenv("PLAYER_STATS_MATCH_BOOTSTRAP_SECONDS", "21600")))

COMMAND_MAP = {
    "!stats": "both",
    "!mystats": "both",
    "!matchstats": "match",
    "!serverstats": "server",
}


class CommandRuntime:
    def __init__(self) -> None:
        self.task: asyncio.Task[Any] | None = None
        self.started_at = datetime.now(UTC)
        self.last_poll_at: datetime | None = None
        self.last_error: str | None = None
        self.bootstrapped = False
        self.cooldowns: dict[str, datetime] = {}


runtime = CommandRuntime()


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


def _init_command_db() -> None:
    _init_db()
    with _connect_db() as db:
        db.executescript(
            """
            CREATE TABLE IF NOT EXISTS stats_command_events (
                event_key TEXT PRIMARY KEY,
                seen_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS current_match_revives (
                player_id TEXT PRIMARY KEY,
                revives INTEGER NOT NULL DEFAULT 0,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS stats_command_meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_stats_command_events_seen
            ON stats_command_events(seen_at);
            """
        )


def _claim_event(entry: Any) -> bool:
    key = _event_key(entry)
    with _connect_db() as db:
        try:
            db.execute(
                "INSERT INTO stats_command_events(event_key, seen_at) VALUES(?, ?)",
                (key, _iso()),
            )
            return True
        except sqlite3.IntegrityError:
            return False


def _mark_event(entry: Any) -> None:
    with _connect_db() as db:
        db.execute(
            "INSERT OR IGNORE INTO stats_command_events(event_key, seen_at) VALUES(?, ?)",
            (_event_key(entry), _iso()),
        )


def _meta_get(key: str) -> str | None:
    with _connect_db() as db:
        row = db.execute("SELECT value FROM stats_command_meta WHERE key = ?", (key,)).fetchone()
    return str(row[0]) if row else None


def _meta_set(key: str, value: str) -> None:
    with _connect_db() as db:
        db.execute(
            """
            INSERT INTO stats_command_meta(key, value) VALUES(?, ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
            """,
            (key, value),
        )


def _is_match_start(entry: Any) -> bool:
    try:
        if bool(HLLVMatchStartAdminLog) and isinstance(entry, HLLVMatchStartAdminLog):
            return True
    except TypeError:
        pass
    return "matchstart" in type(entry).__name__.lower()


def _is_player_chat(entry: Any) -> bool:
    try:
        if bool(HLLVPlayerSendMessageAdminLog) and isinstance(entry, HLLVPlayerSendMessageAdminLog):
            return True
    except TypeError:
        pass
    return "playersendmessageadminlog" in type(entry).__name__.lower()


def _reset_match_revives(match_key: str | None = None) -> None:
    with _connect_db() as db:
        db.execute("DELETE FROM current_match_revives")
    if match_key:
        _meta_set("current_match_start", match_key)


def _increment_match_revive(player_id: str) -> None:
    if not player_id:
        return
    now = _iso()
    with _connect_db() as db:
        db.execute(
            """
            INSERT INTO current_match_revives(player_id, revives, updated_at)
            VALUES(?, 1, ?)
            ON CONFLICT(player_id) DO UPDATE SET
                revives = current_match_revives.revives + 1,
                updated_at = excluded.updated_at
            """,
            (player_id, now),
        )


def _match_revives(player_id: str) -> int:
    with _connect_db() as db:
        row = db.execute(
            "SELECT revives FROM current_match_revives WHERE player_id = ?",
            (player_id,),
        ).fetchone()
    return int(row[0]) if row else 0


def _kd(kills: int, deaths: int) -> str:
    kills = max(0, int(kills or 0))
    deaths = max(0, int(deaths or 0))
    if deaths == 0:
        return "INF" if kills > 0 else "0.00"
    return f"{kills / deaths:.2f}"


def _favorite_text(favorite: dict[str, Any] | None) -> str:
    if not favorite:
        return "NOT ENOUGH DATA"
    name = str(favorite.get("name") or "UNKNOWN").strip()[:48]
    kills = max(0, int(favorite.get("kills") or 0))
    return f"{name} ({kills} {'KILL' if kills == 1 else 'KILLS'})"


def _build_stats_message(
    mode: str,
    player_name: str,
    match_kills: int,
    match_deaths: int,
    match_revives: int,
    totals: dict[str, Any] | None,
) -> str:
    lines = ["[ 1ST M.I. PLAYER STATS ]", "", str(player_name or "PLAYER").upper()[:40], ""]

    if mode in {"both", "match"}:
        lines += [
            "THIS MATCH",
            f"KILLS: {match_kills} | DEATHS: {match_deaths} | K/D: {_kd(match_kills, match_deaths)}",
            f"REVIVES: {match_revives}",
            "",
        ]

    if mode in {"both", "server"}:
        totals = totals or {}
        kills = max(0, int(totals.get("kills") or 0))
        deaths = max(0, int(totals.get("deaths") or 0))
        revives = max(0, int(totals.get("revives") or 0))
        lines += [
            "SERVER TOTALS",
            f"KILLS: {kills} | DEATHS: {deaths} | K/D: {_kd(kills, deaths)}",
            f"REVIVES: {revives}",
            "",
            f"FAV WEAPON: {_favorite_text(totals.get('favorite_weapon'))}",
            f"FAV VEHICLE: {_favorite_text(totals.get('favorite_vehicle'))}",
        ]

    return "\n".join(lines).strip()[:500]


async def _send_private(player_id: str, message: str) -> None:
    await _call(_client().message_player(player_id, message[:500]))


async def _current_player_stats(player_id: str) -> tuple[str, int, int]:
    response = await _call(_client().get_players())
    for player in list(getattr(response, "players", []) or []):
        current_id = str(getattr(player, "id", "") or "").strip()
        if current_id != player_id:
            continue
        name = str(getattr(player, "name", "") or player_id).strip()
        kills, deaths = _current_match_stats(player)
        return name, kills, deaths
    raise RuntimeError("Player is no longer connected")


async def _handle_stats_command(entry: Any, mode: str) -> None:
    player_id = str(getattr(entry, "player_id", "") or "").strip()
    player_name = str(getattr(entry, "player_name", "") or player_id).strip()
    if not player_id:
        return

    now = _utcnow()
    previous = runtime.cooldowns.get(player_id)
    if previous:
        remaining = COMMAND_COOLDOWN_SECONDS - int((now - previous).total_seconds())
        if remaining > 0:
            try:
                await _send_private(
                    player_id,
                    "[ 1ST M.I. PLAYER STATS ]\n\n"
                    f"PLEASE WAIT {remaining} SECONDS BEFORE REQUESTING YOUR STATS AGAIN.",
                )
            except Exception as exc:
                logger.debug("Could not send stats cooldown message to %s: %s", player_id, exc)
            return

    runtime.cooldowns[player_id] = now

    try:
        live_name, match_kills, match_deaths = await _current_player_stats(player_id)
        player_name = live_name or player_name
        totals = _player_row(player_id)
        message = _build_stats_message(
            mode,
            player_name,
            match_kills,
            match_deaths,
            _match_revives(player_id),
            totals,
        )
        await _send_private(player_id, message)
        logger.info("Sent %s stats to %s (%s)", mode, player_name, player_id)
    except Exception as exc:
        runtime.last_error = f"Stats command failed for {player_name}: {exc}"
        logger.warning("Could not answer stats command for %s (%s): %s", player_name, player_id, exc)
        try:
            await _send_private(
                player_id,
                "[ 1ST M.I. PLAYER STATS ]\n\nSTATS ARE TEMPORARILY UNAVAILABLE. PLEASE TRY AGAIN SHORTLY.",
            )
        except Exception:
            pass


async def _bootstrap_match_revives() -> None:
    response = await _call(_client().get_admin_log(seconds_span=BOOTSTRAP_LOG_SECONDS))
    entries = list(getattr(response, "entries", []) or [])
    entries.sort(key=lambda entry: _entry_time(entry) or datetime.min.replace(tzinfo=UTC))

    match_starts = [entry for entry in entries if _is_match_start(entry)]
    if not match_starts:
        runtime.bootstrapped = True
        return

    latest_start = match_starts[-1]
    start_key = _event_key(latest_start)
    if _meta_get("current_match_start") == start_key:
        runtime.bootstrapped = True
        return

    _reset_match_revives(start_key)
    _mark_event(latest_start)
    start_time = _entry_time(latest_start)

    for entry in entries:
        entry_time = _entry_time(entry)
        if start_time and entry_time and entry_time < start_time:
            continue
        revive = _revive_actor(entry)
        if not revive:
            continue
        _increment_match_revive(revive[0])
        _mark_event(entry)

    runtime.bootstrapped = True
    logger.info("Bootstrapped current-match revive counters from recent HLL:V logs")


async def _process_logs() -> None:
    response = await _call(_client().get_admin_log(seconds_span=COMMAND_LOG_WINDOW_SECONDS))
    entries = list(getattr(response, "entries", []) or [])
    entries.sort(key=lambda entry: _entry_time(entry) or datetime.min.replace(tzinfo=UTC))

    for entry in entries:
        if _is_match_start(entry):
            if _claim_event(entry):
                _reset_match_revives(_event_key(entry))
            continue

        revive = _revive_actor(entry)
        if revive:
            if _claim_event(entry):
                _increment_match_revive(revive[0])
            continue

        if not _is_player_chat(entry):
            continue

        message = str(getattr(entry, "message", "") or "").strip().lower()
        mode = COMMAND_MAP.get(message)
        if not mode:
            continue

        entry_time = _entry_time(entry)
        if entry_time and entry_time < runtime.started_at - timedelta(seconds=2):
            _mark_event(entry)
            continue

        if not _claim_event(entry):
            continue
        await _handle_stats_command(entry, mode)

    runtime.last_poll_at = _utcnow()


def _cleanup_seen_events() -> None:
    cutoff = _iso(_utcnow() - timedelta(days=2))
    with _connect_db() as db:
        db.execute("DELETE FROM stats_command_events WHERE seen_at < ?", (cutoff,))


async def _worker_loop() -> None:
    logger.info(
        "Player stats chat commands started: commands=%s cooldown=%ss poll=%ss",
        ", ".join(COMMAND_MAP),
        COMMAND_COOLDOWN_SECONDS,
        COMMAND_POLL_SECONDS,
    )
    cleanup_counter = 0
    while True:
        try:
            if state.client is not None and state.client.is_connected():
                if not runtime.bootstrapped:
                    await _bootstrap_match_revives()
                await _process_logs()
                runtime.last_error = None
                cleanup_counter += 1
                if cleanup_counter >= max(1, int(3600 / COMMAND_POLL_SECONDS)):
                    _cleanup_seen_events()
                    cleanup_counter = 0
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            runtime.last_error = str(exc)
            logger.warning("Player stats command worker failed: %s", exc)
        await asyncio.sleep(COMMAND_POLL_SECONDS)


@app.on_event("startup")
async def start_stats_command_worker() -> None:
    if not COMMANDS_ENABLED:
        logger.info("Player stats chat commands disabled by PLAYER_STATS_CHAT_COMMANDS")
        return
    _init_command_db()
    runtime.task = asyncio.create_task(_worker_loop(), name="hllv-player-stats-commands")


@app.on_event("shutdown")
async def stop_stats_command_worker() -> None:
    if runtime.task:
        runtime.task.cancel()
        try:
            await runtime.task
        except asyncio.CancelledError:
            pass
        runtime.task = None


@app.get("/api/v2/player-stats/commands/status")
async def stats_command_status() -> dict[str, Any]:
    return {
        "enabled": COMMANDS_ENABLED,
        "commands": COMMAND_MAP,
        "cooldown_seconds": COMMAND_COOLDOWN_SECONDS,
        "poll_seconds": COMMAND_POLL_SECONDS,
        "log_window_seconds": COMMAND_LOG_WINDOW_SECONDS,
        "database": str(DB_PATH),
        "bootstrapped_current_match": runtime.bootstrapped,
        "last_poll_at": runtime.last_poll_at.isoformat() if runtime.last_poll_at else None,
        "last_error": runtime.last_error,
    }
