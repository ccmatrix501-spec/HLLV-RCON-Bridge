from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import re
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from fastapi import HTTPException, Query

from message_everyone_api import app
from app import _call, _client, state

try:
    from hllrcon.admin_logs import HLLVPlayerConnectAdminLog, HLLVPlayerKillAdminLog
except ImportError:  # pragma: no cover - compatibility guard
    HLLVPlayerConnectAdminLog = ()  # type: ignore[assignment,misc]
    HLLVPlayerKillAdminLog = ()  # type: ignore[assignment,misc]

logger = logging.getLogger("hllv-rcon-bridge.stats")

TRACKER_ENABLED = os.getenv("PLAYER_STATS_TRACKER", "true").strip().lower() not in {"0", "false", "no", "off"}
DB_PATH = Path(os.getenv("PLAYER_STATS_DB_PATH", "/data/player-stats.db"))
POLL_SECONDS = max(2, int(os.getenv("PLAYER_STATS_POLL_SECONDS", "5")))
LOG_WINDOW_SECONDS = max(POLL_SECONDS * 3, int(os.getenv("PLAYER_STATS_LOG_WINDOW_SECONDS", "30")))
JOIN_MESSAGE_ENABLED = os.getenv("PLAYER_STATS_JOIN_MESSAGE", "true").strip().lower() not in {"0", "false", "no", "off"}

# Best-effort fallback for a raw HLL:V log line if a future server build exposes
# revives without the hllrcon parser assigning a dedicated event class.
REVIVE_RAW_RE = re.compile(
    r"^REVIVE:\s*(?P<name>.+?)\((?:Allies|Axis)/(?P<id>\d{17}|[\da-f]{32})\)",
    flags=re.IGNORECASE,
)

VEHICLE_DISPLAY_NAMES = {
    "Gaz 63 (Supply)": "GAZ-63 Supply Truck",
    "Gaz 63 (Transport)": "GAZ-63 Transport Truck",
    "M35 (Supply)": "M35 Supply Truck",
    "M35 (Transport)": "M35 Transport Truck",
    "NVA Boat": "NVA Boat",
    "T54": "T-54",
    "M48Patton": "M48 Patton",
    "US Boat": "US Boat",
    "UH-1 Huey Supply": "UH-1 Huey Supply",
    "UH-1 Huey Transport": "UH-1 Huey Transport",
}


class TrackerRuntime:
    def __init__(self) -> None:
        self.task: asyncio.Task[Any] | None = None
        self.started_at = datetime.now(timezone.utc)
        self.last_error: str | None = None
        self.last_poll_at: datetime | None = None
        self.poll_count = 0


runtime = TrackerRuntime()


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _iso(dt: datetime | None = None) -> str:
    return (dt or _utcnow()).isoformat()


def _connect_db() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(DB_PATH, timeout=30)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA synchronous=NORMAL")
    db.execute("PRAGMA busy_timeout=30000")
    return db


def _init_db() -> None:
    with _connect_db() as db:
        db.executescript(
            """
            CREATE TABLE IF NOT EXISTS players (
                player_id TEXT PRIMARY KEY,
                player_name TEXT NOT NULL,
                kills INTEGER NOT NULL DEFAULT 0,
                deaths INTEGER NOT NULL DEFAULT 0,
                revives INTEGER NOT NULL DEFAULT 0,
                first_seen TEXT NOT NULL,
                last_seen TEXT NOT NULL,
                last_join_message_at TEXT
            );

            CREATE TABLE IF NOT EXISTS snapshots (
                player_id TEXT PRIMARY KEY,
                match_kills INTEGER NOT NULL DEFAULT 0,
                match_deaths INTEGER NOT NULL DEFAULT 0,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS processed_events (
                event_key TEXT PRIMARY KEY,
                seen_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS weapon_kills (
                player_id TEXT NOT NULL,
                weapon_id TEXT NOT NULL,
                weapon_name TEXT NOT NULL,
                kills INTEGER NOT NULL DEFAULT 0,
                last_used TEXT NOT NULL,
                PRIMARY KEY(player_id, weapon_id)
            );

            CREATE TABLE IF NOT EXISTS vehicle_kills (
                player_id TEXT NOT NULL,
                vehicle_id TEXT NOT NULL,
                vehicle_name TEXT NOT NULL,
                kills INTEGER NOT NULL DEFAULT 0,
                last_used TEXT NOT NULL,
                PRIMARY KEY(player_id, vehicle_id)
            );

            CREATE TABLE IF NOT EXISTS meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_players_kills ON players(kills DESC);
            CREATE INDEX IF NOT EXISTS idx_processed_events_seen ON processed_events(seen_at);
            CREATE INDEX IF NOT EXISTS idx_weapon_kills_player ON weapon_kills(player_id, kills DESC);
            CREATE INDEX IF NOT EXISTS idx_vehicle_kills_player ON vehicle_kills(player_id, kills DESC);
            """
        )
        db.execute(
            "INSERT OR IGNORE INTO meta(key, value) VALUES('tracking_started_at', ?)",
            (_iso(runtime.started_at),),
        )


def _favorite_rows(player_id: str) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    with _connect_db() as db:
        weapon = db.execute(
            """
            SELECT weapon_id AS id, weapon_name AS name, kills
            FROM weapon_kills
            WHERE player_id = ?
            ORDER BY kills DESC, last_used DESC, weapon_name ASC
            LIMIT 1
            """,
            (player_id,),
        ).fetchone()
        vehicle = db.execute(
            """
            SELECT vehicle_id AS id, vehicle_name AS name, kills
            FROM vehicle_kills
            WHERE player_id = ?
            ORDER BY kills DESC, last_used DESC, vehicle_name ASC
            LIMIT 1
            """,
            (player_id,),
        ).fetchone()
    return (dict(weapon) if weapon else None, dict(vehicle) if vehicle else None)


def _player_row(player_id: str) -> dict[str, Any] | None:
    with _connect_db() as db:
        row = db.execute(
            "SELECT player_id, player_name, kills, deaths, revives, first_seen, last_seen, last_join_message_at "
            "FROM players WHERE player_id = ?",
            (player_id,),
        ).fetchone()
    if not row:
        return None
    result = dict(row)
    favorite_weapon, favorite_vehicle = _favorite_rows(player_id)
    result["favorite_weapon"] = favorite_weapon
    result["favorite_vehicle"] = favorite_vehicle
    return result


def _upsert_player(player_id: str, player_name: str) -> None:
    now = _iso()
    safe_name = player_name.strip() or player_id
    with _connect_db() as db:
        db.execute(
            """
            INSERT INTO players(player_id, player_name, first_seen, last_seen)
            VALUES(?, ?, ?, ?)
            ON CONFLICT(player_id) DO UPDATE SET
                player_name = excluded.player_name,
                last_seen = excluded.last_seen
            """,
            (player_id, safe_name, now, now),
        )


def _current_match_stats(player: Any) -> tuple[int, int]:
    stats = getattr(player, "stats", None)
    if stats is None:
        return 0, 0
    infantry_kills = max(0, int(getattr(stats, "infantry_kills", 0) or 0))
    vehicle_kills = max(0, int(getattr(stats, "vehicle_kills", 0) or 0))
    deaths = max(0, int(getattr(stats, "deaths", 0) or 0))
    # Team kills are deliberately not counted as kills. HLL:V exposes infantry and
    # vehicle kills as separate counters, so the lifetime total combines those two.
    return infantry_kills + vehicle_kills, deaths


def _accumulate_snapshot(player_id: str, player_name: str, current_kills: int, current_deaths: int) -> None:
    now = _iso()
    with _connect_db() as db:
        db.execute(
            """
            INSERT INTO players(player_id, player_name, first_seen, last_seen)
            VALUES(?, ?, ?, ?)
            ON CONFLICT(player_id) DO UPDATE SET
                player_name = excluded.player_name,
                last_seen = excluded.last_seen
            """,
            (player_id, player_name or player_id, now, now),
        )

        snap = db.execute(
            "SELECT match_kills, match_deaths FROM snapshots WHERE player_id = ?",
            (player_id,),
        ).fetchone()

        if snap is None:
            # First observation: import the player's counters from the current match.
            # Historical matches before the tracker existed cannot be reconstructed.
            kill_delta = current_kills
            death_delta = current_deaths
        else:
            old_kills = int(snap["match_kills"])
            old_deaths = int(snap["match_deaths"])
            # A lower counter means the game/map counters reset. In that case the
            # new value itself is the delta accumulated since the reset.
            kill_delta = current_kills - old_kills if current_kills >= old_kills else current_kills
            death_delta = current_deaths - old_deaths if current_deaths >= old_deaths else current_deaths

        if kill_delta or death_delta:
            db.execute(
                "UPDATE players SET kills = kills + ?, deaths = deaths + ?, last_seen = ? WHERE player_id = ?",
                (max(0, kill_delta), max(0, death_delta), now, player_id),
            )

        db.execute(
            """
            INSERT INTO snapshots(player_id, match_kills, match_deaths, updated_at)
            VALUES(?, ?, ?, ?)
            ON CONFLICT(player_id) DO UPDATE SET
                match_kills = excluded.match_kills,
                match_deaths = excluded.match_deaths,
                updated_at = excluded.updated_at
            """,
            (player_id, current_kills, current_deaths, now),
        )


def _event_key(entry: Any) -> str:
    timestamp = getattr(entry, "timestamp", None)
    raw = str(getattr(entry, "raw_message", "") or repr(entry))
    seed = f"{timestamp!s}|{raw}".encode("utf-8", errors="replace")
    return hashlib.sha256(seed).hexdigest()


def _claim_event(entry: Any) -> bool:
    key = _event_key(entry)
    with _connect_db() as db:
        try:
            db.execute(
                "INSERT INTO processed_events(event_key, seen_at) VALUES(?, ?)",
                (key, _iso()),
            )
            return True
        except sqlite3.IntegrityError:
            return False


def _record_revive(player_id: str, player_name: str) -> None:
    _upsert_player(player_id, player_name)
    with _connect_db() as db:
        db.execute(
            "UPDATE players SET revives = revives + 1, last_seen = ? WHERE player_id = ?",
            (_iso(), player_id),
        )


def _revive_actor(entry: Any) -> tuple[str, str] | None:
    weapon_id = str(getattr(entry, "weapon_id", "") or "").upper()
    if weapon_id == "REVIVE" or "REVIVE" in weapon_id:
        player_id = str(getattr(entry, "instigator_id", "") or "").strip()
        player_name = str(getattr(entry, "instigator_name", "") or player_id).strip()
        if player_id:
            return player_id, player_name

    raw = str(getattr(entry, "raw_message", "") or "").strip()
    match = REVIVE_RAW_RE.match(raw)
    if match:
        return match.group("id"), match.group("name").strip()
    return None


def _entry_is_connect(entry: Any) -> bool:
    try:
        return bool(HLLVPlayerConnectAdminLog) and isinstance(entry, HLLVPlayerConnectAdminLog)
    except TypeError:
        return False


def _entry_is_kill(entry: Any) -> bool:
    try:
        return bool(HLLVPlayerKillAdminLog) and isinstance(entry, HLLVPlayerKillAdminLog)
    except TypeError:
        return False


def _friendly_weapon_and_vehicle(entry: Any) -> tuple[str, str, str | None, str | None]:
    raw_weapon_id = str(getattr(entry, "weapon_id", "") or "UNKNOWN").strip() or "UNKNOWN"
    weapon_name = raw_weapon_id
    vehicle_id: str | None = None
    vehicle_name: str | None = None

    try:
        weapon = entry.get_weapon()
        weapon_name = str(getattr(weapon, "name", "") or raw_weapon_id).strip() or raw_weapon_id
        raw_vehicle_id = getattr(weapon, "vehicle_id", None)
        if raw_vehicle_id:
            vehicle_id = str(raw_vehicle_id)
            vehicle_name = VEHICLE_DISPLAY_NAMES.get(vehicle_id, vehicle_id)
            try:
                get_vehicle = getattr(weapon, "get_vehicle", None)
                if callable(get_vehicle):
                    vehicle = get_vehicle()
                    if vehicle is not None:
                        vehicle_name = str(getattr(vehicle, "name", "") or vehicle_name)
            except Exception:
                pass
    except Exception:
        # Unknown future weapon IDs still get tracked by their raw RCON ID.
        bracket = re.search(r"\[([^\]]+)\]\s*$", raw_weapon_id)
        if bracket:
            candidate = bracket.group(1).strip()
            if candidate in VEHICLE_DISPLAY_NAMES:
                vehicle_id = candidate
                vehicle_name = VEHICLE_DISPLAY_NAMES[candidate]

    return raw_weapon_id, weapon_name, vehicle_id, vehicle_name


def _record_kill_preference(entry: Any) -> None:
    if not _entry_is_kill(entry):
        return

    player_id = str(getattr(entry, "instigator_id", "") or "").strip()
    player_name = str(getattr(entry, "instigator_name", "") or player_id).strip()
    if not player_id:
        return

    _upsert_player(player_id, player_name)
    weapon_id, weapon_name, vehicle_id, vehicle_name = _friendly_weapon_and_vehicle(entry)
    now = _iso()

    with _connect_db() as db:
        if vehicle_id:
            db.execute(
                """
                INSERT INTO vehicle_kills(player_id, vehicle_id, vehicle_name, kills, last_used)
                VALUES(?, ?, ?, 1, ?)
                ON CONFLICT(player_id, vehicle_id) DO UPDATE SET
                    vehicle_name = excluded.vehicle_name,
                    kills = vehicle_kills.kills + 1,
                    last_used = excluded.last_used
                """,
                (player_id, vehicle_id, vehicle_name or vehicle_id, now),
            )
        else:
            db.execute(
                """
                INSERT INTO weapon_kills(player_id, weapon_id, weapon_name, kills, last_used)
                VALUES(?, ?, ?, 1, ?)
                ON CONFLICT(player_id, weapon_id) DO UPDATE SET
                    weapon_name = excluded.weapon_name,
                    kills = weapon_kills.kills + 1,
                    last_used = excluded.last_used
                """,
                (player_id, weapon_id, weapon_name, now),
            )


def _entry_time(entry: Any) -> datetime | None:
    value = getattr(entry, "timestamp", None)
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    return None


def _favorite_line(label: str, favorite: dict[str, Any] | None) -> str:
    if not favorite:
        return f"{label}: NOT ENOUGH DATA"
    kills = int(favorite.get("kills", 0) or 0)
    suffix = "KILL" if kills == 1 else "KILLS"
    return f"{label}: {favorite.get('name', 'UNKNOWN')} ({kills} {suffix})"


async def _send_stats_message(player_id: str, player_name: str) -> None:
    if not JOIN_MESSAGE_ENABLED:
        return
    row = _player_row(player_id)
    if not row:
        return

    message = (
        "[ 1ST M.I. SERVER STATS ]\n\n"
        f"WELCOME, {player_name or row['player_name']}\n\n"
        "YOUR TRACKED SERVER TOTALS\n"
        f"KILLS: {row['kills']}\n"
        f"DEATHS: {row['deaths']}\n"
        f"REVIVES: {row['revives']}\n\n"
        f"{_favorite_line('FAVOURITE WEAPON', row.get('favorite_weapon'))}\n"
        f"{_favorite_line('FAVOURITE VEHICLE', row.get('favorite_vehicle'))}\n\n"
        "GOOD LUCK, TROOPER."
    )
    await _call(_client().message_player(player_id, message))
    with _connect_db() as db:
        db.execute(
            "UPDATE players SET last_join_message_at = ? WHERE player_id = ?",
            (_iso(), player_id),
        )


async def _update_current_players() -> dict[str, tuple[str, int, int]]:
    response = await _call(_client().get_players())
    players = list(getattr(response, "players", []) or [])
    current: dict[str, tuple[str, int, int]] = {}
    for player in players:
        player_id = str(getattr(player, "id", "") or "").strip()
        if not player_id:
            continue
        player_name = str(getattr(player, "name", "") or player_id).strip()
        kills, deaths = _current_match_stats(player)
        _accumulate_snapshot(player_id, player_name, kills, deaths)
        current[player_id] = (player_name, kills, deaths)
    return current


async def _process_admin_logs(current_players: dict[str, tuple[str, int, int]]) -> None:
    response = await _call(_client().get_admin_log(seconds_span=LOG_WINDOW_SECONDS))
    entries = list(getattr(response, "entries", []) or [])

    for entry in entries:
        if not _claim_event(entry):
            continue

        revive = _revive_actor(entry)
        if revive:
            _record_revive(*revive)

        _record_kill_preference(entry)

        if not _entry_is_connect(entry):
            continue

        entry_time = _entry_time(entry)
        # Do not fire stale join messages for players whose CONNECT event pre-dates
        # this tracker process. This prevents mass PMs whenever Railway redeploys.
        if entry_time and entry_time < runtime.started_at - timedelta(seconds=2):
            continue

        player_id = str(getattr(entry, "player_id", "") or "").strip()
        player_name = str(getattr(entry, "player_name", "") or player_id).strip()
        if not player_id:
            continue

        _upsert_player(player_id, player_name)
        # Only PM if the player is still online by the time the event is processed.
        if player_id in current_players:
            try:
                await _send_stats_message(player_id, player_name)
            except Exception as exc:
                logger.warning("Could not send join stats to %s (%s): %s", player_name, player_id, exc)


def _cleanup_old_event_keys() -> None:
    cutoff = (_utcnow() - timedelta(days=2)).isoformat()
    with _connect_db() as db:
        db.execute("DELETE FROM processed_events WHERE seen_at < ?", (cutoff,))


async def _tracker_tick() -> None:
    if state.client is None or not state.client.is_connected():
        return
    current = await _update_current_players()
    await _process_admin_logs(current)
    runtime.last_poll_at = _utcnow()
    runtime.poll_count += 1
    if runtime.poll_count % max(1, int(3600 / POLL_SECONDS)) == 0:
        _cleanup_old_event_keys()


async def _tracker_loop() -> None:
    logger.info(
        "Player stats tracker started: db=%s poll=%ss log_window=%ss join_message=%s",
        DB_PATH,
        POLL_SECONDS,
        LOG_WINDOW_SECONDS,
        JOIN_MESSAGE_ENABLED,
    )
    while True:
        try:
            await _tracker_tick()
            runtime.last_error = None
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            runtime.last_error = str(exc)
            logger.warning("Player stats tracker poll failed: %s", exc)
        await asyncio.sleep(POLL_SECONDS)


@app.on_event("startup")
async def start_player_stats_tracker() -> None:
    if not TRACKER_ENABLED:
        logger.info("Player stats tracker disabled by PLAYER_STATS_TRACKER")
        return
    _init_db()
    runtime.task = asyncio.create_task(_tracker_loop(), name="hllv-player-stats-tracker")


@app.on_event("shutdown")
async def stop_player_stats_tracker() -> None:
    if runtime.task:
        runtime.task.cancel()
        try:
            await runtime.task
        except asyncio.CancelledError:
            pass
        runtime.task = None


@app.get("/api/v2/player-stats/status")
async def player_stats_status() -> dict[str, Any]:
    total_players = 0
    if TRACKER_ENABLED:
        _init_db()
        with _connect_db() as db:
            total_players = int(db.execute("SELECT COUNT(*) FROM players").fetchone()[0])
    return {
        "enabled": TRACKER_ENABLED,
        "database": str(DB_PATH),
        "poll_seconds": POLL_SECONDS,
        "log_window_seconds": LOG_WINDOW_SECONDS,
        "join_message": JOIN_MESSAGE_ENABLED,
        "tracked_players": total_players,
        "favorite_weapon_basis": "enemy kills with non-vehicle weapons",
        "favorite_vehicle_basis": "enemy kills with vehicle-associated weapons/roadkills",
        "last_poll_at": runtime.last_poll_at.isoformat() if runtime.last_poll_at else None,
        "last_error": runtime.last_error,
    }


@app.get("/api/v2/player-stats")
async def player_stats(limit: int = Query(default=100, ge=1, le=1000)) -> dict[str, Any]:
    if not TRACKER_ENABLED:
        raise HTTPException(status_code=503, detail="Player stats tracker is disabled")
    _init_db()
    with _connect_db() as db:
        rows = db.execute(
            "SELECT player_id, player_name, kills, deaths, revives, first_seen, last_seen "
            "FROM players ORDER BY kills DESC, revives DESC, deaths ASC LIMIT ?",
            (limit,),
        ).fetchall()

    players: list[dict[str, Any]] = []
    for raw_row in rows:
        row = dict(raw_row)
        favorite_weapon, favorite_vehicle = _favorite_rows(str(row["player_id"]))
        row["favorite_weapon"] = favorite_weapon
        row["favorite_vehicle"] = favorite_vehicle
        players.append(row)
    return {"players": players}


@app.get("/api/v2/player-stats/{player_id}")
async def player_stats_for_player(player_id: str) -> dict[str, Any]:
    if not TRACKER_ENABLED:
        raise HTTPException(status_code=503, detail="Player stats tracker is disabled")
    row = _player_row(player_id)
    if not row:
        raise HTTPException(status_code=404, detail="No tracked stats for this player yet")
    return row
