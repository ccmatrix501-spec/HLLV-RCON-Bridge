from __future__ import annotations

import asyncio
import logging
import os
import re
from datetime import datetime, timezone
from typing import Any

import match_leaderboard_api as match_board
import stats_commands_api as commands
import stats_tracker_api as tracker
from app import _call, _client, state

logger = logging.getLogger("hllv-rcon-bridge.revives")
app = tracker.app

BACKFILL_SECONDS = max(300, min(86400, int(os.getenv("PLAYER_STATS_REVIVE_BACKFILL_SECONDS", "21600"))))
BACKFILL_RETRY_SECONDS = max(2.0, float(os.getenv("PLAYER_STATS_REVIVE_BACKFILL_RETRY_SECONDS", "5")))

# Current hllrcon releases do not declare a revive field in HLLV player stats.
# Pydantic keeps unknown response fields by default, though, so probe common names
# in case the live HLL:V server/API exposes one before the library catches up.
_DIRECT_REVIVE_KEYS = {
    "revives",
    "revivecount",
    "revivescount",
    "playerrevives",
    "playersrevived",
    "revivesperformed",
}

_PLAYER_TOKEN = r"(?P<name>.+?)\((?:Allies|Axis)/(?P<id>\d{17}|[\da-fA-F]{32})\)"
_REVIVE_PREFIX_RE = re.compile(
    rf"^(?:(?:PLAYER|MEDIC)\s+)?REVIV(?:E|ED)\s*:?\s*{_PLAYER_TOKEN}",
    flags=re.IGNORECASE,
)
_REVIVE_ACTOR_THEN_RE = re.compile(
    rf"^{_PLAYER_TOKEN}\s+(?:REVIVED|REVIVES|REVIVE)\b",
    flags=re.IGNORECASE,
)
_REVIVE_BY_RE = re.compile(
    rf"\bREVIV(?:E|ED|ES|ING)\b.*?\bBY\s+{_PLAYER_TOKEN}",
    flags=re.IGNORECASE,
)
_REVIVE_WORD_RE = re.compile(r"\bREVIV(?:E|ED|ES|ING)\b", flags=re.IGNORECASE)


class ReviveRuntime:
    def __init__(self) -> None:
        self.backfill_task: asyncio.Task[Any] | None = None
        self.backfill_done = False
        self.backfill_reason: str | None = None
        self.last_error: str | None = None
        self.last_direct_field_at: datetime | None = None
        self.last_log_revive_at: datetime | None = None
        self.direct_field_names: set[str] = set()
        self.unparsed_revive_like = 0
        self.unparsed_event_keys: set[str] = set()


runtime = ReviveRuntime()

_ORIGINAL_UPDATE_CURRENT_PLAYERS = tracker._update_current_players
_ORIGINAL_COMMAND_MATCH_REVIVES = commands._match_revives
_ORIGINAL_RESET_MATCH_REVIVES = commands._reset_match_revives


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _iso(value: datetime | None = None) -> str:
    return (value or _utcnow()).isoformat()


def _normalise_key(value: Any) -> str:
    return re.sub(r"[^a-z0-9]", "", str(value or "").lower())


def _init_revive_db() -> None:
    tracker._init_db()
    with tracker._connect_db() as db:
        db.executescript(
            """
            CREATE TABLE IF NOT EXISTS revive_event_ledger (
                event_key TEXT PRIMARY KEY,
                player_id TEXT NOT NULL,
                player_name TEXT NOT NULL,
                source TEXT NOT NULL,
                seen_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS revive_live_snapshots (
                player_id TEXT PRIMARY KEY,
                current_revives INTEGER NOT NULL DEFAULT 0,
                source_field TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_revive_event_seen
            ON revive_event_ledger(seen_at);
            """
        )


def _mapping(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return dict(value)
    if hasattr(value, "model_dump"):
        for kwargs in ({"by_alias": True}, {}):
            try:
                dumped = value.model_dump(**kwargs)
                if isinstance(dumped, dict):
                    return dumped
            except Exception:
                pass
    try:
        return dict(getattr(value, "__dict__", {}) or {})
    except Exception:
        return {}


def _direct_revive_value(player: Any) -> tuple[int, str] | None:
    containers = [getattr(player, "stats", None), player]
    for container in containers:
        data = _mapping(container)
        for key, raw in data.items():
            if _normalise_key(key) not in _DIRECT_REVIVE_KEYS:
                continue
            try:
                value = int(raw)
            except (TypeError, ValueError):
                continue
            if value < 0:
                continue
            return value, str(key)

        # model_dump normally catches declared + extra fields, but also try direct
        # attributes for forward compatibility with non-Pydantic response objects.
        for key in ("revives", "revive_count", "revives_count", "players_revived", "player_revives"):
            if not hasattr(container, key):
                continue
            try:
                value = int(getattr(container, key))
            except (TypeError, ValueError):
                continue
            if value >= 0:
                return value, key
    return None


def _event_key(entry: Any) -> str:
    return tracker._event_key(entry)


def _detect_revive_actor(entry: Any) -> tuple[str, str] | None:
    # If HLL:V exposes a revive as a structured weapon/action, the instigator is
    # the reviving player. Both US "REVIVE" and NVA "Revive" normalise here.
    weapon_id = str(getattr(entry, "weapon_id", "") or "").strip()
    if "REVIVE" in weapon_id.upper():
        player_id = str(getattr(entry, "instigator_id", "") or "").strip()
        player_name = str(getattr(entry, "instigator_name", "") or player_id).strip()
        if player_id:
            return player_id, player_name

    raw = str(getattr(entry, "raw_message", "") or "").strip()
    type_name = type(entry).__name__.lower()
    revive_like = bool(_REVIVE_WORD_RE.search(raw)) or "revive" in type_name
    if not revive_like:
        return None

    # Support a future dedicated parser/event without requiring another bridge
    # release. Prefer explicit reviver/instigator fields before generic player_id.
    for id_attr, name_attr in (
        ("reviver_id", "reviver_name"),
        ("instigator_id", "instigator_name"),
        ("actor_id", "actor_name"),
    ):
        player_id = str(getattr(entry, id_attr, "") or "").strip()
        if player_id:
            player_name = str(getattr(entry, name_attr, "") or player_id).strip()
            return player_id, player_name

    # Conservative raw-log patterns. Do not guess from a victim-only revive line;
    # a wrong revive owner is worse than leaving the event unresolved.
    for pattern in (_REVIVE_PREFIX_RE, _REVIVE_ACTOR_THEN_RE, _REVIVE_BY_RE):
        match = pattern.search(raw)
        if match:
            return match.group("id"), match.group("name").strip()

    if "revive" in type_name:
        player_id = str(getattr(entry, "player_id", "") or "").strip()
        if player_id:
            player_name = str(getattr(entry, "player_name", "") or player_id).strip()
            return player_id, player_name

    key = _event_key(entry)
    if key not in runtime.unparsed_event_keys:
        runtime.unparsed_event_keys.add(key)
        runtime.unparsed_revive_like += 1
        logger.warning(
            "Unparsed revive-like HLL:V admin log (%s): %s",
            type(entry).__name__,
            raw[:500],
        )
    return None


def _claim_revive_event(entry: Any, player_id: str, player_name: str, source: str) -> bool:
    _init_revive_db()
    key = _event_key(entry)
    with tracker._connect_db() as db:
        try:
            db.execute(
                "INSERT INTO revive_event_ledger(event_key, player_id, player_name, source, seen_at) VALUES(?, ?, ?, ?, ?)",
                (key, player_id, player_name or player_id, source, _iso()),
            )
            return True
        except Exception as exc:
            # Duplicate event keys are expected while polling overlapping log windows.
            if "UNIQUE constraint failed" in str(exc):
                return False
            raise


def _mark_generic_processed(entry: Any) -> None:
    with tracker._connect_db() as db:
        db.execute(
            "INSERT OR IGNORE INTO processed_events(event_key, seen_at) VALUES(?, ?)",
            (_event_key(entry), _iso()),
        )


def _has_direct_snapshot(player_id: str) -> bool:
    _init_revive_db()
    with tracker._connect_db() as db:
        row = db.execute(
            "SELECT 1 FROM revive_live_snapshots WHERE player_id = ? LIMIT 1",
            (player_id,),
        ).fetchone()
    return bool(row)


def _tracker_revive_actor(entry: Any) -> tuple[str, str] | None:
    actor = _detect_revive_actor(entry)
    if not actor:
        return None
    player_id, player_name = actor

    # A direct player-stat counter is preferable because it is an exact match
    # counter. Once available for this player, admin logs remain diagnostics only.
    if _has_direct_snapshot(player_id):
        return None
    if not _claim_revive_event(entry, player_id, player_name, "admin_log"):
        return None
    runtime.last_log_revive_at = _utcnow()
    return actor


def _read_current_match_log_revives(db: Any, player_id: str) -> int:
    try:
        row = db.execute(
            "SELECT revives FROM current_match_revives WHERE player_id = ?",
            (player_id,),
        ).fetchone()
        return max(0, int(row[0])) if row else 0
    except Exception:
        return 0


def _apply_direct_revive_observations(observations: list[tuple[str, str, int, str]]) -> None:
    if not observations:
        return
    _init_revive_db()
    now = _iso()
    with tracker._connect_db() as db:
        for player_id, player_name, current_revives, source_field in observations:
            safe_name = player_name.strip() or player_id
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

            previous = db.execute(
                "SELECT current_revives FROM revive_live_snapshots WHERE player_id = ?",
                (player_id,),
            ).fetchone()

            if previous is None:
                # If admin-log fallback already saw revives in this match, import
                # only the difference so switching to a newly exposed exact field
                # cannot double count the same revive.
                fallback_match = _read_current_match_log_revives(db, player_id)
                delta = max(0, current_revives - fallback_match)
            else:
                old_value = max(0, int(previous[0]))
                delta = current_revives - old_value if current_revives >= old_value else current_revives

            if delta:
                db.execute(
                    "UPDATE players SET revives = revives + ?, last_seen = ? WHERE player_id = ?",
                    (max(0, delta), now, player_id),
                )

            db.execute(
                """
                INSERT INTO revive_live_snapshots(player_id, current_revives, source_field, updated_at)
                VALUES(?, ?, ?, ?)
                ON CONFLICT(player_id) DO UPDATE SET
                    current_revives = excluded.current_revives,
                    source_field = excluded.source_field,
                    updated_at = excluded.updated_at
                """,
                (player_id, current_revives, source_field, now),
            )
            runtime.direct_field_names.add(source_field)

    runtime.last_direct_field_at = _utcnow()


async def _patched_update_current_players() -> dict[str, tuple[str, int, int]]:
    # Keep the original tracker behaviour, but inspect the same get_players()
    # response for a forward-compatible revive counter so no extra polling request
    # is added to the RCON bridge.
    response = await _call(_client().get_players())
    players = list(getattr(response, "players", []) or [])
    current: dict[str, tuple[str, int, int]] = {}
    revive_observations: list[tuple[str, str, int, str]] = []

    for player in players:
        player_id = str(getattr(player, "id", "") or "").strip()
        if not player_id:
            continue
        player_name = str(getattr(player, "name", "") or player_id).strip()
        kills, deaths = tracker._current_match_stats(player)
        tracker._accumulate_snapshot(player_id, player_name, kills, deaths)
        current[player_id] = (player_name, kills, deaths)

        direct = _direct_revive_value(player)
        if direct is not None:
            revive_observations.append((player_id, player_name, direct[0], direct[1]))

    _apply_direct_revive_observations(revive_observations)
    return current


def _effective_match_revives(player_id: str) -> int:
    _init_revive_db()
    with tracker._connect_db() as db:
        row = db.execute(
            "SELECT current_revives FROM revive_live_snapshots WHERE player_id = ?",
            (player_id,),
        ).fetchone()
    if row:
        return max(0, int(row[0]))
    return max(0, int(_ORIGINAL_COMMAND_MATCH_REVIVES(player_id) or 0))


def _patched_reset_match_revives(match_key: str | None = None) -> None:
    _ORIGINAL_RESET_MATCH_REVIVES(match_key)
    _init_revive_db()
    with tracker._connect_db() as db:
        db.execute("DELETE FROM revive_live_snapshots")


async def _backfill_recent_revives() -> None:
    # Wait for the persistent RCON keeper/manual connection rather than creating a
    # connection of our own. This runs once per bridge process.
    while True:
        if state.client is not None and state.client.is_connected():
            break
        await asyncio.sleep(BACKFILL_RETRY_SECONDS)

    try:
        # First inspect live player payloads. If HLL:V has gained a direct revive
        # field, use that exact source and skip speculative log backfill.
        response = await _call(_client().get_players())
        players = list(getattr(response, "players", []) or [])
        observations: list[tuple[str, str, int, str]] = []
        for player in players:
            player_id = str(getattr(player, "id", "") or "").strip()
            if not player_id:
                continue
            direct = _direct_revive_value(player)
            if direct is None:
                continue
            player_name = str(getattr(player, "name", "") or player_id).strip()
            observations.append((player_id, player_name, direct[0], direct[1]))
        _apply_direct_revive_observations(observations)
        if observations:
            runtime.backfill_done = True
            runtime.backfill_reason = "direct_player_stats_field"
            logger.info("Revive tracking is using a direct HLL:V player-stat field: %s", sorted(runtime.direct_field_names))
            return

        # The previous implementation stored no per-revive event ledger. Only
        # backfill historical admin logs when all saved revive totals are zero;
        # otherwise replaying old events could duplicate legitimate old counts.
        _init_revive_db()
        with tracker._connect_db() as db:
            saved_revives = int(db.execute("SELECT COALESCE(SUM(revives), 0) FROM players").fetchone()[0])
        if saved_revives > 0:
            runtime.backfill_done = True
            runtime.backfill_reason = "skipped_existing_revive_totals"
            return

        logs = await _call(_client().get_admin_log(seconds_span=BACKFILL_SECONDS))
        entries = list(getattr(logs, "entries", []) or [])
        recovered = 0
        for entry in entries:
            actor = _detect_revive_actor(entry)
            if not actor:
                continue
            if not _claim_revive_event(entry, actor[0], actor[1], "startup_backfill"):
                continue
            # Prevent the normal lifetime tracker from immediately recording the
            # same event again if its overlapping log window sees it after us.
            _mark_generic_processed(entry)
            tracker._record_revive(*actor)
            recovered += 1

        runtime.backfill_done = True
        runtime.backfill_reason = f"admin_log_backfill_{recovered}_events"
        if recovered:
            runtime.last_log_revive_at = _utcnow()
        logger.info("Revive backfill scanned %ss of logs and recovered %s event(s)", BACKFILL_SECONDS, recovered)
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        runtime.last_error = str(exc)
        runtime.backfill_done = True
        runtime.backfill_reason = "backfill_failed"
        logger.warning("Revive backfill failed: %s", exc)


def _db_diagnostics() -> dict[str, Any]:
    _init_revive_db()
    with tracker._connect_db() as db:
        recorded = int(db.execute("SELECT COUNT(*) FROM revive_event_ledger").fetchone()[0])
        direct_players = int(db.execute("SELECT COUNT(*) FROM revive_live_snapshots").fetchone()[0])
        total_saved = int(db.execute("SELECT COALESCE(SUM(revives), 0) FROM players").fetchone()[0])
        sources = {
            str(row[0]): int(row[1])
            for row in db.execute(
                "SELECT source, COUNT(*) FROM revive_event_ledger GROUP BY source ORDER BY source"
            ).fetchall()
        }
    return {
        "saved_server_revives": total_saved,
        "recorded_log_events": recorded,
        "log_event_sources": sources,
        "players_with_direct_counter": direct_players,
    }


@app.get("/api/v2/revive-tracking/status")
async def revive_tracking_status() -> dict[str, Any]:
    db_info = _db_diagnostics()
    direct = db_info["players_with_direct_counter"] > 0
    return {
        "ok": True,
        "source": "direct_player_stats_field" if direct else "admin_logs_best_effort",
        "direct_revive_field_detected": direct,
        "direct_field_names": sorted(runtime.direct_field_names),
        "hllrcon_declared_revive_field": False,
        "admin_log_revive_parser_is_best_effort": not direct,
        "backfill_seconds": BACKFILL_SECONDS,
        "backfill_done": runtime.backfill_done,
        "backfill_reason": runtime.backfill_reason,
        "unparsed_revive_like_events_this_process": runtime.unparsed_revive_like,
        "last_direct_field_at": runtime.last_direct_field_at.isoformat() if runtime.last_direct_field_at else None,
        "last_log_revive_at": runtime.last_log_revive_at.isoformat() if runtime.last_log_revive_at else None,
        "last_error": runtime.last_error,
        **db_info,
    }


# Install the patches after the stats/leaderboard modules have registered. Python
# function globals are resolved at call time, so the lifetime worker starts using
# these functions immediately without another polling worker.
tracker._revive_actor = _tracker_revive_actor
tracker._update_current_players = _patched_update_current_players
commands._revive_actor = _detect_revive_actor
commands._match_revives = _effective_match_revives
commands._reset_match_revives = _patched_reset_match_revives
match_board._match_revives = _effective_match_revives


@app.on_event("startup")
async def start_revive_tracking_patch() -> None:
    _init_revive_db()
    runtime.backfill_task = asyncio.create_task(_backfill_recent_revives(), name="hllv-revive-backfill")


@app.on_event("shutdown")
async def stop_revive_tracking_patch() -> None:
    if runtime.backfill_task and not runtime.backfill_task.done():
        runtime.backfill_task.cancel()
        try:
            await runtime.backfill_task
        except asyncio.CancelledError:
            pass
    runtime.backfill_task = None
