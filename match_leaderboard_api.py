from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import sqlite3
from datetime import UTC, datetime, timedelta
from typing import Any

import leaderboard_api as lifetime_leaderboard
from leaderboard_api import app
from app import _call, _client, state
from stats_commands_api import _match_revives
from stats_tracker_api import _connect_db, _current_match_stats, _init_db

try:
    from hllrcon.admin_logs import HLLVPlayerSendMessageAdminLog
except ImportError:  # pragma: no cover
    HLLVPlayerSendMessageAdminLog = ()  # type: ignore[assignment,misc]

# !leaderboard is now reserved for the server-wide CURRENT MATCH board.
# Keep !topstats / !topkills / !toprevives / !topkd as private lifetime-stat commands.
lifetime_leaderboard.COMMANDS.pop("!leaderboard", None)

logger = logging.getLogger("hllv-rcon-bridge.match-leaderboard")

COMMAND = "!leaderboard"
POLL_SECONDS = max(1.0, float(os.getenv("MATCH_LEADERBOARD_POLL_SECONDS", "5")))
LOG_WINDOW_SECONDS = max(10, int(os.getenv("MATCH_LEADERBOARD_LOG_WINDOW_SECONDS", "30")))
CHAT_COOLDOWN_SECONDS = max(30, int(os.getenv("MATCH_LEADERBOARD_CHAT_COOLDOWN_SECONDS", "60")))
HALFTIME_CHECK_SECONDS = max(15.0, float(os.getenv("MATCH_LEADERBOARD_HALFTIME_CHECK_SECONDS", "30")))
HALFTIME_ENABLED = os.getenv("MATCH_LEADERBOARD_HALFTIME_ENABLED", "true").strip().lower() not in {"0", "false", "no", "off"}


class Runtime:
    def __init__(self) -> None:
        self.task: asyncio.Task[Any] | None = None
        self.started_at = datetime.now(UTC)
        self.last_poll_at: datetime | None = None
        self.last_error: str | None = None
        self.last_broadcast_at: datetime | None = None
        self.halftime_task: asyncio.Task[Any] | None = None
        self.match_key: str | None = None
        self.match_initial_seconds: int | None = None
        self.halftime_sent_for: str | None = None


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


def _init_match_leaderboard_db() -> None:
    _init_db()
    with _connect_db() as db:
        db.executescript(
            """
            CREATE TABLE IF NOT EXISTS match_leaderboard_command_events (
                event_key TEXT PRIMARY KEY,
                seen_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_match_leaderboard_command_events_seen
            ON match_leaderboard_command_events(seen_at);
            """
        )


def _claim_event(entry: Any) -> bool:
    with _connect_db() as db:
        try:
            db.execute(
                "INSERT INTO match_leaderboard_command_events(event_key, seen_at) VALUES(?, ?)",
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


def _faction_id(player: Any) -> int | None:
    value = getattr(player, "faction_id", None)
    if value is None:
        value = getattr(player, "factionId", None)

    if value is None:
        faction = getattr(player, "faction", None)
        if faction is not None:
            value = getattr(faction, "id", None)
            if value is None:
                value = getattr(faction, "value", None)

    if value is None:
        try:
            dump = player.model_dump(by_alias=True)
            value = dump.get("factionId", dump.get("faction_id"))
        except Exception:
            pass

    if hasattr(value, "value"):
        value = getattr(value, "value")
    try:
        return int(value)
    except (TypeError, ValueError):
        text = str(value or "").strip().lower()
        if text in {"us", "usa", "unitedstates", "united_states"}:
            return 1
        if text in {"nva", "northvietnam", "north_vietnam"}:
            return 6
        return None


def _kd_text(kills: int, deaths: int) -> str:
    kills = max(0, int(kills or 0))
    deaths = max(0, int(deaths or 0))
    if deaths == 0:
        return "INF" if kills > 0 else "0.00"
    return f"{kills / deaths:.2f}"


def _clean_name(value: Any, limit: int = 15) -> str:
    text = " ".join(str(value or "Unknown").split())
    return text[:limit]


def _row_line(index: int, row: dict[str, Any]) -> str:
    return (
        f"{index}. {_clean_name(row['name'])} "
        f"K{row['kills']} D{row['deaths']} KD{_kd_text(row['kills'], row['deaths'])} R{row['revives']}"
    )


def _sort_side(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        rows,
        key=lambda row: (
            -int(row["kills"]),
            -int(row["revives"]),
            int(row["deaths"]),
            str(row["name"]).casefold(),
        ),
    )[:5]


async def _current_match_snapshot() -> tuple[list[Any], list[dict[str, Any]], list[dict[str, Any]]]:
    response = await _call(_client().get_players())
    connected = list(getattr(response, "players", []) or [])
    us: list[dict[str, Any]] = []
    nva: list[dict[str, Any]] = []

    for player in connected:
        player_id = str(getattr(player, "id", "") or "").strip()
        if not player_id:
            continue
        name = str(getattr(player, "name", "") or player_id).strip()
        kills, deaths = _current_match_stats(player)
        row = {
            "player_id": player_id,
            "name": name,
            "kills": max(0, int(kills or 0)),
            "deaths": max(0, int(deaths or 0)),
            "revives": max(0, int(_match_revives(player_id) or 0)),
        }
        faction = _faction_id(player)
        if faction == 1:
            us.append(row)
        elif faction == 6:
            nva.append(row)

    return connected, _sort_side(us), _sort_side(nva)


def _build_message(us: list[dict[str, Any]], nva: list[dict[str, Any]]) -> str:
    lines = ["[ 1ST M.I. MATCH LEADERBOARD ]", "", "TOP 5 - US"]
    if us:
        lines.extend(_row_line(index, row) for index, row in enumerate(us, start=1))
    else:
        lines.append("NO US PLAYERS")

    lines += ["", "TOP 5 - NVA"]
    if nva:
        lines.extend(_row_line(index, row) for index, row in enumerate(nva, start=1))
    else:
        lines.append("NO NVA PLAYERS")

    lines += ["", "CURRENT MATCH - RANKED BY KILLS"]
    return "\n".join(lines)[:500]


async def _send_private(player_id: str, message: str) -> None:
    await _call(_client().message_player(player_id, message[:500]))


async def _broadcast_current_match_leaderboard() -> dict[str, Any]:
    connected, us, nva = await _current_match_snapshot()
    message = _build_message(us, nva)

    sent = 0
    failed = 0
    for player in connected:
        player_id = str(getattr(player, "id", "") or "").strip()
        if not player_id:
            continue
        try:
            await _send_private(player_id, message)
            sent += 1
        except Exception as exc:
            failed += 1
            logger.debug("Match leaderboard delivery failed for %s: %s", player_id, exc)

    runtime.last_broadcast_at = _utcnow()
    return {
        "ok": True,
        "sent": sent,
        "failed": failed,
        "message": message,
        "us": us,
        "nva": nva,
        "us_count": len(us),
        "nva_count": len(nva),
    }


async def _handle_chat_command(entry: Any) -> None:
    player_id = str(getattr(entry, "player_id", "") or "").strip()
    now = _utcnow()

    if runtime.last_broadcast_at is not None:
        remaining = CHAT_COOLDOWN_SECONDS - int((now - runtime.last_broadcast_at).total_seconds())
        if remaining > 0:
            if player_id:
                try:
                    await _send_private(
                        player_id,
                        f"[ 1ST M.I. MATCH LEADERBOARD ]\n\nPLEASE WAIT {remaining} SECONDS BEFORE BROADCASTING IT AGAIN.",
                    )
                except Exception:
                    pass
            return

    try:
        result = await _broadcast_current_match_leaderboard()
        logger.info(
            "Server-wide match leaderboard requested from chat: sent=%s failed=%s",
            result["sent"],
            result["failed"],
        )
    except Exception as exc:
        runtime.last_error = str(exc)
        logger.warning("Could not broadcast current match leaderboard: %s", exc)
        if player_id:
            try:
                await _send_private(
                    player_id,
                    "[ 1ST M.I. MATCH LEADERBOARD ]\n\nTHE LEADERBOARD IS TEMPORARILY UNAVAILABLE.",
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
        message = str(getattr(entry, "message", "") or "").strip().lower()
        if message != COMMAND:
            continue

        entry_time = _entry_time(entry)
        if entry_time and entry_time < runtime.started_at - timedelta(seconds=2):
            _claim_event(entry)
            continue
        if not _claim_event(entry):
            continue
        await _handle_chat_command(entry)

    runtime.last_poll_at = _utcnow()


def _cleanup_seen() -> None:
    cutoff = _iso(_utcnow() - timedelta(days=2))
    with _connect_db() as db:
        db.execute("DELETE FROM match_leaderboard_command_events WHERE seen_at < ?", (cutoff,))



def _duration_seconds(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, timedelta):
        return max(0, int(value.total_seconds()))
    text = str(value).strip().upper()
    if not text:
        return None
    if text.startswith("PT"):
        text = text[2:]
        hours = minutes = seconds = 0.0
        import re
        m = re.fullmatch(r"(?:(\d+(?:\.\d+)?)H)?(?:(\d+(?:\.\d+)?)M)?(?:(\d+(?:\.\d+)?)S)?", text)
        if m:
            hours = float(m.group(1) or 0)
            minutes = float(m.group(2) or 0)
            seconds = float(m.group(3) or 0)
            return max(0, int(hours * 3600 + minutes * 60 + seconds))
    try:
        return max(0, int(float(text)))
    except (TypeError, ValueError):
        return None


def _session_value(session: Any, *names: str) -> Any:
    for name in names:
        value = getattr(session, name, None)
        if value is not None and value != "":
            return value
    try:
        data = session.model_dump(by_alias=True)
    except Exception:
        data = {}
    for name in names:
        if name in data and data[name] not in (None, ""):
            return data[name]
    return None


async def _halftime_worker() -> None:
    """Broadcast once when the current match clock reaches half of its observed duration.

    This is deliberately a separate low-frequency worker. It only requests the
    server session every 30 seconds and performs the heavier player snapshot/send
    once per match, so it does not add another rapid player/log polling loop.
    """
    logger.info("Half-match leaderboard broadcaster started: enabled=%s check=%ss", HALFTIME_ENABLED, HALFTIME_CHECK_SECONDS)
    while True:
        try:
            if HALFTIME_ENABLED and state.client is not None and state.client.is_connected():
                session = await _call(_client().get_server_session())
                map_name = str(_session_value(session, "map_name", "mapName", "map", "map_id", "mapId") or "unknown")
                remaining = _duration_seconds(_session_value(
                    session, "remaining_match_time", "remainingMatchTime",
                    "remaining_time", "remainingTime", "time_remaining", "timeRemaining"
                ))
                if remaining is not None:
                    # A large upward jump or a map change means a new match.
                    if runtime.match_key is None or not runtime.match_key.startswith(map_name + "|") or (
                        runtime.match_initial_seconds is not None and remaining > runtime.match_initial_seconds + 120
                    ):
                        runtime.match_key = f"{map_name}|{_iso()}"
                        runtime.match_initial_seconds = remaining
                        runtime.halftime_sent_for = None
                    elif runtime.match_initial_seconds is None or remaining > runtime.match_initial_seconds:
                        runtime.match_initial_seconds = remaining

                    initial = runtime.match_initial_seconds or remaining
                    halfway = initial / 2
                    if (
                        runtime.match_key
                        and runtime.halftime_sent_for != runtime.match_key
                        and initial >= 600
                        and remaining <= halfway
                    ):
                        result = await _broadcast_current_match_leaderboard()
                        runtime.halftime_sent_for = runtime.match_key
                        logger.info(
                            "Half-match leaderboard broadcast: map=%s remaining=%ss initial=%ss sent=%s failed=%s",
                            map_name, remaining, initial, result["sent"], result["failed"]
                        )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning("Half-match leaderboard check failed: %s", exc)
        await asyncio.sleep(HALFTIME_CHECK_SECONDS)


async def _worker() -> None:
    logger.info(
        "Server-wide current match leaderboard command started: command=%s cooldown=%ss",
        COMMAND,
        CHAT_COOLDOWN_SECONDS,
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
            logger.warning("Match leaderboard worker failed: %s", exc)
        await asyncio.sleep(POLL_SECONDS)


@app.on_event("startup")
async def start_match_leaderboard_worker() -> None:
    _init_match_leaderboard_db()
    runtime.task = asyncio.create_task(_worker(), name="hllv-current-match-leaderboard")
    runtime.halftime_task = asyncio.create_task(_halftime_worker(), name="hllv-half-match-leaderboard")


@app.on_event("shutdown")
async def stop_match_leaderboard_worker() -> None:
    if runtime.task:
        runtime.task.cancel()
        try:
            await runtime.task
        except asyncio.CancelledError:
            pass
        runtime.task = None
    if runtime.halftime_task:
        runtime.halftime_task.cancel()
        try:
            await runtime.halftime_task
        except asyncio.CancelledError:
            pass
        runtime.halftime_task = None


@app.post("/api/v2/leaderboard/broadcast")
async def broadcast_match_leaderboard() -> dict[str, Any]:
    """Broadcast the current-match top five US and NVA players to everyone online."""
    return await _broadcast_current_match_leaderboard()


@app.get("/api/v2/leaderboard/match")
async def current_match_leaderboard() -> dict[str, Any]:
    """Return the current-match side-split leaderboard without broadcasting it."""
    connected, us, nva = await _current_match_snapshot()
    return {
        "connected_players": len(connected),
        "us": us,
        "nva": nva,
        "message": _build_message(us, nva),
        "ranked_by": "kills",
    }


@app.get("/api/v2/leaderboard/broadcast/status")
async def match_leaderboard_status() -> dict[str, Any]:
    return {
        "command": COMMAND,
        "scope": "server-wide",
        "stats_scope": "current-match",
        "top_per_side": 5,
        "ranked_by": "kills",
        "chat_cooldown_seconds": CHAT_COOLDOWN_SECONDS,
        "last_broadcast_at": runtime.last_broadcast_at.isoformat() if runtime.last_broadcast_at else None,
        "last_poll_at": runtime.last_poll_at.isoformat() if runtime.last_poll_at else None,
        "last_error": runtime.last_error,
        "halftime_enabled": HALFTIME_ENABLED,
        "halftime_check_seconds": HALFTIME_CHECK_SECONDS,
        "halftime_match_key": runtime.match_key,
        "halftime_initial_seconds": runtime.match_initial_seconds,
        "halftime_sent_for_current_match": bool(runtime.match_key and runtime.halftime_sent_for == runtime.match_key),
    }
