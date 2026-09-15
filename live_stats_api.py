from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from fastapi import Query

from message_everyone_api import app
from stats_tracker_api import TRACKER_ENABLED, _connect_db, _favorite_rows, _init_db


@app.get("/api/v2/public-player-stats-live")
async def public_player_stats_live(
    active_seconds: int = Query(default=20, ge=5, le=120),
    limit: int = Query(default=250, ge=1, le=500),
) -> dict[str, Any]:
    """Return only players whose stats have been touched recently.

    The normal public endpoint can contain thousands of historical players. This
    smaller feed is intended for frequent website polling so current in-game
    kills/deaths/revives can update without retransmitting the entire database.
    """
    if not TRACKER_ENABLED:
        return {"players": [], "active_players": 0, "updated_at": datetime.now(timezone.utc).isoformat()}

    _init_db()
    cutoff = (datetime.now(timezone.utc) - timedelta(seconds=active_seconds)).isoformat()

    with _connect_db() as db:
        rows = db.execute(
            """
            SELECT player_id, player_name, kills, deaths, revives, first_seen, last_seen
            FROM players
            WHERE last_seen >= ?
            ORDER BY last_seen DESC, kills DESC, player_name COLLATE NOCASE ASC
            LIMIT ?
            """,
            (cutoff, limit),
        ).fetchall()

    players: list[dict[str, Any]] = []
    for raw in rows:
        row = dict(raw)
        favorite_weapon, favorite_vehicle = _favorite_rows(str(row["player_id"]))
        players.append(
            {
                "player_id": row["player_id"],
                "player_name": row["player_name"],
                "kills": int(row["kills"] or 0),
                "deaths": int(row["deaths"] or 0),
                "revives": int(row["revives"] or 0),
                "first_seen": row["first_seen"],
                "last_seen": row["last_seen"],
                "favorite_weapon": favorite_weapon,
                "favorite_vehicle": favorite_vehicle,
            }
        )

    return {
        "players": players,
        "active_players": len(players),
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
