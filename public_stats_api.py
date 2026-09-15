from __future__ import annotations

from typing import Any

from fastapi import Query

from message_everyone_api import app
from stats_tracker_api import TRACKER_ENABLED, _connect_db, _init_db


@app.get("/api/v2/public-player-stats")
async def public_player_stats(
    limit: int = Query(default=10000, ge=1, le=10000),
    search: str = Query(default="", max_length=120),
) -> dict[str, Any]:
    """Read-only player stats optimized for the public website.

    This endpoint can return every tracked player (up to 10,000) in one request,
    while calculating favourite weapon/vehicle data with window functions instead
    of opening separate SQLite connections for every player.
    """
    if not TRACKER_ENABLED:
        return {"players": [], "tracked_players": 0, "matched_players": 0}

    _init_db()
    needle = search.strip()
    where_sql = ""
    params: list[Any] = []
    if needle:
        where_sql = "WHERE LOWER(p.player_name) LIKE LOWER(?)"
        params.append(f"%{needle}%")

    with _connect_db() as db:
        tracked_players = int(db.execute("SELECT COUNT(*) FROM players").fetchone()[0])
        if needle:
            matched_players = int(
                db.execute(
                    "SELECT COUNT(*) FROM players WHERE LOWER(player_name) LIKE LOWER(?)",
                    (f"%{needle}%",),
                ).fetchone()[0]
            )
        else:
            matched_players = tracked_players

        rows = db.execute(
            f"""
            WITH ranked_weapons AS (
                SELECT
                    player_id,
                    weapon_id,
                    weapon_name,
                    kills,
                    ROW_NUMBER() OVER (
                        PARTITION BY player_id
                        ORDER BY kills DESC, last_used DESC, weapon_name ASC
                    ) AS rn
                FROM weapon_kills
            ),
            ranked_vehicles AS (
                SELECT
                    player_id,
                    vehicle_id,
                    vehicle_name,
                    kills,
                    ROW_NUMBER() OVER (
                        PARTITION BY player_id
                        ORDER BY kills DESC, last_used DESC, vehicle_name ASC
                    ) AS rn
                FROM vehicle_kills
            )
            SELECT
                p.player_id,
                p.player_name,
                p.kills,
                p.deaths,
                p.revives,
                p.first_seen,
                p.last_seen,
                rw.weapon_id AS favorite_weapon_id,
                rw.weapon_name AS favorite_weapon_name,
                rw.kills AS favorite_weapon_kills,
                rv.vehicle_id AS favorite_vehicle_id,
                rv.vehicle_name AS favorite_vehicle_name,
                rv.kills AS favorite_vehicle_kills
            FROM players p
            LEFT JOIN ranked_weapons rw
                ON rw.player_id = p.player_id AND rw.rn = 1
            LEFT JOIN ranked_vehicles rv
                ON rv.player_id = p.player_id AND rv.rn = 1
            {where_sql}
            ORDER BY p.kills DESC, p.revives DESC, p.deaths ASC, p.player_name COLLATE NOCASE ASC
            LIMIT ?
            """,
            (*params, limit),
        ).fetchall()

    players: list[dict[str, Any]] = []
    for raw in rows:
        row = dict(raw)
        weapon = None
        if row.get("favorite_weapon_name"):
            weapon = {
                "id": row.get("favorite_weapon_id") or "",
                "name": row.get("favorite_weapon_name") or "",
                "kills": int(row.get("favorite_weapon_kills") or 0),
            }
        vehicle = None
        if row.get("favorite_vehicle_name"):
            vehicle = {
                "id": row.get("favorite_vehicle_id") or "",
                "name": row.get("favorite_vehicle_name") or "",
                "kills": int(row.get("favorite_vehicle_kills") or 0),
            }

        players.append(
            {
                "player_id": row["player_id"],
                "player_name": row["player_name"],
                "kills": int(row["kills"] or 0),
                "deaths": int(row["deaths"] or 0),
                "revives": int(row["revives"] or 0),
                "first_seen": row["first_seen"],
                "last_seen": row["last_seen"],
                "favorite_weapon": weapon,
                "favorite_vehicle": vehicle,
            }
        )

    return {
        "players": players,
        "tracked_players": tracked_players,
        "matched_players": matched_players,
    }
