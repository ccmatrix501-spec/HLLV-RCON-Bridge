from __future__ import annotations

from typing import Any

from fastapi import HTTPException, Request

from stats_tracker_api import app
from app import _call, _client, _ok


@app.delete("/api/v2/vips")
async def remove_vip(request: Request) -> dict[str, Any]:
    body = await request.json()
    player_id = str(body.get("player_id", "")).strip()
    if not player_id:
        raise HTTPException(status_code=400, detail="player_id is required")

    await _call(_client().remove_vip(player_id))
    return _ok(player_id=player_id, removed="vip")


@app.delete("/api/v2/admins")
async def remove_admin(request: Request) -> dict[str, Any]:
    body = await request.json()
    player_id = str(body.get("player_id", "")).strip()
    if not player_id:
        raise HTTPException(status_code=400, detail="player_id is required")

    await _call(_client().remove_admin(player_id))
    return _ok(player_id=player_id, removed="admin")
