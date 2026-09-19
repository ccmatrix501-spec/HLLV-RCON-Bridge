from __future__ import annotations

from typing import Any
from fastapi import HTTPException, Request
from app import _call, _client, _dump, _ok
from match_leaderboard_api import app

async def _invoke(names: tuple[str, ...], *args: Any) -> Any:
    client = _client()
    for name in names:
        fn = getattr(client, name, None)
        if callable(fn):
            return await _call(fn(*args))
    raise HTTPException(status_code=501, detail=f"Current HLL:V RCON library does not expose: {', '.join(names)}")

@app.get("/api/v2/admin-groups")
async def admin_groups(): return _dump(await _invoke(("get_admin_groups",)))

@app.get("/api/v2/server-information")
async def server_information(): return _dump(await _invoke(("get_server_information","get_server_info")))

@app.get("/api/v2/players/{player_id}/details")
async def player_details(player_id: str): return _dump(await _invoke(("get_player","get_player_info","get_player_information"), player_id))

@app.post("/api/v2/platoon/remove-player")
async def remove_player_from_platoon(request: Request):
    b=await request.json(); pid=str(b.get("player_id","")).strip(); reason=str(b.get("reason","")).strip() or "Removed by server administration"
    if not pid: raise HTTPException(400,"player_id is required")
    return _ok(result=_dump(await _invoke(("remove_player_from_platoon","remove_player_from_squad"),pid,reason)))

@app.post("/api/v2/platoon/disband")
async def disband_platoon(request: Request):
    b=await request.json()
    return _ok(result=_dump(await _invoke(("disband_platoon","disband_squad"),int(b.get("team_index")),int(b.get("squad_index")),str(b.get("reason","")).strip() or "Disbanded by server administration")))

@app.post("/api/v2/match-timer")
async def set_match_timer(request: Request):
    b=await request.json(); mode=str(b.get("game_mode","")).strip(); minutes=int(b.get("minutes"))
    if not mode or minutes < 1: raise HTTPException(400,"game_mode and positive minutes are required")
    return _ok(result=_dump(await _invoke(("set_match_timer","set_match_timer_override"),mode,minutes)))

@app.delete("/api/v2/match-timer/{game_mode}")
async def remove_match_timer(game_mode: str): return _ok(result=_dump(await _invoke(("remove_match_timer","remove_match_timer_override"),game_mode)))

@app.post("/api/v2/warmup-timer")
async def set_warmup_timer(request: Request):
    b=await request.json(); mode=str(b.get("game_mode","")).strip(); minutes=int(b.get("minutes"))
    if not mode or minutes < 0: raise HTTPException(400,"game_mode and minutes are required")
    return _ok(result=_dump(await _invoke(("set_warmup_timer","set_warmup_timer_override"),mode,minutes)))

@app.delete("/api/v2/warmup-timer/{game_mode}")
async def remove_warmup_timer(game_mode: str): return _ok(result=_dump(await _invoke(("remove_warmup_timer","remove_warmup_timer_override"),game_mode)))

@app.post("/api/v2/dynamic-weather")
async def dynamic_weather(request: Request):
    b=await request.json(); map_id=str(b.get("map_id","")).strip(); enabled=bool(b.get("enabled"))
    if not map_id: raise HTTPException(400,"map_id is required")
    return _ok(result=_dump(await _invoke(("set_dynamic_weather_enabled",),map_id,enabled)))

@app.post("/api/v2/map-rotation/item")
async def add_rotation_item(request: Request):
    b=await request.json(); map_name=str(b.get("map_name","")).strip(); index=int(b.get("index",0))
    if not map_name: raise HTTPException(400,"map_name is required")
    return _ok(result=_dump(await _invoke(("add_map_to_rotation",),map_name,index)))

@app.delete("/api/v2/map-rotation/item/{index}")
async def remove_rotation_item(index: int): return _ok(result=_dump(await _invoke(("remove_map_from_rotation",),index)))

@app.post("/api/v2/rcon-password")
async def change_rcon_password(request: Request):
    b=await request.json(); password=str(b.get("password",""))
    if len(password) < 8: raise HTTPException(400,"New RCON password must be at least 8 characters")
    return _ok(result=_dump(await _invoke(("set_rcon_password","change_password","set_password"),password)))
