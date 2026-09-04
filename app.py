from __future__ import annotations

import asyncio
import logging
import os
from datetime import datetime, timezone
from typing import Any

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
from hllrcon import HLLVRcon
from hllrcon.exceptions import (
    RconAuthError,
    RconCommandError,
    RconConnectionClosedError,
    RconConnectionError,
    RconConnectionLostError,
    RconConnectionRefusedError,
    RconMessageError,
)
from hllrcon.responses import ForceMode

APP_VERSION = "0.1.0"
CONNECT_TIMEOUT = float(os.getenv("RCON_CONNECT_TIMEOUT", "35"))
COMMAND_TIMEOUT = float(os.getenv("RCON_COMMAND_TIMEOUT", "30"))

logging.basicConfig(
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("hllv-rcon-bridge")

app = FastAPI(
    title="1st M.I. HLL:V RCON Bridge",
    version=APP_VERSION,
    docs_url=None,
    redoc_url=None,
)


class BridgeState:
    def __init__(self) -> None:
        self.client: HLLVRcon | None = None
        self.host: str | None = None
        self.port: int | None = None
        self.connected_at: datetime | None = None
        self.lock = asyncio.Lock()

    def disconnect(self) -> None:
        if self.client is not None:
            try:
                self.client.disconnect()
            except Exception:
                logger.exception("Error while disconnecting RCON client")
        self.client = None
        self.host = None
        self.port = None
        self.connected_at = None


state = BridgeState()


def _error_detail(exc: Exception) -> tuple[int, str]:
    if isinstance(exc, RconAuthError):
        return 401, "RCON authentication failed. Check the RCON password."
    if isinstance(exc, RconConnectionRefusedError):
        return 502, "RCON connection was refused by the game server."
    if isinstance(exc, (RconConnectionLostError, RconConnectionClosedError)):
        return 502, "RCON connection was closed or lost."
    if isinstance(exc, RconConnectionError):
        return 502, f"RCON connection failed: {exc}"
    if isinstance(exc, RconCommandError):
        return 400, f"HLL:V rejected the RCON command: {exc}"
    if isinstance(exc, RconMessageError):
        return 502, f"Unexpected HLL:V RCON response: {exc}"
    if isinstance(exc, TimeoutError):
        return 504, "HLL:V RCON command timed out."
    if isinstance(exc, OSError):
        return 502, f"Network error while talking to HLL:V RCON: {exc}"
    return 500, f"RCON bridge error: {exc}"


@app.exception_handler(RconAuthError)
@app.exception_handler(RconConnectionRefusedError)
@app.exception_handler(RconConnectionLostError)
@app.exception_handler(RconConnectionClosedError)
@app.exception_handler(RconConnectionError)
@app.exception_handler(RconCommandError)
@app.exception_handler(RconMessageError)
async def handle_rcon_error(_: Request, exc: Exception) -> JSONResponse:
    status, detail = _error_detail(exc)
    logger.warning("RCON request failed: %s", detail)
    return JSONResponse(status_code=status, content={"error": detail})


@app.exception_handler(asyncio.TimeoutError)
async def handle_timeout(_: Request, exc: asyncio.TimeoutError) -> JSONResponse:
    status, detail = _error_detail(exc)
    logger.warning("RCON request timed out")
    return JSONResponse(status_code=status, content={"error": detail})


def _client() -> HLLVRcon:
    if state.client is None:
        raise HTTPException(status_code=409, detail="RCON is not connected")
    return state.client


async def _call(awaitable: Any, timeout: float = COMMAND_TIMEOUT) -> Any:
    try:
        return await asyncio.wait_for(awaitable, timeout=timeout)
    except asyncio.TimeoutError as exc:
        raise HTTPException(status_code=504, detail="HLL:V RCON command timed out") from exc


def _dump(value: Any) -> Any:
    return jsonable_encoder(value)


def _ok(**extra: Any) -> dict[str, Any]:
    return {"ok": True, **extra}


@app.get("/")
async def root() -> dict[str, Any]:
    return {
        "service": "1st M.I. HLL:V RCON Bridge",
        "version": APP_VERSION,
        "game": "Hell Let Loose: Vietnam",
        "public": False,
    }


@app.get("/health")
async def health() -> dict[str, Any]:
    return {
        "ok": True,
        "service": "hllv-rcon",
        "version": APP_VERSION,
    }


@app.get("/version")
async def version() -> dict[str, Any]:
    return {
        "version": APP_VERSION,
        "backend": "timraay/hllrcon",
        "game": "HLLV",
    }


@app.get("/api/v2/connection/status")
async def connection_status() -> dict[str, Any]:
    connected = bool(state.client and state.client.is_connected())
    return {
        "connected": connected,
        "host": state.host if connected else None,
        "port": state.port if connected else None,
        "connected_at": state.connected_at.isoformat() if connected and state.connected_at else None,
        "game": "HLLV",
    }


@app.post("/api/v2/connect")
async def connect(request: Request) -> dict[str, Any]:
    body = await request.json()
    host = str(body.get("host", "")).strip()
    password = str(body.get("password", ""))
    try:
        port = int(body.get("port"))
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail="RCON port must be a number") from exc

    if not host:
        raise HTTPException(status_code=400, detail="RCON host is required")
    if not 1 <= port <= 65535:
        raise HTTPException(status_code=400, detail="RCON port is invalid")
    if not password:
        raise HTTPException(status_code=400, detail="RCON password is required")

    async with state.lock:
        candidate = HLLVRcon(host=host, port=port, password=password, logger=logger)
        try:
            await asyncio.wait_for(candidate.connect(), timeout=CONNECT_TIMEOUT)
            session = await asyncio.wait_for(candidate.get_server_session(), timeout=COMMAND_TIMEOUT)
        except Exception:
            candidate.disconnect()
            raise

        state.disconnect()
        state.client = candidate
        state.host = host
        state.port = port
        state.connected_at = datetime.now(timezone.utc)
        logger.info("Connected to HLL:V RCON at %s:%s", host, port)
        return _ok(connected=True, server=_dump(session))


@app.post("/api/v2/disconnect")
async def disconnect() -> dict[str, Any]:
    async with state.lock:
        state.disconnect()
    return _ok(connected=False)


@app.get("/api/v2/server")
async def server(type: str = Query(default="session")) -> Any:
    client = _client()
    if type == "config":
        result = await _call(client.get_server_config())
    else:
        result = await _call(client.get_server_session())

    data = _dump(result)
    if isinstance(data, dict) and type != "config":
        data.setdefault("map", data.get("map_id") or data.get("map_name"))
        data.setdefault("max_players", data.get("max_player_count"))
        data.setdefault("current_players", data.get("player_count"))
        data.setdefault("next_map", data.get("next_map_id") or data.get("next_map_name"))
    return data


@app.get("/api/v2/players")
async def players() -> Any:
    return _dump(await _call(_client().get_players()))


@app.get("/api/v2/maps")
async def maps() -> Any:
    return _dump(await _call(_client().get_available_maps()))


@app.get("/api/v2/map-rotation")
async def map_rotation() -> Any:
    return _dump(await _call(_client().get_map_rotation()))


@app.get("/api/v2/map-sequence")
async def map_sequence() -> Any:
    return _dump(await _call(_client().get_map_sequence()))


@app.post("/api/v2/change-map")
async def change_map(request: Request) -> dict[str, Any]:
    body = await request.json()
    map_name = str(body.get("map_name", "")).strip()
    if not map_name:
        raise HTTPException(status_code=400, detail="map_name is required")
    await _call(_client().change_map(map_name))
    return _ok(map_name=map_name)


@app.post("/api/v2/broadcast")
async def broadcast(request: Request) -> dict[str, Any]:
    body = await request.json()
    message = str(body.get("message", "")).strip()
    if not message:
        raise HTTPException(status_code=400, detail="message is required")
    await _call(_client().broadcast(message))
    return _ok()


@app.post("/api/v2/players/{player_id}/message")
async def player_message(player_id: str, request: Request) -> dict[str, Any]:
    body = await request.json()
    message = str(body.get("message", "")).strip()
    if not message:
        raise HTTPException(status_code=400, detail="message is required")
    await _call(_client().message_player(player_id, message))
    return _ok()


@app.post("/api/v2/punish")
async def punish(request: Request) -> dict[str, Any]:
    body = await request.json()
    player_id = str(body.get("player_id", "")).strip()
    reason = str(body.get("reason", "")).strip() or None
    if not player_id:
        raise HTTPException(status_code=400, detail="player_id is required")
    result = await _call(_client().kill_player(player_id, reason))
    return _ok(result=result)


@app.post("/api/v2/kick")
async def kick(request: Request) -> dict[str, Any]:
    body = await request.json()
    player_id = str(body.get("player_id", "")).strip()
    reason = str(body.get("reason", "")).strip() or "Removed by server administration"
    if not player_id:
        raise HTTPException(status_code=400, detail="player_id is required")
    result = await _call(_client().kick_player(player_id, reason))
    return _ok(result=result)


@app.post("/api/v2/force-team-switch")
async def force_team_switch(request: Request) -> dict[str, Any]:
    body = await request.json()
    player_id = str(body.get("player_id", "")).strip()
    mode = str(body.get("force_mode", "1"))
    if not player_id:
        raise HTTPException(status_code=400, detail="player_id is required")
    force_mode = ForceMode.IMMEDIATE if mode == "1" else ForceMode.AFTER_DEATH
    result = await _call(_client().force_team_switch(player_id, force_mode))
    return _ok(result=result)


@app.post("/api/v2/temp-ban")
async def temp_ban(request: Request) -> dict[str, Any]:
    body = await request.json()
    player_id = str(body.get("player_id", "")).strip()
    reason = str(body.get("reason", "")).strip() or "Banned by server administration"
    admin_name = str(body.get("admin_name", "")).strip() or "1st M.I. Admin"
    try:
        duration = int(body.get("duration", 24))
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail="duration must be an integer number of hours") from exc
    if not player_id:
        raise HTTPException(status_code=400, detail="player_id is required")
    if duration < 1:
        raise HTTPException(status_code=400, detail="duration must be at least 1 hour")
    await _call(_client().ban_player(player_id, reason, admin_name, duration_hours=duration))
    return _ok()


@app.post("/api/v2/perma-ban")
async def perma_ban(request: Request) -> dict[str, Any]:
    body = await request.json()
    player_id = str(body.get("player_id", "")).strip()
    reason = str(body.get("reason", "")).strip() or "Banned by server administration"
    admin_name = str(body.get("admin_name", "")).strip() or "1st M.I. Admin"
    if not player_id:
        raise HTTPException(status_code=400, detail="player_id is required")
    await _call(_client().ban_player(player_id, reason, admin_name))
    return _ok()


@app.delete("/api/v2/temp-ban")
async def delete_temp_ban(request: Request) -> dict[str, Any]:
    body = await request.json()
    player_id = str(body.get("player_id", "")).strip()
    if not player_id:
        raise HTTPException(status_code=400, detail="player_id is required")
    result = await _call(_client().remove_temporary_ban(player_id))
    return _ok(result=result)


@app.delete("/api/v2/perma-ban")
async def delete_perma_ban(request: Request) -> dict[str, Any]:
    body = await request.json()
    player_id = str(body.get("player_id", "")).strip()
    if not player_id:
        raise HTTPException(status_code=400, detail="player_id is required")
    result = await _call(_client().remove_permanent_ban(player_id))
    return _ok(result=result)


@app.get("/api/v2/bans")
async def bans(type: str = Query(default="temp")) -> Any:
    client = _client()
    if type.lower() in {"perma", "permanent"}:
        return _dump(await _call(client.get_permanent_bans()))
    return _dump(await _call(client.get_temporary_bans()))


@app.get("/api/v2/vips")
async def vips() -> Any:
    return _dump(await _call(_client().get_vip_users()))


@app.post("/api/v2/vips")
async def add_vip(request: Request) -> dict[str, Any]:
    body = await request.json()
    player_id = str(body.get("player_id", "")).strip()
    comment = str(body.get("comment", "")).strip()
    if not player_id:
        raise HTTPException(status_code=400, detail="player_id is required")
    await _call(_client().add_vip(player_id, comment))
    return _ok()


@app.get("/api/v2/admins")
async def admins() -> Any:
    return _dump(await _call(_client().get_admin_users()))


@app.post("/api/v2/admins")
async def add_admin(request: Request) -> dict[str, Any]:
    body = await request.json()
    player_id = str(body.get("player_id", "")).strip()
    admin_group = str(body.get("admin_group", "")).strip()
    comment = str(body.get("comment", "")).strip()
    if not player_id or not admin_group:
        raise HTTPException(status_code=400, detail="player_id and admin_group are required")
    await _call(_client().add_admin(player_id, admin_group, comment))
    return _ok()


@app.get("/api/v2/logs")
async def logs(seconds: int = Query(default=3600, ge=0, le=604800)) -> Any:
    return _dump(await _call(_client().get_admin_log(seconds_span=seconds)))


@app.post("/api/v2/welcome-message")
async def set_welcome_message(request: Request) -> dict[str, Any]:
    body = await request.json()
    await _call(_client().set_welcome_message(str(body.get("message", ""))))
    return _ok()


@app.post("/api/v2/max-queued-players")
async def set_max_queued_players(request: Request) -> dict[str, Any]:
    body = await request.json()
    await _call(_client().set_max_queued_players(int(body.get("max_queued_players", 0))))
    return _ok()


@app.post("/api/v2/vip-slots")
async def set_vip_slots(request: Request) -> dict[str, Any]:
    body = await request.json()
    await _call(_client().set_num_vip_slots(int(body.get("vip_slot_count", 0))))
    return _ok()


@app.post("/api/v2/idle-kick-duration")
async def set_idle_kick_duration(request: Request) -> dict[str, Any]:
    body = await request.json()
    await _call(_client().set_idle_kick_duration(int(body.get("idle_timeout_minutes", 0))))
    return _ok()


@app.post("/api/v2/high-ping-threshold")
async def set_high_ping_threshold(request: Request) -> dict[str, Any]:
    body = await request.json()
    await _call(_client().set_high_ping_threshold(int(body.get("high_ping_threshold_ms", 0))))
    return _ok()


@app.post("/api/v2/team-switch-cooldown")
async def set_team_switch_cooldown(request: Request) -> dict[str, Any]:
    body = await request.json()
    await _call(_client().set_team_switch_cooldown(int(body.get("team_switch_timer", 0))))
    return _ok()


@app.post("/api/v2/auto-balance/enabled")
async def set_auto_balance_enabled(request: Request) -> dict[str, Any]:
    body = await request.json()
    await _call(_client().set_auto_balance_enabled(enabled=bool(body.get("enable", False))))
    return _ok()


@app.post("/api/v2/auto-balance/threshold")
async def set_auto_balance_threshold(request: Request) -> dict[str, Any]:
    body = await request.json()
    await _call(_client().set_auto_balance_threshold(int(body.get("auto_balance_threshold", 0))))
    return _ok()


@app.post("/api/v2/vote-kick/enabled")
async def set_vote_kick_enabled(request: Request) -> dict[str, Any]:
    body = await request.json()
    await _call(_client().set_vote_kick_enabled(enabled=bool(body.get("enable", False))))
    return _ok()


@app.on_event("shutdown")
async def shutdown_event() -> None:
    state.disconnect()
