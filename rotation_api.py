from __future__ import annotations

import asyncio
import logging
from typing import Any

from fastapi import HTTPException, Request

from app import app, _call, _client, _dump, _ok

logger = logging.getLogger("hllv-rcon-bridge.rotation")
rotation_lock = asyncio.Lock()

HLLV_ALLOWED_MAPS = (
    "wdeva_warfare_day",
    "wdeva_offensivenva_day",
    "wdeva_offensiveus_day",
    "wdeva_domination_day",
    "wdeva_conquest_day",
    "wdevb_warfare_day",
    "wdevb_offensivenva_day",
    "wdevb_offensiveus_day",
    "wdevb_domination_day",
    "wdevb_conquest_day",
    "wdevc_warfare_day",
    "wdevc_offensivenva_day",
    "wdevc_offensiveus_day",
    "wdevc_domination_day",
    "wdevc_conquest_day",
    "wdevd_warfare_day",
    "wdevd_offensivenva_day",
    "wdevd_offensiveus_day",
    "wdevd_domination_day",
    "wdevd_conquest_day",
    "wdeve_warfare_day",
    "wdeve_conquest_day",
    "wdeve_offensivenva_day",
    "wdeve_offensiveus_day",
    "wdeve_domination_day",
    "wdevf_warfare_day",
    "wdevf_offensivenva_day",
    "wdevf_offensiveus_day",
    "wdevf_domination_day",
    "wdevf_conquest_day",
)
HLLV_ALLOWED_MAP_SET = set(HLLV_ALLOWED_MAPS)


def _rotation_names(rotation: Any) -> list[str]:
    entries = getattr(rotation, "maps", None)
    if entries is None and isinstance(rotation, dict):
        entries = rotation.get("maps") or rotation.get("mAPS") or []
    names: list[str] = []
    for entry in entries or []:
        if isinstance(entry, str):
            name = entry
        elif isinstance(entry, dict):
            name = str(entry.get("name") or entry.get("mapName") or entry.get("map_name") or "")
        else:
            name = str(getattr(entry, "name", ""))
        name = name.strip().lower()
        if name:
            names.append(name)
    return names


def _mode_for_map(map_name: str) -> str:
    name = map_name.lower()
    if "_warfare_" in name:
        return "warfare"
    if "_offensivenva_" in name:
        return "offensivenva"
    if "_offensiveus_" in name:
        return "offensiveus"
    if "_domination_" in name:
        return "domination"
    if "_conquest_" in name:
        return "conquest"
    return "other"


@app.get("/api/v2/map-catalog")
async def map_catalog() -> dict[str, Any]:
    return {
        "maps": [
            {
                "id": name,
                "game_mode": _mode_for_map(name),
                "time_of_day": "day" if name.endswith("_day") else "unknown",
            }
            for name in HLLV_ALLOWED_MAPS
        ],
        "game_modes": [
            {"id": "warfare", "label": "Warfare"},
            {"id": "offensivenva", "label": "Offensive - NVA"},
            {"id": "offensiveus", "label": "Offensive - US"},
            {"id": "domination", "label": "Domination"},
            {"id": "conquest", "label": "Conquest"},
        ],
    }


@app.post("/api/v2/map-change")
async def change_map_from_catalog(request: Request) -> dict[str, Any]:
    body = await request.json()
    map_name = str(body.get("map_name", "")).strip().lower()
    if not map_name:
        raise HTTPException(status_code=400, detail="map_name is required")
    if map_name not in HLLV_ALLOWED_MAP_SET:
        raise HTTPException(
            status_code=400,
            detail={
                "message": "Map is not in the configured HLL:V map pool",
                "map": map_name,
                "allowed_maps": list(HLLV_ALLOWED_MAPS),
            },
        )
    await _call(_client().change_map(map_name))
    return _ok(map_name=map_name, game_mode=_mode_for_map(map_name))


@app.get("/api/v2/map-shuffle")
async def map_shuffle_status() -> dict[str, Any]:
    enabled = bool(await _call(_client().get_map_shuffle_enabled()))
    return {"enabled": enabled}


@app.post("/api/v2/map-shuffle")
async def set_map_shuffle(request: Request) -> dict[str, Any]:
    body = await request.json()
    enabled = bool(body.get("enabled", False))
    await _call(_client().set_map_shuffle_enabled(enabled=enabled))
    return _ok(enabled=enabled)


@app.put("/api/v2/map-rotation")
async def replace_map_rotation(request: Request) -> dict[str, Any]:
    body = await request.json()
    requested = body.get("maps")
    if not isinstance(requested, list):
        raise HTTPException(status_code=400, detail="maps must be an array")

    maps = [str(item).strip().lower() for item in requested if str(item).strip()]
    if not maps:
        raise HTTPException(status_code=400, detail="Map rotation must contain at least one map")
    if len(maps) > 100:
        raise HTTPException(status_code=400, detail="Map rotation cannot contain more than 100 entries")

    invalid = sorted({name for name in maps if name not in HLLV_ALLOWED_MAP_SET})
    if invalid:
        raise HTTPException(
            status_code=400,
            detail={
                "message": "One or more maps are outside the configured HLL:V map pool",
                "invalid_maps": invalid,
                "allowed_maps": list(HLLV_ALLOWED_MAPS),
            },
        )

    shuffle_requested = body.get("shuffle") if "shuffle" in body else None
    client = _client()

    async with rotation_lock:
        previous_rotation = await _call(client.get_map_rotation())
        previous_names = _rotation_names(previous_rotation)
        previous_shuffle = bool(await _call(client.get_map_shuffle_enabled()))

        appended = 0
        removed_old = 0
        try:
            # Append the replacement first so the server is never left with an empty
            # rotation during a normal update.
            base_index = len(previous_names)
            for offset, map_name in enumerate(maps):
                await _call(client.add_map_to_rotation(map_name, base_index + offset))
                appended += 1

            if shuffle_requested is not None:
                await _call(client.set_map_shuffle_enabled(enabled=bool(shuffle_requested)))

            # Remove the old entries from the front. The newly appended entries shift
            # forward and become the complete replacement rotation.
            for _ in previous_names:
                await _call(client.remove_map_from_rotation(0))
                removed_old += 1

        except Exception:
            logger.exception("Map rotation apply failed; attempting rollback")
            try:
                # Remove the entries appended for the failed update.
                start_index = max(0, len(previous_names) - removed_old)
                for index in reversed(range(start_index, start_index + appended)):
                    await _call(client.remove_map_from_rotation(index))

                # Put back any original entries already removed from the front.
                for index, map_name in enumerate(previous_names[:removed_old]):
                    await _call(client.add_map_to_rotation(map_name, index))

                await _call(client.set_map_shuffle_enabled(enabled=previous_shuffle))
                logger.info("Map rotation rollback completed")
            except Exception:
                logger.exception("Map rotation rollback also failed")
            raise

        updated = await _call(client.get_map_rotation())
        enabled = bool(await _call(client.get_map_shuffle_enabled()))
        logger.info("Applied HLL:V map rotation with %d entries (shuffle=%s)", len(maps), enabled)
        return _ok(rotation=_dump(updated), maps=maps, shuffle=enabled)
