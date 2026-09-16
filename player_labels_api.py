from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
from typing import Any

from fastapi import HTTPException, Request

from stats_tracker_api import app

PLAYER_LABELS_PATH = Path(os.getenv("PLAYER_LABELS_PATH", "/data/player-name-labels.json"))
_labels_lock = asyncio.Lock()


def _load_labels() -> dict[str, str]:
    try:
        if not PLAYER_LABELS_PATH.exists():
            return {}
        raw = json.loads(PLAYER_LABELS_PATH.read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            return {}
        return {
            str(player_id).strip(): str(name).strip()
            for player_id, name in raw.items()
            if str(player_id).strip() and str(name).strip()
        }
    except Exception:
        return {}


def _save_labels(labels: dict[str, str]) -> None:
    PLAYER_LABELS_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = PLAYER_LABELS_PATH.with_suffix(PLAYER_LABELS_PATH.suffix + ".tmp")
    tmp.write_text(json.dumps(labels, indent=2, ensure_ascii=False), encoding="utf-8")
    os.replace(tmp, PLAYER_LABELS_PATH)


@app.get("/api/v2/player-labels")
async def get_player_labels() -> dict[str, Any]:
    labels = _load_labels()
    return {
        "labels": labels,
        "count": len(labels),
    }


@app.put("/api/v2/player-labels/{player_id}")
async def set_player_label(player_id: str, request: Request) -> dict[str, Any]:
    pid = str(player_id or "").strip()
    if not pid:
        raise HTTPException(status_code=400, detail="player_id is required")

    body = await request.json()
    name = str(body.get("name", "")).strip()
    if not name:
        raise HTTPException(status_code=400, detail="name is required")
    if len(name) > 80:
        raise HTTPException(status_code=400, detail="name cannot exceed 80 characters")

    async with _labels_lock:
        labels = _load_labels()
        labels[pid] = name
        _save_labels(labels)

    return {"ok": True, "player_id": pid, "name": name}


@app.delete("/api/v2/player-labels/{player_id}")
async def delete_player_label(player_id: str) -> dict[str, Any]:
    pid = str(player_id or "").strip()
    if not pid:
        raise HTTPException(status_code=400, detail="player_id is required")

    async with _labels_lock:
        labels = _load_labels()
        existed = pid in labels
        labels.pop(pid, None)
        _save_labels(labels)

    return {"ok": True, "player_id": pid, "removed": existed}
