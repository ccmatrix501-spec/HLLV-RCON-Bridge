from __future__ import annotations

from typing import Any

from app import app, state


@app.get("/api/v2/connection/pool")
async def connection_pool_status() -> dict[str, Any]:
    client = state.client
    if client is None:
        return {
            "connected": False,
            "enabled": True,
            "target_connections": 0,
            "connected_connections": 0,
            "primary_connected": False,
            "read_connections": 0,
            "busy_connections": 0,
            "last_error": None,
        }

    status_fn = getattr(client, "pool_status", None)
    if callable(status_fn):
        data = status_fn()
        if isinstance(data, dict):
            return {"connected": bool(client.is_connected()), **data}

    connected = bool(client.is_connected())
    return {
        "connected": connected,
        "enabled": False,
        "target_connections": 1,
        "connected_connections": 1 if connected else 0,
        "primary_connected": connected,
        "read_connections": 0,
        "busy_connections": 0,
        "last_error": None,
    }
