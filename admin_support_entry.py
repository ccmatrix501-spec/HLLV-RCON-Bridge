from __future__ import annotations

from fastapi import Request

from admin_support_api import admin_support_action, app


@app.post("/api/v2/admin-support/command")
async def admin_support_command(request: Request):
    """Typed FastAPI entrypoint for Discord button/modal support actions."""
    return await admin_support_action(request)
