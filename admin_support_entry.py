from __future__ import annotations

from fastapi import Request

from admin_support_api import admin_support_action, app

# admin_support_api originally registered the action endpoint with an untyped
# request parameter. Remove that route here and replace it with the proper
# FastAPI Request-typed entrypoint while keeping the public path stable for the
# Discord bot.
app.router.routes = [
    route
    for route in app.router.routes
    if not (
        getattr(route, "path", None) == "/api/v2/admin-support/action"
        and "POST" in (getattr(route, "methods", set()) or set())
    )
]


@app.post("/api/v2/admin-support/action")
async def admin_support_command(request: Request):
    """Authenticated Discord button/modal actions for an HLL:V admin request."""
    return await admin_support_action(request)
