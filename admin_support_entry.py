from __future__ import annotations

from fastapi import Request

import admin_request_api as admin_api
import admin_support_api as support
import match_leaderboard_api  # Registers server-wide current match leaderboard command/routes.
from admin_support_api import admin_support_action, app

# Persist the support request before the Discord DM is sent so its buttons are
# valid immediately, even if the admin clicks as soon as the DM arrives.
async def _eager_support_admin_request(entry, reason: str) -> None:
    player_id = str(getattr(entry, "player_id", "") or "").strip()
    player_name = str(getattr(entry, "player_name", "") or player_id or "Unknown player").strip()
    if not player_id:
        return

    existing = support._active_request(player_id)
    if existing:
        await admin_api._send_private(
            player_id,
            "[ 1ST M.I. ADMIN ]\n\n"
            "YOU ALREADY HAVE AN OPEN ADMIN REQUEST.\n\n"
            "USE !reply <MESSAGE> TO CONTINUE TALKING TO THE ADMIN.",
        )
        return

    request_id = admin_api._event_key(entry)[:16]
    support._save_request(request_id, player_id, player_name, reason)
    await support._original_handle_admin_request(entry, reason)


admin_api._handle_admin_request = _eager_support_admin_request

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
