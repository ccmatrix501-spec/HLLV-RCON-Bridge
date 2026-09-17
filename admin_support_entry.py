from __future__ import annotations

from fastapi import Request

# Install the pooled HLLVRcon wrapper before app.py, connection_keeper.py or any
# feature module imports HLLVRcon. This gives the entire bridge one command lane
# plus multiple independent read lanes without rewriting every API module.
import rcon_pool_patch  # noqa: F401
import pool_policy  # noqa: F401

import admin_request_api as admin_api
import admin_support_api as support
import match_leaderboard_api  # Registers server-wide current match leaderboard command/routes.
import public_stats_api  # Registers the efficient read-only public player stats route.
import live_stats_api  # Registers the lightweight near-live current-player stats route.
import player_labels_api  # Registers persistent controller display-name labels.
import pool_status_api  # Registers RCON connection-pool diagnostics.
from admin_support_api import admin_support_action, app

# HLL:V's declared player-stat response currently omits revives. Install the
# compatibility layer after the stats modules are assembled so it can use an
# exact future revive field when present and otherwise improve best-effort log
# detection without creating another permanent RCON polling worker.
import revive_tracking_patch  # noqa: E402,F401

# Add bridge-wide load shedding only after the production app has been assembled.
# This protects all current and future /api routes from unbounded HTTP concurrency
# without forcing feature modules to know anything about the RCON pool internals.
import bridge_resilience  # noqa: E402,F401

# Public /api/v2 calls must carry the shared integration secret. Railway-private
# calls from the web controller remain internal and do not need the public header.
from public_api_guard import install_public_api_guard  # noqa: E402

install_public_api_guard(app)

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
