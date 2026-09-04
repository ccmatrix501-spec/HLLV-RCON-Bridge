from __future__ import annotations

import logging
from typing import Any

from fastapi import HTTPException, Request

from rotation_api import app
from app import _call, _client, _make_broadcast_visible, _ok

logger = logging.getLogger("hllv-rcon-bridge.message-everyone")

# Replace the original HLL broadcast/notice endpoint. HLL:V's broadcast command is
# rendered as an admin/server notice. The controller's "broadcast" action should
# instead behave like a normal player message delivered to every player currently
# online, so we fan the message out with MessagePlayer.
app.router.routes = [
    route
    for route in app.router.routes
    if not (
        getattr(route, "path", None) == "/api/v2/broadcast"
        and "POST" in (getattr(route, "methods", set()) or set())
    )
]


@app.post("/api/v2/broadcast")
async def message_everyone(request: Request) -> dict[str, Any]:
    body = await request.json()
    message = str(body.get("message", "")).strip()
    if not message:
        raise HTTPException(status_code=400, detail="message is required")

    # Keep the readability formatting already used by the controller, but deliver it
    # through MessagePlayer so every recipient sees the normal player-message box.
    visible_message = _make_broadcast_visible(message)
    client = _client()
    players_response = await _call(client.get_players())
    players = list(getattr(players_response, "players", []) or [])

    sent = 0
    failed: list[dict[str, str]] = []

    # Send sequentially. HLL RCON is a single command channel and serial fan-out is
    # much less likely to corrupt/overlap responses than firing 100 commands at once.
    for player in players:
        player_id = str(getattr(player, "id", "") or "").strip()
        player_name = str(getattr(player, "name", "") or player_id or "Unknown")
        if not player_id:
            failed.append({"player": player_name, "error": "Missing player ID"})
            continue
        try:
            await _call(client.message_player(player_id, visible_message))
            sent += 1
        except Exception as exc:
            # A player can disconnect between GetPlayers and MessagePlayer. Do not
            # abort delivery to everybody else because of one stale roster entry.
            failed.append({"player": player_name, "error": str(exc)})
            logger.warning("Message-everyone delivery failed for %s (%s): %s", player_name, player_id, exc)

    if players and sent == 0:
        raise HTTPException(
            status_code=502,
            detail={
                "message": "The message could not be delivered to any online players.",
                "failed": failed[:20],
            },
        )

    logger.info(
        "Message-everyone delivered to %d/%d player(s); %d failed",
        sent,
        len(players),
        len(failed),
    )
    return _ok(
        delivery="player_message_to_everyone",
        online_players=len(players),
        sent=sent,
        failed=len(failed),
        failures=failed[:20],
        formatted_message=visible_message,
    )
