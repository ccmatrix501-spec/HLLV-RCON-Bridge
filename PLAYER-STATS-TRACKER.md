# HLL:V Persistent Player Stats Tracker

The bridge records per-player server totals using the player's HLL:V EOS player ID.

Tracked totals:

- Kills: infantry kills + vehicle kills; team kills are excluded.
- Deaths.
- Revives: counted when HLL:V RCON/admin logs expose a revive event (`REVIVE`). This should be validated against live HLL:V logs because the player-info response does not contain a native revive counter.
- Favourite weapon: the non-vehicle weapon with the most tracked enemy kills.
- Favourite vehicle: the vehicle associated with the most tracked enemy kills, using HLL:V weapon metadata/vehicle IDs. Roadkills that identify a vehicle can also contribute.

When a new `CONNECTED` admin-log event is received, the bridge sends that player a private message containing their tracked totals and favourites.

## Railway persistence

For totals to survive Railway redeployments and container replacement, add a Railway Volume to the `hllv-rcon` service and mount it at:

```text
/data
```

Then add these variables to `hllv-rcon`:

```env
PLAYER_STATS_TRACKER=true
PLAYER_STATS_DB_PATH=/data/player-stats.db
PLAYER_STATS_POLL_SECONDS=5
PLAYER_STATS_LOG_WINDOW_SECONDS=30
PLAYER_STATS_JOIN_MESSAGE=true
```

If no persistent Railway volume is mounted, the tracker still runs but `/data/player-stats.db` lives on the service's ephemeral filesystem and can be lost when Railway replaces/redeploys the container.

## How totals work

The tracker polls the current player list and stores each player's current-match kill/death counters. It only adds positive deltas to the lifetime database. When the counters reset after a map/match change, the new values are treated as the start of the next match.

Weapon and vehicle favourites are built from parsed `KILL` admin-log events. Teamkills are not included. Vehicle-mounted weapons are credited to the vehicle rather than the personal-weapon table.

On a player's first observation, the current match's existing kill/death counters are imported. Matches played before this tracker was enabled cannot be reconstructed automatically. Likewise, weapon/vehicle favourites begin accumulating from the point this feature is deployed; older kill events that were never stored cannot be reconstructed.

## Private join message

Example:

```text
[ 1ST M.I. SERVER STATS ]

WELCOME, PLAYER

YOUR TRACKED SERVER TOTALS
KILLS: 412
DEATHS: 233
REVIVES: 78

FAVOURITE WEAPON: M16A1 (96 KILLS)
FAVOURITE VEHICLE: M48 Patton (31 KILLS)

GOOD LUCK, TROOPER.
```

If there is not enough weapon or vehicle data yet, that line displays `NOT ENOUGH DATA`.

The message is delivered with HLL:V `MessagePlayer`, so only that player receives it.

## Internal API

The bridge also exposes:

```text
GET /api/v2/player-stats/status
GET /api/v2/player-stats?limit=100
GET /api/v2/player-stats/{player_id}
```

The player-stat responses now include `favorite_weapon` and `favorite_vehicle` objects containing the tracked ID, display name and kill count.

These remain behind the controller's existing authenticated proxy when accessed through the public controller.
