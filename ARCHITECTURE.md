# HLLV RCON Bridge Architecture

The bridge isolates controller features from the HLL:V RCON socket so new APIs can be added without creating new unmanaged connections.

## Connection model

The production entrypoint installs `rcon_pool_patch.py` before feature modules import `HLLVRcon`.

Default pool:

- one command lane for state-changing/moderation operations;
- additional read lanes for server state, players, logs, bans and stats;
- one in-flight RCON operation per lane;
- bounded lane queue waits and bounded execution timeouts;
- read failover/retry across healthy lanes;
- no automatic replay of write commands;
- background health probes, reconnect and pool repair.

`pool_policy.py` controls which method naming patterns are considered safe reads. Unknown methods intentionally stay on the command lane until explicitly classified.

## HTTP protection

`bridge_resilience.py` applies bridge-wide admission control to all `/api/` routes. This prevents future feature modules, multiple controller tabs or a bad poll loop from building unlimited HTTP/RCON work.

Defaults can be overridden with:

- `HLLV_HTTP_MAX_ACTIVE_READS=12`
- `HLLV_HTTP_MAX_ACTIVE_WRITES=16`
- `HLLV_HTTP_MAX_ACTIVE_TOTAL=28`
- `HLLV_RCON_POOL_SIZE=3`

Keep the pool small unless testing proves the game server handles more simultaneous RCON sessions reliably.

## Rules for new bridge features

1. Reuse `app`, `_client()` and `_call()` from the existing bridge modules.
2. Do not instantiate a separate `HLLVRcon` client inside feature modules.
3. Name read-only client methods with a recognised safe read prefix or extend `pool_policy.py` deliberately.
4. Never automatically retry state-changing commands after a timeout because the game server may already have applied them.
5. Keep background poll loops bounded and sleep between iterations.
6. Expose diagnostics for persistent background workers where practical.
7. Add new production modules to the Dockerfile and validation workflow.

## Diagnostics

- `/health` - process health.
- `/api/v2/connection/status` - primary RCON state.
- `/api/v2/connection/pool` - per-lane RCON pool metrics.
- `/api/v2/resilience/status` - active/rejected HTTP API request metrics.

## Deployment safety

The GitHub workflow compiles every Python module, verifies critical production routes, boots the actual Uvicorn production entrypoint without a live game server, checks diagnostics, and builds the production Docker image. Railway should use **Wait for CI** before deploying new commits.
