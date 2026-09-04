# 1st M.I. HLL:V RCON Bridge

Private Railway bridge between the 1st M.I. HLL:V Controller and a Hell Let Loose: Vietnam game server.

This service uses [`timraay/hllrcon`](https://github.com/timraay/hllrcon) and explicitly creates an `HLLVRcon` client, so the bridge speaks the Vietnam RCON implementation rather than the original Hell Let Loose implementation.

## Railway

Deploy this repository as its own service in the same Railway project/environment as the web controller.

Recommended service name:

```text
hllv-rcon
```

Variables:

```env
PORT=8080
LOG_LEVEL=INFO
RCON_CONNECT_TIMEOUT=35
RCON_COMMAND_TIMEOUT=30
```

Do **not** generate a public domain for this service. It is intended to be reached only over Railway private networking.

The controller should use:

```env
RCON_BACKEND=http://${{hllv-rcon.RAILWAY_PRIVATE_DOMAIN}}:8080
```

## Health check

```text
GET /health
```

Expected response:

```json
{"ok":true,"service":"hllv-rcon","version":"0.1.0"}
```

## Connection flow

The controller sends the RCON host, port and password to `POST /api/v2/connect`. The password is held only in process memory by this bridge and is not written to the repository or a configuration file.

For the current Qonzer server:

```text
RCON host: 82.21.28.146
RCON port: 7779
```

Enter the RCON password through the controller UI; do not commit it to GitHub.

## API compatibility

The bridge intentionally exposes the `/api/v2/...` routes used by the existing 1st M.I. controller, including connection state, server/session info, players, maps, map rotation/sequence, broadcasts, player moderation, bans, VIPs, admins, logs and the current server-setting forms.
