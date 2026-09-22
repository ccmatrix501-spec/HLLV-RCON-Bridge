from __future__ import annotations

import asyncio
import logging
import os
from datetime import UTC, datetime
from typing import Any

from hllrcon import HLLVRcon

from admin_request_api import app
from app import COMMAND_TIMEOUT, CONNECT_TIMEOUT, _apply_default_welcome_message, _await_rcon, state

logger = logging.getLogger("hllv-rcon-bridge.connection-keeper")


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() not in {"0", "false", "no", "off"}


AUTO_CONNECT_ENABLED = _env_bool("HLLV_RCON_AUTO_CONNECT", True)
AUTO_HOST = os.getenv("HLLV_RCON_HOST", "").strip()
AUTO_PASSWORD = os.getenv("HLLV_RCON_PASSWORD", "")
try:
    AUTO_PORT = int(os.getenv("HLLV_RCON_PORT", "7779"))
except ValueError:
    AUTO_PORT = 7779
RETRY_SECONDS = max(2.0, float(os.getenv("HLLV_RCON_RETRY_SECONDS", "5")))
CHECK_SECONDS = max(1.0, float(os.getenv("HLLV_RCON_CHECK_SECONDS", "3")))


class KeeperRuntime:
    def __init__(self) -> None:
        self.task: asyncio.Task[Any] | None = None
        self.last_attempt_at: datetime | None = None
        self.last_connected_at: datetime | None = None
        self.last_error: str | None = None
        self.reconnect_count = 0
        self.started = False


runtime = KeeperRuntime()


def _configured() -> bool:
    return bool(AUTO_HOST and AUTO_PASSWORD and 1 <= AUTO_PORT <= 65535)


def _connected() -> bool:
    try:
        return bool(state.client and state.client.is_connected())
    except Exception:
        return False


async def _connect_once() -> bool:
    if not AUTO_CONNECT_ENABLED or not _configured():
        return False

    runtime.last_attempt_at = datetime.now(UTC)

    async with state.lock:
        if _connected():
            return True

        # Dispose of any stale client before creating a new TCP/RCON session.
        state.disconnect()
        candidate = HLLVRcon(
            host=AUTO_HOST,
            port=AUTO_PORT,
            password=AUTO_PASSWORD,
            logger=logging.getLogger("hllv-rcon-bridge"),
        )
        try:
            await _await_rcon(candidate.connect(), CONNECT_TIMEOUT)
            # Validate that authentication and the command channel are usable.
            await _await_rcon(candidate.get_server_session(), COMMAND_TIMEOUT)
            await _apply_default_welcome_message(candidate)
        except Exception as exc:
            try:
                candidate.disconnect()
            except Exception:
                pass
            runtime.last_error = str(exc)
            logger.warning(
                "Automatic RCON connection to %s:%s failed: %s; retrying in %.1fs",
                AUTO_HOST,
                AUTO_PORT,
                exc,
                RETRY_SECONDS,
            )
            return False

        state.client = candidate
        state.host = AUTO_HOST
        state.port = AUTO_PORT
        state.connected_at = datetime.now(UTC)
        runtime.last_connected_at = state.connected_at
        runtime.last_error = None
        runtime.reconnect_count += 1
        logger.info(
            "Automatic RCON connection established to %s:%s (connection #%s)",
            AUTO_HOST,
            AUTO_PORT,
            runtime.reconnect_count,
        )
        return True


async def _keeper_loop() -> None:
    runtime.started = True

    if not AUTO_CONNECT_ENABLED:
        logger.info("Automatic RCON reconnect is disabled by HLLV_RCON_AUTO_CONNECT")
    elif not _configured():
        missing = []
        if not AUTO_HOST:
            missing.append("HLLV_RCON_HOST")
        if not AUTO_PASSWORD:
            missing.append("HLLV_RCON_PASSWORD")
        if not 1 <= AUTO_PORT <= 65535:
            missing.append("HLLV_RCON_PORT")
        logger.warning(
            "Automatic RCON reconnect is waiting for Railway variables: %s",
            ", ".join(missing) or "invalid configuration",
        )
    else:
        logger.info(
            "Automatic RCON reconnect enabled for %s:%s; check=%.1fs retry=%.1fs",
            AUTO_HOST,
            AUTO_PORT,
            CHECK_SECONDS,
            RETRY_SECONDS,
        )

    while True:
        try:
            if AUTO_CONNECT_ENABLED and _configured() and not _connected():
                await _connect_once()
                await asyncio.sleep(RETRY_SECONDS if not _connected() else CHECK_SECONDS)
                continue
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            runtime.last_error = str(exc)
            logger.exception("Automatic RCON reconnect worker failed")

        await asyncio.sleep(CHECK_SECONDS)


@app.on_event("startup")
async def start_connection_keeper() -> None:
    if runtime.task is None:
        runtime.task = asyncio.create_task(_keeper_loop(), name="hllv-rcon-connection-keeper")


@app.on_event("shutdown")
async def stop_connection_keeper() -> None:
    if runtime.task:
        runtime.task.cancel()
        try:
            await runtime.task
        except asyncio.CancelledError:
            pass
        runtime.task = None


@app.get("/api/v2/autoconnect/status")
async def autoconnect_status() -> dict[str, Any]:
    return {
        "enabled": AUTO_CONNECT_ENABLED,
        "configured": _configured(),
        "connected": _connected(),
        "host": AUTO_HOST or None,
        "port": AUTO_PORT if AUTO_HOST else None,
        "check_seconds": CHECK_SECONDS,
        "retry_seconds": RETRY_SECONDS,
        "reconnect_count": runtime.reconnect_count,
        "last_attempt_at": runtime.last_attempt_at.isoformat() if runtime.last_attempt_at else None,
        "last_connected_at": runtime.last_connected_at.isoformat() if runtime.last_connected_at else None,
        "last_error": runtime.last_error,
    }
