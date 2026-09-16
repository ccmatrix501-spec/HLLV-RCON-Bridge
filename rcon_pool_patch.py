from __future__ import annotations

import asyncio
import inspect
import logging
import os
import time
from dataclasses import dataclass
from typing import Any

import hllrcon
from hllrcon.exceptions import (
    RconConnectionClosedError,
    RconConnectionError,
    RconConnectionLostError,
    RconMessageError,
)

_ORIGINAL_HLLVRCON = hllrcon.HLLVRcon
logger = logging.getLogger("hllv-rcon-pool")


def _env_int(name: str, default: int, minimum: int, maximum: int) -> int:
    try:
        value = int(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        value = default
    return max(minimum, min(maximum, value))


def _env_float(name: str, default: float, minimum: float, maximum: float) -> float:
    try:
        value = float(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        value = default
    return max(minimum, min(maximum, value))


POOL_SIZE = _env_int("HLLV_RCON_POOL_SIZE", 3, 1, 5)
CONNECT_TIMEOUT = _env_float("HLLV_RCON_POOL_CONNECT_TIMEOUT_SECONDS", 10.0, 3.0, 60.0)
READ_TIMEOUT = _env_float("HLLV_RCON_READ_TIMEOUT_SECONDS", 22.0, 5.0, 120.0)
WRITE_TIMEOUT = _env_float("HLLV_RCON_WRITE_TIMEOUT_SECONDS", 22.0, 5.0, 120.0)
REPAIR_SECONDS = _env_float("HLLV_RCON_POOL_REPAIR_SECONDS", 10.0, 3.0, 120.0)
READ_RETRIES = _env_int("HLLV_RCON_READ_RETRIES", 1, 0, 3)

_READ_PREFIXES = ("get_", "list_", "fetch_", "query_")
_TRANSIENT_ERRORS = (
    RconConnectionClosedError,
    RconConnectionLostError,
    RconConnectionError,
    RconMessageError,
    OSError,
    TimeoutError,
    asyncio.TimeoutError,
)


@dataclass
class _PoolSlot:
    client: Any
    index: int
    lock: asyncio.Lock
    created_at: float
    last_used_at: float = 0.0
    last_error: str | None = None

    def connected(self) -> bool:
        try:
            return bool(self.client and self.client.is_connected())
        except Exception:
            return False


class PooledHLLVRcon:
    """Drop-in HLLVRcon wrapper with one command lane and multiple read lanes.

    Slot 0 is kept for moderation/write commands whenever possible. Additional
    slots service read-heavy polling such as players, logs, bans and stats.
    Each TCP/RCON session has its own lock, so two coroutines never issue
    commands concurrently on the same socket.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self._init_args = args
        self._init_kwargs = dict(kwargs)
        self._slots: list[_PoolSlot] = []
        self._primary: _PoolSlot | None = None
        self._pool_lock = asyncio.Lock()
        self._read_cursor = 0
        self._repair_task: asyncio.Task[Any] | None = None
        self._closed = False
        self._last_pool_error: str | None = None

        self.host = kwargs.get("host", args[0] if len(args) > 0 else None)
        self.port = kwargs.get("port", args[1] if len(args) > 1 else None)

    def _new_real_client(self) -> Any:
        return _ORIGINAL_HLLVRCON(*self._init_args, **self._init_kwargs)

    def _healthy_slots(self) -> list[_PoolSlot]:
        return [slot for slot in self._slots if slot.connected()]

    def _promote_primary(self) -> _PoolSlot | None:
        if self._primary and self._primary.connected():
            return self._primary
        healthy = self._healthy_slots()
        self._primary = healthy[0] if healthy else None
        return self._primary

    def _drop_slot(self, slot: _PoolSlot, error: BaseException | str | None = None) -> None:
        if error is not None:
            slot.last_error = str(error)
            self._last_pool_error = str(error)
        try:
            slot.client.disconnect()
        except Exception:
            pass
        try:
            self._slots.remove(slot)
        except ValueError:
            pass
        if self._primary is slot:
            self._primary = None
            self._promote_primary()

    async def _connect_one(self, index: int) -> _PoolSlot:
        client = self._new_real_client()
        try:
            await asyncio.wait_for(client.connect(), timeout=CONNECT_TIMEOUT)
        except Exception:
            try:
                client.disconnect()
            except Exception:
                pass
            raise
        return _PoolSlot(
            client=client,
            index=index,
            lock=asyncio.Lock(),
            created_at=time.monotonic(),
        )

    async def _fill_pool(self, *, require_one: bool) -> int:
        async with self._pool_lock:
            for slot in list(self._slots):
                if not slot.connected():
                    self._drop_slot(slot, slot.last_error or "RCON connection is no longer active")

            while not self._closed and len(self._healthy_slots()) < POOL_SIZE:
                index = max((slot.index for slot in self._slots), default=-1) + 1
                try:
                    slot = await self._connect_one(index)
                except Exception as exc:
                    self._last_pool_error = str(exc)
                    if require_one and not self._healthy_slots():
                        raise
                    logger.warning(
                        "Could not add RCON pool connection %s/%s to %s:%s: %s",
                        len(self._healthy_slots()) + 1,
                        POOL_SIZE,
                        self.host,
                        self.port,
                        exc,
                    )
                    break

                self._slots.append(slot)
                if self._primary is None:
                    self._primary = slot
                logger.info(
                    "RCON pool connection ready %s/%s for %s:%s",
                    len(self._healthy_slots()),
                    POOL_SIZE,
                    self.host,
                    self.port,
                )

            return len(self._healthy_slots())

    async def connect(self) -> Any:
        self._closed = False
        connected = await self._fill_pool(require_one=True)
        self._start_repair_task()
        logger.info(
            "RCON pool online for %s:%s with %s/%s connection(s)",
            self.host,
            self.port,
            connected,
            POOL_SIZE,
        )
        return None

    def disconnect(self) -> None:
        self._closed = True
        if self._repair_task and not self._repair_task.done():
            self._repair_task.cancel()
        self._repair_task = None
        for slot in list(self._slots):
            try:
                slot.client.disconnect()
            except Exception:
                pass
        self._slots.clear()
        self._primary = None

    def is_connected(self) -> bool:
        return bool(self._promote_primary())

    def pool_status(self) -> dict[str, Any]:
        healthy = self._healthy_slots()
        primary = self._promote_primary()
        return {
            "enabled": POOL_SIZE > 1,
            "target_connections": POOL_SIZE,
            "connected_connections": len(healthy),
            "primary_connected": bool(primary),
            "read_connections": max(0, len(healthy) - 1),
            "busy_connections": sum(1 for slot in healthy if slot.lock.locked()),
            "read_timeout_seconds": READ_TIMEOUT,
            "write_timeout_seconds": WRITE_TIMEOUT,
            "connect_timeout_seconds": CONNECT_TIMEOUT,
            "repair_interval_seconds": REPAIR_SECONDS,
            "last_error": self._last_pool_error,
        }

    def _start_repair_task(self) -> None:
        if self._closed:
            return
        if self._repair_task is not None and not self._repair_task.done():
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        self._repair_task = loop.create_task(self._repair_loop(), name="hllv-rcon-pool-repair")

    async def _repair_loop(self) -> None:
        try:
            while not self._closed:
                await asyncio.sleep(REPAIR_SECONDS)
                try:
                    await self._fill_pool(require_one=False)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    self._last_pool_error = str(exc)
                    logger.warning("RCON pool repair failed: %s", exc)
        except asyncio.CancelledError:
            pass

    def _read_candidates(self, excluded: set[int]) -> list[_PoolSlot]:
        healthy = [slot for slot in self._healthy_slots() if id(slot) not in excluded]
        if not healthy:
            return []

        primary = self._promote_primary()
        readers = [slot for slot in healthy if slot is not primary]
        return readers or healthy

    def _choose_read_slot(self, excluded: set[int]) -> _PoolSlot | None:
        candidates = self._read_candidates(excluded)
        if not candidates:
            return None

        unlocked = [slot for slot in candidates if not slot.lock.locked()]
        pool = unlocked or candidates
        slot = pool[self._read_cursor % len(pool)]
        self._read_cursor = (self._read_cursor + 1) % 1_000_000
        return slot

    async def _read_call(self, method_name: str, *args: Any, **kwargs: Any) -> Any:
        excluded: set[int] = set()
        last_error: BaseException | None = None
        attempts = max(1, READ_RETRIES + 1)

        for _ in range(attempts):
            slot = self._choose_read_slot(excluded)
            if slot is None:
                break
            excluded.add(id(slot))

            try:
                async with slot.lock:
                    if not slot.connected():
                        raise RconConnectionLostError("RCON pool lane disconnected")
                    method = getattr(slot.client, method_name)
                    slot.last_used_at = time.monotonic()
                    return await asyncio.wait_for(method(*args, **kwargs), timeout=READ_TIMEOUT)
            except asyncio.CancelledError:
                raise
            except _TRANSIENT_ERRORS as exc:
                last_error = exc
                self._drop_slot(slot, exc)
                self._start_repair_task()
                logger.warning(
                    "RCON read %s failed on one pool lane; trying another if available: %s",
                    method_name,
                    exc,
                )
                continue

        if last_error is not None:
            raise last_error
        raise RconConnectionError("No healthy RCON connection is available")

    async def _write_call(self, method_name: str, *args: Any, **kwargs: Any) -> Any:
        slot = self._promote_primary()
        if slot is None:
            raise RconConnectionError("No healthy RCON command connection is available")

        try:
            async with slot.lock:
                if not slot.connected():
                    raise RconConnectionLostError("RCON command connection disconnected")
                method = getattr(slot.client, method_name)
                slot.last_used_at = time.monotonic()
                return await asyncio.wait_for(method(*args, **kwargs), timeout=WRITE_TIMEOUT)
        except asyncio.CancelledError:
            raise
        except _TRANSIENT_ERRORS as exc:
            # Never automatically replay a moderation/write command. A timeout can
            # happen after the game server has already applied it, so replaying could
            # duplicate side effects. Drop the bad lane and let the next command use
            # a promoted healthy connection instead.
            self._drop_slot(slot, exc)
            self._start_repair_task()
            raise

    def __getattr__(self, name: str) -> Any:
        if name.startswith("_"):
            raise AttributeError(name)

        primary = self._promote_primary()
        if primary is None:
            async def unavailable(*args: Any, **kwargs: Any) -> Any:
                raise RconConnectionError("RCON is not connected")
            return unavailable

        attribute = getattr(primary.client, name)
        if not callable(attribute):
            return attribute

        if name.startswith(_READ_PREFIXES):
            async def read_proxy(*args: Any, **kwargs: Any) -> Any:
                return await self._read_call(name, *args, **kwargs)
            return read_proxy

        if inspect.iscoroutinefunction(attribute):
            async def write_proxy(*args: Any, **kwargs: Any) -> Any:
                return await self._write_call(name, *args, **kwargs)
            return write_proxy

        return attribute


if hllrcon.HLLVRcon is not PooledHLLVRcon:
    hllrcon.HLLVRcon = PooledHLLVRcon
    logger.info(
        "Installed HLL:V RCON connection pool: target=%s, read timeout=%.1fs, write timeout=%.1fs",
        POOL_SIZE,
        READ_TIMEOUT,
        WRITE_TIMEOUT,
    )
