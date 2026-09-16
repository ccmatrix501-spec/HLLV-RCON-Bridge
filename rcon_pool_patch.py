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
READ_QUEUE_TIMEOUT = _env_float("HLLV_RCON_READ_QUEUE_TIMEOUT_SECONDS", 4.0, 1.0, 30.0)
WRITE_QUEUE_TIMEOUT = _env_float("HLLV_RCON_WRITE_QUEUE_TIMEOUT_SECONDS", 15.0, 2.0, 60.0)
REPAIR_SECONDS = _env_float("HLLV_RCON_POOL_REPAIR_SECONDS", 8.0, 2.0, 120.0)
MAX_FILL_BACKOFF = _env_float("HLLV_RCON_POOL_MAX_BACKOFF_SECONDS", 60.0, 10.0, 300.0)
READ_RETRIES = _env_int("HLLV_RCON_READ_RETRIES", 1, 0, 3)
IDLE_PROBE_SECONDS = _env_float("HLLV_RCON_IDLE_PROBE_SECONDS", 45.0, 15.0, 300.0)
PROBE_TIMEOUT = _env_float("HLLV_RCON_PROBE_TIMEOUT_SECONDS", 6.0, 2.0, 30.0)

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


class _PoolBusyError(RuntimeError):
    """All suitable RCON lanes stayed busy beyond the bounded queue wait."""


@dataclass
class _PoolSlot:
    client: Any
    index: int
    lock: asyncio.Lock
    created_at: float
    last_used_at: float = 0.0
    last_success_at: float = 0.0
    last_latency_ms: float = 0.0
    calls: int = 0
    failures: int = 0
    queue_timeouts: int = 0
    last_error: str | None = None

    def connected(self) -> bool:
        try:
            return bool(self.client and self.client.is_connected())
        except Exception:
            return False

    def idle_for(self, now: float | None = None) -> float:
        current = now if now is not None else time.monotonic()
        reference = self.last_used_at or self.created_at
        return max(0.0, current - reference)


class PooledHLLVRcon:
    """Drop-in HLLVRcon wrapper with isolated command and read lanes.

    Slot 0 (or the first surviving slot) is the command lane. Additional slots
    handle read-heavy polling. Every socket is serialized with its own lock.

    Startup waits only for one valid session and warms extra lanes in the
    background. Queue waits are bounded separately from command execution, so
    many browser tabs/features cannot create an indefinitely growing RCON backlog.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self._init_args = args
        self._init_kwargs = dict(kwargs)
        self._slots: list[_PoolSlot] = []
        self._primary: _PoolSlot | None = None
        self._pool_lock = asyncio.Lock()
        self._repair_task: asyncio.Task[Any] | None = None
        self._closed = False
        self._last_pool_error: str | None = None
        self._total_calls = 0
        self._total_failures = 0
        self._total_queue_timeouts = 0
        self._started_at = time.monotonic()
        self._fill_backoff_seconds = REPAIR_SECONDS
        self._next_fill_at = 0.0

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
        return _PoolSlot(client=client, index=index, lock=asyncio.Lock(), created_at=time.monotonic())

    async def _ensure_first_connection(self) -> int:
        async with self._pool_lock:
            for slot in list(self._slots):
                if not slot.connected():
                    self._drop_slot(slot, slot.last_error or "RCON connection is no longer active")
            healthy = self._healthy_slots()
            if healthy:
                if self._primary is None:
                    self._primary = healthy[0]
                return len(healthy)

            slot = await self._connect_one(0)
            self._slots.append(slot)
            self._primary = slot
            self._fill_backoff_seconds = REPAIR_SECONDS
            self._next_fill_at = 0.0
            logger.info("Primary RCON connection ready for %s:%s", self.host, self.port)
            return 1

    async def _fill_pool(self) -> int:
        async with self._pool_lock:
            for slot in list(self._slots):
                if not slot.connected():
                    self._drop_slot(slot, slot.last_error or "RCON connection is no longer active")

            healthy_count = len(self._healthy_slots())
            if healthy_count and time.monotonic() < self._next_fill_at:
                return healthy_count

            while not self._closed and len(self._healthy_slots()) < POOL_SIZE:
                index = max((slot.index for slot in self._slots), default=-1) + 1
                try:
                    slot = await self._connect_one(index)
                except Exception as exc:
                    self._last_pool_error = str(exc)
                    self._next_fill_at = time.monotonic() + self._fill_backoff_seconds
                    self._fill_backoff_seconds = min(MAX_FILL_BACKOFF, max(REPAIR_SECONDS, self._fill_backoff_seconds * 2.0))
                    logger.warning(
                        "Could not add RCON pool connection %s/%s to %s:%s: %s; extra-lane retry in %.1fs",
                        len(self._healthy_slots()) + 1,
                        POOL_SIZE,
                        self.host,
                        self.port,
                        exc,
                        max(0.0, self._next_fill_at - time.monotonic()),
                    )
                    break

                self._slots.append(slot)
                if self._primary is None:
                    self._primary = slot
                self._fill_backoff_seconds = REPAIR_SECONDS
                self._next_fill_at = 0.0
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
        connected = await self._ensure_first_connection()
        self._start_repair_task()
        logger.info(
            "RCON pool online for %s:%s with %s connection(s); warming toward %s in background",
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
        now = time.monotonic()
        return {
            "enabled": POOL_SIZE > 1,
            "target_connections": POOL_SIZE,
            "connected_connections": len(healthy),
            "degraded": len(healthy) < POOL_SIZE,
            "primary_connected": bool(primary),
            "read_connections": max(0, len(healthy) - 1),
            "busy_connections": sum(1 for slot in healthy if slot.lock.locked()),
            "read_timeout_seconds": READ_TIMEOUT,
            "write_timeout_seconds": WRITE_TIMEOUT,
            "read_queue_timeout_seconds": READ_QUEUE_TIMEOUT,
            "write_queue_timeout_seconds": WRITE_QUEUE_TIMEOUT,
            "connect_timeout_seconds": CONNECT_TIMEOUT,
            "repair_interval_seconds": REPAIR_SECONDS,
            "idle_probe_seconds": IDLE_PROBE_SECONDS,
            "read_retries": READ_RETRIES,
            "total_calls": self._total_calls,
            "total_failures": self._total_failures,
            "total_queue_timeouts": self._total_queue_timeouts,
            "uptime_seconds": round(max(0.0, now - self._started_at), 1),
            "next_extra_connection_retry_seconds": round(max(0.0, self._next_fill_at - now), 1),
            "last_error": self._last_pool_error,
            "slots": [
                {
                    "index": slot.index,
                    "role": "command" if slot is primary else "read",
                    "connected": slot.connected(),
                    "busy": slot.lock.locked(),
                    "idle_seconds": round(slot.idle_for(now), 1),
                    "calls": slot.calls,
                    "failures": slot.failures,
                    "queue_timeouts": slot.queue_timeouts,
                    "last_latency_ms": round(slot.last_latency_ms, 1),
                    "last_error": slot.last_error,
                }
                for slot in healthy
            ],
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

    async def _probe_idle_slots(self) -> None:
        now = time.monotonic()
        for slot in list(self._healthy_slots()):
            if self._closed or slot.lock.locked() or slot.idle_for(now) < IDLE_PROBE_SECONDS:
                continue
            try:
                await self._run_on_slot(slot, "get_server_session", PROBE_TIMEOUT, PROBE_TIMEOUT)
            except asyncio.CancelledError:
                raise
            except _PoolBusyError:
                continue
            except Exception as exc:
                self._drop_slot(slot, exc)
                logger.warning("RCON idle health probe failed on slot %s: %s", slot.index, exc)

    async def _repair_loop(self) -> None:
        try:
            while not self._closed:
                try:
                    await self._fill_pool()
                    await self._probe_idle_slots()
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    self._last_pool_error = str(exc)
                    logger.warning("RCON pool repair failed: %s", exc)
                await asyncio.sleep(REPAIR_SECONDS)
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
        return min(pool, key=lambda slot: (slot.last_used_at or slot.created_at, slot.index))

    async def _run_on_slot(
        self,
        slot: _PoolSlot,
        method_name: str,
        timeout: float,
        queue_timeout: float,
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        started = time.monotonic()
        acquired = False
        try:
            try:
                await asyncio.wait_for(slot.lock.acquire(), timeout=queue_timeout)
                acquired = True
            except asyncio.TimeoutError as exc:
                slot.queue_timeouts += 1
                self._total_queue_timeouts += 1
                raise _PoolBusyError(
                    f"RCON lane {slot.index} stayed busy for more than {queue_timeout:.1f}s"
                ) from exc

            if not slot.connected():
                raise RconConnectionLostError("RCON pool lane disconnected")

            method = getattr(slot.client, method_name)
            slot.calls += 1
            self._total_calls += 1
            slot.last_used_at = time.monotonic()
            try:
                result = await asyncio.wait_for(method(*args, **kwargs), timeout=timeout)
            except Exception as exc:
                slot.failures += 1
                self._total_failures += 1
                slot.last_error = str(exc)
                raise
            else:
                slot.last_success_at = time.monotonic()
                slot.last_latency_ms = (slot.last_success_at - started) * 1000.0
                slot.last_error = None
                return result
        finally:
            if acquired and slot.lock.locked():
                slot.lock.release()

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
                return await self._run_on_slot(
                    slot,
                    method_name,
                    READ_TIMEOUT,
                    READ_QUEUE_TIMEOUT,
                    *args,
                    **kwargs,
                )
            except asyncio.CancelledError:
                raise
            except _PoolBusyError as exc:
                # Congestion is not a broken socket. Do not discard a healthy lane.
                last_error = exc
                continue
            except _TRANSIENT_ERRORS as exc:
                last_error = exc
                self._drop_slot(slot, exc)
                self._start_repair_task()
                logger.warning(
                    "RCON read %s failed on slot %s; trying another lane if available: %s",
                    method_name,
                    slot.index,
                    exc,
                )
                continue

        if last_error is not None:
            raise last_error
        raise RconConnectionError("No healthy RCON read connection is available")

    async def _write_call(self, method_name: str, *args: Any, **kwargs: Any) -> Any:
        slot = self._promote_primary()
        if slot is None:
            raise RconConnectionError("No healthy RCON command connection is available")

        try:
            return await self._run_on_slot(
                slot,
                method_name,
                WRITE_TIMEOUT,
                WRITE_QUEUE_TIMEOUT,
                *args,
                **kwargs,
            )
        except asyncio.CancelledError:
            raise
        except _PoolBusyError:
            # Busy does not mean broken; leave the lane connected for the next action.
            raise
        except _TRANSIENT_ERRORS as exc:
            # Never automatically replay moderation/write commands. A timeout may
            # occur after HLL:V already applied the action, so replaying could duplicate
            # a kick, ban, message, map change, etc.
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
        "Installed HLL:V RCON pool: target=%s, read timeout=%.1fs, write timeout=%.1fs, read queue=%.1fs, write queue=%.1fs",
        POOL_SIZE,
        READ_TIMEOUT,
        WRITE_TIMEOUT,
        READ_QUEUE_TIMEOUT,
        WRITE_QUEUE_TIMEOUT,
    )
