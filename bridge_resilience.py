from __future__ import annotations

import asyncio
import os
import time
import uuid
from typing import Any

from fastapi import Request
from fastapi.responses import JSONResponse

from app import app


def _env_int(name: str, default: int, minimum: int, maximum: int) -> int:
    try:
        value = int(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        value = default
    return max(minimum, min(maximum, value))


MAX_ACTIVE_READS = _env_int("HLLV_HTTP_MAX_ACTIVE_READS", 12, 2, 100)
MAX_ACTIVE_WRITES = _env_int("HLLV_HTTP_MAX_ACTIVE_WRITES", 16, 2, 100)
MAX_ACTIVE_TOTAL = _env_int(
    "HLLV_HTTP_MAX_ACTIVE_TOTAL",
    max(24, MAX_ACTIVE_READS + MAX_ACTIVE_WRITES),
    4,
    200,
)


class Runtime:
    def __init__(self) -> None:
        self.lock = asyncio.Lock()
        self.active_reads = 0
        self.active_writes = 0
        self.active_total = 0
        self.high_water_reads = 0
        self.high_water_writes = 0
        self.high_water_total = 0
        self.requests = 0
        self.rejected = 0
        self.failures = 0
        self.started_at = time.monotonic()


runtime = Runtime()


def _is_read(request: Request) -> bool:
    return request.method.upper() in {"GET", "HEAD", "OPTIONS"}


def _bypass_limit(path: str) -> bool:
    # Health/status endpoints must stay responsive even while the main API is busy.
    return path in {"/", "/health", "/version", "/api/v2/resilience/status"}


@app.middleware("http")
async def bridge_load_guard(request: Request, call_next):
    path = request.url.path
    request_id = request.headers.get("X-Request-ID") or uuid.uuid4().hex[:16]

    if not path.startswith("/api/") or _bypass_limit(path):
        response = await call_next(request)
        response.headers["X-Request-ID"] = request_id
        return response

    read_request = _is_read(request)
    admitted = False

    async with runtime.lock:
        runtime.requests += 1
        read_at_limit = read_request and runtime.active_reads >= MAX_ACTIVE_READS
        write_at_limit = (not read_request) and runtime.active_writes >= MAX_ACTIVE_WRITES
        total_at_limit = runtime.active_total >= MAX_ACTIVE_TOTAL

        if read_at_limit or write_at_limit or total_at_limit:
            runtime.rejected += 1
        else:
            admitted = True
            runtime.active_total += 1
            if read_request:
                runtime.active_reads += 1
            else:
                runtime.active_writes += 1
            runtime.high_water_reads = max(runtime.high_water_reads, runtime.active_reads)
            runtime.high_water_writes = max(runtime.high_water_writes, runtime.active_writes)
            runtime.high_water_total = max(runtime.high_water_total, runtime.active_total)

    if not admitted:
        return JSONResponse(
            status_code=503,
            content={
                "error": "RCON bridge is busy; retry shortly.",
                "retry_after_ms": 1000,
                "request_id": request_id,
            },
            headers={
                "Retry-After": "1",
                "X-Request-ID": request_id,
                "Cache-Control": "no-store",
            },
        )

    try:
        response = await call_next(request)
        if response.status_code >= 500:
            runtime.failures += 1
        response.headers["X-Request-ID"] = request_id
        response.headers["X-HLLV-Bridge-Load"] = (
            f"{runtime.active_total}/{MAX_ACTIVE_TOTAL}"
        )
        return response
    except Exception:
        runtime.failures += 1
        raise
    finally:
        async with runtime.lock:
            runtime.active_total = max(0, runtime.active_total - 1)
            if read_request:
                runtime.active_reads = max(0, runtime.active_reads - 1)
            else:
                runtime.active_writes = max(0, runtime.active_writes - 1)


@app.get("/api/v2/resilience/status")
async def resilience_status() -> dict[str, Any]:
    return {
        "ok": True,
        "uptime_seconds": round(max(0.0, time.monotonic() - runtime.started_at), 1),
        "limits": {
            "active_reads": MAX_ACTIVE_READS,
            "active_writes": MAX_ACTIVE_WRITES,
            "active_total": MAX_ACTIVE_TOTAL,
        },
        "active": {
            "reads": runtime.active_reads,
            "writes": runtime.active_writes,
            "total": runtime.active_total,
        },
        "high_water": {
            "reads": runtime.high_water_reads,
            "writes": runtime.high_water_writes,
            "total": runtime.high_water_total,
        },
        "requests": runtime.requests,
        "rejected": runtime.rejected,
        "failures": runtime.failures,
    }
