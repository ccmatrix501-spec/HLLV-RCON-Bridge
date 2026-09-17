from __future__ import annotations

import hmac
import os

from fastapi import Request
from fastapi.responses import JSONResponse


PUBLIC_CONTROL_HEADER = "x-hllv-secret"
ALT_CONTROL_HEADER = "x-hllv-control-secret"


def _is_private_host(host_header: str) -> bool:
    host = str(host_header or "").strip().lower().split(":", 1)[0]
    return (
        host.endswith(".railway.internal")
        or host in {"localhost", "127.0.0.1", "::1"}
    )


def _safe_equal(a: str, b: str) -> bool:
    aa = str(a or "").encode("utf-8")
    bb = str(b or "").encode("utf-8")
    return bool(aa) and len(aa) == len(bb) and hmac.compare_digest(aa, bb)


def install_public_api_guard(app) -> None:
    """Protect public /api/v2 control calls while leaving Railway-private calls alone.

    The web controller talks to this bridge over Railway private networking and does
    not need a secret header. Cross-project integrations such as the Discord bot use
    the public Railway URL and must supply the existing HLLV_ADMIN_ALERT_SECRET in
    X-HLLV-Secret (or X-HLLV-Control-Secret).
    """

    if getattr(app.state, "public_api_guard_installed", False):
        return
    app.state.public_api_guard_installed = True

    @app.middleware("http")
    async def _guard_public_control(request: Request, call_next):
        path = str(request.url.path or "")
        if not path.startswith("/api/v2/"):
            return await call_next(request)

        if _is_private_host(request.headers.get("host", "")):
            return await call_next(request)

        expected = str(os.getenv("HLLV_ADMIN_ALERT_SECRET", "") or "").strip()
        if not expected:
            return JSONResponse(
                status_code=503,
                content={"error": "Public HLL:V control API is disabled because its shared secret is not configured."},
            )

        supplied = str(
            request.headers.get(PUBLIC_CONTROL_HEADER)
            or request.headers.get(ALT_CONTROL_HEADER)
            or ""
        ).strip()
        if not _safe_equal(expected, supplied):
            return JSONResponse(
                status_code=401,
                content={"error": "Invalid HLL:V control secret."},
            )

        return await call_next(request)
