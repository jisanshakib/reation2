"""Lightweight /healthz keep-alive endpoint for Render / Railway."""

from __future__ import annotations

import logging
from aiohttp import web

log = logging.getLogger(__name__)


async def _healthz(_request: web.Request) -> web.Response:
    return web.json_response({"status": "ok"})


async def start_health_server(port: int = 8080) -> web.AppRunner:
    """Start a minimal HTTP server on *port* and return its runner
    (call ``runner.cleanup()`` to stop)."""
    app = web.Application()
    app.router.add_get("/healthz", _healthz)
    app.router.add_get("/", _healthz)  # root fallback

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    log.info("Health-check server listening on 0.0.0.0:%d", port)
    return runner
