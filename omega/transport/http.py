"""
HTTP Transport — Ingress to Bus

Receives HTTP requests, converts to bus events, returns responses.
"""

import json
import logging
from typing import Optional

from aiohttp import web

from omega.core.bus import FederationBus, OmegaEvent
from omega.core.permissions import PermissionEngine

logger = logging.getLogger("omega.transport.http")


class HTTPTransport:
    """HTTP ingress transport."""

    def __init__(
        self,
        bus: FederationBus,
        permissions: PermissionEngine,
        host: str = "0.0.0.0",
        port: int = 7777,
    ):
        self.bus = bus
        self.permissions = permissions
        self.host = host
        self.port = port
        self.app = web.Application()
        self._setup_routes()
        self.runner: Optional[web.AppRunner] = None

    def _setup_routes(self):
        self.app.router.add_get("/health", self.handle_health)
        self.app.router.add_get("/status", self.handle_status)
        self.app.router.add_post("/event", self.handle_event)
        self.app.router.add_get("/bus/stats", self.handle_bus_stats)
        self.app.router.add_post("/connector/execute", self.handle_connector_execute)

    async def handle_health(self, request):
        return web.json_response({"status": "ok", "transport": "http"})

    async def handle_status(self, request):
        return web.json_response({
            "transport": "http",
            "host": self.host,
            "port": self.port,
        })

    async def handle_event(self, request):
        """Receive an event via HTTP and emit to bus."""
        try:
            data = await request.json()

            event = OmegaEvent(
                event_id=data.get("event_id", f"http-{id(data)}"),
                event_type=data["event_type"],
                source=data.get("source", "http.ingress"),
                payload=data.get("payload", {}),
                parent_id=data.get("parent_id"),
            )

            principal = data.get("source", "anonymous")
            verdict = self.permissions.check_event(principal, event.event_type)
            if verdict.value == "deny":
                return web.json_response({"status": "denied"}, status=403)

            success = await self.bus.emit(event)
            return web.json_response({
                "status": "emitted" if success else "dropped",
                "event_id": event.event_id,
            })
        except Exception as e:
            logger.error(f"HTTP event handler error: {e}")
            return web.json_response({"status": "error", "reason": str(e)}, status=500)

    async def handle_connector_execute(self, request):
        """Direct connector execution via HTTP."""
        try:
            data = await request.json()
            event = OmegaEvent(
                event_id=f"http-conn-{id(data)}",
                event_type="connector.execute",
                source=data.get("source", "http.ingress"),
                payload=data.get("payload", {}),
            )
            success = await self.bus.emit(event)
            return web.json_response({
                "status": "emitted" if success else "dropped",
                "event_id": event.event_id,
            })
        except Exception as e:
            logger.error(f"HTTP connector execute error: {e}")
            return web.json_response({"status": "error", "reason": str(e)}, status=500)

    async def handle_bus_stats(self, request):
        return web.json_response(self.bus.stats())

    async def start(self):
        """Start HTTP server."""
        self.runner = web.AppRunner(self.app)
        await self.runner.setup()
        site = web.TCPSite(self.runner, self.host, self.port)
        await site.start()
        logger.info(f"HTTP transport listening on http://{self.host}:{self.port}")

    async def stop(self):
        """Stop HTTP server."""
        if self.runner:
            await self.runner.cleanup()
            self.runner = None
        logger.info("HTTP transport stopped")

    def health(self) -> bool:
        return self.runner is not None
