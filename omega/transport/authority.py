"""
Ingress Authority — Unified Transport Abstraction

The Silent Listener formalized. All ingress (HTTP, WebSocket, future TCP/UDP)
flows through a single authority that validates, logs, and emits to the bus.
"""

import asyncio
import logging
from typing import Optional, Dict, Any

from omega.core.bus import FederationBus, OmegaEvent
from omega.core.permissions import PermissionEngine
from .http import HTTPTransport
from .ws import WebSocketTransport

logger = logging.getLogger("omega.transport.authority")


class IngressAuthority:
    """
    Unified ingress authority.

    Manages all transport listeners:
    - HTTP REST API
    - WebSocket real-time stream
    - (Future: TCP socket, UDP, Unix domain socket)

    Every incoming request is:
    1. Authenticated (if auth enabled)
    2. Authorized via permissions engine
    3. Converted to OmegaEvent
    4. Emitted to bus
    5. Response returned (async for HTTP, broadcast for WS)
    """

    def __init__(self, bus: FederationBus, permissions: PermissionEngine, config: dict):
        self.bus = bus
        self.permissions = permissions
        self.config = config
        self.http: Optional[HTTPTransport] = None
        self.ws: Optional[WebSocketTransport] = None
        self._running = False

    async def start(self):
        """Start all configured transports."""
        self._running = True

        http_config = self.config.get("http", {})
        if http_config.get("enabled", True):
            self.http = HTTPTransport(
                self.bus,
                self.permissions,
                host=http_config.get("host", "0.0.0.0"),
                port=http_config.get("port", 7777),
            )
            await self.http.start()

        ws_config = self.config.get("websocket", {})
        if ws_config.get("enabled", True):
            self.ws = WebSocketTransport(
                self.bus,
                self.permissions,
                host=ws_config.get("host", "0.0.0.0"),
                port=ws_config.get("port", 7778),
            )
            await self.ws.start()

        logger.info("Ingress Authority operational")

    async def stop(self):
        """Stop all transports."""
        self._running = False

        if self.http:
            await self.http.stop()
        if self.ws:
            await self.ws.stop()

        logger.info("Ingress Authority stopped")

    def health(self) -> bool:
        """Health check: at least one transport must be running."""
        if not self._running:
            return False
        http_ok = self.http is not None and getattr(self.http, "runner", None) is not None
        ws_ok = self.ws is not None and getattr(self.ws, "_server", None) is not None
        return http_ok or ws_ok

    async def broadcast(self, message: dict):
        """Broadcast a message to all WS clients."""
        if self.ws:
            await self.ws.broadcast(message)
