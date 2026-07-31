"""
WebSocket Transport — Real-time Ingress to Bus

Bidirectional WebSocket for real-time event streaming.
"""

import json
import logging
from typing import Optional, Set

import websockets

from omega.core.bus import FederationBus, OmegaEvent
from omega.core.permissions import PermissionEngine

logger = logging.getLogger("omega.transport.ws")


class WebSocketTransport:
    """WebSocket ingress transport."""

    def __init__(self, bus: FederationBus, permissions: PermissionEngine, host: str = "0.0.0.0", port: int = 7778):
        self.bus = bus
        self.permissions = permissions
        self.host = host
        self.port = port
        self._clients: Set = set()
        self._server = None

    async def handler(self, websocket, path=None):
        """Handle a WebSocket connection."""
        self._clients.add(websocket)
        logger.info(f"WS client connected: {websocket.remote_address}")
        
        try:
            async for message in websocket:
                try:
                    data = json.loads(message)
                    
                    event = OmegaEvent(
                        event_id=data.get("event_id", f"ws-{id(data)}"),
                        event_type=data["event_type"],
                        source=data.get("source", f"ws.{websocket.remote_address}"),
                        payload=data.get("payload", {}),
                        parent_id=data.get("parent_id"),
                    )
                    
                    principal = data.get("source", "anonymous")
                    verdict = self.permissions.check_event(principal, event.event_type)
                    if verdict.value == "deny":
                        await websocket.send(json.dumps({"status": "denied"}))
                        continue
                    
                    success = await self.bus.emit(event)
                    await websocket.send(json.dumps({
                        "status": "emitted" if success else "dropped",
                        "event_id": event.event_id,
                    }))
                    
                except json.JSONDecodeError:
                    await websocket.send(json.dumps({"status": "error", "reason": "invalid_json"}))
                except Exception as e:
                    logger.error(f"WS handler error: {e}")
                    await websocket.send(json.dumps({"status": "error", "reason": str(e)}))
                    
        except websockets.exceptions.ConnectionClosed:
            pass
        finally:
            self._clients.discard(websocket)
            logger.info(f"WS client disconnected: {websocket.remote_address}")

    async def start(self):
        """Start WebSocket server."""
        self._server = await websockets.serve(
            self.handler,
            self.host,
            self.port,
            ping_interval=20,
            ping_timeout=10,
        )
        logger.info(f"WebSocket transport listening on ws://{self.host}:{self.port}")

    async def stop(self):
        """Stop WebSocket server."""
        if self._server:
            self._server.close()
            await self._server.wait_closed()
            self._server = None
        
        for client in list(self._clients):
            try:
                await client.close()
            except Exception:
                pass
        self._clients.clear()
        
        logger.info("WebSocket transport stopped")

    async def broadcast(self, message: dict):
        """Broadcast a message to all connected WS clients."""
        if not self._clients:
            return
        
        data = json.dumps(message)
        disconnected = set()
        
        for client in self._clients:
            try:
                await client.send(data)
            except Exception:
                disconnected.add(client)
        
        self._clients -= disconnected
