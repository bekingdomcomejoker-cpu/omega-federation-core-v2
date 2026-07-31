"""
Federation Mesh — Bus Service for Multi-Node Coordination

The federation layer runs as a service on the bus, not as a separate process.
"""

import asyncio
import json
import logging
from typing import Optional

from omega.core.bus import FederationBus, OmegaEvent

logger = logging.getLogger("omega.federation.mesh")


class FederationMeshService:
    """
    Federation mesh as an Omega bus service.
    
    Subscribes to federation events on the bus and coordinates
    with peer nodes via the separate omega-federation-unified protocol.
    """

    def __init__(self, bus: FederationBus, config: dict):
        self.bus = bus
        self.config = config
        self.enabled = config.get("enabled", False)
        self.bootstrap_peers = config.get("bootstrap_peers", [])
        self._running = False

    async def start(self):
        """Start the federation mesh service."""
        if not self.enabled:
            logger.info("Federation mesh disabled")
            return
        
        self._running = True
        
        self.bus.subscribe("federation.join", self._handle_join)
        self.bus.subscribe("federation.gossip", self._handle_gossip)
        self.bus.subscribe("federation.propose", self._handle_propose)
        
        for peer in self.bootstrap_peers:
            await self._bootstrap_peer(peer)
        
        logger.info(f"Federation mesh started with {len(self.bootstrap_peers)} bootstrap peers")

    async def stop(self):
        """Stop the federation mesh service."""
        self._running = False
        logger.info("Federation mesh stopped")

    async def _handle_join(self, event: OmegaEvent):
        """Handle a federation join event."""
        logger.info(f"Federation join request from {event.source}: {event.payload}")
        return {"status": "acknowledged"}

    async def _handle_gossip(self, event: OmegaEvent):
        """Handle federation gossip."""
        logger.debug(f"Federation gossip from {event.source}")
        return {"status": "received"}

    async def _handle_propose(self, event: OmegaEvent):
        """Handle a consensus proposal."""
        logger.info(f"Federation proposal from {event.source}: {event.payload.get('proposal_id')}")
        return {"status": "received"}

    async def _bootstrap_peer(self, peer_addr: str):
        """Connect to a bootstrap peer."""
        logger.info(f"Bootstrapping to peer: {peer_addr}")

    def health(self) -> bool:
        return self._running if self.enabled else True
