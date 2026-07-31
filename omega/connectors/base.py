"""
Connector Framework — Pluggable Capability System

Every external system becomes a connector: ChatGPT, Gemini, GitHub, Drive, Dropbox, MikroTik, Android, Ubuntu.
All eventually become omega.execute(...).
"""

import logging
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
from dataclasses import dataclass

from omega.core.bus import OmegaEvent, FederationBus

logger = logging.getLogger("omega.connectors")


@dataclass
class ConnectorConfig:
    """Configuration for a connector."""
    name: str
    enabled: bool = True
    config: Dict[str, Any] = None
    
    def __post_init__(self):
        if self.config is None:
            self.config = {}


class BaseConnector(ABC):
    """
    Base class for all Omega connectors.
    
    Connectors:
    - Subscribe to bus events they handle
    - Emit events for results or state changes
    - Declare required capabilities
    """

    def __init__(self, config: ConnectorConfig, bus: FederationBus):
        self.config = config
        self.bus = bus
        self.name = config.name
        self._running = False

    @property
    @abstractmethod
    def capabilities(self) -> list:
        """Return list of capabilities this connector provides."""
        pass

    @abstractmethod
    async def start(self):
        """Start the connector."""
        pass

    @abstractmethod
    async def stop(self):
        """Stop the connector."""
        pass

    @abstractmethod
    async def execute(self, action: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """Execute an action. This is the main interface."""
        pass

    async def emit(self, event_type: str, payload: Dict[str, Any], source: str = None):
        """Emit an event to the bus."""
        from omega.core.bus import OmegaEvent
        import time
        
        event = OmegaEvent(
            event_id=f"{self.name}-{int(time.time()*1000)}",
            event_type=event_type,
            source=source or self.name,
            payload=payload,
        )
        await self.bus.emit(event)
        return event

    def health(self) -> bool:
        """Return health status. Override in subclass."""
        return True


class ConnectorRegistry:
    """Registry for all loaded connectors."""

    def __init__(self, bus: FederationBus):
        self.bus = bus
        self._connectors: Dict[str, BaseConnector] = {}

    def register(self, connector: BaseConnector):
        """Register a connector."""
        self._connectors[connector.name] = connector
        logger.info(f"Connector registered: {connector.name}")

    def get(self, name: str) -> Optional[BaseConnector]:
        return self._connectors.get(name)

    def list_connectors(self) -> Dict[str, dict]:
        return {
            name: {
                "capabilities": conn.capabilities,
                "running": conn._running,
                "healthy": conn.health(),
            }
            for name, conn in self._connectors.items()
        }

    async def start_all(self):
        for conn in self._connectors.values():
            if conn.config.enabled:
                await conn.start()
                conn._running = True

    async def stop_all(self):
        for conn in self._connectors.values():
            await conn.stop()
            conn._running = False
