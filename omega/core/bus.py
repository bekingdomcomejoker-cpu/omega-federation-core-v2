"""
Federation Bus — Central Async Pub/Sub Event Backbone

Every component speaks only through the bus.
Observable. Replayable. Recoverable.
"""

import asyncio
import json
import logging
import time
import hashlib
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Any
from collections import defaultdict

logger = logging.getLogger("omega.bus")


@dataclass(frozen=True)
class OmegaEvent:
    """Immutable event on the bus."""
    event_id: str
    event_type: str
    source: str
    payload: dict
    timestamp: float = field(default_factory=time.time)
    parent_id: Optional[str] = None
    signature: Optional[str] = None

    def canonical_bytes(self) -> bytes:
        data = {
            "event_id": self.event_id,
            "event_type": self.event_type,
            "source": self.source,
            "payload": self.payload,
            "timestamp": self.timestamp,
            "parent_id": self.parent_id,
        }
        return json.dumps(data, sort_keys=True, default=str).encode()

    def compute_hash(self) -> str:
        return hashlib.sha3_256(self.canonical_bytes()).hexdigest()


class FederationBus:
    """
    Central async pub/sub event backbone.
    
    All internal communication flows through here:
    Transport → Bus → Permissions → Bus → Router → Bus → Connector → Bus → Ledger
    """

    def __init__(self, max_queue_size: int = 1000):
        self._subscribers: Dict[str, List[Callable]] = defaultdict(list)
        self._global_subscribers: List[Callable] = []
        self._queue: asyncio.Queue = asyncio.Queue(maxsize=max_queue_size)
        self._running = False
        self._task: Optional[asyncio.Task] = None
        self._event_count = 0
        self._dropped_count = 0

    async def start(self):
        """Start the bus dispatch loop."""
        self._running = True
        self._task = asyncio.create_task(self._dispatch_loop())
        logger.info("Federation Bus started")

    async def stop(self):
        """Graceful shutdown."""
        self._running = False
        if self._task:
            await self._task
            while not self._queue.empty():
                event = self._queue.get_nowait()
                await self._route_event(event)
        logger.info("Federation Bus stopped")

    async def emit(self, event: OmegaEvent) -> bool:
        """Emit an event to the bus. Returns False if queue full."""
        try:
            self._queue.put_nowait(event)
            self._event_count += 1
            return True
        except asyncio.QueueFull:
            self._dropped_count += 1
            logger.warning(f"Bus queue full, dropped event {event.event_id}")
            return False

    def subscribe(self, event_type: str, handler: Callable[[OmegaEvent], Any]):
        """Subscribe to a specific event type."""
        self._subscribers[event_type].append(handler)
        logger.debug(f"Subscribed {handler.__name__} to {event_type}")

    def subscribe_all(self, handler: Callable[[OmegaEvent], Any]):
        """Subscribe to ALL events (for ledger, monitoring)."""
        self._global_subscribers.append(handler)

    def unsubscribe(self, event_type: str, handler: Callable):
        """Unsubscribe a handler."""
        if event_type in self._subscribers:
            self._subscribers[event_type] = [
                h for h in self._subscribers[event_type] if h != handler
            ]

    async def _dispatch_loop(self):
        """Main dispatch loop."""
        while self._running:
            try:
                event = await asyncio.wait_for(self._queue.get(), timeout=1.0)
                await self._route_event(event)
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logger.error(f"Bus dispatch error: {e}")

    async def _route_event(self, event: OmegaEvent):
        """Route event to all matching subscribers."""
        for handler in self._global_subscribers:
            try:
                result = handler(event)
                if asyncio.iscoroutine(result):
                    await result
            except Exception as e:
                logger.error(f"Global handler error for {event.event_id}: {e}")

        handlers = self._subscribers.get(event.event_type, [])
        for handler in handlers:
            try:
                result = handler(event)
                if asyncio.iscoroutine(result):
                    await result
            except Exception as e:
                logger.error(f"Handler error for {event.event_type}/{event.event_id}: {e}")

    def stats(self) -> dict:
        return {
            "events_processed": self._event_count,
            "events_dropped": self._dropped_count,
            "queue_size": self._queue.qsize(),
            "subscriber_types": len(self._subscribers),
            "global_subscribers": len(self._global_subscribers),
        }
