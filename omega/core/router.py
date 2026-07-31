"""
Router — Event Routing, Dispatch, Handler Registry

Receives events from the bus, checks permissions, routes to connectors or internal handlers.
"""

import logging
from typing import Callable, Dict, List, Optional, Any
from dataclasses import dataclass

from .bus import OmegaEvent, FederationBus
from .permissions import PermissionEngine, PermissionVerdict

logger = logging.getLogger("omega.router")


@dataclass
class Route:
    """A registered route."""
    event_type: str
    handler: Callable
    required_capability: Optional[str] = None
    priority: int = 0


class EventRouter:
    """
    Central router for the Omega runtime.
    
    Flow:
    Bus Event → Permission Check → Route Match → Handler Execution → Ledger Record
    """

    def __init__(self, bus: FederationBus, permissions: PermissionEngine):
        self.bus = bus
        self.permissions = permissions
        self._routes: Dict[str, List[Route]] = {}
        self._fallback_handler: Optional[Callable] = None
        self._error_handler: Optional[Callable] = None

    def register(self, event_type: str, handler: Callable, required_capability: str = None, priority: int = 0):
        """Register a handler for an event type."""
        if event_type not in self._routes:
            self._routes[event_type] = []
        
        route = Route(event_type, handler, required_capability, priority)
        self._routes[event_type].append(route)
        self._routes[event_type].sort(key=lambda r: r.priority, reverse=True)
        
        self.bus.subscribe(event_type, self._wrap_handler(route))
        
        logger.info(f"Registered route: {event_type} → {handler.__name__} (cap={required_capability})")

    def register_fallback(self, handler: Callable):
        """Handler for unmatched events."""
        self._fallback_handler = handler
        self.bus.subscribe_all(self._wrap_fallback(handler))

    def register_error_handler(self, handler: Callable):
        """Handler for routing errors."""
        self._error_handler = handler

    def _wrap_handler(self, route: Route) -> Callable:
        """Wrap a handler with permission check and error handling."""
        async def wrapped(event: OmegaEvent):
            principal_id = event.source
            if route.required_capability:
                verdict = self.permissions.check(principal_id, route.required_capability)
                if verdict == PermissionVerdict.DENY:
                    logger.warning(f"Permission denied: {principal_id} → {route.required_capability}")
                    return {"status": "denied", "reason": "insufficient_capability"}
            
            try:
                import inspect
                if inspect.iscoroutinefunction(route.handler):
                    result = await route.handler(event)
                else:
                    result = route.handler(event)
                
                return {"status": "ok", "result": result}
            except Exception as e:
                logger.error(f"Handler error for {event.event_id}: {e}")
                if self._error_handler:
                    return await self._error_handler(event, e)
                return {"status": "error", "reason": str(e)}
        
        return wrapped

    def _wrap_fallback(self, handler: Callable) -> Callable:
        """Wrap fallback handler."""
        async def wrapped(event: OmegaEvent):
            if event.event_type in self._routes and self._routes[event.event_type]:
                return None
            
            try:
                import inspect
                if inspect.iscoroutinefunction(handler):
                    return await handler(event)
                return handler(event)
            except Exception as e:
                logger.error(f"Fallback handler error: {e}")
                return None
        
        return wrapped

    def list_routes(self) -> List[dict]:
        """List all registered routes."""
        routes = []
        for event_type, route_list in self._routes.items():
            for route in route_list:
                routes.append({
                    "event_type": event_type,
                    "handler": route.handler.__name__,
                    "capability": route.required_capability,
                    "priority": route.priority,
                })
        return routes
