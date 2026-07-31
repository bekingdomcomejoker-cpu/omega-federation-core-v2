"""
Supervisor — Service Lifecycle Management

Start, stop, monitor, restart services with configurable policies.
"""

import asyncio
import logging
import time
from enum import Enum
from typing import Callable, Dict, Optional, Any
from dataclasses import dataclass, field

logger = logging.getLogger("omega.supervisor")


class ServiceState(Enum):
    STOPPED = "stopped"
    STARTING = "starting"
    RUNNING = "running"
    DEGRADED = "degraded"
    FAILED = "failed"
    RESTARTING = "restarting"


@dataclass
class Service:
    """A managed service."""
    name: str
    start_fn: Callable
    stop_fn: Optional[Callable] = None
    health_fn: Optional[Callable] = None
    restart_policy: str = "exponential_backoff"
    max_restarts: int = 5
    restart_window: int = 60
    dependencies: list = field(default_factory=list)
    state: ServiceState = ServiceState.STOPPED
    restart_count: int = 0
    last_restart: float = 0.0
    _task: Optional[asyncio.Task] = None


class Supervisor:
    """
    Manages the lifecycle of all runtime services.
    
    Services:
    - Bus
    - Ledger
    - Transport (HTTP, WS)
    - Federation Mesh
    - Connectors
    - Checkpoint timer
    """

    def __init__(self, restart_policy: str = "exponential_backoff", max_restarts: int = 5, restart_window: int = 60):
        self.restart_policy = restart_policy
        self.max_restarts = max_restarts
        self.restart_window = restart_window
        self._services: Dict[str, Service] = {}
        self._running = False
        self._monitor_task: Optional[asyncio.Task] = None

    def register(self, service: Service):
        """Register a service with the supervisor."""
        self._services[service.name] = service
        logger.info(f"Registered service: {service.name}")

    async def start_all(self):
        """Start all registered services in dependency order."""
        self._running = True
        started = set()
        
        while len(started) < len(self._services):
            for name, svc in self._services.items():
                if name in started:
                    continue
                if all(dep in started for dep in svc.dependencies):
                    await self._start_service(svc)
                    started.add(name)
        
        self._monitor_task = asyncio.create_task(self._health_monitor())
        logger.info("Supervisor: all services started")

    async def stop_all(self):
        """Graceful shutdown of all services."""
        self._running = False
        
        if self._monitor_task:
            self._monitor_task.cancel()
            try:
                await self._monitor_task
            except asyncio.CancelledError:
                pass
        
        stopped = set()
        while len(stopped) < len(self._services):
            for name, svc in self._services.items():
                if name in stopped:
                    continue
                dependents = [s for s in self._services.values() if name in s.dependencies and s.name not in stopped]
                if not dependents:
                    await self._stop_service(svc)
                    stopped.add(name)
        
        logger.info("Supervisor: all services stopped")

    async def restart_service(self, name: str):
        """Restart a specific service."""
        if name not in self._services:
            logger.error(f"Unknown service: {name}")
            return
        
        svc = self._services[name]
        await self._stop_service(svc)
        await self._start_service(svc)

    def get_status(self) -> Dict[str, dict]:
        """Get status of all services."""
        return {
            name: {
                "state": svc.state.value,
                "restarts": svc.restart_count,
                "last_restart": svc.last_restart,
            }
            for name, svc in self._services.items()
        }

    async def _start_service(self, svc: Service):
        """Start a single service."""
        svc.state = ServiceState.STARTING
        try:
            import inspect
            if inspect.iscoroutinefunction(svc.start_fn):
                await svc.start_fn()
            else:
                svc.start_fn()
            
            svc.state = ServiceState.RUNNING
            logger.info(f"Service started: {svc.name}")
        except Exception as e:
            svc.state = ServiceState.FAILED
            logger.error(f"Service start failed: {svc.name}: {e}")
            await self._handle_failure(svc)

    async def _stop_service(self, svc: Service):
        """Stop a single service."""
        if svc._task and not svc._task.done():
            svc._task.cancel()
            try:
                await svc._task
            except asyncio.CancelledError:
                pass
        
        if svc.stop_fn:
            try:
                import inspect
                if inspect.iscoroutinefunction(svc.stop_fn):
                    await svc.stop_fn()
                else:
                    svc.stop_fn()
            except Exception as e:
                logger.error(f"Service stop error: {svc.name}: {e}")
        
        svc.state = ServiceState.STOPPED
        logger.info(f"Service stopped: {svc.name}")

    async def _health_monitor(self):
        """Periodic health check of all services."""
        while self._running:
            await asyncio.sleep(10)
            for name, svc in self._services.items():
                if svc.state != ServiceState.RUNNING:
                    continue
                
                if svc.health_fn:
                    try:
                        import inspect
                        if inspect.iscoroutinefunction(svc.health_fn):
                            healthy = await svc.health_fn()
                        else:
                            healthy = svc.health_fn()
                        
                        if not healthy:
                            logger.warning(f"Service unhealthy: {name}")
                            svc.state = ServiceState.DEGRADED
                            await self._handle_failure(svc)
                    except Exception as e:
                        logger.error(f"Health check error: {name}: {e}")
                        await self._handle_failure(svc)

    async def _handle_failure(self, svc: Service):
        """Handle service failure based on restart policy."""
        now = time.time()
        
        if now - svc.last_restart > self.restart_window:
            svc.restart_count = 0
        
        if svc.restart_count >= self.max_restarts:
            logger.error(f"Service {svc.name} exceeded max restarts, leaving failed")
            svc.state = ServiceState.FAILED
            return
        
        if svc.restart_policy == "never":
            svc.state = ServiceState.FAILED
            return
        
        svc.state = ServiceState.RESTARTING
        svc.restart_count += 1
        svc.last_restart = now
        
        delay = 0
        if self.restart_policy == "exponential_backoff":
            delay = min(2 ** svc.restart_count, 60)
        elif self.restart_policy == "fixed":
            delay = 5
        
        logger.info(f"Restarting {svc.name} in {delay}s (attempt {svc.restart_count})")
        await asyncio.sleep(delay)
        await self._start_service(svc)
