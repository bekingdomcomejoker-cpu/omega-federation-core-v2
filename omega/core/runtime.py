"""
Omega Runtime — Main Daemon, Signal Handling, Lifecycle Orchestration

The single entry point. One daemon. One bus. Immutable truth.

Usage:
    python -m omega start --config omega/config.yaml
"""

import asyncio
import importlib
import json
import logging
import signal
import sys
import time
from pathlib import Path
from typing import Optional, Dict, Any

import yaml

from .bus import FederationBus, OmegaEvent
from .ledger import EventLedger
from .checkpoint import CheckpointEngine
from .permissions import PermissionEngine, Capability
from .router import EventRouter
from .supervisor import Supervisor, Service, ServiceState

logger = logging.getLogger("omega.runtime")


class OmegaRuntime:
    """
    The unified orchestration runtime.

    Lifecycle:
        initialize → restore checkpoint → load connectors → start transport → start federation bus → begin listening
    """

    def __init__(self, config_path: str = "omega/config.yaml"):
        self.config = self._load_config(config_path)
        self.name = self.config["runtime"]["name"]
        self.data_dir = Path(self.config["runtime"]["data_dir"]).expanduser()
        self.data_dir.mkdir(parents=True, exist_ok=True)

        self.bus: Optional[FederationBus] = None
        self.ledger: Optional[EventLedger] = None
        self.checkpoint: Optional[CheckpointEngine] = None
        self.permissions: Optional[PermissionEngine] = None
        self.router: Optional[EventRouter] = None
        self.supervisor: Optional[Supervisor] = None
        self.connectors = None  # ConnectorRegistry
        self.authority = None  # IngressAuthority

        self._shutdown_event = asyncio.Event()
        self._running = False

    def _load_config(self, path: str) -> dict:
        """Load YAML configuration."""
        config_path = Path(path)
        if config_path.exists():
            with open(config_path) as f:
                return yaml.safe_load(f)
        return self._default_config()

    def _default_config(self) -> dict:
        return {
            "runtime": {"name": "omega", "data_dir": "~/.omega", "log_level": "INFO"},
            "bus": {"max_queue_size": 1000},
            "ledger": {"max_entries_per_file": 5000},
            "checkpoint": {"interval_seconds": 300, "max_checkpoints": 10, "compress": True},
            "transport": {"http": {"enabled": True, "host": "0.0.0.0", "port": 7777}},
            "permissions": {"default_policy": "deny"},
            "supervisor": {"restart_policy": "exponential_backoff", "max_restarts": 5},
            "connectors": {"autoload": [], "config": {}},
        }

    async def initialize(self):
        """Initialize all core components."""
        logger.info(f"Initializing Omega Runtime: {self.name}")

        log_level = self.config["runtime"].get("log_level", "INFO")
        logging.basicConfig(
            level=getattr(logging, log_level),
            format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
        )

        self.bus = FederationBus(max_queue_size=self.config["bus"]["max_queue_size"])
        self.ledger = EventLedger(
            self.data_dir,
            max_entries_per_file=self.config["ledger"]["max_entries_per_file"],
        )
        self.checkpoint = CheckpointEngine(
            self.data_dir,
            max_checkpoints=self.config["checkpoint"]["max_checkpoints"],
            compress=self.config["checkpoint"]["compress"],
        )
        self.permissions = PermissionEngine(
            default_policy=self.config["permissions"]["default_policy"]
        )
        self.router = EventRouter(self.bus, self.permissions)
        self.supervisor = Supervisor(
            restart_policy=self.config["supervisor"]["restart_policy"],
            max_restarts=self.config["supervisor"]["max_restarts"],
        )

        for cap_config in self.config.get("permissions", {}).get("capabilities", []):
            self.permissions.register_capability(
                Capability(
                    name=cap_config["name"],
                    description=cap_config.get("description", ""),
                )
            )

        admin_caps = [
            "system.read",
            "system.write",
            "connector.execute",
            "federation.join",
            "ledger.read",
            "ledger.write",
            "filesystem.read",
            "filesystem.write",
            "http.request",
            "git.read",
            "git.write",
        ]
        self.permissions.register_role("admin", admin_caps)
        self.permissions.create_principal("system", "service", roles=["admin"])

        self.bus.subscribe_all(self._ledger_handler)

        from omega.connectors.base import ConnectorRegistry

        self.connectors = ConnectorRegistry(self.bus)

        from omega.transport.authority import IngressAuthority
        self.authority = IngressAuthority(
            self.bus, self.permissions, self.config.get("transport", {})
        )

        logger.info("Omega Runtime initialized")

    async def restore(self):
        """Restore from latest checkpoint."""
        logger.info("Restoring checkpoint...")
        checkpoint_data = self.checkpoint.restore_latest()

        if checkpoint_data:
            seq = checkpoint_data.get("ledger_sequence", 0)
            logger.info(f"Restored from checkpoint at ledger sequence {seq}")

            if self.ledger:
                for entry in self.ledger.iter_entries(since_sequence=seq):
                    event = OmegaEvent(
                        event_id=entry["event_id"],
                        event_type=entry["event_type"],
                        source=entry["source"],
                        payload={},
                    )
                    await self.bus.emit(event)
        else:
            logger.info("No checkpoint found, starting fresh")

    async def load_connectors(self):
        """Load and register connectors from config."""
        from omega.connectors.base import ConnectorConfig

        autoload = self.config.get("connectors", {}).get("autoload", [])
        connector_configs = self.config.get("connectors", {}).get("config", {})

        for module_path in autoload:
            try:
                module = importlib.import_module(module_path)
                module_name = module_path.split(".")[-1]
                # Try common naming variants (HTTPClientConnector, HttpClientConnector, etc.)
                parts = module_name.split("_")
                candidates = [
                    "".join(p.capitalize() for p in parts) + "Connector",
                    "".join(p.upper() if len(p) <= 3 else p.capitalize() for p in parts) + "Connector",
                    module_name.replace("_", "").title().replace("Http", "HTTP").replace("Api", "API") + "Connector",
                ]
                # Also scan module for *Connector classes
                connector_class = None
                for name in candidates:
                    connector_class = getattr(module, name, None)
                    if connector_class:
                        break
                if not connector_class:
                    for attr_name in dir(module):
                        if attr_name.endswith("Connector") and attr_name != "BaseConnector":
                            connector_class = getattr(module, attr_name)
                            break
                if not connector_class:
                    logger.warning(f"Could not find connector class in {module_path} (tried {candidates})")
                    continue

                config = ConnectorConfig(
                    name=module_name,
                    enabled=True,
                    config=connector_configs.get(module_name, {}),
                )

                connector = connector_class(config, self.bus)
                self.connectors.register(connector)

                self.supervisor.register(
                    Service(
                        name=f"connector.{module_name}",
                        start_fn=connector.start,
                        stop_fn=connector.stop,
                        health_fn=connector.health,
                        restart_policy="exponential_backoff",
                    )
                )

                for cap in connector.capabilities:
                    self.permissions.register_capability(
                        Capability(
                            name=cap, description=f"Capability from {module_name}"
                        )
                    )

                logger.info(f"Loaded connector: {module_name}")
            except Exception as e:
                logger.error(f"Failed to load connector {module_path}: {e}")

    async def start(self):
        """Start the runtime."""
        await self.initialize()
        await self.restore()
        await self.load_connectors()

        self._register_internal_routes()
        self._register_services()

        await self.supervisor.start_all()

        self._running = True
        logger.info("Ω Omega Runtime is operational")

        await self.bus.emit(
            OmegaEvent(
                event_id=f"boot-{int(time.time() * 1000)}",
                event_type="system.boot",
                source="system",
                payload={"runtime": self.name, "version": "0.1.0"},
            )
        )

        loop = asyncio.get_event_loop()
        try:
            for sig in (signal.SIGINT, signal.SIGTERM):
                loop.add_signal_handler(
                    sig, lambda: asyncio.create_task(self.shutdown())
                )
        except NotImplementedError:
            logger.debug("Signal handlers not supported on this platform")

        checkpoint_interval = self.config["checkpoint"]["interval_seconds"]
        asyncio.create_task(self._checkpoint_loop(checkpoint_interval))

        await self._shutdown_event.wait()

    async def shutdown(self):
        """Graceful shutdown."""
        if not self._running:
            return

        logger.info("Shutting down Omega Runtime...")
        self._running = False

        await self.bus.emit(
            OmegaEvent(
                event_id=f"shutdown-{int(time.time() * 1000)}",
                event_type="system.shutdown",
                source="system",
                payload={"reason": "signal"},
            )
        )

        if self.checkpoint and self.ledger:
            self.checkpoint.save(ledger_sequence=self.ledger.get_last_sequence())

        if self.supervisor:
            await self.supervisor.stop_all()

        if self.bus:
            await self.bus.stop()

        logger.info("Omega Runtime shutdown complete")
        self._shutdown_event.set()

    def _register_internal_routes(self):
        """Register internal system routes."""

        async def handle_health(event: OmegaEvent):
            return {"status": "ok", "runtime": self.name, "uptime": time.time()}

        async def handle_status(event: OmegaEvent):
            return {
                "runtime": self.name,
                "services": self.supervisor.get_status() if self.supervisor else {},
                "bus": self.bus.stats() if self.bus else {},
                "ledger": self.ledger.stats() if self.ledger else {},
                "connectors": self.connectors.list_connectors()
                if self.connectors
                else {},
            }

        async def handle_checkpoint(event: OmegaEvent):
            if self.checkpoint and self.ledger:
                path = self.checkpoint.save(
                    ledger_sequence=self.ledger.get_last_sequence()
                )
                return {"checkpoint": str(path)}
            return {"error": "checkpoint engine not available"}

        async def handle_connector_execute(event: OmegaEvent):
            """Route connector execution requests."""
            payload = event.payload
            connector_name = payload.get("connector")
            action = payload.get("action")
            params = payload.get("params", {})

            if not connector_name or not action:
                return {"error": "missing_connector_or_action"}

            connector = (
                self.connectors.get(connector_name) if self.connectors else None
            )
            if not connector:
                return {"error": f"connector_not_found: {connector_name}"}

            return await connector.execute(action, params)

        self.router.register(
            "system.health", handle_health, required_capability="system.read"
        )
        self.router.register(
            "system.status", handle_status, required_capability="system.read"
        )
        self.router.register(
            "system.checkpoint",
            handle_checkpoint,
            required_capability="system.write",
        )
        self.router.register(
            "connector.execute",
            handle_connector_execute,
            required_capability="connector.execute",
        )

    def _register_services(self):
        """Register core services with the supervisor."""
        self.supervisor.register(
            Service(
                name="bus",
                start_fn=self.bus.start,
                stop_fn=self.bus.stop,
                restart_policy="immediate",
            )
        )
        self.supervisor.register(
            Service(
                name="authority",
                start_fn=self.authority.start,
                stop_fn=self.authority.stop,
                health_fn=self.authority.health,
                restart_policy="exponential_backoff",
            )
        )

    async def _ledger_handler(self, event: OmegaEvent):
        """Global subscriber: write every event to ledger."""
        if self.ledger:
            self.ledger.append(event)

    async def _checkpoint_loop(self, interval: int):
        """Periodic checkpoint creation."""
        while self._running:
            await asyncio.sleep(interval)
            if self.ledger:
                self.checkpoint.save(
                    ledger_sequence=self.ledger.get_last_sequence()
                )
                logger.debug(
                    f"Checkpoint created at sequence {self.ledger.get_last_sequence()}"
                )

    def get_status(self) -> dict:
        """Return full runtime status."""
        return {
            "name": self.name,
            "running": self._running,
            "data_dir": str(self.data_dir),
            "services": self.supervisor.get_status() if self.supervisor else {},
            "bus": self.bus.stats() if self.bus else {},
            "ledger": self.ledger.stats() if self.ledger else {},
            "checkpoints": self.checkpoint.list_checkpoints()
            if self.checkpoint
            else [],
            "connectors": self.connectors.list_connectors()
            if self.connectors
            else {},
        }
