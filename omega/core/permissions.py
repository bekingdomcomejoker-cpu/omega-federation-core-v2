"""
Permission Engine — Capability-Based Access Control

Every event is checked against declared capabilities before routing.
Default deny. Explicit grant required.
"""

import logging
from enum import Enum
from typing import Dict, List, Set, Optional
from dataclasses import dataclass, field

logger = logging.getLogger("omega.permissions")


class PermissionVerdict(Enum):
    ALLOW = "allow"
    DENY = "deny"
    DEFER = "defer"


@dataclass
class Capability:
    """A declared capability with metadata."""
    name: str
    description: str = ""
    requires: List[str] = field(default_factory=list)


@dataclass
class Principal:
    """An entity that can hold capabilities."""
    id: str
    type: str
    capabilities: Set[str] = field(default_factory=set)
    roles: Set[str] = field(default_factory=set)


class PermissionEngine:
    """
    Capability-based access control.
    
    Policies:
    - default_policy: "deny" or "allow"
    - explicit grants override defaults
    - roles bundle capabilities
    """

    def __init__(self, default_policy: str = "deny"):
        self.default_policy = default_policy
        self._capabilities: Dict[str, Capability] = {}
        self._principals: Dict[str, Principal] = {}
        self._roles: Dict[str, Set[str]] = {}
        self._explicit_grants: Dict[str, Set[str]] = {}
        self._explicit_denies: Dict[str, Set[str]] = {}

    def register_capability(self, cap: Capability):
        """Register a known capability."""
        self._capabilities[cap.name] = cap
        logger.debug(f"Registered capability: {cap.name}")

    def register_role(self, role_name: str, capabilities: List[str]):
        """Define a role as a bundle of capabilities."""
        self._roles[role_name] = set(capabilities)
        logger.debug(f"Registered role {role_name}: {capabilities}")

    def create_principal(self, principal_id: str, ptype: str, roles: List[str] = None) -> Principal:
        """Create a principal with role-derived capabilities."""
        caps = set()
        if roles:
            for role in roles:
                caps.update(self._roles.get(role, set()))
        
        principal = Principal(id=principal_id, type=ptype, roles=set(roles or []), capabilities=caps)
        self._principals[principal_id] = principal
        return principal

    def grant(self, principal_id: str, capability: str):
        """Explicitly grant a capability to a principal."""
        if principal_id not in self._explicit_grants:
            self._explicit_grants[principal_id] = set()
        self._explicit_grants[principal_id].add(capability)
        
        if principal_id in self._principals:
            self._principals[principal_id].capabilities.add(capability)

    def deny(self, principal_id: str, capability: str):
        """Explicitly deny a capability. Deny overrides grant."""
        if principal_id not in self._explicit_denies:
            self._explicit_denies[principal_id] = set()
        self._explicit_denies[principal_id].add(capability)

    def check(self, principal_id: str, capability: str) -> PermissionVerdict:
        """Check if principal has capability."""
        if principal_id in self._explicit_denies and capability in self._explicit_denies[principal_id]:
            return PermissionVerdict.DENY
        
        if principal_id in self._explicit_grants and capability in self._explicit_grants[principal_id]:
            return PermissionVerdict.ALLOW
        
        principal = self._principals.get(principal_id)
        if principal and capability in principal.capabilities:
            return PermissionVerdict.ALLOW
        
        if self.default_policy == "allow":
            return PermissionVerdict.ALLOW
        return PermissionVerdict.DENY

    def check_event(self, principal_id: str, event_type: str) -> PermissionVerdict:
        """Map event types to required capabilities and check."""
        capability_map = {
            "system.read": ["system.state.get", "system.health", "ledger.read"],
            "system.write": ["system.state.set", "connector.execute"],
            "federation.join": ["federation.join"],
            "federation.gossip": ["federation.join"],
            "ledger.append": ["ledger.write"],
            "connector.invoke": ["connector.execute"],
        }
        
        required = capability_map.get(event_type, ["system.read"])
        
        for cap in required:
            verdict = self.check(principal_id, cap)
            if verdict == PermissionVerdict.DENY:
                return PermissionVerdict.DENY
        
        return PermissionVerdict.ALLOW

    def list_capabilities(self) -> List[str]:
        return list(self._capabilities.keys())

    def list_principals(self) -> List[str]:
        return list(self._principals.keys())
