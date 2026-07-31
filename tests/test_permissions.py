"""Tests for omega.core.permissions"""

import pytest
from omega.core.permissions import PermissionEngine, PermissionVerdict, Capability


@pytest.fixture
def engine():
    pe = PermissionEngine(default_policy="deny")
    pe.register_capability(Capability("system.read", "Read system"))
    pe.register_capability(Capability("system.write", "Write system"))
    pe.register_role("admin", ["system.read", "system.write"])
    return pe


def test_default_deny(engine):
    """Unknown principal denied by default."""
    assert engine.check("unknown", "system.read") == PermissionVerdict.DENY


def test_role_grant(engine):
    """Role-derived capabilities work."""
    engine.create_principal("alice", "user", roles=["admin"])
    assert engine.check("alice", "system.read") == PermissionVerdict.ALLOW
    assert engine.check("alice", "system.write") == PermissionVerdict.ALLOW


def test_explicit_grant(engine):
    """Explicit grants override defaults."""
    engine.create_principal("bob", "user")
    engine.grant("bob", "system.read")
    assert engine.check("bob", "system.read") == PermissionVerdict.ALLOW
    assert engine.check("bob", "system.write") == PermissionVerdict.DENY


def test_explicit_deny_overrides_grant(engine):
    """Deny always wins."""
    engine.create_principal("charlie", "user", roles=["admin"])
    engine.deny("charlie", "system.read")
    assert engine.check("charlie", "system.read") == PermissionVerdict.DENY


def test_event_type_mapping(engine):
    """Event types map to capabilities correctly."""
    engine.create_principal("svc", "service", roles=["admin"])
    assert engine.check_event("svc", "system.health") == PermissionVerdict.ALLOW
