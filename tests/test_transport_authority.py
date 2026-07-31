"""Tests for omega.transport.authority"""

import pytest
import pytest_asyncio
import asyncio
from omega.core.bus import FederationBus
from omega.core.permissions import PermissionEngine
from omega.transport.authority import IngressAuthority


@pytest_asyncio.fixture
async def authority():
    bus = FederationBus(max_queue_size=100)
    perms = PermissionEngine(default_policy="allow")
    auth = IngressAuthority(
        bus,
        perms,
        {
            "http": {"enabled": True, "host": "127.0.0.1", "port": 17777},
            "websocket": {"enabled": True, "host": "127.0.0.1", "port": 17778},
        },
    )
    await bus.start()
    yield auth
    try:
        await auth.stop()
    except Exception:
        pass
    await bus.stop()


@pytest.mark.asyncio
async def test_authority_starts_transports(authority):
    """Authority starts HTTP and WS transports."""
    await authority.start()

    assert authority.http is not None
    assert authority.ws is not None
    assert authority.health() is True

    await authority.stop()


@pytest.mark.asyncio
async def test_authority_health_after_stop(authority):
    """Health returns False after stop."""
    await authority.start()
    await authority.stop()

    assert authority.health() is False


@pytest.mark.asyncio
async def test_authority_disabled_transports():
    """Authority handles disabled transports gracefully."""
    bus = FederationBus(max_queue_size=10)
    perms = PermissionEngine(default_policy="allow")
    auth = IngressAuthority(
        bus,
        perms,
        {
            "http": {"enabled": False},
            "websocket": {"enabled": False},
        },
    )
    await bus.start()

    await auth.start()
    assert auth.http is None
    assert auth.ws is None
    assert auth.health() is False

    await auth.stop()
    await bus.stop()
