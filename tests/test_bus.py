"""Tests for omega.core.bus"""

import pytest
import pytest_asyncio
import asyncio
from omega.core.bus import FederationBus, OmegaEvent


@pytest_asyncio.fixture
async def bus():
    b = FederationBus(max_queue_size=100)
    await b.start()
    yield b
    await b.stop()


@pytest.mark.asyncio
async def test_emit_and_subscribe(bus):
    """Events are routed to subscribers."""
    received = []
    
    def handler(event):
        received.append(event)
    
    bus.subscribe("test.event", handler)
    
    event = OmegaEvent(
        event_id="e1",
        event_type="test.event",
        source="test",
        payload={"x": 1}
    )
    
    await bus.emit(event)
    await asyncio.sleep(0.1)
    
    assert len(received) == 1
    assert received[0].event_id == "e1"


@pytest.mark.asyncio
async def test_global_subscriber(bus):
    """Global subscribers receive all events."""
    received = []
    
    def handler(event):
        received.append(event.event_type)
    
    bus.subscribe_all(handler)
    
    await bus.emit(OmegaEvent("e1", "type.a", "src", {}))
    await bus.emit(OmegaEvent("e2", "type.b", "src", {}))
    await asyncio.sleep(0.1)
    
    assert len(received) == 2
    assert "type.a" in received
    assert "type.b" in received


@pytest.mark.asyncio
async def test_event_hash_determinism():
    """Same event data yields same hash."""
    e1 = OmegaEvent("e1", "test", "src", {"a": 1}, timestamp=1000.0)
    e2 = OmegaEvent("e1", "test", "src", {"a": 1}, timestamp=1000.0)
    
    assert e1.compute_hash() == e2.compute_hash()


@pytest.mark.asyncio
async def test_queue_full():
    """Full queue drops events gracefully."""
    bus = FederationBus(max_queue_size=1)
    await bus.start()
    
    await bus.emit(OmegaEvent("e1", "test", "src", {}))
    result = await bus.emit(OmegaEvent("e2", "test", "src", {}))
    
    await bus.stop()
    assert result is False
