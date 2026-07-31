"""Tests for omega.core.supervisor"""

import pytest
import asyncio
from omega.core.supervisor import Supervisor, Service, ServiceState


@pytest.mark.asyncio
async def test_service_lifecycle():
    """Services start and stop correctly."""
    started = False
    stopped = False
    
    async def start_fn():
        nonlocal started
        started = True
    
    async def stop_fn():
        nonlocal stopped
        stopped = True
    
    svc = Service(name="test", start_fn=start_fn, stop_fn=stop_fn)
    sup = Supervisor()
    sup.register(svc)
    
    await sup.start_all()
    assert started is True
    assert svc.state == ServiceState.RUNNING
    
    await sup.stop_all()
    assert stopped is True
    assert svc.state == ServiceState.STOPPED


@pytest.mark.asyncio
async def test_dependency_order():
    """Services start in dependency order."""
    order = []
    
    async def make_start(name):
        async def start():
            order.append(name)
        return start
    
    svc_a = Service(name="a", start_fn=await make_start("a"))
    svc_b = Service(name="b", start_fn=await make_start("b"), dependencies=["a"])
    svc_c = Service(name="c", start_fn=await make_start("c"), dependencies=["b"])
    
    sup = Supervisor()
    sup.register(svc_a)
    sup.register(svc_b)
    sup.register(svc_c)
    
    await sup.start_all()
    assert order == ["a", "b", "c"]


@pytest.mark.asyncio
async def test_restart_policy():
    """Failed services restart with backoff."""
    attempts = 0
    
    async def failing_start():
        nonlocal attempts
        attempts += 1
        raise RuntimeError("fail")
    
    svc = Service(name="fail", start_fn=failing_start, restart_policy="exponential_backoff", max_restarts=2)
    sup = Supervisor(max_restarts=2, restart_window=60)
    sup.register(svc)
    
    await sup.start_all()
    await asyncio.sleep(7)
    
    assert attempts >= 2
    assert svc.state == ServiceState.FAILED
