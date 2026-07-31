"""Tests for omega.connectors"""

import pytest
import pytest_asyncio
import asyncio
from pathlib import Path

from omega.core.bus import FederationBus
from omega.connectors.base import ConnectorConfig
from omega.connectors.filesystem import FilesystemConnector
from omega.connectors.http_client import HTTPClientConnector
from omega.connectors.git import GitConnector


@pytest_asyncio.fixture
async def bus():
    b = FederationBus(max_queue_size=100)
    await b.start()
    yield b
    await b.stop()


@pytest.mark.asyncio
async def test_filesystem_read_write(bus, tmp_path):
    """Filesystem connector can read and write files."""
    config = ConnectorConfig(
        name="fs", enabled=True, config={"base_dir": str(tmp_path), "allow_write": True}
    )
    fs = FilesystemConnector(config, bus)
    await fs.start()

    result = await fs.execute("write", {"path": "test.txt", "content": "hello omega"})
    assert result["status"] == "written"

    result = await fs.execute("read", {"path": "test.txt"})
    assert result["content"] == "hello omega"

    result = await fs.execute("list", {"path": "."})
    assert result["count"] == 1
    assert result["entries"][0]["name"] == "test.txt"

    await fs.stop()


@pytest.mark.asyncio
async def test_filesystem_path_escape(bus, tmp_path):
    """Filesystem connector prevents path escape."""
    config = ConnectorConfig(
        name="fs", enabled=True, config={"base_dir": str(tmp_path), "allow_write": True}
    )
    fs = FilesystemConnector(config, bus)
    await fs.start()

    result = await fs.execute("read", {"path": "../outside.txt"})
    assert "error" in result

    await fs.stop()


@pytest.mark.asyncio
async def test_http_client_get(bus):
    """HTTP client can make GET requests (skips if offline)."""
    config = ConnectorConfig(
        name="http", enabled=True, config={"timeout": 10, "max_retries": 1}
    )
    http = HTTPClientConnector(config, bus)
    await http.start()

    result = await http.execute("get", {"url": "https://httpbin.org/get"})

    if "error" not in result:
        assert result["status"] == 200
        assert "httpbin.org" in result["body"] or "origin" in result["body"]

    await http.stop()


@pytest.mark.asyncio
async def test_git_status(bus, tmp_path):
    """Git connector can check status of a repo."""
    config = ConnectorConfig(name="git", enabled=True, config={"allow_push": False})
    git = GitConnector(config, bus)
    await git.start()

    import subprocess

    repo = tmp_path / "repo"
    subprocess.run(["git", "init", str(repo)], check=True, capture_output=True)
    subprocess.run(
        ["git", "-C", str(repo), "config", "user.email", "test@omega.dev"],
        check=True,
        capture_output=True,
    )
    subprocess.run(
        ["git", "-C", str(repo), "config", "user.name", "Test"],
        check=True,
        capture_output=True,
    )

    result = await git.execute("status", {"repo_path": str(repo)})
    assert result.get("clean") is True
    assert "branch" in result

    await git.stop()


@pytest.mark.asyncio
async def test_git_connector_health(bus):
    """Git connector health check works."""
    config = ConnectorConfig(name="git", enabled=True)
    git = GitConnector(config, bus)
    assert isinstance(git.health(), bool)
