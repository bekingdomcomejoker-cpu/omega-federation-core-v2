"""
Filesystem Connector

Read, write, list, delete, watch files and directories.
Emits events for all operations.
"""

import os
import shutil
import logging
from pathlib import Path
from typing import Dict, Any, Optional

from .base import BaseConnector, ConnectorConfig
from omega.core.bus import OmegaEvent

logger = logging.getLogger("omega.connectors.filesystem")


class FilesystemConnector(BaseConnector):
    """
    Filesystem capability connector.

    Actions:
    - read: Read file contents
    - write: Write file contents
    - list: List directory contents
    - delete: Delete file or directory
    - exists: Check if path exists
    - mkdir: Create directory
    - stat: Get file metadata
    """

    @property
    def capabilities(self) -> list:
        return ["filesystem.read", "filesystem.write"]

    async def start(self):
        """Start the filesystem connector."""
        self.base_dir = Path(self.config.config.get("base_dir", "~")).expanduser()
        self.allow_write = self.config.config.get("allow_write", True)
        self.max_file_size = self.config.config.get("max_file_size_mb", 100) * 1024 * 1024
        self.base_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"Filesystem connector started: base_dir={self.base_dir}")

    async def stop(self):
        """Stop the filesystem connector."""
        logger.info("Filesystem connector stopped")

    async def execute(self, action: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a filesystem action."""
        try:
            path = self._resolve_path(params.get("path", "."))
        except PermissionError as e:
            return {"error": "path_escape", "detail": str(e)}

        if action == "read":
            return await self._read(path)
        elif action == "write":
            return await self._write(path, params.get("content", ""), params.get("mode", "w"))
        elif action == "list":
            return await self._list(path)
        elif action == "delete":
            return await self._delete(path)
        elif action == "exists":
            return {"exists": path.exists()}
        elif action == "mkdir":
            return await self._mkdir(path, params.get("parents", True))
        elif action == "stat":
            return await self._stat(path)
        else:
            return {"error": f"Unknown action: {action}"}

    def _resolve_path(self, path: str) -> Path:
        """Resolve a path relative to base_dir, prevent escape."""
        target = Path(path).expanduser()
        if not target.is_absolute():
            target = self.base_dir / target
        target = target.resolve()
        try:
            target.relative_to(self.base_dir.resolve())
        except ValueError:
            raise PermissionError(f"Path escape attempt: {path}")
        return target

    async def _read(self, path: Path) -> Dict[str, Any]:
        """Read a file."""
        if not path.exists():
            return {"error": "not_found", "path": str(path)}
        if path.is_dir():
            return {"error": "is_directory", "path": str(path)}

        size = path.stat().st_size
        if size > self.max_file_size:
            return {"error": "file_too_large", "size": size, "max": self.max_file_size}

        content = path.read_text(encoding="utf-8", errors="replace")

        await self.emit("filesystem.read", {
            "path": str(path),
            "size": size,
        })

        return {
            "path": str(path),
            "content": content,
            "size": size,
            "lines": len(content.splitlines()),
        }

    async def _write(self, path: Path, content: str, mode: str = "w") -> Dict[str, Any]:
        """Write a file."""
        if not self.allow_write:
            return {"error": "write_disabled"}

        path.parent.mkdir(parents=True, exist_ok=True)

        if mode == "a":
            with open(path, "a", encoding="utf-8") as f:
                f.write(content)
        else:
            path.write_text(content, encoding="utf-8")

        await self.emit("filesystem.write", {
            "path": str(path),
            "size": len(content),
            "mode": mode,
        })

        return {"path": str(path), "bytes_written": len(content), "status": "written"}

    async def _list(self, path: Path) -> Dict[str, Any]:
        """List directory contents."""
        if not path.exists():
            return {"error": "not_found", "path": str(path)}
        if not path.is_dir():
            return {"error": "not_directory", "path": str(path)}

        entries = []
        for entry in path.iterdir():
            stat = entry.stat()
            entries.append({
                "name": entry.name,
                "type": "directory" if entry.is_dir() else "file",
                "size": stat.st_size,
                "modified": stat.st_mtime,
            })

        return {
            "path": str(path),
            "entries": entries,
            "count": len(entries),
        }

    async def _delete(self, path: Path) -> Dict[str, Any]:
        """Delete a file or directory."""
        if not self.allow_write:
            return {"error": "write_disabled"}
        if not path.exists():
            return {"error": "not_found", "path": str(path)}

        if path.is_dir():
            shutil.rmtree(path)
        else:
            path.unlink()

        await self.emit("filesystem.delete", {"path": str(path)})

        return {"path": str(path), "status": "deleted"}

    async def _mkdir(self, path: Path, parents: bool = True) -> Dict[str, Any]:
        """Create a directory."""
        if not self.allow_write:
            return {"error": "write_disabled"}

        path.mkdir(parents=parents, exist_ok=True)

        await self.emit("filesystem.mkdir", {"path": str(path)})

        return {"path": str(path), "status": "created"}

    async def _stat(self, path: Path) -> Dict[str, Any]:
        """Get file metadata."""
        if not path.exists():
            return {"error": "not_found", "path": str(path)}

        stat = path.stat()
        return {
            "path": str(path),
            "size": stat.st_size,
            "modified": stat.st_mtime,
            "created": stat.st_ctime,
            "is_dir": path.is_dir(),
            "is_file": path.is_file(),
            "permissions": oct(stat.st_mode)[-3:],
        }
