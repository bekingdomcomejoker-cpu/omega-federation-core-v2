"""
Git Connector

Clone, pull, commit, push, status, branch operations.
Emits events for all git operations.
"""

import logging
import subprocess
from pathlib import Path
from typing import Dict, Any, Optional

from .base import BaseConnector, ConnectorConfig

logger = logging.getLogger("omega.connectors.git")


class GitConnector(BaseConnector):
    """
    Git capability connector.

    Actions:
    - clone: Clone a repository
    - pull: Pull latest changes
    - status: Get working tree status
    - commit: Commit changes
    - push: Push to remote
    - branch: List or switch branches
    - log: Get commit history
    - diff: Get diff
    """

    @property
    def capabilities(self) -> list:
        return ["git.read", "git.write"]

    async def start(self):
        """Start the git connector."""
        self.default_author = self.config.config.get(
            "default_author", "Omega Runtime <omega@sovereign.dev>"
        )
        self.ssh_key_path = Path(
            self.config.config.get("ssh_key_path", "~/.ssh/id_rsa")
        ).expanduser()
        self.allow_push = self.config.config.get("allow_push", True)
        logger.info("Git connector started")

    async def stop(self):
        """Stop the git connector."""
        logger.info("Git connector stopped")

    async def execute(self, action: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a git action."""
        repo_path = params.get("repo_path")
        if not repo_path and action != "clone":
            return {"error": "missing_repo_path"}

        repo = Path(repo_path).expanduser() if repo_path else None

        if action == "clone":
            return await self._clone(params.get("url"), repo, params.get("branch"))
        elif action == "pull":
            return await self._pull(repo)
        elif action == "status":
            return await self._status(repo)
        elif action == "commit":
            return await self._commit(
                repo, params.get("message", "Omega commit"), params.get("files", ["-a"])
            )
        elif action == "push":
            return await self._push(repo, params.get("remote", "origin"), params.get("branch"))
        elif action == "branch":
            return await self._branch(
                repo, params.get("operation", "list"), params.get("branch_name")
            )
        elif action == "log":
            return await self._log(repo, params.get("max_count", 10))
        elif action == "diff":
            return await self._diff(repo)
        else:
            return {"error": f"Unknown action: {action}"}

    async def _run_git(self, repo: Path, *args) -> tuple:
        """Run a git command and return (stdout, stderr, returncode)."""
        cmd = ["git", "-C", str(repo)] + list(args)
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=60,
            )
            return result.stdout, result.stderr, result.returncode
        except subprocess.TimeoutExpired:
            return "", "Command timed out", 1
        except FileNotFoundError:
            return "", "git not found", 127

    async def _clone(
        self, url: str, repo: Path, branch: Optional[str] = None
    ) -> Dict[str, Any]:
        """Clone a repository."""
        if not url:
            return {"error": "missing_url"}
        if repo is None:
            return {"error": "missing_repo_path"}

        repo.parent.mkdir(parents=True, exist_ok=True)

        cmd = ["git", "clone", url, str(repo)]
        if branch:
            cmd.extend(["--branch", branch, "--single-branch"])

        result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)

        await self.emit(
            "git.clone", {"url": url, "repo": str(repo), "success": result.returncode == 0}
        )

        return {
            "url": url,
            "repo": str(repo),
            "success": result.returncode == 0,
            "stdout": result.stdout,
            "stderr": result.stderr,
        }

    async def _pull(self, repo: Path) -> Dict[str, Any]:
        """Pull latest changes."""
        stdout, stderr, rc = await self._run_git(repo, "pull")

        await self.emit("git.pull", {"repo": str(repo), "success": rc == 0})

        return {
            "repo": str(repo),
            "success": rc == 0,
            "stdout": stdout,
            "stderr": stderr,
        }

    async def _status(self, repo: Path) -> Dict[str, Any]:
        """Get working tree status."""
        stdout, stderr, rc = await self._run_git(repo, "status", "--porcelain", "-b")

        if rc != 0:
            return {"error": "git_status_failed", "stderr": stderr}

        lines = stdout.strip().split("\n") if stdout.strip() else []
        branch_line = lines[0] if lines else ""
        file_lines = lines[1:] if len(lines) > 1 else []

        files = []
        for line in file_lines:
            if len(line) >= 3:
                files.append({
                    "status": line[:2].strip(),
                    "path": line[3:],
                })

        branch = (
            branch_line.replace("## ", "").split("...")[0]
            if branch_line.startswith("##")
            else "unknown"
        )

        return {
            "repo": str(repo),
            "branch": branch,
            "files": files,
            "clean": len(files) == 0,
        }

    async def _commit(self, repo: Path, message: str, files: list) -> Dict[str, Any]:
        """Commit changes."""
        if not self.allow_push:
            return {"error": "write_disabled"}

        if files == ["-a"]:
            _, stderr, rc = await self._run_git(repo, "add", "-A")
        else:
            for f in files:
                _, stderr, rc = await self._run_git(repo, "add", f)

        stdout, stderr, rc = await self._run_git(
            repo, "commit", "-m", message, "--author", self.default_author
        )

        await self.emit(
            "git.commit", {"repo": str(repo), "message": message, "success": rc == 0}
        )

        return {
            "repo": str(repo),
            "message": message,
            "success": rc == 0,
            "stdout": stdout,
            "stderr": stderr,
        }

    async def _push(
        self, repo: Path, remote: str = "origin", branch: Optional[str] = None
    ) -> Dict[str, Any]:
        """Push to remote."""
        if not self.allow_push:
            return {"error": "push_disabled"}

        cmd = ["push", remote]
        if branch:
            cmd.append(branch)

        stdout, stderr, rc = await self._run_git(repo, *cmd)

        await self.emit(
            "git.push", {"repo": str(repo), "remote": remote, "success": rc == 0}
        )

        return {
            "repo": str(repo),
            "remote": remote,
            "success": rc == 0,
            "stdout": stdout,
            "stderr": stderr,
        }

    async def _branch(
        self, repo: Path, operation: str = "list", branch_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """Branch operations."""
        if operation == "list":
            stdout, _, rc = await self._run_git(repo, "branch", "-a")
            branches = [b.strip().lstrip("* ") for b in stdout.split("\n") if b.strip()]
            current = [
                b.strip().lstrip("* ")
                for b in stdout.split("\n")
                if b.strip().startswith("*")
            ]
            return {
                "repo": str(repo),
                "branches": branches,
                "current": current[0] if current else None,
            }
        elif operation == "create":
            if not branch_name:
                return {"error": "missing_branch_name"}
            stdout, stderr, rc = await self._run_git(repo, "checkout", "-b", branch_name)
            return {
                "repo": str(repo),
                "branch": branch_name,
                "success": rc == 0,
                "stderr": stderr,
            }
        elif operation == "switch":
            if not branch_name:
                return {"error": "missing_branch_name"}
            stdout, stderr, rc = await self._run_git(repo, "checkout", branch_name)
            return {
                "repo": str(repo),
                "branch": branch_name,
                "success": rc == 0,
                "stderr": stderr,
            }
        else:
            return {"error": f"Unknown branch operation: {operation}"}

    async def _log(self, repo: Path, max_count: int = 10) -> Dict[str, Any]:
        """Get commit history."""
        stdout, stderr, rc = await self._run_git(
            repo, "log", f"--max-count={max_count}", "--pretty=format:%H|%an|%ae|%ad|%s"
        )

        if rc != 0:
            return {"error": "git_log_failed", "stderr": stderr}

        commits = []
        for line in stdout.strip().split("\n"):
            if "|" in line:
                parts = line.split("|", 4)
                if len(parts) >= 5:
                    commits.append({
                        "hash": parts[0],
                        "author": parts[1],
                        "email": parts[2],
                        "date": parts[3],
                        "message": parts[4],
                    })

        return {"repo": str(repo), "commits": commits, "count": len(commits)}

    async def _diff(self, repo: Path) -> Dict[str, Any]:
        """Get working tree diff."""
        stdout, stderr, rc = await self._run_git(repo, "diff")

        return {
            "repo": str(repo),
            "diff": stdout,
            "has_changes": len(stdout) > 0,
        }

    def health(self) -> bool:
        """Check if git is available."""
        try:
            subprocess.run(["git", "--version"], capture_output=True, check=True)
            return True
        except (FileNotFoundError, subprocess.CalledProcessError):
            return False
