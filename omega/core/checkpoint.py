"""
Checkpoint Engine — State Snapshots, Restore, Recovery

The runtime can be paused, checkpointed, resumed, or replayed from any point.
"""

import json
import gzip
import logging
import os
import tempfile
import time
from pathlib import Path
from typing import Optional, Any, Dict

logger = logging.getLogger("omega.checkpoint")


class CheckpointEngine:
    """
    Manages state snapshots for recovery and replay.
    
    Checkpoints include:
    - Runtime state (counters, flags)
    - Bus subscriber registry
    - Connector states
    - Ledger sequence position
    """

    def __init__(self, data_dir: Path, max_checkpoints: int = 10, compress: bool = True):
        self.data_dir = Path(data_dir)
        self.checkpoint_dir = self.data_dir / "checkpoints"
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)
        self.max_checkpoints = max_checkpoints
        self.compress = compress
        self._state: Dict[str, Any] = {}
        self._last_checkpoint_time = 0.0

    def update_state(self, key: str, value: Any):
        """Update a piece of runtime state to be checkpointed."""
        self._state[key] = value

    def get_state(self, key: str, default: Any = None) -> Any:
        return self._state.get(key, default)

    def save(self, ledger_sequence: int = 0) -> Path:
        """Create a checkpoint. Returns the checkpoint file path."""
        timestamp = time.time()
        checkpoint = {
            "version": "0.1.0",
            "timestamp": timestamp,
            "ledger_sequence": ledger_sequence,
            "state": dict(self._state),
        }
        
        filename = f"checkpoint.{int(timestamp)}.{ledger_sequence}.json"
        if self.compress:
            filename += ".gz"
        
        path = self.checkpoint_dir / filename
        
        data = json.dumps(checkpoint, indent=2, default=str).encode()
        
        if self.compress:
            with gzip.open(path, "wb") as f:
                f.write(data)
        else:
            path.write_bytes(data)
        
        self._last_checkpoint_time = timestamp
        self._prune_old_checkpoints()
        
        logger.info(f"Checkpoint saved: {path.name} (seq={ledger_sequence})")
        return path

    def restore_latest(self) -> Optional[dict]:
        """Restore from the most recent checkpoint. Returns checkpoint data."""
        checkpoints = sorted(
            self.checkpoint_dir.glob("checkpoint.*.json*"),
            key=lambda p: p.stat().st_mtime,
            reverse=True
        )
        
        if not checkpoints:
            logger.info("No checkpoints found, starting fresh")
            return None
        
        latest = checkpoints[0]
        
        try:
            if latest.suffix == ".gz":
                with gzip.open(latest, "rb") as f:
                    data = json.loads(f.read().decode())
            else:
                data = json.loads(latest.read_text())
            
            self._state = data.get("state", {})
            logger.info(f"Restored from checkpoint: {latest.name} (seq={data.get('ledger_sequence', 0)})")
            return data
        except Exception as e:
            logger.error(f"Checkpoint restore failed: {e}")
            return None

    def restore_at_sequence(self, ledger_sequence: int) -> Optional[dict]:
        """Restore checkpoint at or before a specific ledger sequence."""
        all_checkpoints = sorted(
            self.checkpoint_dir.glob("checkpoint.*.json*"),
            key=lambda p: int(p.stem.split(".")[-2]) if ".json" in p.stem else int(p.stem.split(".")[-3])
        )
        
        for cp in reversed(all_checkpoints):
            seq = int(cp.stem.split(".")[-2]) if ".json" in cp.stem else int(cp.stem.split(".")[-3])
            if seq <= ledger_sequence:
                if cp.suffix == ".gz":
                    with gzip.open(cp, "rb") as f:
                        data = json.loads(f.read().decode())
                else:
                    data = json.loads(cp.read_text())
                logger.info(f"Restored from nearest checkpoint: {cp.name}")
                return data
        
        return None

    def _prune_old_checkpoints(self):
        """Keep only max_checkpoints most recent."""
        checkpoints = sorted(
            self.checkpoint_dir.glob("checkpoint.*.json*"),
            key=lambda p: p.stat().st_mtime,
            reverse=True
        )
        
        for old in checkpoints[self.max_checkpoints:]:
            old.unlink()
            logger.debug(f"Pruned old checkpoint: {old.name}")

    def list_checkpoints(self) -> list:
        """List all available checkpoints."""
        checkpoints = []
        for cp in sorted(self.checkpoint_dir.glob("checkpoint.*.json*"), reverse=True):
            try:
                if cp.suffix == ".gz":
                    with gzip.open(cp, "rb") as f:
                        data = json.loads(f.read().decode())
                else:
                    data = json.loads(cp.read_text())
                checkpoints.append({
                    "file": cp.name,
                    "timestamp": data["timestamp"],
                    "ledger_sequence": data["ledger_sequence"],
                    "state_keys": list(data["state"].keys()),
                })
            except Exception:
                continue
        return checkpoints
