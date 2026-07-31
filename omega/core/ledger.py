"""
Truth Engine — Immutable SHA-3-256 Event Ledger

Every event is immutable. Event → Hash → Ledger → Checkpoint → Archive.
You can replay, audit, resume, verify history.
"""

import json
import hashlib
import logging
import os
import tempfile
import time
from pathlib import Path
from typing import List, Optional, Iterator

from .bus import OmegaEvent

logger = logging.getLogger("omega.ledger")


class EventLedger:
    """
    Append-only event ledger with SHA-3-256 hash chaining.
    
    Each entry contains:
    - event: the full OmegaEvent
    - event_hash: SHA-3-256 of the event
    - previous_hash: hash of previous ledger entry (chain integrity)
    - ledger_sequence: monotonic sequence number
    """

    def __init__(self, data_dir: Path, max_entries_per_file: int = 5000):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.max_entries_per_file = max_entries_per_file
        self._ledger_file = self.data_dir / "omega.ledger.jsonl"
        self._archive_dir = self.data_dir / "ledger_archive"
        self._archive_dir.mkdir(exist_ok=True)
        self._sequence = 0
        self._last_hash = "0" * 64
        self._current_file_entries = 0
        self._load_state()

    def _load_state(self):
        """Load the latest sequence and hash from existing ledger."""
        if not self._ledger_file.exists():
            return
        
        try:
            with open(self._ledger_file, "r") as f:
                for line in f:
                    entry = json.loads(line.strip())
                    self._sequence = entry["ledger_sequence"]
                    self._last_hash = entry["entry_hash"]
                    self._current_file_entries += 1
        except (json.JSONDecodeError, IOError) as e:
            logger.warning(f"Ledger load error: {e}, starting fresh")
            self._sequence = 0
            self._last_hash = "0" * 64

    def append(self, event: OmegaEvent) -> dict:
        """Append an event to the ledger. Returns the ledger entry."""
        event_hash = event.compute_hash()
        self._sequence += 1
        
        chain_input = (self._last_hash + event_hash).encode()
        entry_hash = hashlib.sha3_256(chain_input).hexdigest()
        
        entry = {
            "ledger_sequence": self._sequence,
            "timestamp": time.time(),
            "event_id": event.event_id,
            "event_type": event.event_type,
            "event_hash": event_hash,
            "previous_hash": self._last_hash,
            "entry_hash": entry_hash,
            "source": event.source,
            "payload_summary": str(event.payload)[:200],
        }
        
        self._last_hash = entry_hash
        self._write_entry(entry)
        
        if self._current_file_entries >= self.max_entries_per_file:
            self._rotate_file()
        
        return entry

    def _write_entry(self, entry: dict):
        """Atomic append to ledger file."""
        line = json.dumps(entry, default=str) + "\n"
        
        fd, tmp = tempfile.mkstemp(dir=self.data_dir, prefix=".ledger.tmp")
        try:
            if self._ledger_file.exists():
                with open(self._ledger_file, "rb") as src:
                    existing = src.read()
            else:
                existing = b""
            
            with os.fdopen(fd, "wb") as f:
                f.write(existing)
                f.write(line.encode())
                f.flush()
                os.fsync(fd)
            
            os.replace(tmp, self._ledger_file)
            self._current_file_entries += 1
        except Exception:
            if os.path.exists(tmp):
                os.unlink(tmp)
            raise

    def _rotate_file(self):
        """Archive current ledger and start fresh."""
        timestamp = int(time.time())
        archive_path = self._archive_dir / f"omega.ledger.{timestamp}.{self._sequence}.jsonl"
        os.replace(self._ledger_file, archive_path)
        self._current_file_entries = 0
        logger.info(f"Ledger rotated to {archive_path}")

    def verify_chain(self) -> bool:
        """Verify the entire ledger chain integrity."""
        if not self._ledger_file.exists():
            return True
        
        expected_hash = "0" * 64
        
        with open(self._ledger_file, "r") as f:
            for line in f:
                entry = json.loads(line.strip())
                
                if entry["previous_hash"] != expected_hash:
                    logger.error(
                        f"Chain break at sequence {entry['ledger_sequence']}: "
                        f"expected {expected_hash[:16]}... got {entry['previous_hash'][:16]}..."
                    )
                    return False
                
                expected_hash = entry["entry_hash"]
        
        logger.info("Ledger chain verified: intact")
        return True

    def iter_entries(self, since_sequence: int = 0) -> Iterator[dict]:
        """Iterate ledger entries from a sequence number."""
        if self._ledger_file.exists():
            with open(self._ledger_file, "r") as f:
                for line in f:
                    entry = json.loads(line.strip())
                    if entry["ledger_sequence"] >= since_sequence:
                        yield entry
        
        for archive in sorted(self._archive_dir.glob("omega.ledger.*.jsonl")):
            with open(archive, "r") as f:
                for line in f:
                    entry = json.loads(line.strip())
                    if entry["ledger_sequence"] >= since_sequence:
                        yield entry

    def get_last_sequence(self) -> int:
        return self._sequence

    def stats(self) -> dict:
        return {
            "sequence": self._sequence,
            "current_file_entries": self._current_file_entries,
            "archives": len(list(self._archive_dir.glob("*.jsonl"))),
            "data_dir": str(self.data_dir),
        }
