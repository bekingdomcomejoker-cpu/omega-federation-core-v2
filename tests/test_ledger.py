"""Tests for omega.core.ledger"""

import pytest
from pathlib import Path
from omega.core.ledger import EventLedger
from omega.core.bus import OmegaEvent


@pytest.fixture
def ledger(tmp_path):
    return EventLedger(tmp_path, max_entries_per_file=10)


def test_append_and_hash_chain(ledger):
    """Ledger maintains hash chain integrity."""
    e1 = OmegaEvent("e1", "test", "src", {"x": 1})
    entry1 = ledger.append(e1)
    
    assert entry1["ledger_sequence"] == 1
    assert entry1["previous_hash"] == "0" * 64
    assert len(entry1["entry_hash"]) == 64
    
    e2 = OmegaEvent("e2", "test", "src", {"x": 2})
    entry2 = ledger.append(e2)
    
    assert entry2["ledger_sequence"] == 2
    assert entry2["previous_hash"] == entry1["entry_hash"]


def test_verify_chain(ledger):
    """Chain verification passes for valid ledger."""
    for i in range(5):
        ledger.append(OmegaEvent(f"e{i}", "test", "src", {"i": i}))
    
    assert ledger.verify_chain() is True


def test_iter_entries(ledger):
    """Can iterate entries from a sequence number."""
    for i in range(5):
        ledger.append(OmegaEvent(f"e{i}", "test", "src", {"i": i}))
    
    entries = list(ledger.iter_entries(since_sequence=3))
    assert len(entries) == 3
    assert entries[0]["ledger_sequence"] == 3


def test_file_rotation(ledger, tmp_path):
    """Ledger rotates files when max entries reached."""
    for i in range(15):
        ledger.append(OmegaEvent(f"e{i}", "test", "src", {"i": i}))
    
    archives = list((tmp_path / "ledger_archive").glob("*.jsonl"))
    assert len(archives) >= 1
