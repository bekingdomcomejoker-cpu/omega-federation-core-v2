"""Tests for omega.core.checkpoint"""

import pytest
from pathlib import Path
from omega.core.checkpoint import CheckpointEngine


@pytest.fixture
def cp(tmp_path):
    return CheckpointEngine(tmp_path, max_checkpoints=3)


def test_save_and_restore(cp):
    """Checkpoint saves and restores state."""
    cp.update_state("counter", 42)
    cp.update_state("flag", True)
    
    path = cp.save(ledger_sequence=10)
    assert path.exists()
    
    cp2 = CheckpointEngine(path.parent.parent, max_checkpoints=3)
    data = cp.restore_latest()
    
    assert data is not None
    assert data["ledger_sequence"] == 10
    assert cp.get_state("counter") == 42
    assert cp.get_state("flag") is True


def test_prune_old(cp):
    """Old checkpoints are pruned."""
    for i in range(5):
        cp.update_state("seq", i)
        cp.save(ledger_sequence=i)
    
    checkpoints = list(cp.checkpoint_dir.glob("checkpoint.*"))
    assert len(checkpoints) <= 3


def test_restore_at_sequence(cp):
    """Can restore at or before a sequence."""
    cp.update_state("v", 1)
    cp.save(ledger_sequence=5)
    cp.update_state("v", 2)
    cp.save(ledger_sequence=10)
    
    data = cp.restore_at_sequence(7)
    assert data["ledger_sequence"] == 5
