import uuid
import os
import tempfile
from internal.sequencer.persistent_sequencer import PersistentSequencer
from internal.sequencer.wal import Entry


def test_sequencer_recovery():
    # Setup
    test_dir = tempfile.mkdtemp()
    wal_path = os.path.join(test_dir, "wal.log")
    room_id = uuid.uuid4()
    
    # Create sequencer and add messages
    sequencer = PersistentSequencer(wal_path)
    seq1 = sequencer.get_next_sequence(room_id)
    sequencer.record_entry(room_id, uuid.uuid4(), "Test message", "text")
    sequencer.close()
    
    # Recover sequencer
    sequencer2 = PersistentSequencer(wal_path)
    seq2 = sequencer2.get_next_sequence(room_id)
    sequencer2.close()
    
    # Verify recovery
    assert seq1 == 1
    assert seq2 == 2
    
    # Cleanup
    os.remove(wal_path)
    os.rmdir(test_dir)
