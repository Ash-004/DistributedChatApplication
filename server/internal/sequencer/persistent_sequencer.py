import threading
from typing import Dict
import uuid
import datetime
from .wal import WAL, Entry

class PersistentSequencer:
    def __init__(self, wal_path: str):
        self.sequences: Dict[uuid.UUID, int] = {}
        self.wal = WAL(wal_path)
        self.lock = threading.Lock()
        self.wal.open()
        self.recover()

    def recover(self) -> None:
        # Read WAL and rebuild sequence numbers
        entries = self.wal.read_all()
        for entry in entries:
            room_id = entry.room_id
            if room_id not in self.sequences or entry.sequence_number > self.sequences[room_id]:
                self.sequences[room_id] = entry.sequence_number

    def get_next_sequence(self, room_id: uuid.UUID) -> int:
        with self.lock:
            current_seq = self.sequences.get(room_id, 0) + 1
            self.sequences[room_id] = current_seq
            return current_seq

    def record_entry(self, room_id: uuid.UUID, user_id: uuid.UUID, content: str, message_type: str) -> Entry:
        sequence_number = self.get_next_sequence(room_id)
        entry = Entry(
            id=uuid.uuid4(),
            room_id=room_id,
            user_id=user_id,
            sequence_number=sequence_number,
            content=content,
            message_type=message_type,
            created_at=datetime.datetime.now()
        )
        self.wal.append(entry)
        return entry

    def close(self) -> None:
        self.wal.close()
