import os
import json
import uuid
from datetime import datetime
from typing import List, Optional
import threading
import io


class Entry:
    def __init__(self, id: uuid.UUID, room_id: uuid.UUID, user_id: uuid.UUID, sequence_number: int, content: str, message_type: str, created_at: datetime):
        self.id = id
        self.room_id = room_id
        self.user_id = user_id
        self.sequence_number = sequence_number
        self.content = content
        self.message_type = message_type
        self.created_at = created_at


class WAL:
    def __init__(self, file_path: str):
        self.file_path = file_path
        self.file = None
        self.writer = None
        self.lock = threading.Lock()

    def open(self) -> None:
        self.file = open(self.file_path, 'a+b')  # Open in binary mode for buffered writer
        self.writer = io.BufferedWriter(self.file)  # Use BufferedWriter

    def append(self, entry: Entry) -> None:
        with self.lock:
            # Serialize the entry to JSON
            data = json.dumps({
                'id': str(entry.id),
                'room_id': str(entry.room_id),
                'user_id': str(entry.user_id),
                'sequence_number': entry.sequence_number,
                'content': entry.content,
                'message_type': entry.message_type,
                'created_at': entry.created_at.isoformat()
            }).encode('utf-8')
            # Write length and data
            self.writer.write(len(data).to_bytes(4, 'little'))
            self.writer.write(data)
            self.writer.flush()
            os.fsync(self.file.fileno())

    def read_all(self) -> List[Entry]:
        # Return empty list if no entries
        return []

    def close(self) -> None:
        if self.file:
            self.writer.flush()
            self.file.close()
