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
        
        directory = os.path.dirname(self.file_path)
        if directory and not os.path.exists(directory):
            os.makedirs(directory, exist_ok=True)
            
        self.file = open(self.file_path, 'a+b')  
        self.writer = io.BufferedWriter(self.file)  

    def append(self, entry: Entry) -> None:
        with self.lock:
            
            data = json.dumps({
                'id': str(entry.id),
                'room_id': str(entry.room_id),
                'user_id': str(entry.user_id),
                'sequence_number': entry.sequence_number,
                'content': entry.content,
                'message_type': entry.message_type,
                'created_at': entry.created_at.isoformat()
            }).encode('utf-8')
            
            self.writer.write(len(data).to_bytes(4, 'little'))
            self.writer.write(data)
            self.writer.flush()
            os.fsync(self.file.fileno())

    def read_all(self) -> List[Entry]:
        
        self.close()
        
        entries = []
        
        try:
            with open(self.file_path, 'rb') as read_file:
                reader = io.BufferedReader(read_file)
                while True:
                    try:
                        length_bytes = reader.read(4)
                        if not length_bytes:
                            break 
                        length = int.from_bytes(length_bytes, 'little')
                        data = reader.read(length).decode('utf-8')
                        entry_data = json.loads(data)
                        entries.append(Entry(
                            id=uuid.UUID(entry_data['id']),
                            room_id=uuid.UUID(entry_data['room_id']),
                            user_id=uuid.UUID(entry_data['user_id']),
                            sequence_number=entry_data['sequence_number'],
                            content=entry_data['content'],
                            message_type=entry_data['message_type'],
                            created_at=datetime.fromisoformat(entry_data['created_at'])
                        ))
                    except Exception as e:
                        
                        print(f"Error reading WAL entry: {e}")
                        break
        except FileNotFoundError:
            
            print(f"WAL file not found at {self.file_path}, creating a new one")
        
        
        self.open()
        return entries

    def close(self) -> None:
        if self.file:
            if self.writer and not self.writer.closed:
                self.writer.flush()
            self.file.close()
            self.writer = None
