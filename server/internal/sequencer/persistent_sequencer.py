import threading
import time
from typing import Dict
import uuid
import datetime
from .wal import WAL, Entry
from .minimal_raft import MinimalRaftNode

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


class DistributedSequencer(MinimalRaftNode):
    def __init__(self, node_id: str, peers: list, rpc_port: int, node_addresses: dict):
        self.node_addresses = node_addresses
        super().__init__(node_id, peers, rpc_port, node_addresses)
        print(f"Initialized DistributedSequencer on port {rpc_port}")
        
    def replicate_entry(self, entry):
        """Replicate entry to followers using Raft consensus"""
        for peer in self.peers:
            try:
                # In a real implementation, we'd make RPC calls to peers
                # For now, just append to our own log for testing
                pass
            except Exception as e:
                print(f"Failed to replicate to {peer}: {e}")
        
        # Append to local log
        self.log.append(entry)
        return True

    def record_entry(self, room_id, user_id, content, msg_type):
        if self.state != 'leader':
            raise Exception("Not leader")
            
        # Generate sequence number based on current log length
        sequence_number = len(self.log) + 1
        
        entry = {
            'id': str(uuid.uuid4()),
            'room_id': room_id,
            'user_id': user_id,
            'content': content,
            'msg_type': msg_type,
            'sequence_number': sequence_number,
            'timestamp': time.time()
        }
        
        # Replicate via Raft
        if not self.replicate_entry(entry):
            raise Exception("Failed to replicate entry")
        
        return {
            'id': entry['id'],
            'sequence_number': sequence_number
        }
