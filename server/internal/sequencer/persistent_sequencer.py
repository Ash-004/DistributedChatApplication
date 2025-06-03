import threading
import time
from typing import Dict, Iterator
import uuid
import datetime
from .wal import WAL, Entry
from .raft import RaftNode, NotLeaderException
from ..storage.database import get_db, SessionLocal
from ..storage.models import Message

class PersistentSequencer:
    def __init__(self, wal_path: str, db_session_factory: SessionLocal):
        self.sequences: Dict[uuid.UUID, int] = {}
        self.wal = WAL(wal_path)
        self.lock = threading.Lock()
        self.wal.open()
        self.recover()
        self.db_session_factory = db_session_factory

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

    def record_entry(self, entry_data: dict) -> dict:
        # This method is called by RaftNode.apply_entries
        # It should directly apply the entry to the database
        db = self.db_session_factory()
        try:
            # Check for idempotency
            existing_message = db.query(Message).filter(Message.id == uuid.UUID(entry_data['id'])).first()
            if existing_message:
                print(f"Message {entry_data['id']} already exists, skipping.")
                return {'status': 'skipped'}

            # Assign sequence number
            sequence_number = self.get_next_sequence(uuid.UUID(entry_data['room_id']))

            message = Message(
                id=uuid.UUID(entry_data['id']),
                room_id=uuid.UUID(entry_data['room_id']),
                user_id=uuid.UUID(entry_data['user_id']),
                sequence_number=sequence_number,
                content=entry_data['content'] or '', # Ensure content is not None
                message_type=entry_data['msg_type'] or '', # Ensure message_type is not None
                created_at=datetime.datetime.fromtimestamp(entry_data['timestamp'])
            )
            db.add(message)
            db.commit()
            print(f"Successfully applied entry {entry_data['id']} to DB.")
            return {'status': 'applied', 'sequence_number': sequence_number}
        except Exception as e:
            print(f"Error applying entry {entry_data['id']} to DB: {e}")
            db.rollback()
            raise # Re-raise to signal failure to RaftNode
        finally:
            db.close()

    def get_state(self) -> dict:
        return {
            'sequences': {str(k): v for k, v in self.sequences.items()}
        }

    def load_state(self, state: dict) -> None:
        self.sequences = {uuid.UUID(k): v for k, v in state['sequences'].items()}

    def close(self) -> None:
        self.wal.close()


class DistributedSequencer(RaftNode):
    def __init__(self, node_id: str, peers: list, node_address: str, rpc_port: int, sequencer: PersistentSequencer):
        super().__init__(node_id, peers, node_address, rpc_port)
        self.sequencer = sequencer
        print(f"Initialized DistributedSequencer on port {rpc_port}")
        


    def record_entry(self, room_id, user_id, content, msg_type):
        if self.state != 'leader':
            raise Exception("Not leader")
            
        entry_data = {
            'id': str(uuid.uuid4()),
            'room_id': str(room_id),
            'user_id': str(user_id),
            'content': content,
            'msg_type': msg_type,
            'timestamp': time.time()
        }
        
        # Use the RaftNode's append_entries to replicate the command
        # This will add the entry to the Raft log and replicate it
        # The actual sequence number will be assigned when applied to the WAL
        try:
            self.append_entries(self.current_term, self.node_id, len(self.log) - 1, 
                                self.log[-1]['term'] if self.log else 0, 
                                [{'entry': entry_data, 'term': self.current_term}], self.commit_index)
            return {'id': entry_data['id'], 'status': 'replicated'}
        except NotLeaderException as e:
            return {'error': 'Not leader', 'leader_address': e.leader_addr}
        except Exception as e:
            print(f"Error recording entry: {e}")
            return {'error': str(e)}
