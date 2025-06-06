import threading
import time
from typing import Dict, Iterator
import uuid
import datetime
from .wal import WAL, Entry
from .raft import RaftNode, NotLeaderException
from ..storage.database import get_db, SessionLocal
from ..storage.models import Message, Room

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

    def get_next_sequence(self, room_id: str) -> int:
        with self.lock:
            current_seq = self.sequences.get(room_id, 0) + 1
            self.sequences[room_id] = current_seq
            return current_seq

    def record_entry(self, entry_data: dict, absolute_index: int = None) -> dict:
        # This method is called by RaftNode.apply_entries
        # It should directly apply the entry to the database
        db = self.db_session_factory()
        try:
            # Check for idempotency - FIX: Use string ID directly instead of converting to UUID
            existing_message = db.query(Message).filter(Message.id == entry_data['id']).first()
            if existing_message:
                print(f"Message {entry_data['id']} already exists, skipping.")
                return {'status': 'skipped'}

            # For CREATE_ROOM message type, create the room first
            if entry_data['msg_type'] == 'CREATE_ROOM':
                # Check if room already exists
                existing_room = db.query(Room).filter(Room.id == entry_data['room_id']).first()
                if not existing_room:
                    # Create the room
                    room = Room(
                        id=entry_data['room_id'],
                        name=entry_data['content'],
                        is_private=False,
                        created_at=entry_data['timestamp']
                    )
                    db.add(room)
                    # Flush to ensure the room is created before the message
                    db.flush()
                    print(f"Created room {entry_data['room_id']} with name {entry_data['content']}")

            # Assign sequence number - FIX: Use string room_id directly
            sequence_number = self.get_next_sequence(entry_data['room_id'])

            message = Message(
                id=entry_data['id'],  # FIX: Use string ID directly
                room_id=entry_data['room_id'],  # FIX: Use string room_id directly
                user_id=entry_data['user_id'],  # FIX: Use string user_id directly
                sequence_number=sequence_number,
                content=entry_data['content'] or '', # Ensure content is not None
                message_type=entry_data['msg_type'] or '', # Ensure message_type is not None
                created_at=entry_data['timestamp']  # Use the timestamp directly as a float
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
        # Create required parameters for RaftNode initialization
        from server.internal.discovery.etcd_client import EtcdClient
        from server.internal.sequencer.raft_rpc_client import RaftRPCClient
        from server.config import app_config
        
        # Initialize peer clients dictionary
        peer_clients = {}
        for peer_id in peers:
            _, _, peer_host, peer_port = app_config.get_all_node_addresses().get(peer_id)
            peer_clients[peer_id] = RaftRPCClient(peer_host, peer_port)
        
        # Create persistent state dictionary
        persistent_state = {}
        
        # Format node API address string
        node_api_address_str = node_address
        
        # Format node RPC address string
        node_rpc_address_str = f"http://{app_config.get_current_node_rpc_host()}:{rpc_port}"
        
        # Get etcd client instance
        etcd_client_instance = EtcdClient(
            etcd_endpoints=app_config.get_etcd_endpoints(),
            node_id=node_id,
            api_address=node_address,
            rpc_address=node_rpc_address_str
        )
        
        # Call parent class constructor with all required parameters
        super().__init__(
            peer_clients=peer_clients,
            persistent_state=persistent_state,
            etcd_client_instance=etcd_client_instance,
            node_api_address_str=node_api_address_str,
            node_rpc_address_str=node_rpc_address_str,
            sequencer_instance=sequencer
        )
        
        self.sequencer = sequencer
        print(f"Initialized DistributedSequencer on port {rpc_port}")
        


    def record_entry(self, room_id, user_id, content, msg_type):
        if self.state != 'LEADER':
            raise NotLeaderException("Not leader", self.leader_id)
            
        entry_data = {
            'id': str(uuid.uuid4()),
            'room_id': str(room_id),
            'user_id': str(user_id),
            'content': content,
            'msg_type': msg_type,
            'timestamp': time.time()
        }
        
        # Use the RaftNode's propose_command to properly handle the client command
        try:
            success, error = self.propose_command(entry_data)
            if success:
                return {'id': entry_data['id'], 'status': 'success'}
            else:
                return {'error': error or 'Unknown error'}
        except NotLeaderException as e:
            return {'error': 'Not leader', 'leader_address': e.leader_addr}
        except Exception as e:
            print(f"Error recording entry: {e}")
            return {'error': str(e)}