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
    """A class to manage persistent sequencing for a chat application.

    Attributes:
        sequences (Dict[uuid.UUID, int]): Dictionary mapping room IDs to their latest sequence numbers.
        wal (WAL): Write-Ahead Log instance for persistence.
        lock (threading.Lock): Thread lock for thread-safe operations.
        db_session_factory (SessionLocal): Factory for creating database sessions.
    """

    def __init__(self, wal_path: str, db_session_factory: SessionLocal):
        """Initialize the PersistentSequencer with the given configuration.

        Args:
            wal_path (str): File path for the Write-Ahead Log.
            db_session_factory (SessionLocal): Factory for creating database sessions.
        """
        self.sequences: Dict[uuid.UUID, int] = {}
        self.wal = WAL(wal_path)
        self.lock = threading.Lock()
        self.wal.open()
        self.recover()
        self.db_session_factory = db_session_factory

    def recover(self) -> None:
        """Recover the sequencer state by replaying entries from the Write-Ahead Log."""
        entries = self.wal.read_all()
        for entry in entries:
            room_id = entry.room_id
            if room_id not in self.sequences or entry.sequence_number > self.sequences[room_id]:
                self.sequences[room_id] = entry.sequence_number

    def get_next_sequence(self, room_id: str) -> int:
        """Get the next sequence number for a given room.

        Args:
            room_id (str): ID of the chat room.

        Returns:
            int: The next sequence number for the room.
        """
        with self.lock:
            current_seq = self.sequences.get(room_id, 0) + 1
            self.sequences[room_id] = current_seq
            return current_seq

    def record_entry(self, entry_data: dict, absolute_index: int = None) -> dict:
        """Record a new entry in the database and assign it a sequence number.

        Args:
            entry_data (dict): Dictionary containing entry details (id, room_id, user_id, content, msg_type, timestamp).
            absolute_index (int, optional): Absolute index for the entry, if applicable.

        Returns:
            dict: A dictionary indicating the status of the operation.
                  If successful, returns {'status': 'applied', 'sequence_number': sequence_number}.
                  If skipped (e.g., duplicate), returns {'status': 'skipped'}.

        Raises:
            Exception: If there is an error applying the entry to the database.
        """
        db = self.db_session_factory()
        try:
            existing_message = db.query(Message).filter(Message.id == entry_data['id']).first()
            if existing_message:
                print(f"Message {entry_data['id']} already exists, skipping.")
                return {'status': 'skipped'}

            if entry_data['msg_type'] == 'CREATE_ROOM':
                existing_room = db.query(Room).filter(Room.id == entry_data['room_id']).first()
                if not existing_room:
                    room = Room(
                        id=entry_data['room_id'],
                        name=entry_data['content'],
                        is_private=False,
                        created_at=entry_data['timestamp']
                    )
                    db.add(room)
                    db.flush()
                    print(f"Created room {entry_data['room_id']} with name {entry_data['content']}")

            sequence_number = self.get_next_sequence(entry_data['room_id'])

            message = Message(
                id=entry_data['id'],
                room_id=entry_data['room_id'],
                user_id=entry_data['user_id'],
                sequence_number=sequence_number,
                content=entry_data['content'] or '',
                message_type=entry_data['msg_type'] or '',
                created_at=entry_data['timestamp']
            )
            db.add(message)
            db.commit()
            print(f"Successfully applied entry {entry_data['id']} to DB.")
            return {'status': 'applied', 'sequence_number': sequence_number}
        except Exception as e:
            print(f"Error applying entry {entry_data['id']} to DB: {e}")
            db.rollback()
            raise
        finally:
            db.close()

    def get_state(self) -> dict:
        """Get the current state of the sequencer.

        Returns:
            dict: A dictionary containing the current sequences with room IDs as strings.
        """
        return {
            'sequences': {str(k): v for k, v in self.sequences.items()}
        }

    def load_state(self, state: dict) -> None:
        """Load the sequencer state from a given state dictionary.

        Args:
            state (dict): A dictionary containing the sequences with room IDs as strings.
        """
        self.sequences = {uuid.UUID(k): v for k, v in state['sequences'].items()}

    def close(self) -> None:
        """Close the Write-Ahead Log and clean up resources."""
        self.wal.close()


class DistributedSequencer(RaftNode):
    """A class to manage distributed sequencing for a chat application using Raft consensus.

    Attributes:
        sequencer (PersistentSequencer): The persistent sequencer instance for handling sequence operations.
    """

    def __init__(self, node_id: str, peers: list, node_address: str, rpc_port: int, sequencer: PersistentSequencer):
        """Initialize the DistributedSequencer with the given configuration.

        Args:
            node_id (str): ID of the current node.
            peers (list): List of peer node IDs.
            node_address (str): API address of the current node.
            rpc_port (int): Port for RPC communication.
            sequencer (PersistentSequencer): Persistent sequencer instance to handle sequence operations.
        """
        from server.internal.discovery.etcd_client import EtcdClient
        from server.internal.sequencer.raft_rpc_client import RaftRPCClient
        from server.config import app_config

        peer_clients = {}
        for peer_id in peers:
            _, _, peer_host, peer_port = app_config.get_all_node_addresses().get(peer_id)
            peer_clients[peer_id] = RaftRPCClient(peer_host, peer_port)

        persistent_state = {}

        node_api_address_str = node_address

        node_rpc_address_str = f"http://{app_config.get_current_node_rpc_host()}:{rpc_port}"

        etcd_client_instance = EtcdClient(
            etcd_endpoints=app_config.get_etcd_endpoints(),
            node_id=node_id,
            api_address=node_address,
            rpc_address=node_rpc_address_str
        )

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
        """Record a new entry in the distributed system.

        Args:
            room_id (str): ID of the chat room.
            user_id (str): ID of the user sending the message.
            content (str): Content of the message.
            msg_type (str): Type of the message.

        Returns:
            dict: A dictionary containing the status of the operation.
                  If successful, includes {'id': entry_id, 'status': 'success'}.
                  If failed, includes {'error': error_message} or
                  {'error': 'Not leader', 'leader_address': leader_addr}.

        Raises:
            NotLeaderException: If the current node is not the leader.
        """
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