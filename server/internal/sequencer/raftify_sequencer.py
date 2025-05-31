from raftify.raft_node import RaftNode
from raftify.raft_facade import RaftFacade
from raftify.states import State
from raftify.utils import SocketAddr
from typing import Dict, Any, Optional
import uuid

class RaftifySequencer(RaftFacade):
    def __init__(self, node_id: int, peers: Dict[int, SocketAddr], bind_addr: SocketAddr):
        super().__init__(node_id, peers, bind_addr)
        self.sequences: Dict[uuid.UUID, int] = {}
        
    def on_message(self, message: Any) -> None:
        # Handle committed log entries
        if 'command' in message:
            cmd = message['command']
            room_id = uuid.UUID(cmd['room_id'])
            
            # Update sequence number for the room
            if room_id not in self.sequences:
                self.sequences[room_id] = 0
            self.sequences[room_id] += 1
            
    def record_entry(self, room_id: uuid.UUID, user_id: uuid.UUID, content: str, msg_type: str) -> Optional[dict]:
        if self.get_state() != State.Leader:
            # Forward to leader if we're not the leader
            return None
            
        # Create log entry
        entry = {
            'command': {
                'room_id': str(room_id),
                'user_id': str(user_id),
                'content': content,
                'msg_type': msg_type
            }
        }
        
        # Propose entry to Raft cluster
        future = self.propose(entry)
        result = future.result()  # Wait for commit
        
        if not result.success:
            return None
            
        # Return the sequence number for this room
        sequence_number = self.sequences.get(room_id, 0)
        return {
            'id': uuid.uuid4(),
            'sequence_number': sequence_number
        }