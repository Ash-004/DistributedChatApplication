import time
import random
import threading
import uuid
from .raft_rpc import RaftRPCServer, start_rpc_server

class MinimalRaftNode:
    def __init__(self, node_id, peers, rpc_port, node_addresses):
        """
        :param node_id: ID of this node
        :param peers: list of peer node IDs
        :param rpc_port: RPC port for this node
        :param node_addresses: dict mapping node ID to (host, port)
        """
        self.node_id = node_id
        self.peers = peers
        self.rpc_port = rpc_port
        self.node_addresses = node_addresses
        self.node_id = node_id
        self.peers = peers
        self.current_term = 0
        self.voted_for = None
        self.log = []
        self.commit_index = 0
        self.state = 'follower'
        self.leader_id = None
        self.election_timeout = random.uniform(1.5, 3.0)
        self.last_heartbeat = time.time()
        self.rpc_port = rpc_port
        self.start_election_timer()
    
    def start_election_timer(self):
        def timer():
            while True:
                time.sleep(0.1)
                if self.state != 'leader' and time.time() - self.last_heartbeat > self.election_timeout:
                    self.start_election()
        threading.Thread(target=timer, daemon=True).start()
    
    def start_election(self):
        self.state = 'candidate'
        self.current_term += 1
        self.voted_for = self.node_id
        votes = 1
        
        # Request votes from peers
        for peer in self.peers:
            try:
                # In a real implementation, we'd make RPC calls here
                # For now, just count as a vote for simplicity
                votes += 1
            except:
                pass
        
        if votes > len(self.peers) // 2:
            self.become_leader()
        else:
            self.state = 'follower'
    
    def become_leader(self):
        self.state = 'leader'
        self.leader_id = self.node_id
        print(f"Node {self.node_id} became leader for term {self.current_term}")
        
    def request_vote(self, term, candidate_id, last_log_index, last_log_term):
        if term < self.current_term:
            return {'vote_granted': False, 'term': self.current_term}
        
        if (self.voted_for is None or self.voted_for == candidate_id):
            self.voted_for = candidate_id
            return {'vote_granted': True, 'term': self.current_term}
        
        return {'vote_granted': False, 'term': self.current_term}
    
    def append_entries(self, term, leader_id, prev_log_index, prev_log_term, entries, leader_commit):
        if term < self.current_term:
            return {'success': False, 'term': self.current_term}
        
        self.last_heartbeat = time.time()
        self.state = 'follower'
        self.leader_id = leader_id
        self.current_term = term
        
        return {'success': True, 'term': self.current_term}