import logging
import requests
from typing import List, Tuple
import time
import random
from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import threading

class RaftNode:
    def __init__(self, node_id: str, peers: List[str], node_address: str, rpc_port: int):
        self.node_id = node_id
        self.peers = peers
        self.state = 'FOLLOWER'
        self.current_term = 0
        self.voted_for = None
        self.log = []
        self.commit_index = 0
        self.last_applied = 0
        self.election_timeout = random.uniform(0.5, 1.5)  # Random election timeout between 500ms and 1.5s
        self.last_heartbeat = time.time()
        self.leader_address = None
        self.node_address = node_address
        self.peer_clients = {peer: RaftRPCClient(peer) for peer in peers}
        self.start_rpc_server(rpc_port)

    def start_rpc_server(self, port):
        server_address = ('', port)
        httpd = HTTPServer(server_address, RaftRPCServer)
        httpd.node = self
        thread = threading.Thread(target=httpd.serve_forever)
        thread.daemon = True
        thread.start()
        self.rpc_server = httpd

    def request_vote(self, candidate_id: str, term: int, last_log_index: int, last_log_term: int) -> Tuple[bool, int]:
        """
        RequestVote RPC implementation
        Returns (vote_granted, current_term)
        """
        if term < self.current_term:
            return (False, self.current_term)
            
        if self.voted_for is not None and self.voted_for != candidate_id:
            return (False, self.current_term)
            
        if last_log_term < self.log[-1]['term'] if self.log else 0:
            return (False, self.current_term)
            
        self.voted_for = candidate_id
        self.current_term = term
        return (True, self.current_term)
        
    def append_entries(self, leader_id: str, term: int, prev_log_index: int, 
                      prev_log_term: int, entries: list, leader_commit: int) -> bool:
        """
        AppendEntries RPC implementation
        Returns success
        """
        if term < self.current_term:
            return False
            
        if prev_log_index > 0 and (prev_log_index >= len(self.log) or self.log[prev_log_index]['term'] != prev_log_term):
            return False
            
        self.log = self.log[:prev_log_index + 1]
        self.log.extend(entries)
        self.commit_index = min(leader_commit, len(self.log) - 1)
        self.apply_entries()
        return True

    def become_leader(self):
        self.state = 'LEADER'
        # Initialize nextIndex and matchIndex for peers
        self.next_index = {peer: len(self.log) for peer in self.peers}
        self.match_index = {peer: 0 for peer in self.peers}
        
        # Start sending heartbeats
        self.send_heartbeats()
        
    def step_down(self, term):
        self.current_term = term
        self.state = 'FOLLOWER'
        self.voted_for = None

    def start_election(self):
        """Start leader election process"""
        self.current_term += 1
        self.state = 'CANDIDATE'
        self.voted_for = self.node_id
        
        # Request votes from all peers
        votes_received = 1  # Vote for self
        
        for peer in self.peers:
            try:
                # Send RequestVote RPC to peer
                # This would be an RPC call in real implementation
                vote_granted, term = self.peer_clients[peer].request_vote(
                    candidate_id=self.node_id,
                    term=self.current_term,
                    last_log_index=len(self.log) - 1,
                    last_log_term=self.log[-1]['term'] if self.log else 0
                )
                
                if term > self.current_term:
                    self.step_down(term)
                    return
                    
                if vote_granted:
                    votes_received += 1
                    
                    # Check if we have majority
                    if votes_received > len(self.peers) / 2:
                        self.become_leader()
                        return
                        
            except Exception as e:
                logging.error(f"Vote request to {peer} failed: {e}")
                
        # If election timeout, restart election
        self.start_election()

    def start_election_timer(self):
        self.last_heartbeat = time.time()

    def send_heartbeats(self):
        """Send periodic heartbeats to all followers"""
        if self.state != 'LEADER':
            return
            
        for peer in self.peers:
            try:
                prev_log_index = self.next_index[peer] - 1
                prev_log_term = self.log[prev_log_index]['term'] if prev_log_index >= 0 else 0
                entries = self.log[self.next_index[peer]:]
                
                request = AppendEntriesRequest(
                    leader_id=self.node_id,
                    term=self.current_term,
                    prev_log_index=prev_log_index,
                    prev_log_term=prev_log_term,
                    entries=entries,
                    leader_commit=self.commit_index,
                    leader_address=self.node_address
                )
                
                # Send AppendEntries RPC
                success = self.peer_clients[peer].append_entries(
                    leader_id=self.node_id,
                    term=self.current_term,
                    prev_log_index=prev_log_index,
                    prev_log_term=prev_log_term,
                    entries=entries,
                    leader_commit=self.commit_index
                )
                
                if success:
                    self.next_index[peer] = len(self.log)
                    self.match_index[peer] = len(self.log) - 1
                else:
                    self.next_index[peer] -= 1
                    
            except Exception as e:
                logging.error(f"Heartbeat to {peer} failed: {e}")

    def replicate_log(self):
        """Replicate log entries to followers"""
        for peer in self.peers:
            # Prepare AppendEntries RPC
            prev_log_index = self.next_index[peer] - 1
            prev_log_term = self.log[prev_log_index]["term"] if prev_log_index >= 0 else 0
            entries = self.log[self.next_index[peer]:]
            
            # Send AppendEntries RPC
            # This is simplified - in real Raft you'd handle responses and retries
            try:
                # Make RPC call to peer
                success = self.peer_clients[peer].append_entries(
                    leader_id=self.node_id,
                    term=self.current_term,
                    prev_log_index=prev_log_index,
                    prev_log_term=prev_log_term,
                    entries=entries,
                    leader_commit=self.commit_index
                )
                
                # Update next_index on success
                if success:
                    self.next_index[peer] = len(self.log)
                    self.match_index[peer] = len(self.log) - 1
            except Exception as e:
                # Decrement next_index and retry later
                self.next_index[peer] -= 1

    def apply_entries(self):
        """Apply committed entries to state machine"""
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            entry = self.log[self.last_applied]
            self.sequencer.record_entry(
                entry['entry']['room_id'],
                entry['entry']['user_id'],
                entry['entry']['content'],
                entry['entry']['msg_type']
            )
        
    def advance_commit_index(self):
        """Advance commit index based on peer match indices"""
        if self.state != 'LEADER':
            return
            
        # Find median match index
        match_indices = sorted(self.match_index.values())
        new_commit_index = match_indices[len(match_indices) // 2]
        
        if new_commit_index > self.commit_index:
            # Only commit entries from current term
            if self.log[new_commit_index]['term'] == self.current_term:
                self.commit_index = new_commit_index
                # Apply to state machine
                self.apply_entries()

    def get_leader(self):
        # In real implementation, we'd know the leader address
        # For simplicity, assume the first peer is the leader
        return self.peers[0]

    def forward_to_leader(self, room_id, user_id, content, msg_type):
        """Forward request to current leader"""
        leader = self.get_leader()
        return self.peer_clients[leader].record_entry(room_id, user_id, content, msg_type)

    def run(self):
        self.start_election_timer()
        
        while True:
            if self.state == 'FOLLOWER':
                # Check election timeout
                if time.time() - self.last_heartbeat > self.election_timeout:
                    self.start_election()
                    
            elif self.state == 'LEADER':
                self.send_heartbeats()
                time.sleep(0.05)  # 50ms heartbeat interval


class NotLeaderException(Exception):
    def __init__(self, leader_addr):
        self.leader_addr = leader_addr


class DistributedSequencer(RaftNode):
    def __init__(self, node_id: str, peers: List[str], node_address: str, sequencer_address: str, rpc_port: int):
        super().__init__(node_id, peers, node_address, rpc_port)
        self.sequencer = PersistentSequencer(sequencer_address)
        
    def record_entry(self, room_id, user_id, content, msg_type):
        if self.state != 'LEADER':
            # Forward to leader
            return self.forward_to_leader(room_id, user_id, content, msg_type)
            
        # Create log entry
        entry = {
            'room_id': room_id,
            'user_id': user_id,
            'content': content,
            'msg_type': msg_type
        }
        
        # Replicate log entry
        self.replicate_log()
        
        # Wait for majority replication
        while True:
            match_indices = sorted(self.match_index.values())
            new_commit_index = match_indices[len(match_indices) // 2]
            
            if new_commit_index >= len(self.log) - 1:
                self.commit_index = new_commit_index
                self.apply_entries()
                break
                
        return {'sequence_number': self.log[-1]['entry']['sequence_number']}

class AppendEntriesRequest:
    def __init__(self, leader_id, term, prev_log_index, prev_log_term, entries, leader_commit, leader_address):
        self.leader_id = leader_id
        self.term = term
        self.prev_log_index = prev_log_index
        self.prev_log_term = prev_log_term
        self.entries = entries
        self.leader_commit = leader_commit
        self.leader_address = leader_address

class RaftRPCClient:
    def __init__(self, peer):
        self.peer = peer

    def request_vote(self, candidate_id, term, last_log_index, last_log_term):
        url = f"http://{self.peer}/request_vote"
        data = {
            'candidate_id': candidate_id,
            'term': term,
            'last_log_index': last_log_index,
            'last_log_term': last_log_term
        }
        response = requests.post(url, json=data)
        return response.json()

    def append_entries(self, leader_id, term, prev_log_index, prev_log_term, entries, leader_commit):
        url = f"http://{self.peer}/append_entries"
        data = {
            'leader_id': leader_id,
            'term': term,
            'prev_log_index': prev_log_index,
            'prev_log_term': prev_log_term,
            'entries': entries,
            'leader_commit': leader_commit
        }
        response = requests.post(url, json=data)
        return response.json()

    def record_entry(self, room_id, user_id, content, msg_type):
        url = f"http://{self.peer}/record_entry"
        data = {
            'room_id': room_id,
            'user_id': user_id,
            'content': content,
            'msg_type': msg_type
        }
        response = requests.post(url, json=data)
        return response.json()

class RaftRPCServer(BaseHTTPRequestHandler):
    def do_POST(self):
        if self.path == '/request_vote':
            data = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
            candidate_id = data['candidate_id']
            term = data['term']
            last_log_index = data['last_log_index']
            last_log_term = data['last_log_term']
            vote_granted, current_term = self.server.node.request_vote(candidate_id, term, last_log_index, last_log_term)
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({'vote_granted': vote_granted, 'current_term': current_term}).encode())
        elif self.path == '/append_entries':
            data = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
            leader_id = data['leader_id']
            term = data['term']
            prev_log_index = data['prev_log_index']
            prev_log_term = data['prev_log_term']
            entries = data['entries']
            leader_commit = data['leader_commit']
            success = self.server.node.append_entries(leader_id, term, prev_log_index, prev_log_term, entries, leader_commit)
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({'success': success}).encode())
        elif self.path == '/record_entry':
            data = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
            room_id = data['room_id']
            user_id = data['user_id']
            content = data['content']
            msg_type = data['msg_type']
            result = self.server.node.record_entry(room_id, user_id, content, msg_type)
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps(result).encode())

class PersistentSequencer:
    def __init__(self, sequencer_address):
        self.sequencer_address = sequencer_address

    def record_entry(self, room_id, user_id, content, msg_type):
        # Simulate recording entry
        pass
