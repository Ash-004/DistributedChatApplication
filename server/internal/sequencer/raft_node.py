import time
import random
import threading
import requests
import json
import os

class RaftNode:
    def __init__(self, node_id, peers, rpc_port, wal_file='raft_wal.log'):
        self.wal_file = wal_file
        self.load_wal()
        self.node_id = node_id
        self.peers = peers
        self.current_term = 0
        self.voted_for = None
        self.log = []
        self.commit_index = 0
        self.last_applied = 0
        self.state = 'follower'
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
        
        for peer in self.peers:
            try:
                response = requests.post(
                    f"http://{peer}/request_vote",
                    json={
                        'term': self.current_term,
                        'candidate_id': self.node_id,
                        'last_log_index': len(self.log) - 1,
                        'last_log_term': self.log[-1]['term'] if self.log else 0
                    },
                    timeout=0.5
                )
                if response.json().get('vote_granted'):
                    votes += 1
            except:
                pass
        
        if votes > len(self.peers) // 2:
            self.become_leader()
        else:
            self.state = 'follower'
    
    def persist_state(self):
        state = {
            'current_term': self.current_term,
            'voted_for': self.voted_for,
            'log': self.log,
            'commit_index': self.commit_index
        }
        with open(self.wal_file, 'w') as f:
            json.dump(state, f)
            
    def load_wal(self):
        if os.path.exists(self.wal_file):
            with open(self.wal_file, 'r') as f:
                state = json.load(f)
                self.current_term = state['current_term']
                self.voted_for = state['voted_for']
                self.log = state['log']
                self.commit_index = state['commit_index']
        else:
            self.current_term = 0
            self.voted_for = None
            self.log = []
            self.commit_index = 0
            
    def become_leader(self):
        self.state = 'leader'
        self.next_index = {peer: len(self.log) for peer in self.peers}
        self.match_index = {peer: 0 for peer in self.peers}
        
        def send_heartbeats():
            while self.state == 'leader':
                for peer in self.peers:
                    try:
                        requests.post(
                            f"http://{peer}/append_entries",
                            json={
                                'term': self.current_term,
                                'leader_id': self.node_id,
                                'prev_log_index': self.next_index[peer] - 1,
                                'prev_log_term': self.log[self.next_index[peer]-1]['term'] if self.next_index[peer] > 0 else 0,
                                'entries': [],
                                'leader_commit': self.commit_index
                            },
                            timeout=0.5
                        )
                    except:
                        pass
                time.sleep(0.5)
        threading.Thread(target=send_heartbeats, daemon=True).start()
    
    def request_vote(self, term, candidate_id, last_log_index, last_log_term):
        if term < self.current_term:
            return {'vote_granted': False, 'term': self.current_term}
        
        # Check if candidate's log is at least as up-to-date as ours
        last_log_ok = False
        if self.log:
            our_last_log_term = self.log[-1]['term']
            our_last_log_index = len(self.log) - 1
            if last_log_term > our_last_log_term:
                last_log_ok = True
            elif last_log_term == our_last_log_term and last_log_index >= our_last_log_index:
                last_log_ok = True
        else:
            # If we have no logs, then the candidate's log is at least as up-to-date as ours
            last_log_ok = True
        
        if (self.voted_for is None or self.voted_for == candidate_id) and last_log_ok:
            self.voted_for = candidate_id
            self.persist_state()
            return {'vote_granted': True, 'term': self.current_term}
        
        return {'vote_granted': False, 'term': self.current_term}
    
    def append_entries(self, term, leader_id, prev_log_index, prev_log_term, entries, leader_commit):
        # Reset election timer
        self.last_heartbeat = time.time()
        
        # Reply false if term < currentTerm
        if term < self.current_term:
            return {'success': False, 'term': self.current_term}
            
        # Update term and convert to follower if needed
        if term > self.current_term:
            self.persist_state()
        self.current_term = term
        self.state = 'follower'
        self.voted_for = None
            
        # Check previous log entry
        if prev_log_index >= 0:
            if len(self.log) <= prev_log_index or \
               (prev_log_index >= 0 and self.log[prev_log_index]['term'] != prev_log_term):
                return {'success': False, 'term': self.current_term}
        
        # Append new entries
        if entries:
            self.log = self.log[:prev_log_index+1] + entries
            
        # Update commit index
        if leader_commit > self.commit_index:
            self.commit_index = min(leader_commit, len(self.log) - 1)
            
        return {'success': True, 'term': self.current_term}
        
        self.last_heartbeat = time.time()
        self.state = 'follower'
        self.current_term = term
        
        # Log replication logic would go here
        
        if leader_commit > self.commit_index:
            self.commit_index = min(leader_commit, len(self.log) - 1)
        
        return {'success': True, 'term': self.current_term}