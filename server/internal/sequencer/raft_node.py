import time
import random
import threading
import requests
import json
import os

class RaftNode:
    """A class implementing a Raft consensus node for distributed systems.

    Attributes:
        wal_file (str): File path for the Write-Ahead Log.
        node_id (str): Unique identifier for the node.
        peers (list): List of peer node addresses.
        current_term (int): Current term of the node.
        voted_for (str): ID of the candidate this node voted for in the current term.
        log (list): Log of entries for the Raft protocol.
        commit_index (int): Index of the highest log entry known to be committed.
        last_applied (int): Index of the highest log entry applied to the state machine.
        state (str): Current state of the node ('follower', 'candidate', or 'LEADER').
        election_timeout (float): Random election timeout between 1.5 and 3.0 seconds.
        last_heartbeat (float): Timestamp of the last heartbeat received.
        rpc_port (int): Port for RPC communication.
        next_index (dict): For leaders, the next log index to send to each peer.
        match_index (dict): For leaders, the highest log index known to be replicated on each peer.
    """

    def __init__(self, node_id, peers, rpc_port, wal_file='raft_wal.log'):
        """Initialize the RaftNode with the given configuration.

        Args:
            node_id (str): Unique identifier for the node.
            peers (list): List of peer node addresses.
            rpc_port (int): Port for RPC communication.
            wal_file (str, optional): File path for the Write-Ahead Log. Defaults to 'raft_wal.log'.
        """
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
        """Start a timer to trigger elections if no heartbeat is received within the election timeout."""
        def timer():
            while True:
                time.sleep(0.1)
                if self.state != 'LEADER' and time.time() - self.last_heartbeat > self.election_timeout:
                    self.start_election()
        threading.Thread(target=timer, daemon=True).start()

    def start_election(self):
        """Start an election to become the leader by requesting votes from peers."""
        self.current_term += 1
        self.voted_for = self.node_id
        self.state = 'candidate'
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
                    }
                )
                if response.status_code == 200:
                    result = response.json()
                    if result.get('vote_granted'):
                        votes += 1
            except Exception as e:
                print(f"Error requesting vote from {peer}: {e}")

        if votes > len(self.peers) / 2:
            self.become_leader()
        else:
            self.state = 'follower'
            self.voted_for = None

    def run(self):
        """Run the Raft node, periodically checking for election timeouts."""
        while True:
            if self.state != 'LEADER' and time.time() - self.last_heartbeat > self.election_timeout:
                self.start_election()
            time.sleep(0.1)

    def persist_state(self):
        """Persist the current state to the Write-Ahead Log."""
        state = {
            'current_term': self.current_term,
            'voted_for': self.voted_for,
            'log': self.log,
            'commit_index': self.commit_index
        }
        with open(self.wal_file, 'w') as f:
            json.dump(state, f)

    def load_wal(self):
        """Load the state from the Write-Ahead Log if it exists, otherwise initialize with defaults."""
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
        """Transition the node to the leader state and start sending heartbeats to peers."""
        self.state = 'LEADER'
        self.next_index = {peer: len(self.log) for peer in self.peers}
        self.match_index = {peer: 0 for peer in self.peers}

        def send_heartbeats():
            while self.state == 'LEADER':
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
        """Handle a vote request from a candidate.

        Args:
            term (int): Term of the candidate.
            candidate_id (str): ID of the candidate requesting the vote.
            last_log_index (int): Index of the candidate's last log entry.
            last_log_term (int): Term of the candidate's last log entry.

        Returns:
            dict: A dictionary containing the vote decision and the current term.
                  {'vote_granted': bool, 'term': int}
        """
        if term < self.current_term:
            return {'vote_granted': False, 'term': self.current_term}

        last_log_ok = False
        if self.log:
            our_last_log_term = self.log[-1]['term']
            our_last_log_index = len(self.log) - 1
            if last_log_term > our_last_log_term:
                last_log_ok = True
            elif last_log_term == our_last_log_term and last_log_index >= our_last_log_index:
                last_log_ok = True
        else:
            last_log_ok = True

        if (self.voted_for is None or self.voted_for == candidate_id) and last_log_ok:
            self.voted_for = candidate_id
            self.persist_state()
            return {'vote_granted': True, 'term': self.current_term}

        return {'vote_granted': False, 'term': self.current_term}

    def append_entries(self, term, leader_id, prev_log_index, prev_log_term, entries, leader_commit):
        """Handle an append entries request from the leader.

        Args:
            term (int): Term of the leader.
            leader_id (str): ID of the leader.
            prev_log_index (int): Index of the log entry immediately preceding the new entries.
            prev_log_term (int): Term of the log entry at prev_log_index.
            entries (list): List of log entries to append.
            leader_commit (int): Leader's commit index.

        Returns:
            dict: A dictionary indicating the success of the operation and the current term.
                  {'success': bool, 'term': int}
        """
        self.last_heartbeat = time.time()

        if term < self.current_term:
            return {'success': False, 'term': self.current_term}

        if term > self.current_term:
            self.persist_state()
        self.current_term = term
        self.state = 'follower'
        self.voted_for = None

        if prev_log_index >= 0:
            if len(self.log) <= prev_log_index or \
               (prev_log_index >= 0 and self.log[prev_log_index]['term'] != prev_log_term):
                return {'success': False, 'term': self.current_term}

        if entries:
            self.log = self.log[:prev_log_index+1] + entries
            self.persist_state()

        if leader_commit > self.commit_index:
            self.commit_index = min(leader_commit, len(self.log) - 1)

            while self.last_applied < self.commit_index:
                self.last_applied += 1
                print(f"Node {self.node_id}: Applying log entry {self.last_applied}: {self.log[self.last_applied]}")

        return {'success': True, 'term': self.current_term}

    def handle_client_command(self, command):
        """Handle a client command by appending it to the log if the node is the leader.

        Args:
            command (dict): The client command to process (e.g., {'action': 'create_room'}).

        Returns:
            dict: A dictionary indicating the success of the operation and additional details.
                  If successful, returns {'success': True, 'message': str, 'log_index': int} or
                  {'success': True, 'room_id': str, 'message': str}.
                  If failed, returns {'success': False, 'message': str}.
        """
        if self.state != 'LEADER':
            return {'success': False, 'message': 'Not a leader'}

        entry = {'term': self.current_term, 'command': command}
        self.log.append(entry)
        self.persist_state()
        current_log_index = len(self.log) -1

        if self.commit_index < current_log_index:
             self.commit_index = current_log_index

             while self.last_applied < self.commit_index:
                self.last_applied += 1

                if command['action'] == 'create_room':
                    return {'success': True, 'room_id': f"room_{self.last_applied}", 'message': 'Command processed and committed'}

        return {'success': True, 'message': 'Command received by leader and logged', 'log_index': len(self.log) -1}