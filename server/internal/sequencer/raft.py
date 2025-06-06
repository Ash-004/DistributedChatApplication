
import logging
import requests # Keep for now, might be used by RaftRPCClient if not fully gRPC
from typing import List, Dict, Tuple, Any, Optional
import time
from datetime import datetime
import random
# from http.server import BaseHTTPRequestHandler, HTTPServer # Commenting out as per gRPC migration plan, ensure RaftRPCServer is independent
import json
import threading
import uuid
import traceback

from server.config import app_config # Raft configurations are accessed via app_config
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError, SQLAlchemyError

# Assuming EtcdClient is in this path based on previous work
from server.internal.discovery.etcd_client import EtcdClient
from server.internal.storage.models import Room, Message # Keep if snapshotting involves these
from server.internal.storage.database import SessionLocal, init_db # Keep if snapshotting involves these

# Set up logger for the raft module
logger_raft = logging.getLogger('raft')

# Constants for etcd registration
ETCD_LEASE_TTL = 30  # seconds
ETCD_HEARTBEAT_INTERVAL = 10 # seconds, should be less than TTL

# Raft Node States
LEADER = "LEADER"
FOLLOWER = "FOLLOWER"
CANDIDATE = "CANDIDATE"

class RaftNode:
    def __init__(self,
                 peer_clients: Dict[str, 'RaftRPCClient'],
                 persistent_state: Dict[str, Any],
                 etcd_client_instance: Optional['EtcdClient'],
                 node_api_address_str: Optional[str],
                 node_rpc_address_str: Optional[str], # This is the full string like "http://host:port"
                 sequencer_instance: Optional[Any] = None):
        
        self.node_id = app_config.node_id
        if not self.node_id:
            logger_raft.critical("CRITICAL: NODE_ID is not set. RaftNode cannot initialize.")
            raise ValueError("NODE_ID is not set, cannot initialize RaftNode.")

        # Etcd client and addresses for service discovery
        self.etcd_client = etcd_client_instance
        self.node_api_address = node_api_address_str       # For leader advertisement, e.g. "http://localhost:8001"
        self.node_rpc_address_for_etcd = node_rpc_address_str # For etcd registration, e.g. "http://localhost:5001"

        # Event for signaling background threads to stop
        self.stop_event = threading.Event()

        self.peers = app_config.get_peer_node_ids() # List of peer IDs
        self.peer_clients = peer_clients # Passed in, expected to be constructed using app_config by caller
        
        self.current_term = int(persistent_state.get('current_term', 0))
        self.voted_for = persistent_state.get('voted_for', None)
        self.log = persistent_state.get('log', []) # Log entries
        self.commit_index = int(persistent_state.get('commit_index', -1))
        self.last_applied = int(persistent_state.get('last_applied', -1))
        logger_raft.info(f"Node {self.node_id}: Initialized with term={self.current_term}, voted_for={self.voted_for}, log_len={len(self.log)}, commit_idx={self.commit_index}, last_applied={self.last_applied}")
        
        self.node_rpc_address_tuple = app_config.get_current_node_rpc_address()
        if not self.node_rpc_address_tuple:
            logger_raft.critical(f"CRITICAL: RPC address tuple for current node '{self.node_id}' could not be determined from config.")
            raise ValueError(f"RPC address tuple for current node '{self.node_id}' could not be determined from config.")
        
        self.all_node_rpc_addresses_map = app_config.get_all_node_addresses() # Map of node_id to (host,port)
        
        self.state = FOLLOWER # Explicitly set initial state for clarity before etcd registration

        # Initial Etcd Registration
        if self.etcd_client and self.node_id and self.node_api_address and self.node_rpc_address_for_etcd:
            logger_raft.info(f"Node {self.node_id}: Attempting to register with etcd as FOLLOWER. RPC for Etcd: {self.node_rpc_address_for_etcd}, API for Etcd: {self.node_api_address}, Lease TTL: {ETCD_LEASE_TTL}s, Etcd Heartbeat Interval: {ETCD_HEARTBEAT_INTERVAL}s")
            registered = self.etcd_client.register_node(
                initial_role=FOLLOWER,
                ttl=ETCD_LEASE_TTL,
                heartbeat_interval=ETCD_HEARTBEAT_INTERVAL
            )
            if registered:
                logger_raft.info(f"Node {self.node_id}: Successfully initiated registration with etcd as FOLLOWER.")
            else:
                logger_raft.error(f"Node {self.node_id}: Failed to register with etcd.")
        else:
            logger_raft.warning(f"Node {self.node_id}: Etcd client or node addresses not fully provided. Skipping etcd registration.")
        
        self.min_election_timeout_sec = app_config.get_raft_config('election_timeout_min_ms', 350) / 1000.0
        self.max_election_timeout_sec = app_config.get_raft_config('election_timeout_max_ms', 700) / 1000.0
        self.heartbeat_interval_sec = app_config.get_raft_config('heartbeat_interval_ms', 150) / 1000.0
        
        if not (0 < self.heartbeat_interval_sec < self.min_election_timeout_sec <= self.max_election_timeout_sec):
            logger_raft.error(f"Invalid Raft timing configurations: HB={self.heartbeat_interval_sec}, E_min={self.min_election_timeout_sec}, E_max={self.max_election_timeout_sec}")
            raise ValueError("Invalid Raft timing configurations: ensure 0 < heartbeat < min_election <= max_election.")
        logger_raft.info(f"Raft timings (sec): Heartbeat={self.heartbeat_interval_sec}, Election Min/Max=({self.min_election_timeout_sec}/{self.max_election_timeout_sec})")
        
        self.heartbeat_fail_backoff = 1.0
        self.min_leader_timeout = 1.0
        self.election_timeout = random.uniform(self.min_election_timeout_sec, self.max_election_timeout_sec)
        self.election_timer_start_time = 0.0
        self.election_timer_thread = None
        self.last_election_reset = time.time()
        self.heartbeat_sender_thread = None
        self.leader_id = None
        self.leader_address = None
        
        self.snapshot_path = app_config.get_snapshot_path()
        self.snapshot_entry_interval = app_config.get_raft_config('snapshot_entry_interval', 100)
        self.last_snapshot_time = time.time()
        
        self.last_heartbeat_received_time = time.time()
        self.last_valid_leader_heartbeat_time = 0.0
        
        self.sequencer_instance = sequencer_instance
        if self.sequencer_instance is None:
            logger_raft.warning(f"Node {self.node_id}: No sequencer_instance (state machine) provided. Snapshot and log application might be limited.")
        
        logger_raft.info(f"RaftNode {self.node_id} core attributes initialized. Peers: {self.peers}. My RPC tuple: {self.node_rpc_address_tuple}. My RPC for Etcd: {self.node_rpc_address_for_etcd}. My API for Etcd: {self.node_api_address}. Snapshot path: {self.snapshot_path}")

        # Initial Etcd Registration
        if self.etcd_client and self.node_rpc_address_for_etcd and self.node_api_address:
            try:
                etcd_config_values = app_config.get('etcd', {})
                node_ttl = etcd_config_values.get('ttl', 30)
                heartbeat_interval = etcd_config_values.get('heartbeat_interval', 10) # This is for etcd lease, not Raft heartbeat

                logger_raft.info(f"Node {self.node_id}: Attempting to register with etcd as {self.state}. "
                                 f"RPC for Etcd: {self.node_rpc_address_for_etcd}, API for Etcd: {self.node_api_address}, "
                                 f"Lease TTL: {node_ttl}s, Etcd Heartbeat Interval: {heartbeat_interval}s")
                
                self.etcd_client.register_node(
                    initial_role=self.state,
                    ttl=node_ttl,
                    heartbeat_interval=heartbeat_interval
                )
                logger_raft.info(f"Node {self.node_id}: Successfully initiated registration with etcd as {self.state}.")
            except Exception as e:
                logger_raft.error(f"Node {self.node_id}: Failed to register with etcd during initialization: {e}", exc_info=True)
        else:
            missing_params = []
            if not self.etcd_client: missing_params.append("etcd_client")
            if not self.node_rpc_address_for_etcd: missing_params.append("node_rpc_address_for_etcd")
            if not self.node_api_address: missing_params.append("node_api_address")
            if missing_params:
                logger_raft.warning(f"Node {self.node_id}: Skipping etcd registration due to missing parameters: {', '.join(missing_params)}.")
            else:
                logger_raft.info(f"Node {self.node_id}: Etcd client or addresses not provided, etcd registration skipped by design.")

        self.load_snapshot()
        
        if self.sequencer_instance and hasattr(self, '_pending_state_machine_state') and self._pending_state_machine_state is not None:
            logger_raft.info(f"Node {self.node_id}: Applying pending state machine state to sequencer instance after snapshot load.")
            try:
                self.sequencer_instance.load_state(self._pending_state_machine_state)
                logger_raft.info(f"Node {self.node_id}: Successfully applied pending state to sequencer.")
            except Exception as e:
                logger_raft.error(f"Node {self.node_id}: Error applying pending state to sequencer: {e}", exc_info=True)
            finally:
                del self._pending_state_machine_state
        elif hasattr(self, '_pending_state_machine_state'):
            del self._pending_state_machine_state

    def get_last_log_index(self):
        return len(self.log) - 1 if self.log else -1

    def get_last_log_term(self):
        return self.log[-1]['term'] if self.log else 0

    def verify_peer_connections(self, timeout=30, initial_delay=0.1, max_delay=2.0):
        logging.info(f"Node {self.node_id}: Verifying peer connections...")
        start_time = time.time()
        connected_peers = set()
        
        while time.time() - start_time < timeout:
            for peer_id in self.peers:
                if peer_id not in connected_peers:
                    try:
                        # Send a dummy PreVote RPC to check connectivity
                        # Using current term and log info, but it's just for connectivity check
                        # The actual PreVote logic will handle the response
                        logging.debug(f"Node {self.node_id}: Attempting to connect to peer {peer_id} for verification.")
                        vote_granted, term = self.peer_clients[peer_id].pre_vote(
                            candidate_id=self.node_id,
                            candidate_proposed_term=self.current_term + 1, # Propose a future term
                            candidate_last_log_index=self.get_last_log_index(),
                            candidate_last_log_term=self.get_last_log_term()
                        )
                        if vote_granted is not None: # Check if response was received (even if vote not granted)
                            connected_peers.add(peer_id)
                            logging.info(f"Node {self.node_id}: Successfully connected to peer {peer_id}.")
                    except Exception as e:
                        logging.debug(f"Node {self.node_id}: Failed to connect to peer {peer_id}: {e}")
            
            if len(connected_peers) >= (len(self.peers) // 2) + 1 or len(self.peers) == 0: # Majority or single node
                logging.info(f"Node {self.node_id}: Majority of peers ({len(connected_peers)}/{len(self.peers)}) reachable. Proceeding.")
                return True
            
            time.sleep(initial_delay) # Wait before next attempt
            initial_delay = min(initial_delay * 2, max_delay) # Exponential backoff
        
        logging.warning(f"Node {self.node_id}: Failed to connect to a majority of peers within {timeout} seconds. Only {len(connected_peers)}/{len(self.peers)} reachable.")
        return False

    def start_election_timer(self):
        """Starts or resets the election timer. This method implements the full timer logic."""
        # Stop existing timer if it's running
        if hasattr(self, 'election_timer_thread') and self.election_timer_thread is not None:
            try:
                self.election_timer_thread.cancel()
                # Wait a very short time to ensure timer is actually canceled
                # This helps prevent thread leakage
                time.sleep(0.001)  
            except Exception as e:
                logging.error(f"Node {self.node_id} error canceling election timer: {e}", exc_info=False)
        
        # Generate a random election timeout between min and max values
        self.election_timeout = random.uniform(self.min_election_timeout_sec, self.max_election_timeout_sec)
        
        # Create and start a new timer thread
        self.election_timer_thread = threading.Timer(self.election_timeout, self._handle_election_timeout)
        self.election_timer_thread.daemon = True  # Ensure timer doesn't block program exit
        self.election_timer_start_time = time.time()  # Record when this specific timer period started
        self.election_timer_thread.start()
        
        logging.debug(f"Node {self.node_id} (state: {self.state}, term: {self.current_term}) started election timer for {self.election_timeout:.3f}s")

    def _handle_election_timeout(self):
        print(f"DEBUG: Node {self.node_id}: ENTERING _handle_election_timeout at {time.time()}", flush=True) # Diagnostic print
        """Called when the election timer expires."""
        # Ensure this runs in the main Raft thread or is thread-safe if modifying shared state
        if self.state == 'LEADER': # Leaders don't start elections
            # logging.debug(f"Node {self.node_id} is LEADER, election timer expired but not starting election.")
            return

        # Leader stickiness: Check if we recently heard from the current leader
        # self.election_timeout is the duration of the timer that just expired.
        if self.leader_id is not None and \
           (time.time() - self.last_valid_leader_heartbeat_time) < self.election_timeout:
            logging.info(f"Node {self.node_id} (state: {self.state}, term: {self.current_term}) election timer expired, "
                         f"but deferring election due to recent heartbeat from leader {self.leader_id} (last seen {time.time() - self.last_valid_leader_heartbeat_time:.2f}s ago, timeout {self.election_timeout:.2f}s). Resetting timer.")
            self.start_election_timer() # Reset election timer
            return

        logging.info(f"Node {self.node_id} (state: {self.state}, term: {self.current_term}) election timer expired. Initiating Pre-Vote round.")
        self._conduct_pre_vote_round()
        
    def pre_vote(self, candidate_id, candidate_proposed_term, candidate_last_log_index, candidate_last_log_term):
        """Handles Pre-Vote requests from other nodes.
        This method is called by the RPC server when a Pre-Vote request is received.
        
        Args:
            candidate_id (str): The ID of the candidate requesting the Pre-Vote
            candidate_proposed_term (int): The term the candidate is proposing to start
            candidate_last_log_index (int): Index of candidate's last log entry
            candidate_last_log_term (int): Term of candidate's last log entry
            
        Returns:
            tuple: (vote_granted, current_term) where vote_granted is a boolean and current_term is an integer
        """
        logging.info(f"Node {self.node_id} (term: {self.current_term}) received Pre-Vote request from {candidate_id} for term {candidate_proposed_term}")
        
        # If the candidate's term is less than our current term, reject
        if candidate_proposed_term < self.current_term:
            logging.info(f"Node {self.node_id} rejected Pre-Vote for {candidate_id}: candidate term {candidate_proposed_term} < current term {self.current_term}")
            return False, self.current_term
        
        # If we are already a leader, we should not grant a pre-vote
        if self.state == 'LEADER':
            logging.info(f"Node {self.node_id} rejected Pre-Vote for {candidate_id}: I am already a LEADER for term {self.current_term}")
            return False, self.current_term
        
        # Check if the candidate's log is at least as up-to-date as ours
        my_last_log_term = self.log[-1]['term'] if self.log else 0
        my_last_log_index = len(self.log) - 1 if self.log else -1
        
        log_ok = (candidate_last_log_term > my_last_log_term) or \
                (candidate_last_log_term == my_last_log_term and \
                 candidate_last_log_index >= my_last_log_index)
        
        # Only grant vote if we don't have a current leader that we think is alive
        has_valid_leader = (self.leader_id is not None and \
                          (time.time() - self.last_valid_leader_heartbeat_time) < self.election_timeout)
        
        # Enhanced stickiness for Pre-Vote:
        # If we have received a heartbeat (i.e., election timer was reset) very recently,
        # we shouldn't allow disruption by granting a pre-vote
        recent_heartbeat = False
        if hasattr(self, 'election_timer_start_time') and self.election_timer_start_time > 0:
            time_since_timer_reset = time.time() - self.election_timer_start_time
            recent_heartbeat = time_since_timer_reset < (self.min_election_timeout_sec / 2)
        
        # Don't grant pre-vote if we're in candidate state and already running an election for this or higher term
        already_in_election = (self.state == 'CANDIDATE' and candidate_proposed_term <= self.current_term)
        
        vote_granted = log_ok and not has_valid_leader and not recent_heartbeat and not already_in_election
        
        if vote_granted:
            logging.info(f"Node {self.node_id} granted Pre-Vote to {candidate_id} for term {candidate_proposed_term}")
        else:
            if not log_ok:
                reason = "candidate log not up-to-date"
            elif has_valid_leader:
                reason = "already has valid leader"
            elif recent_heartbeat:
                reason = "received recent heartbeat"
            elif already_in_election:
                reason = "already participating in an election"
            else:
                reason = "unknown reason"
            logging.info(f"Node {self.node_id} rejected Pre-Vote for {candidate_id}: {reason}")
        
        return vote_granted, self.current_term

    def _conduct_pre_vote_round(self):
        """Conducts a Pre-Vote round before starting a full election."""
        # The term for which we are seeking pre-votes
        proposed_term = self.current_term + 1
        logging.info(f"Node {self.node_id} starting Pre-Vote round for proposed term {proposed_term}.")

        my_last_log_term = self.log[-1]['term'] if self.log else 0
        my_last_log_index = len(self.log) - 1 if self.log else -1

        pre_vote_granted_count = 0 # Counts positive pre-votes from *other* nodes
        
        if not self.peers: # Single node cluster
            logging.info(f"Node {self.node_id} is a single-node cluster. Proceeding directly to election for term {proposed_term}.")
            self._start_actual_election(proposed_term)
            return

        total_nodes = len(self.peers) + 1
        # To win an election, a candidate needs (total_nodes // 2) + 1 votes.
        # In Pre-Vote, the candidate implicitly 'agrees' with itself if its log is satisfactory.
        # So, it needs (total_nodes // 2) positive pre-votes from *others*.
        required_pre_votes_from_others = total_nodes // 2 

        for peer_id in self.peers:
            if peer_id == self.node_id: continue # Do not send PreVote to self
            try:
                logging.debug(f"Node {self.node_id} sending PreVote to {peer_id} for term {proposed_term}")
                response = self.peer_clients[peer_id].pre_vote(
                    candidate_id=self.node_id,
                    term=proposed_term, # Term for which we are seeking pre-vote
                    last_log_index=my_last_log_index,
                    last_log_term=my_last_log_term,
                    timeout=0.4 # Timeout for pre-vote RPC, e.g., 400ms
                )
                if response:
                    vote_granted, peer_term_response = response
                    if vote_granted:
                        pre_vote_granted_count += 1
                        logging.info(f"Node {self.node_id} received affirmative PreVote from {peer_id} for term {proposed_term}. Granted by others: {pre_vote_granted_count}/{required_pre_votes_from_others}.")
                    else:
                        logging.info(f"Node {self.node_id} received negative PreVote from {peer_id} for term {proposed_term} (peer's term: {peer_term_response}).")
                    
                    # Important: Check term from peer's response, even if pre-vote denied.
                    # Standard Raft dictates stepping down if a higher term is discovered via an RPC *request* or *response*.
                    # While PreVote itself doesn't make us step down, knowledge of a higher term is critical.
                    # If peer_term_response > self.current_term, an actual RPC (like AE or RV) from that peer would make us step down.
                    # For now, we just log it. The actual step_down will occur if a RequestVote or AppendEntries with that higher term arrives.
                    if peer_term_response > self.current_term:
                         logging.warning(f"Node {self.node_id} detected higher term {peer_term_response} from {peer_id} during PreVote response. Current term {self.current_term}. Will step down if an RPC with this term arrives.")
                else:
                    logging.warning(f"Node {self.node_id} received no PreVote response from {peer_id} for term {proposed_term}.")
            except Exception as e:
                logging.error(f"Node {self.node_id} error sending PreVote to {peer_id}: {e}", exc_info=False)
        
        logging.info(f"Node {self.node_id} Pre-Vote round for term {proposed_term} ended. Granted by others: {pre_vote_granted_count}, Required from others: {required_pre_votes_from_others} (Total nodes: {total_nodes})")

        if pre_vote_granted_count >= required_pre_votes_from_others:
            logging.info(f"Node {self.node_id} received sufficient PreVotes ({pre_vote_granted_count} >= {required_pre_votes_from_others}). Proceeding to actual election for term {proposed_term}.")
            self._start_actual_election(proposed_term) # Proceed with the term we pre-voted for
        else:
            logging.info(f"Node {self.node_id} did not receive sufficient PreVotes for term {proposed_term} ({pre_vote_granted_count} < {required_pre_votes_from_others}). Remaining FOLLOWER. Resetting election timer.")
            self.start_election_timer() # Reset timer and wait for next timeout
            
    def _start_actual_election(self, election_term: int):
        """Starts a full election. Called after a successful Pre-Vote round or directly in single-node mode. Uses the passed election_term."""
        # Transition to CANDIDATE state
        self.state = 'CANDIDATE'
        self.current_term = election_term # Set term for the new election
        self.voted_for = self.node_id    # Vote for self
        self._ensure_persistence_for_term_update() # Persist current_term and voted_for

        # Update etcd for candidate state
        if self.etcd_client:
            try:
                logger_raft.info(f"Node {self.node_id} updating role to CANDIDATE in etcd.")
                self.etcd_client.update_role(CANDIDATE)
            except Exception as e:
                logger_raft.error(f"Node {self.node_id} (CANDIDATE) failed to update etcd: {e}", exc_info=True)
        logging.info(f"Node {self.node_id} became CANDIDATE for term {self.current_term}. Voted for self, state persisted.")
        
        # TODO: Persist state (currentTerm, votedFor) before sending RPCs
        # self.persist_state() # Example: self.persistent_state.save(term=self.current_term, voted_for=self.voted_for) # Original TODO, now addressed by _ensure_persistence_for_term_update

        self.start_election_timer() # Reset election timer as a candidate

        my_last_log_term = self.log[-1]['term'] if self.log else 0
        my_last_log_index = len(self.log) - 1 if self.log else -1

        votes_obtained = 1 # Already voted for self
        total_nodes = len(self.peers) + 1
        required_for_majority = (total_nodes // 2) + 1
        
        logging.info(f"Node {self.node_id} (CANDIDATE, term {self.current_term}) sending RequestVote RPCs. Need {required_for_majority} votes from {total_nodes} total nodes.")

        for peer_id in self.peers:
            if peer_id == self.node_id: continue # Don't send RequestVote to self
            
            # Crucial check: If state changed (e.g., stepped down due to higher term from another RPC) or term changed, abort this election. 
            if self.state != 'CANDIDATE' or self.current_term != election_term:
                logging.info(f"Node {self.node_id} election for term {election_term} aborted. Current state: {self.state}, term: {self.current_term} (expected CANDIDATE, {election_term}).")
                return

            try:
                logging.debug(f"Node {self.node_id} sending RequestVote to {peer_id} for term {self.current_term}")
                response = self.peer_clients[peer_id].request_vote(
                    candidate_id=self.node_id,
                    term=self.current_term, # Send our current (now incremented) term
                    last_log_index=my_last_log_index,
                    last_log_term=my_last_log_term
                    # Timeout for RequestVote is handled by RaftRPCClient's general timeout
                )
                if response:
                    vote_granted, peer_term_response = response
                    # Check again if we are still candidate for the *same term* before processing vote
                    if self.state != 'CANDIDATE' or self.current_term != election_term:
                        logging.info(f"Node {self.node_id} election for term {election_term} aborted after receiving vote response. State/term changed. Current: {self.state}, {self.current_term}.")
                        return
                        
                    if vote_granted:
                        votes_obtained += 1
                        logging.info(f"Node {self.node_id} received vote from {peer_id} for term {self.current_term}. Votes: {votes_obtained}/{required_for_majority}")
                    else:
                        logging.info(f"Node {self.node_id} did not receive vote from {peer_id} for term {self.current_term} (peer's term: {peer_term_response}).")
                        if peer_term_response > self.current_term:
                            logging.info(f"Node {self.node_id} (CANDIDATE) detected higher term {peer_term_response} from {peer_id} during election. Stepping down.")
                            self.step_down(peer_term_response) # This will change state and reset timer
                            return # Abort election
                else:
                    logging.warning(f"Node {self.node_id} received no RequestVote response from {peer_id} for term {self.current_term}.")
            except Exception as e:
                logging.error(f"Node {self.node_id} error sending RequestVote to {peer_id}: {e}", exc_info=False)
            
            # Check if we've won or should stop early (e.g., stepped down or won majority)
            if self.state != 'CANDIDATE' or self.current_term != election_term:
                logging.info(f"Node {self.node_id} election for term {election_term} aborted after RPC to {peer_id}. Current state: {self.state}, term: {self.current_term}.")
                return
            if votes_obtained >= required_for_majority:
                break # Won election, no need to ask more peers

        # After iterating all peers or breaking early if majority achieved
        # Final check: ensure still candidate for this election_term before declaring victory
        if self.state == 'CANDIDATE' and self.current_term == election_term: 
            if votes_obtained >= required_for_majority:
                logging.info(f"Node {self.node_id} ELECTED LEADER for term {self.current_term} with {votes_obtained} votes!")
                self.become_leader()
            else:
                logging.info(f"Node {self.node_id} did not win election for term {self.current_term}. Received {votes_obtained}/{required_for_majority} votes. Remaining CANDIDATE.")
                # Election timer was already reset at the start of _start_actual_election. 
                # It will eventually re-trigger, or we'll step down if a leader emerges/higher term seen.

    def _ensure_persistence_for_term_update(self):
        """
        Placeholder for ensuring current_term and voted_for are persisted.
        IMPORTANT: This is a critical step for Raft's correctness.
        In a real implementation, this method would write to stable storage
        BEFORE responding to RPCs that change these values.
        """
        # Get the current state as a dictionary
        state_dict = self.get_raft_core_state_dict()
        
        # In api_server.py, there's a periodic task that saves this state
        # We just need to ensure the in-memory state is correct
        # No need to modify self.persistent_state as it doesn't exist as an instance attribute
        
        logging.debug(f"Node {self.node_id} (simulated) persisting state: term={self.current_term}, voted_for={self.voted_for}")
        
        # The actual persistence will be handled by periodic_save_raft_state in api_server.py
    # Actual snapshotting to disk happens separately via save_snapshot() or similar
    
    def get_raft_core_state_dict(self) -> Dict[str, Any]:
        """Returns a dictionary of the core Raft state for persistence."""
        return {
            'current_term': self.current_term,
            'voted_for': self.voted_for,
            'log': self.log,
            'commit_index': self.commit_index,
            'last_applied': self.last_applied
        }

    def request_vote(self, candidate_id: str, term: int, last_log_index: int, last_log_term: int) -> Tuple[bool, int]:
        """
        RequestVote RPC implementation. Includes leader stickiness.
        Returns (vote_granted, current_term)
        """
        try:
            candidate_term = int(term)
        except ValueError:
            logging.error(f"Node {self.node_id} received non-integer term '{term}' in request_vote from {candidate_id}. Rejecting vote.")
            return (False, int(self.current_term))

        local_current_term = int(self.current_term)
        vote_granted = False  # Default to not granting vote

        if candidate_term < local_current_term:
            logging.debug(f"Node {self.node_id} (term {local_current_term}) rejecting vote for {candidate_id} (term {candidate_term}): candidate term too low.")
            return (False, local_current_term)

        if candidate_term > local_current_term:
            logging.info(f"Node {self.node_id} (term {local_current_term}) received vote request from {candidate_id} with higher term {candidate_term}. Stepping down.")
            # Persistence call is inside step_down now, which updates term and voted_for
            self.step_down(candidate_term)
            local_current_term = self.current_term  # Update local_current_term after step_down

        # At this point, candidate_term is >= local_current_term.
        # If candidate_term was > local_current_term, we've now updated local_current_term and reset voted_for via step_down.

        # CRITICAL SAFETY CHECK: Prevent a leader from voting for another candidate in the same term
        # This prevents the split-brain scenario where multiple leaders exist in the same term
        if self.state == 'LEADER' and candidate_term == local_current_term:
            logging.warning(f"Node {self.node_id} is already LEADER for term {local_current_term}. "
                           f"Rejecting vote request from {candidate_id} in same term to prevent multiple leaders.")
            return (False, local_current_term)

        # Leader Stickiness Check:
        # If we are a FOLLOWER, know the current leader, and our election timer was reset recently
        # (implying we've heard from the leader), don't vote for another candidate in the same term.
        # This check is only relevant if candidate_term == local_current_term.
        if candidate_term == local_current_term and \
           self.state == 'FOLLOWER' and \
           self.leader_id is not None and \
           hasattr(self, 'election_timer_start_time') and \
           self.election_timer_start_time > 0:  # Ensure timer has been initialized and started

            time_since_timer_reset = time.time() - self.election_timer_start_time
            # self.election_timeout_base is the minimum duration a follower expects a leader to be active without issues.
            # If our timer was reset more recently than this, it implies the leader is likely still active.
            if time_since_timer_reset < self.min_election_timeout_sec: # Using min_election_timeout_sec for stickiness duration
                logging.info(f"Node {self.node_id} (term {local_current_term}, current leader: {self.leader_id}, state: {self.state}) "
                             f"rejecting vote for {candidate_id} (term {candidate_term}) due to leader stickiness. "
                             f"Election timer started {time_since_timer_reset:.3f}s ago (stickiness threshold: {self.min_election_timeout_sec:.3f}s).")
                return (False, local_current_term)

        # Standard Raft voting conditions:
        # 1. Grant vote if votedFor is null or candidateId.
        # 2. Candidate's log must be at least as up-to-date as receiver's log.
        if self.voted_for is None or self.voted_for == candidate_id:
            my_last_log_term = self.log[-1]['term'] if self.log else 0
            my_last_log_index = len(self.log) - 1 if self.log else -1  # index of last entry; -1 if log is empty

            candidate_log_is_newer_or_same_term_longer = (
                last_log_term > my_last_log_term or
                (last_log_term == my_last_log_term and last_log_index >= my_last_log_index)
            )

            if candidate_log_is_newer_or_same_term_longer:
                if self.voted_for is None:  # Only log "voting for" if we weren't already voting for them in this term
                    logging.info(f"Node {self.node_id} (term {local_current_term}, state: {self.state}) voting for {candidate_id} (term {candidate_term}). "
                                 f"My log: (idx={my_last_log_index}, term={my_last_log_term}). Cand log: (idx={last_log_index}, term={last_log_term}).")
                self.voted_for = candidate_id
                self._ensure_persistence_for_term_update()  # Persist vote
                self.start_election_timer()  # Reset election timer upon granting vote
                vote_granted = True
            else:
                logging.info(f"Node {self.node_id} (term {local_current_term}, state: {self.state}) rejecting vote for {candidate_id} (term {candidate_term}): candidate log not up-to-date. "
                             f"My log: (idx={my_last_log_index}, term={my_last_log_term}). Cand log: (idx={last_log_index}, term={last_log_term}).")
        else:
            # This case means self.voted_for is not None and self.voted_for != candidate_id
            logging.info(f"Node {self.node_id} (term {local_current_term}, state: {self.state}) rejecting vote for {candidate_id} (term {candidate_term}): already voted for {self.voted_for} in this term.")

        return (vote_granted, int(local_current_term))
        
    def append_entries(self, leader_id: str, term: int, prev_log_index: int, 
                      prev_log_term: int, entries: list, leader_commit: int) -> Tuple[bool, int]:
        """
        AppendEntries RPC implementation
        Returns (success, current_term)
        """
        try:
            # Ensure term is an integer for comparison
            leader_term = int(term)
            # Ensure our own current_term is an integer
            local_current_term = int(self.current_term)
            # Ensure prev_log_index and leader_commit are integers
            prev_log_index = int(prev_log_index)
            leader_commit = int(leader_commit)
        except (ValueError, TypeError) as e:
            logging.error(f"Type conversion error in append_entries: {e}, got term={term}, prev_log_index={prev_log_index}, leader_commit={leader_commit}")
            return False, int(self.current_term)
        
        if leader_term < local_current_term:
            logging.debug(f"Node {self.node_id}: Rejecting append_entries, leader term {leader_term} < current term {local_current_term}")
            return False, local_current_term
        
        # If new term is higher, update our term and step down
        if leader_term > local_current_term:
            logging.info(f"Node {self.node_id} received higher term {leader_term} from leader {leader_id} (current term: {local_current_term})")
            self.step_down(leader_term)
            local_current_term = self.current_term # Update local_current_term after step_down
        
        # Accept leader and reset timer if in same term
        if leader_term >= local_current_term:
            if self.leader_id != leader_id:
                logging.info(f"Node {self.node_id} recognizing {leader_id} as leader for term {leader_term}")
            self.leader_id = leader_id
            self.last_heartbeat = time.time() # Record time of hearing from leader
            self.last_valid_leader_heartbeat_time = time.time() # Update for leader stickiness
            # Reset election timeout (we heard from leader)
            self.start_election_timer()
        else:
            # If the leader's term is lower, reject their append entries
            # This can happen in split-brain scenarios
            logging.warning(f"Node {self.node_id} rejecting append_entries from {leader_id} with term {leader_term} (current term: {local_current_term})")
            return False, local_current_term
            
        # Check log consistency
        # Raft paper Figure 2 rules: If log doesn't contain an entry at prevLogIndex
        # whose term matches prevLogTerm, then the receiver must return false.
        # If an existing entry conflicts with a new one (same index, different terms),
        # delete the existing entry and all that follow it.
        if prev_log_index > 0:
            if prev_log_index >= len(self.log):
                logging.debug(f"Node {self.node_id}: AppendEntries: prev_log_index ({prev_log_index}) out of bounds (log_len {len(self.log)})")
                return False, int(self.current_term)
            if self.log[prev_log_index]['term'] != prev_log_term:
                logging.debug(f"Node {self.node_id}: AppendEntries: term mismatch at prev_log_index ({prev_log_index}). Expected term {prev_log_term}, got {self.log[prev_log_index]['term']}")
                # Truncate log at the point of inconsistency
                self.log = self.log[:prev_log_index] 
                return False, int(self.current_term)
                
        # If log consistency check passes, append new entries
        # This covers appending new entries or replacing conflicting entries
        if entries:
            # Find the index where new entries should start being appended.
            # This is `prev_log_index + 1`.
            append_start_index = prev_log_index + 1
            
            # If existing log entries conflict with new entries, truncate.
            # Conflict exists if the entry at append_start_index has a different term than the first new entry.
            # We only need to check this if append_start_index is within our current log bounds
            # and there are new entries to append.
            if append_start_index < len(self.log):
                # Find the point where the existing log matches the new entries, or truncate before conflict
                i = 0
                while i < len(entries) and (append_start_index + i) < len(self.log):
                    if self.log[append_start_index + i]['term'] != entries[i]['term']:
                        # Conflict found, truncate log from this point
                        self.log = self.log[:append_start_index + i]
                        logging.debug(f"Node {self.node_id}: AppendEntries: Truncating log at index {append_start_index + i} due to term conflict.")
                        break # Truncation point found, exit inner loop
                    i += 1
                # After checking/truncating, append the remaining new entries
                # We append entries starting from the index 'i' where the match stopped or conflict was found
                self.log.extend(entries[i:])
                logging.debug(f"Node {self.node_id}: Appended {len(entries[i:])} entries starting from index {append_start_index + i}. New log length: {len(self.log)}")
            else:
                 # If append_start_index is beyond our current log end, simply append all new entries.
                 # This handles cases where we are missing entries.
                 self.log.extend(entries)
                 logging.debug(f"Node {self.node_id}: Appended {len(entries)} entries. New log length: {len(self.log)}")

        # Update commit index
        # Rule: If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry).
        if leader_commit > self.commit_index:
            # Index of the last new entry is len(self.log) - 1
            self.commit_index = min(leader_commit, len(self.log) - 1)
            logging.debug(f"Node {self.node_id}: Updated commit index to {self.commit_index}")
            # Apply committed entries to state machine
            self.apply_entries()

        return True, int(self.current_term)

    def take_snapshot(self):
        logger_raft.info(f"Node {self.node_id}: Attempting to take snapshot. Last applied index: {self.last_applied}")
        
        # Cannot snapshot if nothing has been applied
        if self.last_applied < 0:
            logger_raft.warning(f"Node {self.node_id}: Cannot take snapshot, last_applied is {self.last_applied}. Log length: {len(self.log)}")
            return

        # Determine the last index and term to include in the snapshot.
        # These correspond to the entry *at* the self.last_applied index in the *full conceptual* log.
        # Since self.log might be truncated, we need to be careful here.
        # In a system with real log persistence and truncation, you'd need to retrieve the term
        # from the entry at the absolute log index `self.last_applied`.
        # Assuming for now that `self.log` is relative to the *start* of the log (index 0 is the first entry).
        # A correct implementation would store absolute log indices in the log entries themselves.
        
        # --- Simplified Snapshot Term/Index Calculation (Needs Refinement for Real Truncation) ---
        # If self.log has not been truncated yet, self.log[self.last_applied] works.
        # If log has been truncated, the required term is from an entry that is no longer in self.log.
        # A robust implementation stores the last_included_index and last_included_term from the *previous* snapshot.
        # The *current* last_included_index is `self.last_applied`.
        # The *current* last_included_term is the term of the entry at `self.last_applied`.
        # If `self.last_applied` is an index in the current `self.log`, we can get the term.
        # If `self.last_applied` is from a previous snapshot's last_included_index, its term would be stored separately.
        
        # For this simplified model, assume `self.log` always contains entries starting from index 0
        # *relative to the point after the previous snapshot*.
        # The absolute index `self.last_applied` is the index *in the total log history*.
        # We need the term of the entry at absolute index `self.last_applied`.
        # Let's assume `self.log` stores entries *after* the previous snapshot's `last_included_index`.
        # The index of `self.last_applied` relative to the start of the current `self.log` is `self.last_applied - previous_snapshot_last_included_index`.
        # This requires knowing `previous_snapshot_last_included_index`.

        # --- Corrected approach based on Raft spec ---
        # The log entry *at* `self.last_applied` is the last one applied.
        # We need its term. If this entry is still in our current `self.log` array, its index in `self.log` is `self.last_applied - base_log_index`, where `base_log_index` is the index of the first entry in the current `self.log` array (which is the index *after* the previous snapshot's last_included_index, or 0 if no previous snapshot).
        # A more correct approach is to store the *absolute* log index within each log entry in `self.log`.
        # E.g., `{'index': 100, 'term': 5, 'command': {...}}`.
        # Assuming `self.log` entries *do* contain their absolute index and term:

        last_included_index = self.last_applied
        last_included_term = 0 # Default if last_applied is before the first entry of the current log segment

        # Find the term of the entry at self.last_applied within the current log segment
        # Iterate backwards from commit_index (or end of log) to find the entry at last_applied
        found_entry = None
        # Search from the end of the current log segment backwards
        current_log_base_index = 0 # This needs to be tracked if log is truncated relative to absolute 0
                                   # For now, assuming self.log[i] corresponds to absolute index i.
        if self.last_applied >= current_log_base_index and (self.last_applied - current_log_base_index) < len(self.log):
             # The entry at last_applied is within the current log segment
            found_entry = self.log[self.last_applied - current_log_base_index]
            if 'term' in found_entry:
                 last_included_term = found_entry['term']
            else:
                 logger_raft.error(f"Node {self.node_id}: Log entry at index {self.last_applied} is missing 'term'. Cannot take accurate snapshot.")
                 # Decide how to handle: abort snapshot, use a default term (like 0), etc.
                 # Aborting is safer for correctness.
                 return
        else:
            # This case should ideally not happen if last_applied is less than commit_index and we just applied it.
            # It would happen if last_applied index is *before* the current log segment's base index.
            # In that scenario, the term should be read from the previous snapshot's metadata.
            # For this simplified model, let's assume last_applied is always an index into the current log `self.log`.
            # If log truncation is implemented properly, this section needs revision.
            logger_raft.error(f"Node {self.node_id}: last_applied index {self.last_applied} is not found in current log segment (length {len(self.log)}, base index {current_log_base_index}). Cannot determine last_included_term for snapshot.")
            return # Abort snapshot if we can't determine the term

        # --- End Corrected Approach ---


        state_machine_data = None
        if self.sequencer_instance:
            try:
                state_machine_data = self.sequencer_instance.get_state()
            except Exception as e:
                logger_raft.error(f"Node {self.node_id}: Error getting state from sequencer_instance: {e}", exc_info=True)
                # Decide if we should proceed with a snapshot that doesn't have state machine data
                # For now, we'll proceed but log the error.
                state_machine_data = {"error": "Failed to get state machine state"}
        else:
            logger_raft.warning(f"Node {self.node_id}: No sequencer_instance, state_machine_state will be null in snapshot.")

        snapshot_data = {
            'last_included_index': last_included_index,
            'last_included_term': last_included_term,
            'state_machine_state': state_machine_data
        }

        try:
            # Ensure the snapshot directory exists
            import os
            snapshot_dir = os.path.dirname(self.snapshot_path)
            if snapshot_dir and not os.exists(snapshot_dir):
                os.makedirs(snapshot_dir)
                logger_raft.info(f"Created snapshot directory: {snapshot_dir}")

            with open(self.snapshot_path, 'w') as f:
                json.dump(snapshot_data, f)
            logger_raft.info(f"Node {self.node_id}: Snapshot saved to {self.snapshot_path} up to index {last_included_index}, term {last_included_term}.")
        except IOError as e:
            logger_raft.error(f"Node {self.node_id}: IO Error saving snapshot to {self.snapshot_path}: {e}", exc_info=True)
            return # Failed to save snapshot, do not truncate log

        # Truncate log: discard entries up to and including last_included_index.
        # The new log starts from the entry *after* last_included_index.
        # This requires knowing the base index of the current `self.log`.
        # If self.log[i] is entry with absolute index i, then we just slice.
        # If self.log[i] is entry with absolute index `base_log_index + i`, it's more complex.

        # Assuming `self.log` entries have absolute 'index' fields for now:
        # Find the index in the *current* `self.log` array that corresponds to `last_included_index`.
        index_in_current_log = -1
        # Search for the entry with 'index' matching last_included_index
        # This search is inefficient; a real implementation would manage log segments or have faster lookup.
        # For simplicity, assume current_log starts at index 0 and corresponds to absolute indices.
        
        # --- Simplified Truncation (Needs Refinement for Real Truncation) ---
        # If `self.log` is *not* relative to a base index (i.e., `self.log[i]` is entry `i`),
        # then entries up to `last_included_index` (inclusive) should be removed.
        # The new log starts from `last_included_index + 1`.
        
        # This simplified truncation assumes self.log[i] corresponds to log entry at index `i`.
        # This is ONLY safe if log is never truncated.
        # A correct implementation needs to track the index of the first entry in self.log.
        
        # Correct truncation based on the base index of the current log segment:
        # Find the index in the current `self.log` array that corresponds to the absolute index `last_included_index`.
        # This index is `last_included_index - self.current_log_base_index`.
        # The entries to keep start at `last_included_index - self.current_log_base_index + 1`.
        
        # Let's assume `self.log` entries DO have an 'index' field and use that for correct truncation.
        # If self.log is empty or the last applied index is less than the index of the first entry in self.log, there's nothing to truncate.
        if self.log and self.last_applied >= self.log[0]['index']:
             # Find the index in self.log *after* the entry with index `self.last_applied`
             # This search is simplified; ideally, we'd know the base index.
             # Let's assume self.log entries have an 'index' field and they are sorted by index.
             truncation_point = -1
             for i, entry in enumerate(self.log):
                 if entry['index'] == self.last_applied:
                     truncation_point = i + 1 # Keep entries from this index onwards in the list
                     break
             
             if truncation_point != -1:
                 self.log = self.log[truncation_point:]
                 # Update the base index of the current log segment
                 self.current_log_base_index = self.last_applied + 1
                 logger_raft.info(f"Node {self.node_id}: Log truncated after index {self.last_applied}. New log length: {len(self.log)}, new base index: {self.current_log_base_index}.")
             else:
                 logger_raft.warning(f"Node {self.node_id}: Could not find log entry with index {self.last_applied} in current log segment. Truncation skipped.")
        else:
             logger_raft.info(f"Node {self.node_id}: Log not truncated (empty log or last_applied < first log entry index).")
        
        # --- End Corrected Truncation ---

        # Important: The persistent state should also be updated to reflect the new log start (last_included_index, last_included_term)
        # This is typically done separately via the persistence layer.
        # For this in-memory simulation, we'd update the persistent_state dict.
        self.persistent_state['last_applied'] = self.last_applied # This is already kept up-to-date
        self.persistent_state['commit_index'] = max(self.commit_index, self.last_applied) # Ensure commit index isn't less than last applied
        # The log itself is now truncated in memory; a real system would persist this truncated log state.
        
        # A real system would save the fact that log entries up to last_included_index are replaced by the snapshot.
        # This involves updating metadata about the log's start point.
        self.last_included_index = last_included_index
        self.last_included_term = last_included_term
        
        # The commit_index should also be persisted after any change.
        self.persistent_state['commit_index'] = self.commit_index
        # self._ensure_persistence_for_term_update() is for term/vote, need a general persistence save.
        # self.persist_state() # A separate method for saving full persistent state

    def load_snapshot(self):
        """Load the latest snapshot if available.
        This method is called during initialization. It loads Raft metadata (last_applied, commit_index, term)
        and stores the state machine's serialized state in self._pending_state_machine_state.
        The actual application of this state to the sequencer_instance happens in __init__ after this call.
        """
        try:
            with open(self.snapshot_path, 'r') as f:
                snapshot_data = json.load(f)
            
            # It's crucial that last_applied from snapshot is respected.
            # commit_index should also be at least last_applied.
            # The log itself is effectively replaced by the snapshot up to last_included_index.
            self.last_applied = snapshot_data['last_included_index']
            self.commit_index = max(self.commit_index, self.last_applied) 
            # The term should also be updated if the snapshot's term is higher, 
            # though typically current_term is loaded from persistent Raft state before snapshot loading.
            self.current_term = max(self.current_term, snapshot_data.get('last_included_term', self.current_term))
            
            # Store the last included index and term from the loaded snapshot.
            # These represent the starting point of the log *after* the snapshot.
            self.last_included_index = snapshot_data['last_included_index']
            self.last_included_term = snapshot_data['last_included_term']
            # The base index of the current log segment is the index immediately after the last included index.
            self.current_log_base_index = self.last_included_index + 1

            self._pending_state_machine_state = snapshot_data.get('state_machine_state')
            
            # The log entries covered by the snapshot are no longer needed in memory here.
            # The actual log entries corresponding to indices > last_included_index would be received via AppendEntries.
            # For a newly starting node, self.log would be empty or from its own persistent store, 
            # then InstallSnapshot would replace it.
            # If this is just loading its own most recent snapshot at init, the log should be cleared 
            # as these entries are now represented by the snapshot and its last_applied index.
            self.log = [] 
            logger_raft.info(f"Node {self.node_id}: Loaded snapshot. last_applied set to {self.last_applied}, term to {self.current_term}. Log cleared. Pending state machine state captured.")
        except FileNotFoundError:
            logger_raft.info(f"Node {self.node_id}: No snapshot found at {self.snapshot_path}, starting with empty/current state.")
            self._pending_state_machine_state = None
            # Initialize log base index correctly if no snapshot was loaded
            self.last_included_index = -1
            self.last_included_term = 0
            self.current_log_base_index = 0 # Log starts from index 0 if no snapshot
        except (IOError, json.JSONDecodeError, KeyError) as e:
            logger_raft.error(f"Node {self.node_id}: Error loading or parsing snapshot from {self.snapshot_path}: {e}", exc_info=True)
            self._pending_state_machine_state = None
            # Initialize log base index correctly if snapshot loading failed
            self.last_included_index = -1
            self.last_included_term = 0
            self.current_log_base_index = 0

    def become_leader(self):
        """Transition to leader state and initialize leader-specific data structures"""
        # Add safety checks before becoming leader
        if self.state != 'CANDIDATE':
            logging.warning(f"Node {self.node_id} attempted to become leader while in {self.state} state (not CANDIDATE). Aborting leadership transition.")
            return False
        
        # Check if we know of a leader with a higher or equal term
        if self.leader_id is not None and self.leader_id != self.node_id:
            last_leader_contact = time.time() - self.last_valid_leader_heartbeat_time
            if last_leader_contact < self.election_timeout:
                logging.warning(f"Node {self.node_id} attempted to become leader but detected a recent leader {self.leader_id} "
                              f"({last_leader_contact:.2f}s ago, timeout: {self.election_timeout:.2f}s). Aborting leadership transition.")
                return False
        
        # Log the leadership transition
        prev_state = self.state
        logging.info(f"Node {self.node_id} becoming leader for term {self.current_term} (previous state: {prev_state})")
        
        # Get current leadership status of all known nodes (for logging/debugging)
        leader_status = {}
        for peer_id in self.peers:
            if peer_id == self.node_id: continue
            try:
                # Use a short timeout here to avoid blocking the transition
                if hasattr(self.peer_clients[peer_id], 'is_connected'):
                    health_check = self.peer_clients[peer_id].is_connected(timeout=0.2)
                    if health_check:
                        leader_status[peer_id] = "Connected"
                    else:
                        leader_status[peer_id] = "Not connected"
                else:
                    leader_status[peer_id] = "Connection status unknown"
            except Exception as e:
                leader_status[peer_id] = f"Error checking: {str(e)}"
        
        logging.info(f"Node {self.node_id} peer status before becoming leader: {leader_status}")
        
        # Update state
        self.state = 'LEADER'
        self.leader_id = self.node_id
        self.voted_for = None  # Clear voted_for as we're now leader
        self._ensure_persistence_for_term_update() # Persist state change

        # Update etcd for leader state
        if self.etcd_client:
            try:
                logger_raft.info(f"Node {self.node_id} updating role to LEADER in etcd.")
                self.etcd_client.update_role(LEADER)
                logger_raft.info(f"Node {self.node_id} publishing leader info to etcd: API='{self.node_api_address}', RPC='{self.node_rpc_address_for_etcd}'.")
                self.etcd_client.publish_leader_info(
                leader_api_address=self.node_api_address,
                ttl=ETCD_LEASE_TTL * 2 # Leader info can have a longer TTL
            )
            except Exception as e:
                logger_raft.error(f"Node {self.node_id} (LEADER) failed to update etcd: {e}", exc_info=True)

        # Initialize nextIndex and matchIndex for all peers
        # nextIndex: index of next log entry to send to that server
        # matchIndex: index of highest log entry known to be replicated on server
        self.next_index = {}
        self.match_index = {}
        
        # Initialize for all peers except self
        # nextIndex should be initialized to the leader's log length (absolute index of the next entry)
        # The absolute index of the next entry after the last one is the base index + length of current log segment
        leader_next_log_index = self.current_log_base_index + len(self.log)

        for peer in self.peers:
            if peer != self.node_id:  # Skip self
                self.next_index[peer] = leader_next_log_index
                # matchIndex is initially 0 (or last_included_index from snapshot if any)
                self.match_index[peer] = self.last_included_index if hasattr(self, 'last_included_index') else -1
        
        # Handle single-node case
        is_single_node = not self.peers # Peers are other nodes; an empty peers list means single node
        is_configured_single_node = app_config.is_single_node_mode() # Check config explicitly

        # Correct single node check: cluster size is 1.
        total_nodes = len(self.peers) + 1 # Add self to peers list for total count
        self.single_node_mode = (total_nodes == 1) or is_configured_single_node # Use a flag

        if self.single_node_mode:
            logging.info(f"Single-node mode detected in become_leader (configured={is_configured_single_node})")
            # In single node, we can immediately commit entries as they're written
            # Any pending uncommitted entries are committed.
            if (self.current_log_base_index + len(self.log) - 1) > self.commit_index:
                 self.commit_index = self.current_log_base_index + len(self.log) - 1
                 self.apply_entries() # Apply any newly committed entries

        # Reset election timeout (leader doesn't need this, but reset anyway)
        self.last_heartbeat = time.time()
        
        # Append a no-op entry to the log (recommended in Raft paper)
        # This ensures that previously uncommitted entries from previous terms get committed
        # once the no-op is replicated and committed in the new leader's term.
        # The no-op entry needs to be added to the log with the current term.
        no_op_entry_data = {'type': 'no-op', 'data': None}
        
        # Append entry method needs to handle adding term and getting absolute index.
        # Let's modify append_entry to do this.
        # The index of this new entry will be `leader_next_log_index`.
        no_op_absolute_index = leader_next_log_index # The index where the no-op will be placed
        
        # Add the no-op to the log (in-memory). It will get its term from `self.current_term`.
        no_op_log_entry = {
             'index': no_op_absolute_index, # Store absolute index
             'term': self.current_term,
             'entry': no_op_entry_data
        }
        self.log.append(no_op_log_entry)
        logging.info(f"Leader {self.node_id} appended no-op entry at index {no_op_absolute_index} for term {self.current_term}.")
        
        # Update leader_next_log_index for future appends
        leader_next_log_index += 1

        # Send initial heartbeats immediately to establish leadership
        # These heartbeats will include the newly added no-op entry.
        self.send_append_entries_to_peers() # Renamed from send_heartbeats for clarity, as it sends entries too
        
        # Start periodic heartbeats/replication
        self._start_periodic_append_entries() # Renamed from _start_periodic_heartbeats

    def send_append_entries_to_peers(self):
        """Sends AppendEntries RPCs (including heartbeats or actual entries) to all peers."""
        if self.state != 'LEADER':
            # This shouldn't happen if called correctly, but check defensively
            logging.warning(f"Node {self.node_id} tried to send AppendEntries but is not a leader (state: {self.state})")
            return
            
        # Skip sending RPCs in single-node mode, but still apply entries
        # Single-node application is handled in become_leader and append_entry
        if self.single_node_mode:
             # logging.debug(f"Node {self.node_id} in single-node mode, skipping sending AppendEntries.")
             # Ensure application happens if needed, although become_leader and append_entry should cover it.
             # self.apply_entries() # Maybe redundant here, but safe
             return

        # logging.debug(f"Node {self.node_id} (LEADER, term {self.current_term}, commit {self.commit_index}) sending AppendEntries to {len(self.peers)} peers")

        # Track successful acks within this cycle
        successful_acks_count = 0

        # Iterate through each peer and send AppendEntries
        for peer_id in self.peers:
            if peer_id == self.node_id:
                continue  # Skip self

            # Skip if we don't have a client for this peer
            if peer_id not in self.peer_clients or self.peer_clients[peer_id] is None:
                logging.error(f"Node {self.node_id}: No RPC client available for peer {peer_id}. Cannot send AppendEntries.")
                continue

            try:
                # Determine which entries to send to this peer
                # nextIndex[peerId] is the *absolute* index of the next log entry to send
                next_idx = self.next_index.get(peer_id, self.current_log_base_index + len(self.log)) # Default to sending all if unknown
                
                # If next_idx is <= the last_included_index of the last snapshot,
                # the follower needs the snapshot, not AppendEntries.
                # This requires implementing InstallSnapshot RPC.
                # For this implementation, assume next_idx is always >= self.current_log_base_index.
                # A real Raft would send InstallSnapshot here.
                if next_idx < self.current_log_base_index:
                    logging.warning(f"Node {self.node_id}: Peer {peer_id} needs InstallSnapshot (next_idx {next_idx} < current_log_base_index {self.current_log_base_index}). InstallSnapshot not implemented, cannot replicate.")
                    # TODO: Implement InstallSnapshot and send it here
                    continue # Skip sending AppendEntries for now

                # Indices within the current log segment (self.log)
                # The first entry in self.log is at absolute index self.current_log_base_index
                # The entries to send start from the entry at absolute index `next_idx`.
                # Their index within the current self.log list is `next_idx - self.current_log_base_index`.
                
                entries_to_send = self.log[next_idx - self.current_log_base_index:]
                
                # Determine prevLogIndex and prevLogTerm
                # prevLogIndex is `next_idx - 1`.
                prev_log_index = next_idx - 1
                prev_log_term = 0 # Default term if prev_log_index is before the current log base index or is -1

                # If prev_log_index is valid within the current log segment (>= current_log_base_index)
                if prev_log_index >= self.current_log_base_index:
                    # The index of this entry in self.log list is `prev_log_index - self.current_log_base_index`
                    prev_log_entry_in_log = self.log[prev_log_index - self.current_log_base_index]
                    prev_log_term = prev_log_entry_in_log.get('term', 0) # Get term, default to 0 if missing
                elif prev_log_index == self.last_included_index:
                    # The previous entry is the last one included in the snapshot
                    prev_log_term = self.last_included_term
                # If prev_log_index is -1 (empty log before snapshot), term is 0, which is the default.

                # Send the AppendEntries RPC
                logging.debug(f"Node {self.node_id} sending AppendEntries to {peer_id} (term {self.current_term}, prevIdx {prev_log_index}, prevTerm {prev_log_term}, {len(entries_to_send)} entries)")
                response = self.peer_clients[peer_id].append_entries(
                    leader_id=self.node_id,
                    term=self.current_term,
                    prev_log_index=prev_log_index,
                    prev_log_term=prev_log_term,
                    entries=entries_to_send,
                    leader_commit=self.commit_index,
                    timeout=0.4  # 400ms timeout for AppendEntries
                )
                
                # Handle the response - can be None if request failed
                if response is None:
                    # Connection failed, try again next heartbeat
                    logging.debug(f"No response from peer {peer_id} for AppendEntries")
                    # Consider incrementing a failure counter for backoff strategies
                    continue
                
                # Handle different response formats
                success = False
                term = None
                
                if isinstance(response, tuple) and len(response) == 2:
                    # Old format (success, term)
                    success, term = response
                elif isinstance(response, dict) and 'success' in response and 'term' in response:
                    # New format - structured JSON response
                    success = bool(response['success'])
                    term = int(response['term'])
                # No simple boolean response for AppendEntries as per spec (need term)
                else:
                    # Unexpected format
                    logging.error(f"Could not parse append_entries response from {peer_id}: {response}")
                    continue # Skip this peer for this round

                # Check if term has changed (follower found a higher term)
                if term is not None and term > self.current_term:
                    logging.info(f"Node {self.node_id} (LEADER) discovered higher term {term} from {peer_id}, stepping down.")
                    self.step_down(term)
                    return # Abort sending to other peers, the leader is stepping down
                        
                if success:
                    # Update matchIndex and nextIndex on success
                    # matchIndex[peerId] is the highest index successfully replicated on peerId
                    # If entries were sent, the new matchIndex is the index of the last sent entry.
                    # If no entries were sent (heartbeat), matchIndex doesn't change,
                    # but nextIndex updates based on the prev_log_index that succeeded.
                    
                    if entries_to_send: # If actual entries were sent
                         # The index of the last sent entry is the absolute index `next_idx + len(entries_to_send) - 1`
                         new_match_index = next_idx + len(entries_to_send) - 1
                         self.match_index[peer_id] = new_match_index
                         self.next_index[peer_id] = new_match_index + 1 # Next entry to send
                         logging.debug(f"AppendEntries success to peer {peer_id}. MatchIndex updated to {self.match_index[peer_id]}, nextIndex to {self.next_index[peer_id]}.")
                    else: # Just a heartbeat (0 entries)
                         # If the AE request succeeded (meaning prevLogIndex/Term matched), 
                         # it confirms the follower has up to prev_log_index.
                         # We only need to update nextIndex to prev_log_index + 1 if it's currently lower.
                         # This ensures nextIndex doesn't get stuck too far behind if a follower recovers.
                         # matchIndex doesn't necessarily change with an empty AE, unless the prevLogIndex was higher than its matchIndex.
                         # A simple approach: if the AE succeeded (even empty), nextIndex should be at least prev_log_index + 1.
                         self.next_index[peer_id] = max(self.next_index.get(peer_id, -1), prev_log_index + 1)
                         logging.debug(f"Heartbeat success to peer {peer_id}. NextIndex ensured at least {prev_log_index + 1}.")
                         
                    successful_acks_count += 1 # Count successful acknowledgements
                    
                else:
                    # AppendEntries failed, likely due to log inconsistency.
                    # Decrement nextIndex and retry.
                    # The follower's response might indicate the correct nextIndex to try (e.g., the conflict index).
                    # For now, a simple decrement is used.
                    # The minimum valid nextIndex is the index after the last snapshot.
                    new_next_idx = max(self.current_log_base_index, self.next_index.get(peer_id, 1) - 1)
                    self.next_index[peer_id] = new_next_idx
                    logging.debug(f"AppendEntries failed for peer {peer_id}, decrementing nextIndex to {self.next_index[peer_id]}. Follower term: {term}.")
                    
            except Exception as e:
                logging.error(f"Error sending AppendEntries to peer {peer_id}: {e}", exc_info=False)
                # Potentially decrease nextIndex and apply backoff for this specific peer if needed.

        # After iterating all peers, attempt to advance commitIndex based on matchIndices
        # Only do this if the leader hasn't stepped down.
        if self.state == 'LEADER':
             self.advance_commit_index()
             
             # Check if we've lost contact with a majority and need to step down.
             # This is an important safety check for the leader.
             # A leader needs to have successfully replicated to a majority of nodes (including itself) within the heartbeat interval.
             # Alternatively, check if a majority of nodes have matchIndex >= commitIndex.
             
             # The AppendEntries response indicates if the follower is "alive" and at least processed the request.
             # A more robust check might track how many peers have successfully replicated the *current* leader's entries up to a certain point.
             # A simpler check: are we getting *any* successful responses from a majority within the heartbeat interval?
             # Let's use the number of peers we got a successful response from in this cycle.
             
             # Total nodes = len(self.peers) + 1 (leader included)
             # Required majority = (total_nodes // 2) + 1
             # The leader itself counts as one "successful replica" for entries it appends to its log.
             # So, it needs successful AppendEntries *responses* from `(total_nodes // 2)` peers.
             
             required_successful_peer_responses = (len(self.peers) + 1) // 2
             
             if successful_acks_count < required_successful_peer_responses:
                  logging.warning(f"Node {self.node_id} (Leader, term {self.current_term}) may be partitioned: only {successful_acks_count} successful AppendEntries responses from {len(self.peers)} peers. Required: {required_successful_peer_responses}.")
                  # Note: The Raft paper implies a leader steps down if it discovers a higher term.
                  # Losing majority *doesn't* immediately force a leader to step down in the standard algorithm,
                  # but it means it cannot commit new entries. If its heartbeat timer runs out and it can't get pre-votes/votes,
                  # it will eventually step down. Explicitly stepping down on detecting partition can be an optimization
                  # but needs careful consideration (e.g., preventing split-brain).
                  # Let's stick closer to the paper: step down primarily on discovering a higher term.
                  # The lack of majority responses will prevent commitIndex from advancing, which is the main safety mechanism.
                  # If the partition lasts, its election timer will eventually fire when it stops hearing from peers, leading to a new election.

    def _start_periodic_append_entries(self):
        """Starts the periodic AppendEntries sender for a leader."""
        if hasattr(self, 'append_entries_sender_thread') and self.append_entries_sender_thread and self.append_entries_sender_thread.is_alive():
            self.append_entries_sender_thread.cancel() # Cancel any existing timer

        def append_entries_task():
            if self.state == 'LEADER':
                try:
                    self.send_append_entries_to_peers()
                except Exception as e:
                    logging.error(f"Node {self.node_id} error during periodic AppendEntries task: {e}", exc_info=True)
                    # Decide error handling: step down? retry immediately?
                    # For now, just log and allow the timer to reschedule.
                    
                # Reschedule if still leader
                if self.state == 'LEADER': # Re-check state after sending, as it might step down
                    self.append_entries_sender_thread = threading.Timer(self.heartbeat_interval_sec, append_entries_task)
                    self.append_entries_sender_thread.daemon = True
                    self.append_entries_sender_thread.start()
            else:
                # This condition can be reached if the node steps down during the heartbeat_interval period
                logging.info(f"Node {self.node_id} periodic AppendEntries task running, but no longer LEADER. Stopping.")

        # Initial call to start the cycle
        if self.state == 'LEADER':
            logging.info(f"Node {self.node_id} (LEADER) starting periodic AppendEntries every {self.heartbeat_interval_sec:.3f}s.")
            # Ensure the first call to append_entries_task is scheduled
            self.append_entries_sender_thread = threading.Timer(self.heartbeat_interval_sec, append_entries_task)
            self.append_entries_sender_thread.daemon = True
            self.append_entries_sender_thread.start()
        else:
            # This should ideally not happen if _start_periodic_append_entries is only called when becoming leader
            logging.warning(f"Node {self.node_id} attempted to start periodic AppendEntries but is not LEADER (state: {self.state}).")

    def step_down(self, term):
        """Step down as leader or candidate if we discover a higher term"""
        prev_state = self.state
        prev_term = self.current_term
        
        # Only step down if the discovered term is strictly higher
        if term > self.current_term:
            self.current_term = int(term)  # Ensure term is integer
            self.state = 'FOLLOWER'
            self.voted_for = None
            self._ensure_persistence_for_term_update() # Persist term and voted_for change

         # Update etcd for follower state and clear leader key if this node was leader
            if self.etcd_client:
                try:
                    logger_raft.info(f"Node {self.node_id} updating role to FOLLOWER in etcd after stepping down due to higher term.")
                    self.etcd_client.update_role(FOLLOWER)
                    if prev_state == LEADER:
                        logger_raft.info(f"Node {self.node_id} (was LEADER) clearing its leader info from etcd.")
                        self.etcd_client.clear_leader_info(previous_leader_id=self.node_id)
                except Exception as e:
                    logger_raft.error(f"Node {self.node_id} (FOLLOWER) failed to update etcd after stepping down due to higher term: {e}", exc_info=True)
            self.start_election_timer()
            self.leader_id = None

             # If stepping down from LEADER, stop sending heartbeats/AppendEntries
            if prev_state == 'LEADER' and hasattr(self, 'append_entries_sender_thread') and self.append_entries_sender_thread:
                if self.append_entries_sender_thread.is_alive():
                    self.append_entries_sender_thread.cancel()
                self.append_entries_sender_thread = None
                logging.info(f"Node {self.node_id} (was LEADER) cancelled periodic AppendEntries.")
            
             # Log the step down event
            logging.info(f"Node {self.node_id} stepping down from {prev_state} (term {prev_term}) to FOLLOWER (term {self.current_term}) due to higher term {term}.")
        elif term < self.current_term:
             # This shouldn't happen if RPC handling is correct, but log defensively
            logging.warning(f"Node {self.node_id} received a step down request with a lower term {term} than current {self.current_term}. Ignoring.")
        else: # term == self.current_term
             # If asked to step down in the same term, it might be from a successful leader election
             # We should only step down if we are a CANDIDATE or a LEADER who just discovered another leader.
             # If we are already a FOLLOWER in the same term, we stay FOLLOWER.
            if self.state in ('CANDIDATE', 'LEADER'):
                logging.info(f"Node {self.node_id} stepping down from {prev_state} (term {prev_term}) to FOLLOWER (term {self.current_term}) in the same term.")
                self.state = 'FOLLOWER'
                self.voted_for = None # Reset vote
                self._ensure_persistence_for_term_update() # Persist state change

              # Update etcd for follower state and clear leader key if this node was leader
            if self.etcd_client:
                try:
                    logger_raft.info(f"Node {self.node_id} updating role to FOLLOWER in etcd after stepping down in same term.")
                    self.etcd_client.update_role(FOLLOWER)
                    if prev_state == LEADER:
                        logger_raft.info(f"Node {self.node_id} (was LEADER) clearing its leader info from etcd.")
                        self.etcd_client.clear_leader_info(previous_leader_id=self.node_id)
                except Exception as e:
                    logger_raft.error(f"Node {self.node_id} (FOLLOWER) failed to update etcd after stepping down in same term: {e}", exc_info=True)
            self.start_election_timer()
            # Leader ID might be set if the step down was triggered by a valid leader AE
            # If it was triggered by another means (e.g., failure), leader_id might be None.
            # Let's keep the leader_id if it's set, as it means we know the current leader.
            # self.leader_id = None # Reverted: Keep leader_id if known.

    # def start_election(self): # Removed - Election is now handled by _handle_election_timeout and _conduct_pre_vote_round
    #    """Start leader election process"""
    #    # This method is superseded by the timer-driven _handle_election_timeout
    #    pass

    def replicate_log(self):
        """Replicate log entries to followers"""
        # Simply call send_append_entries_to_peers which handles replication
        self.send_append_entries_to_peers()

    def propose_command(self, command: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        """
        Proposes a new command to the Raft cluster. Only the leader can propose commands.
        The command will be appended to the leader's log and then replicated to followers.
        """
        if self.state != LEADER:
            logger_raft.warning(f"Node {self.node_id}: Received propose_command but not leader. Current state: {self.state}")
            return False, "NOT_LEADER"

        # Create a new log entry
        new_entry = {
            "term": self.current_term,
            "command": command,
            "index": len(self.log) # Assign index based on current log length
        }
        self.log.append(new_entry)
        logger_raft.info(f"Node {self.node_id}: Appended new command to log at index {new_entry['index']}. Log length: {len(self.log)}")

        # Immediately attempt to replicate log to followers
        self.send_append_entries_to_peers()

        # For now, return True immediately. In a full implementation, this would block
        # until the entry is committed (replicated to a majority).
        # For this exercise, we assume eventual consistency via replication.
        return True, None

    def apply_entries(self):
        """Apply committed entries to the state machine (sequencer_instance)."""
        # Raft rule: If commitIndex > lastApplied, increment lastApplied and apply log[lastApplied] to state machine
        
        # Ensure last_applied does not exceed commit_index or log length
        # We need to apply entries from self.last_applied + 1 up to self.commit_index.
        # The index of the entry to apply is the absolute index `self.last_applied + 1`.
        # Its index in the current `self.log` array is `(self.last_applied + 1) - self.current_log_base_index`.

        while self.last_applied < self.commit_index:
            next_apply_absolute_index = self.last_applied + 1
            
            # Calculate the index in the current log segment (self.log array)
            index_in_current_log = next_apply_absolute_index - self.current_log_base_index

            if index_in_current_log < 0 or index_in_current_log >= len(self.log):
                # This should ideally not happen if commit_index management is correct.
                # It implies commit_index points to an index that is before the start of our log segment
                # or beyond the end of our current log segment.
                logger_raft.error(f"Node {self.node_id}: Cannot apply log entry at absolute index {next_apply_absolute_index}. Index {index_in_current_log} out of bounds for current log segment (length {len(self.log)}, base index {self.current_log_base_index}). Commit index: {self.commit_index}. This may indicate a bug.")
                break # Avoid IndexError and potential infinite loop

            entry_to_apply = self.log[index_in_current_log]
            command_to_apply = entry_to_apply.get('command') or entry_to_apply.get('entry') # Command can be stored under 'command' or 'entry' key

            # Skip 'no-op' entries - these are for log compaction and leader establishment, not state machine commands
            if isinstance(command_to_apply, dict) and command_to_apply.get('type') == 'no-op':
                 logger_raft.debug(f"Node {self.node_id}: Skipping application of no-op entry at absolute index {next_apply_absolute_index} (log index {index_in_current_log}).")
                 self.last_applied = next_apply_absolute_index # Still advance last_applied for this index
                 continue # Move to the next entry

            if command_to_apply is None:
                logger_raft.warning(f"Node {self.node_id}: Log entry at absolute index {next_apply_absolute_index} (log index {index_in_current_log}) missing 'entry' or 'command' field: {entry_to_apply}. Skipping application.")
                self.last_applied = next_apply_absolute_index # Still advance last_applied for this entry
                continue

            if self.sequencer_instance:
                logger_raft.info(f"Node {self.node_id}: Applying log entry at absolute index={next_apply_absolute_index} to state machine.")
                try:
                    # The sequencer applies the *command* from the log entry.
                    # Pass the absolute log index as the sequence number to the sequencer.
                    result = self.sequencer_instance.record_entry(command_to_apply, next_apply_absolute_index) # Pass command data and absolute index
                    if result is None:
                        logger_raft.error(f"Node {self.node_id}: State machine failed to apply command at absolute log index {next_apply_absolute_index}. Command: {command_to_apply}. Halting further application in this cycle.")
                        # If a command fails, Raft implies we should probably halt or handle error specifically.
                        # For now, we stop applying further entries in this batch to prevent inconsistent state.
                        # self.last_applied remains at its current value (before this failed entry).
                        break
                    else:
                        logger_raft.info(f"Node {self.node_id}: Successfully applied command at absolute log index {next_apply_absolute_index}. Result: {result}")
                        self.last_applied = next_apply_absolute_index # Successfully applied, advance last_applied
                except Exception as e:
                    logger_raft.error(f"Node {self.node_id}: Exception applying command at absolute log index {next_apply_absolute_index}: {e}. Command: {command_to_apply}. Halting further application.", exc_info=True)
                    break
            else:
                logger_raft.warning(f"Node {self.node_id}: No sequencer_instance. Cannot apply log entry at absolute index {next_apply_absolute_index}. Entries will accumulate until a sequencer is available.")
                # If no sequencer, we can't apply. last_applied won't advance for real applications.
                # Depending on strictness, we might break or just log. Breaking is safer.
                break

            # Check if it's time to take a snapshot based on entry interval
            # Snapshotting is based on applied entries.
            if self.snapshot_entry_interval > 0 and self.last_applied > 0 and (self.last_applied % self.snapshot_entry_interval == 0):
                logger_raft.info(f"Node {self.node_id}: Reached snapshot interval at last_applied={self.last_applied}. Triggering snapshot.")
                self.take_snapshot()

    def append_entry(self, entry_data: Dict[str, Any]) -> int:
        """Append a new entry (command) to the log (used by leader)
        
        Args:
            entry_data: The data representing the command to append (e.g., {'type': 'SEND_MESSAGE', 'data': {...}})
        
        Returns:
            index: The absolute index of the newly appended entry in the log.
        """
        # Only leaders can append entries directly
        if self.state != 'LEADER':
            raise NotLeaderException(f"Cannot append entry - not leader (current state: {self.state}, current leader: {self.leader_id})")
            
        # Determine the absolute index of the new entry.
        # This is the index after the last entry in the current log segment.
        new_absolute_index = self.current_log_base_index + len(self.log)

        # Create the log entry with term and absolute index
        log_entry = {
            'index': new_absolute_index, # Absolute index
            'term': self.current_term,
            'entry': entry_data # The actual command/data
        }
        
        # Append to local log (in-memory list)
        self.log.append(log_entry)
        
        logging.debug(f"Leader {self.node_id} appended entry at absolute index {new_absolute_index}, term {self.current_term}: {entry_data}")
        
        # In single-node mode, immediately commit the entry
        if self.single_node_mode:
            self.commit_index = new_absolute_index
            self.apply_entries()
            
        # In multi-node mode, replication will be triggered by periodic AppendEntries calls or immediately
        # We could trigger immediate replication here for faster commit, but periodic is standard.
        # For this implementation, periodic AppendEntries handles sending new entries.
        # However, for a user request, we might want to trigger replication promptly.
        # Let's trigger replication immediately after appending a client request entry.
        # Check if this is a client request entry (not a no-op)
        if isinstance(entry_data, dict) and entry_data.get('type') != 'no-op':
             logging.debug(f"Node {self.node_id} appended client request entry at index {new_absolute_index}, triggering immediate replication.")
             # Schedule replication to run soon, but don't block the main thread.
             # A separate thread pool or non-blocking RPC calls would be better for performance.
             # For simplicity, let's call the periodic sender method, which handles RPCs in its own logic.
             # We can potentially optimize by targeting only peers that are behind.
             
             # Triggering the periodic sender now effectively sends the new entry quickly.
             # Alternatively, create a dedicated quick-replication method.
             self.send_append_entries_to_peers() # This sends entries to ALL peers as needed.

        return new_absolute_index
        
    def advance_commit_index(self):
        """Advance commit index based on peer match indices"""
        if self.state != 'LEADER':
            return
            
        # In single-node mode, commit happens immediately in append_entry
        if self.single_node_mode:
            # This method is not the primary way commits happen in single-node mode,
            # but can be a safety check.
            last_log_absolute_index = self.current_log_base_index + len(self.log) - 1
            if last_log_absolute_index > self.commit_index:
                self.commit_index = last_log_absolute_index
                self.apply_entries()
            return
            
        # For multi-node setup, only commit when a majority has replicated
        # matchIndex[peerId] stores the highest *absolute* log index replicated on peerId.
        
        # Get all match indices including our own (which is the absolute index of the last entry in our log)
        leader_last_log_absolute_index = self.current_log_base_index + len(self.log) - 1 if self.log else self.current_log_base_index - 1 # Correct for empty log
        match_indices = list(self.match_index.values()) + [leader_last_log_absolute_index] # Include leader's log end
        
        # Sort the indices
        match_indices.sort()
        
        # Find the index that a majority of nodes have replicated (including self).
        # A majority requires `(N / 2) + 1` nodes. The index is the one at `majority - 1` position in the sorted list.
        total_nodes = len(self.peers) + 1 # Leader + Peers
        majority_count = (total_nodes // 2) + 1
        
        # Ensure we have at least `majority_count` indices to pick from.
        if len(match_indices) < majority_count:
             logging.debug(f"Cannot advance commit index: not enough match indices ({len(match_indices)} < {majority_count}).")
             return # Not enough nodes responded or have logs

        # The index that a majority of nodes have replicated is the (majority_count - 1)-th largest index.
        # Since the list is sorted, this is `match_indices[len(match_indices) - majority_count]`.
        new_potential_commit_index = match_indices[len(match_indices) - majority_count]
        
        # Raft safety rule: A leader can only commit entries from its *current* term.
        # This rule is crucial for preventing older, partially replicated entries from
        # being committed incorrectly after a leader changes.
        
        # Check if the entry at `new_potential_commit_index` is from the leader's current term.
        # We need to find this entry in our local log.
        # Its index in the current `self.log` list is `new_potential_commit_index - self.current_log_base_index`.
        
        # Ensure the potential new commit index is valid and higher than current commit index.
        if new_potential_commit_index > self.commit_index:
            # Check if the entry at this potential commit index exists in our current log segment.
            # It might be before the base index if the snapshot included it.
            # If the index is within the current log segment...
            index_in_current_log = new_potential_commit_index - self.current_log_base_index
            
            if index_in_current_log >= 0 and index_in_current_log < len(self.log):
                 # The entry is in our current log segment. Check its term.
                 entry_at_potential_commit = self.log[index_in_current_log]
                 if entry_at_potential_commit.get('term') == self.current_term:
                      # The entry is from the current term and is replicated on a majority. Commit it.
                      logging.info(f"Advancing commit index from {self.commit_index} to {new_potential_commit_index} (entry term {self.current_term})")
                      self.commit_index = new_potential_commit_index
                      # Apply committed entries to state machine
                      self.apply_entries()
                 else:
                      logging.debug(f"Potential commit index {new_potential_commit_index} (term {entry_at_potential_commit.get('term')}) is not from current term {self.current_term}. Cannot commit yet.")
            elif new_potential_commit_index <= self.last_included_index:
                 # The potential commit index is within the range covered by the last snapshot.
                 # This means the entry *was* committed in a previous term (covered by the snapshot).
                 # In this scenario, it's safe to advance commit_index up to this point,
                 # as long as it's higher than the current commit_index.
                 # We don't need to re-apply entries covered by the snapshot.
                 logging.info(f"Advancing commit index from {self.commit_index} to {new_potential_commit_index} (index is within snapshot range).")
                 self.commit_index = new_potential_commit_index
                 # Applying entries will handle applying anything *after* the last_applied (which is >= last_included_index)
                 self.apply_entries()
            else:
                # The potential commit index is higher than the last snapshot but not in the current log segment.
                # This is an unexpected state and might indicate an issue.
                logging.error(f"Node {self.node_id}: Potential commit index {new_potential_commit_index} is outside current log segment (base {self.current_log_base_index}, len {len(self.log)}) and after snapshot (last_included {self.last_included_index}). Cannot commit.")


    def get_leader(self):
        # Simple check: return known leader_id, or None
        return self.leader_id

    def forward_to_leader(self, room_id, user_id, content, msg_type):
        """Forward request to current leader"""
        leader_id = self.get_leader()
        if leader_id is None:
            logging.warning(f"Node {self.node_id}: Cannot forward request, no leader known.")
            return {"status": "error", "message": "No leader known"}
            
        if leader_id == self.node_id:
             logging.warning(f"Node {self.node_id}: Attempted to forward request to self (leader). Processing locally.")
             # If somehow this gets called when we are the leader, handle it locally.
             # This could happen in a race condition or logic error.
             return self.record_entry({'type': 'SEND_MESSAGE', 'data': {'room_id': room_id, 'user_id': user_id, 'content': content, 'msg_type': msg_type}})

        # Get the leader's RPC address from the configuration
        leader_address = app_config.get_node_address(leader_id)
        if not leader_address:
            logging.error(f"Node {self.node_id}: Could not find address for leader {leader_id} in config.")
            return {"status": "error", "message": f"Could not find address for leader {leader_id}"}

        # Get or create an RPC client for the leader
        # self.peer_clients should include a client for the leader if it's in our peer list.
        leader_client = self.peer_clients.get(leader_id)
        if leader_client is None:
             logging.error(f"Node {self.node_id}: No RPC client initialized for leader {leader_id}.")
             # Attempt to create one dynamically if needed, or rely on peer_clients being pre-populated.
             # Assuming peer_clients is pre-populated based on config.
             return {"status": "error", "message": f"No RPC client for leader {leader_id}"}

        logging.info(f"Node {self.node_id} forwarding request to leader {leader_id} at {leader_address}")
        try:
            # The leader will call record_entry on its own RaftNode instance.
            # We need to send the *command data* to the leader's RPC endpoint.
            command_data = {'type': 'SEND_MESSAGE', 'data': {'room_id': room_id, 'user_id': user_id, 'content': content, 'msg_type': msg_type}}
            response = leader_client._make_request("record_entry", command_data) # Assuming record_entry RPC endpoint exists
            
            if response is None:
                 logging.warning(f"Node {self.node_id}: Leader {leader_id} did not respond to forwarded request.")
                 return {"status": "error", "message": f"Leader {leader_id} did not respond"}

            # Leader should return a response indicating success/failure and potentially message ID/seq.
            # Expected response format from leader's record_entry RPC endpoint should be:
            # {"status": "success", "id": "...", "sequence_number": ...} or {"status": "error", ...}
            
            # Check the response structure
            if isinstance(response, dict) and 'status' in response:
                 logging.info(f"Node {self.node_id}: Forwarded request handled by leader {leader_id}. Response: {response['status']}")
                 return response # Return the leader's response directly
            else:
                 logging.error(f"Node {self.node_id}: Unexpected response format from leader {leader_id} for forwarded request: {response}")
                 return {"status": "error", "message": f"Unexpected response from leader {leader_id}"}

        except Exception as e:
            logging.error(f"Node {self.node_id} error forwarding request to leader {leader_id}: {e}", exc_info=True)
            return {"status": "error", "message": f"Failed to forward request to leader {leader_id}: {str(e)}"}

    def run(self):
        """Main Raft node loop that handles state transitions and periodic tasks"""
        # Initialize election timer
        # start_election_timer is called in __init__
        # self.start_election_timer() # Already done in __init__
        
        # Fast-track to leader in single-node mode
        # This logic is now handled within become_leader and __init__ based on self.single_node_mode
        # if self.single_node_mode and self.state != 'LEADER':
        #    logging.info(f"Single-node mode: Node {self.node_id} fast-tracking to leader state")
        #    # Ensure term is at least 1 if starting fresh
        #    if self.current_term == 0:
        #        self.current_term = 1
        #    self.become_leader() # This transitions state and starts heartbeat sender (which does nothing in single mode)

        logging.info(f"Node {self.node_id} starting main Raft loop in state: {self.state}")

        # Main loop runs until node stops
        while True:
            try:
                # Main loop primarily ensures timers are running and checks basic conditions.
                # Raft logic (election, replication, application) is driven by timers or incoming RPCs.

                # Followers and Candidates rely on the election timer.
                # Leaders rely on the AppendEntries sender timer.

                # The election timer runs in a separate thread (`self.election_timer_thread`).
                # When it expires, it calls `_handle_election_timeout`.

                # The AppendEntries sender runs in a separate thread (`self.append_entries_sender_thread`).
                # When it expires, it calls `send_append_entries_to_peers`.

                # The main loop's primary responsibility here is to *keep the node alive* and handle
                # any high-level checks or cleanup not covered by RPC handlers or timers.
                # For this simplified implementation, the timers and RPC handlers do most of the work.
                # The main loop can just sleep, allowing the threads to run.

                # Check if timers are unexpectedly stopped (shouldn't happen with daemon threads normally)
                # and potentially restart them if state requires it.
                
                # If in FOLLOWER or CANDIDATE and election timer is not running:
                if self.state in ('FOLLOWER', 'CANDIDATE'):
                     if not hasattr(self, 'election_timer_thread') or not self.election_timer_thread or not self.election_timer_thread.is_alive():
                         logging.warning(f"Node {self.node_id} ({self.state}) detected election timer not running, restarting.")
                         self.start_election_timer() # Restart timer

                # If in LEADER and AppendEntries sender is not running (and not single-node):
                if self.state == 'LEADER' and not self.single_node_mode:
                     if not hasattr(self, 'append_entries_sender_thread') or not self.append_entries_sender_thread or not self.append_entries_sender_thread.is_alive():
                         logging.warning(f"Node {self.node_id} (LEADER) detected periodic AppendEntries sender not running, restarting.")
                         self._start_periodic_append_entries() # Restart sender


                # Apply committed entries periodically if not already triggered by RPCs
                # The apply_entries method is called after advancing commitIndex in AE response handler
                # and leader's advance_commit_index. Calling it here provides a safety net.
                self.apply_entries()
                
                # Periodically save persistent state (term, votedFor, log, commitIndex, lastApplied)
                # A real implementation would trigger this after any state change that needs persistence.
                # Doing it periodically is a fallback but less reliable for correctness.
                # self.save_persistent_state() # Implement this method

                # Check if snapshot is needed based on entry count and trigger if necessary
                # This check is already integrated into apply_entries now.
                # if self.snapshot_entry_interval > 0 and self.last_applied >= 0 and \
                #   (self.last_applied % self.snapshot_entry_interval == 0) and \
                #   (self.last_applied > (self.last_included_index if hasattr(self, 'last_included_index') else -1)):
                #      logging.info(f"Node {self.node_id}: Main loop triggering snapshot at applied index {self.last_applied}.")
                #      self.take_snapshot() # Take snapshot if needed

                time.sleep(0.1) # Main loop sleep

            except Exception as e:
                # Catch and log any exceptions to keep the Raft node running
                logging.error(f"Error in Raft main loop for node {self.node_id}: {e}")
                logging.error(traceback.format_exc())
                time.sleep(1.0)  # Sleep longer on error to prevent tight loop

    def close(self):
        """
        Gracefully shuts down the RaftNode, including stopping threads and deregistering from etcd.
        """
        logger_raft.info(f"Node {self.node_id}: Initiating graceful shutdown.")
        
        # Stop election timer
        if self.election_timer and self.election_timer.is_running():
            self.election_timer.stop()
            logger_raft.info(f"Node {self.node_id}: Election timer stopped.")

        # Stop heartbeat timer (if leader)
        if self.heartbeat_timer and self.heartbeat_timer.is_running():
            self.heartbeat_timer.stop()
            logger_raft.info(f"Node {self.node_id}: Heartbeat timer stopped.")

        # Stop periodic state saving
        if self.state_saving_task and self.state_saving_task.is_running():
            self.state_saving_task.stop()
            logger_raft.info(f"Node {self.node_id}: Periodic state saving stopped.")

        # Deregister from etcd
        if self.etcd_client:
            self.etcd_client.deregister_node()
            self.etcd_client.close() # Close etcd client connections
            logger_raft.info(f"Node {self.node_id}: Deregistered from etcd and etcd client closed.")
        
        logger_raft.info(f"Node {self.node_id}: Graceful shutdown complete.")

class NotLeaderException(Exception):
    """Custom exception raised when a non-leader node receives a client request that must go to the leader."""
    pass

# DistributedSequencer class inherits from RaftNode and adds state machine specific logic
# This class combines Raft consensus with the application logic (PersistentSequencer)
class DistributedSequencer(RaftNode):
    def __init__(self, peer_clients: Dict[str, 'RaftRPCClient'], persistent_state: Dict[str, Any]):
        # Initialize the persistent sequencer (the state machine)
        # Database init should happen before this.
        self.sequencer_instance = PersistentSequencer()
        
        # Call parent RaftNode initialization
        super().__init__(peer_clients=peer_clients, persistent_state=persistent_state, sequencer_instance=self.sequencer_instance)
        
        # After RaftNode __init__ completes, it will have potentially loaded a snapshot
        # and stored state machine state in _pending_state_machine_state.
        # We now apply this pending state to the PersistentSequencer instance if it exists.
        
        # The base RaftNode.__init__ already handles applying _pending_state_machine_state if sequencer_instance is provided.
        # So, the logic below was moved to the base class __init__.
        # if hasattr(self, '_pending_state_machine_state') and self._pending_state_machine_state:
        #    logging.info(f"Node {self.node_id}: DistributedSequencer attempting to load state machine from pending snapshot data.")
        #    try:
        #        if self.sequencer_instance:
        #            self.sequencer_instance.load_state(self._pending_state_machine_state)
        #            logging.info(f"Node {self.node_id}: Successfully loaded state into PersistentSequencer from snapshot.")
        #        else:
        #            logging.error(f"Node {self.node_id}: sequencer_instance is None, cannot load state from snapshot.")
        #        # Clear pending state after attempting to load
        #        self._pending_state_machine_state = None
        #    except Exception as e:
        #        logging.error(f"Node {self.node_id}: Error loading sequencer state from snapshot: {e}")
        
        # Determine single_node_mode based on peer configuration from app_config (via RaftNode)
        # self.peers is initialized in RaftNode.__init__ from app_config
        # single_node_mode = not self.peers # This was a simple check, app_config check is better
        
        # Check total nodes including self as per config
        total_nodes_in_config = len(app_config.get_all_node_addresses())
        self.single_node_mode = (total_nodes_in_config == 1) or app_config.is_single_node_mode() # Use a flag
        
        if self.single_node_mode and self.state != 'LEADER':
            logging.info(f"Node {self.node_id}: Single node mode detected (total nodes {total_nodes_in_config}, explicit config {app_config.is_single_node_mode()}), attempting to become leader.")
            # Ensure term is at least 1. If loaded state has term 0, this is okay.
            # If current_term is already > 0 from loaded state, this won't reset it, which is correct.
            if self.current_term == 0:
                self.current_term = 1 # Start with term 1 if no persistent state
                # No need to call _ensure_persistence_for_term_update here as become_leader will handle state changes.

            # No voting process in single node mode, become leader directly.
            # self.leader_id = self.node_id # This is set by become_leader
            self.become_leader() # This method handles state transition and initializes leader state

    def get_raft_node(self):
        """Return the underlying Raft node instance (self)."""
        return self

    def run(self):
        """Starts the main Raft node loop."""
        # The main run loop is inherited from RaftNode.
        # We can add DistributedSequencer-specific startup logic here if needed.
        logging.info(f"DistributedSequencer Node {self.node_id} starting Raft loop.")
        super().run() # Call the parent RaftNode's run method

    # Override apply_entries to use the specific PersistentSequencer instance
    # Note: This override is actually not needed anymore as RaftNode uses self.sequencer_instance
    # which is set to the PersistentSequencer instance in this class's __init__.
    # The logic for applying entries is now handled correctly in RaftNode.apply_entries.
    # Keeping the method here as a placeholder or for potential future custom logic.
    def apply_entries(self):
        """Override apply_entries to use the specific PersistentSequencer instance."""
        # The base RaftNode.apply_entries method already uses self.sequencer_instance.
        # We just need to ensure self.sequencer_instance is correctly initialized.
        # This method can call the parent method or add logic before/after.
        # If we are just using the parent's logic, we can remove this override.
        # Let's keep it for clarity that application is handled by the sequencer.
        # Any specific pre/post-application logic for the sequencer can go here.

        # Example: Ensure database session is managed around the apply calls if needed
        # Note: PersistentSequencer manages its own sessions per command.
        
        # Call the base class's apply_entries method
        super().apply_entries()
        
        # Add any DistributedSequencer specific post-application logic here, e.g.,
        # checking database state, triggering application-level callbacks, etc.

    def record_entry(self, room_id=None, user_id=None, content=None, msg_type=None, entry_data=None):
        """
        Client-facing method to record a new entry (command).
        Forwards to leader if not the leader. Appends to log if leader.
        
        This method can be called in two ways:
        1. With individual parameters (room_id, user_id, content, msg_type) - for backward compatibility
        2. With a dictionary (entry_data) containing the command and its data
        
        Args:
            room_id: ID of the room where the message is being sent
            user_id: ID of the user sending the message
            content: Content of the message
            msg_type: Type of the message (e.g., "text")
            entry_data: The dictionary containing the command and its data.
                        e.g., {'type': 'SEND_MESSAGE', 'data': {'room_id': ..., 'user_id': ..., 'content': ...}}
                              {'type': 'CREATE_ROOM', 'data': {'name': ..., 'creator_id': ...}}
        
        Returns:
            A dictionary with status information and relevant details (e.g., message ID, sequence number).
            Returns {"status": "error", "message": "Not leader"} if not the leader.
        """
        # If individual parameters are provided, convert them to the expected dictionary format
        if entry_data is None and room_id is not None and user_id is not None and content is not None:
            # Determine the command type based on msg_type
            command_type = msg_type or 'text'
            
            # Create the entry_data dictionary based on the command type
            if command_type == 'CREATE_ROOM':
                entry_data = {
                    'type': 'CREATE_ROOM', # Raft command type
                    'id': str(uuid.uuid4()),  # Unique ID for this specific log entry/operation
                    'room_id': str(room_id), # ID of the room to be created
                    'user_id': str(user_id), # ID of the user creating the room
                    'content': content,  # Room name
                    'msg_type': 'CREATE_ROOM', # Message type for PersistentSequencer
                    'timestamp': time.time()
                }
            else:  # Default to SEND_MESSAGE
                # For SEND_MESSAGE, the structure was already flat and correct, but we ensure consistency
                # if it was also nested previously. Assuming it needs to be flat like CREATE_ROOM now.
                entry_data = {
                    'type': 'SEND_MESSAGE', # Raft command type
                    'id': str(uuid.uuid4()),  # Generate a unique ID for the message
                    'room_id': str(room_id),
                    'user_id': str(user_id),
                    'content': content,
                    'msg_type': command_type, # Actual message type (e.g., 'text')
                    'timestamp': time.time()
                }
        # Check if this node is the leader
        if self.state != 'LEADER':
            # If not the leader, forward the request to the current leader
            logging.info(f"Node {self.node_id} received request, but not leader. Forwarding to {self.leader_id}.")
            try:
                # self.forward_to_leader expects specific message parameters, not generic command data.
                # We need to refactor forward_to_leader or create a new method for generic commands.
                # For now, let's assume a generic command forwarding mechanism is needed.
                # The record_entry RPC endpoint on the leader should accept the command_data dict.
                
                leader_response = self.forward_command_to_leader(entry_data)
                return leader_response
                
            except NotLeaderException as e:
                 # This exception should ideally not be raised by forward_command_to_leader
                 # if it correctly checks the leader, but handle defensively.
                 logging.error(f"Forwarding failed due to NotLeaderException (should not happen here): {e}")
                 return {"status": "error", "message": "Forwarding failed, leader status uncertain."}
            except Exception as e:
                logging.error(f"Node {self.node_id} failed to forward command to leader: {e}", exc_info=True)
                return {"status": "error", "message": f"Failed to forward command to leader: {str(e)}"}

        # If this node IS the leader, append the entry to the log
        logging.info(f"Node {self.node_id} (LEADER) receiving and processing command locally: {entry_data.get('type')}")
        try:
            # Append the entry data to the Raft log.
            # This will add the term and absolute index to the entry automatically.
            # The append_entry method also handles committing and applying in single-node mode,
            # and triggers replication in multi-node mode.
            absolute_log_index = self.append_entry(entry_data)
            
            # After appending and triggering replication, the entry will be applied
            # asynchronously once it's committed by a majority.
            
            # For a client request, we might want to return a preliminary response
            # immediately, or wait for the entry to be committed and applied.
            # Waiting for commitment and application (linearizable reads) is safer.
            # However, this requires blocking until commit_index >= absolute_log_index.
            
            # For now, let's return a response indicating the entry was accepted by the leader,
            # but not necessarily committed or applied yet. This is potentially weaker consistency.
            # A more robust system would block here or use a callback/future pattern.

            # To provide a useful response, we can return the absolute log index which will be the sequence number.
            # The actual outcome of the command (e.g., created room ID) will be available after application.
            
            # Let's wait for the entry to be committed before responding.
            # This provides linearizable consistency.
            wait_timeout = 5.0 # Max time to wait for the entry to be committed
            start_wait_time = time.time()
            
            logging.debug(f"Leader {self.node_id} waiting for entry at index {absolute_log_index} to be committed (current commit: {self.commit_index}).")
            
            while self.commit_index < absolute_log_index and (time.time() - start_wait_time) < wait_timeout:
                 time.sleep(0.01) # Wait briefly

            if self.commit_index < absolute_log_index:
                 logging.warning(f"Leader {self.node_id}: Timed out waiting for entry at index {absolute_log_index} to be committed (commit_index: {self.commit_index}). Request may or may not eventually succeed.")
                 return {"status": "timeout", "message": "Request accepted by leader but timed out waiting for commitment", "log_index": absolute_log_index}
            
            # If we reach here, the entry is committed (commit_index >= absolute_log_index).
            # It should also be applied or be in the process of being applied (last_applied might lag slightly).
            # The actual result of the command execution is needed for the response.
            # The apply_entries method calls sequencer_instance.record_entry which returns the result.
            # We need a way to get that result back to the client requesting this entry.
            
            # Option 1: Modify apply_entries to store results and retrieve them here.
            # Option 2: Re-query the state machine (database) after commitment for the result.
            # Re-querying the database is simpler if the sequencer stores sufficient info.
            # For a message, the sequence number (log_index) is key. We can query by sequence number.
            
            command_type = entry_data.get('type')
            data = entry_data.get('data')
            
            if command_type == 'SEND_MESSAGE':
                 # Query the database for the message with the correct sequence number (log_index)
                 db: Session = SessionLocal()
                 try:
                     # Find message by sequence_number which is the absolute_log_index
                     message = db.query(Message).filter(Message.sequence_number == absolute_log_index).first()
                     if message:
                         logging.info(f"Leader {self.node_id}: Entry at index {absolute_log_index} committed and applied. Message ID {message.id}, Seq {message.sequence_number}.")
                         return {
                             "status": "success",
                             "id": message.id,
                             "room_id": message.room_id,
                             "user_id": message.user_id,
                             "content": message.content,
                             "sequence_number": message.sequence_number,
                             "timestamp": message.timestamp.isoformat() if message.timestamp else None # Convert datetime to string
                         }
                     else:
                         # This is unexpected if commit_index was advanced and apply_entries ran.
                         # Could happen if application failed silently or db query failed.
                         logging.error(f"Leader {self.node_id}: Entry at index {absolute_log_index} committed but corresponding message not found in DB.")
                         return {"status": "error", "message": "Entry committed but failed to find result in database."}
                 finally:
                     db.close()
                      
            elif command_type == 'CREATE_ROOM':
                # Query the database for the room created with this command
                # Assuming the sequencer adds log_index (sequence_number) to the room record somehow
                # Or, query based on other unique attributes if available.
                db: Session = SessionLocal()
                try:
                    # Assuming room record stores the log_index that created it, or querying by name/creator is sufficient if unique
                    # A safer way might be to have the sequencer return a command-specific ID upon successful application
                    # and store/retrieve that ID.
                    
                    # For simplicity, let's assume the sequencer stored the log_index in the Room table.
                    room = db.query(Room).filter(Room.sequence_number == absolute_log_index).first()
                    if room:
                        logging.info(f"Leader {self.node_id}: Entry at index {absolute_log_index} committed and applied. Room ID {room.id}, Seq {room.sequence_number}.")
                        return {
                            "status": "success",
                            "id": room.id,
                            "name": room.name,
                            "created_by_user_id": room.created_by_user_id,
                            "sequence_number": room.sequence_number
                        }
                    else:
                        logging.error(f"Leader {self.node_id}: Entry at index {absolute_log_index} committed but corresponding room not found in DB.")
                        return {"status": "error", "message": "Entry committed but failed to find result in database."}
                finally:
                    db.close()
            
            # Return a default response for unknown command types
            return {"status": "error", "message": f"Unknown command type: {command_type}"}
        except Exception as e:
            logger_raft.error(f"Node {self.node_id} (LEADER) encountered an error processing command: {e}", exc_info=True)
            return {"status": "error", "message": f"Leader failed to process command: {str(e)}"}

    def forward_command_to_leader(self, command_data: Dict[str, Any]):
        """
        Forwards a generic command dictionary to the current leader's RPC endpoint.
        """
        leader_id = self.get_leader()
        if leader_id is None:
            logging.warning(f"Node {self.node_id}: Cannot forward command, no leader known.")
            raise NotLeaderException(f"Node {self.node_id}: No leader known to forward command.")
            
        if leader_id == self.node_id:
             logging.warning(f"Node {self.node_id}: Attempted to forward command to self (leader). Processing locally.")
             # This shouldn't be called if self.state != 'LEADER', but handle defensively.
             # If this happens, it means the caller somehow called forward when self is leader.
             # Just return an error or process locally (processing locally is better).
             # The calling record_entry method already handles the leader case.
             # So, if this is reached, there's a logic error in the caller.
             # Let's raise an error to indicate the problem.
             raise ValueError(f"Node {self.node_id}: Called forward_command_to_leader when node is the leader.")

        leader_address = app_config.get_node_address(leader_id)
        if not leader_address:
            logging.error(f"Node {self.node_id}: Could not find address for leader {leader_id} in config for command forwarding.")
            raise NotLeaderException(f"Could not find address for leader {leader_id}")

        leader_client = self.peer_clients.get(leader_id)
        if leader_client is None:
             logging.error(f"Node {self.node_id}: No RPC client initialized for leader {leader_id} for command forwarding.")
             raise NotLeaderException(f"No RPC client for leader {leader_id}")

        logging.info(f"Node {self.node_id} forwarding command {command_data.get('type')} to leader {leader_id} at {leader_address}")
        try:
            # Check if this is a SEND_MESSAGE command with the expected structure
            if command_data.get('type') == 'SEND_MESSAGE' and 'data' in command_data:
                data = command_data['data']
                # Use the specific record_entry method for SEND_MESSAGE commands
                response = leader_client.record_entry(
                    room_id=data.get('room_id'),
                    user_id=data.get('user_id'),
                    content=data.get('content'),
                    msg_type=data.get('msg_type', 'text')
                )
            else:
                # For other command types or if the structure is different, use the generic _make_request method
                response = leader_client._make_request("record_entry", command_data) 
            
            if response is None:
                 logging.warning(f"Node {self.node_id}: Leader {leader_id} did not respond to forwarded command.")
                 # Indicate that forwarding failed
                 raise requests.exceptions.RequestException(f"Leader {leader_id} did not respond")

            # The response from the leader's record_entry should be the final result.
            logging.info(f"Node {self.node_id}: Forwarded command handled by leader {leader_id}. Response: {response.get('status')}")
            return response # Return the leader's response directly

        except requests.exceptions.RequestException as e:
            logging.error(f"Node {self.node_id} error forwarding command to leader {leader_id}: {e}", exc_info=True)
            # Wrap the request exception in a more specific Raft exception if needed, or just re-raise.
            raise NotLeaderException(f"Failed to forward command to leader {leader_id}: {str(e)}") from e
        except Exception as e:
            logging.error(f"Node {self.node_id} unexpected error forwarding command to leader {leader_id}: {e}", exc_info=True)
            raise NotLeaderException(f"Unexpected error forwarding command to leader {leader_id}: {str(e)}") from e



    def shutdown(self):
        logger_raft.info(f"Node {self.node_id}: Initiating shutdown sequence...")
        self.stop_event.set()

        # Cancel election timer
        if hasattr(self, 'election_timer_thread') and self.election_timer_thread is not None:
            logger_raft.debug(f"Node {self.node_id}: Cancelling election timer during shutdown.")
            self.election_timer_thread.cancel()
            try:
                # Give a brief moment for the timer thread to acknowledge cancellation or finish if already running
                self.election_timer_thread.join(timeout=0.2) # Increased timeout slightly
            except RuntimeError:
                # Timer may not have been started or already finished and collected
                logger_raft.debug(f"Node {self.node_id}: Election timer join failed, possibly already stopped or not started.")
            except Exception as e:
                logger_raft.error(f"Node {self.node_id}: Error joining election timer thread during shutdown: {e}", exc_info=True)

        # Wait for heartbeat sender thread to terminate
        if hasattr(self, 'heartbeat_sender_thread') and self.heartbeat_sender_thread is not None and self.heartbeat_sender_thread.is_alive():
            logger_raft.debug(f"Node {self.node_id}: Waiting for heartbeat sender thread to terminate...")
            self.heartbeat_sender_thread.join(timeout=max(1.0, self.heartbeat_interval_sec * 2)) # Wait at least 1s or 2x interval
            if self.heartbeat_sender_thread.is_alive():
                logger_raft.warning(f"Node {self.node_id}: Heartbeat sender thread did not terminate in time.")
            else:
                logger_raft.info(f"Node {self.node_id}: Heartbeat sender thread terminated.")
        
        # Deregister from etcd and stop etcd client's heartbeats
        if self.etcd_client:
            logger_raft.info(f"Node {self.node_id}: Shutting down EtcdClient component.")
            try:
                if hasattr(self.etcd_client, 'shutdown') and callable(getattr(self.etcd_client, 'shutdown')):
                    self.etcd_client.shutdown() # Assuming EtcdClient.shutdown handles deregistration and its own thread cleanup
                    logger_raft.info(f"Node {self.node_id}: EtcdClient.shutdown() called.")
                else:
                    logger_raft.warning(f"Node {self.node_id}: EtcdClient does not have a 'shutdown' method. Calling deregister and stop_all_heartbeats manually.")
                    self.etcd_client.deregister_node(self.node_id)
                    self.etcd_client.stop_all_heartbeats()
                    if hasattr(self.etcd_client, 'close') and callable(getattr(self.etcd_client, 'close')):
                        self.etcd_client.close() # Close the underlying etcd3 client connection if available
                    logger_raft.info(f"Node {self.node_id}: Manually handled EtcdClient cleanup (deregister, stop heartbeats, close).")
            except Exception as e:
                logger_raft.error(f"Node {self.node_id}: Error during EtcdClient shutdown/cleanup: {e}", exc_info=True)
        
        # Example: Persist final state if necessary (snapshotting logic might go here)
        # logger_raft.info(f"Node {self.node_id}: Performing final state persistence (if applicable).")
        # self.save_snapshot(is_final_shutdown=True) # Hypothetical: ensure data is saved

        logger_raft.info(f"Node {self.node_id}: Shutdown sequence completed.")


class PersistentSequencer:
    def __init__(self, db_connection_string: Optional[str] = None):
        """
        Initializes the PersistentSequencer.
        The actual database initialization (init_db) should be called once globally.
        This sequencer will use SessionLocal to get DB sessions.
        Args:
            db_connection_string: The database connection string. Currently unused here
                                  as SessionLocal is pre-configured.
        """
        self.db_connection_string = db_connection_string
        # init_db() # This should be called globally at application startup, not per instance.
        logger_raft.info("PersistentSequencer initialized. Database tables should be pre-initialized.")

    def record_entry(self, command: Dict[str, Any], log_index: int) -> Optional[Dict[str, Any]]:
        """
        Records a command from a Raft log entry into the persistent database.
        The log_index is used as the authoritative sequence number.

        Args:
            command: The command dictionary from the Raft log entry.
                     Expected format: {'type': 'COMMAND_TYPE', 'data': {...}}
            log_index: The absolute Raft log index for this command. This will be used as the sequence number.

        Returns:
            A dictionary representing the outcome or the created/modified resource,
            or None if the command failed or is unknown.
            Returns the result structure that the client API expects for this command.
        """
        command_type = command.get('type')
        data = command.get('data')

        if not command_type or data is None:
            logger_raft.error(f"Invalid command structure: {command} at log_index: {log_index}")
            return None

        db: Session = SessionLocal()
        try:
            logger_raft.info(f"Sequencer processing command: {command_type} at log index: {log_index}")

            if command_type == 'CREATE_ROOM':
                room_name = data.get('name')
                creator_id = data.get('creator_id')
                if not room_name or not creator_id:
                    logger_raft.error(f"CREATE_ROOM: Missing name or creator_id in command data at log_index {log_index}. Data: {data}")
                    return {"status": "error", "message": "Invalid CREATE_ROOM command data"}
                
                # Check if a room with this name already exists to prevent duplicates (optional, depends on desired behavior)
                existing_room = db.query(Room).filter(Room.name == room_name).first()
                if existing_room:
                    logger_raft.warning(f"CREATE_ROOM: Room with name '{room_name}' already exists at log_index {log_index}.")
                    # Decide whether to return an error or the existing room details.
                    # For idempotency, returning the existing room might be preferable if command includes enough info.
                    # For simplicity here, let's assume room name must be unique and treat as a failure if exists.
                    # OR, maybe the Raft command itself should enforce uniqueness *before* appending if possible,
                    # or the state machine applies idempotently.
                    # For now, log and return error.
                    return {"status": "error", "message": f"Room with name '{room_name}' already exists"}

                new_room = Room(
                    name=room_name, 
                    created_by_user_id=creator_id,
                    sequence_number=log_index # Store the log index as the sequence number
                )
                db.add(new_room)
                db.commit()
                db.refresh(new_room)
                logger_raft.info(f"Room '{room_name}' created with ID {new_room.id} by user {creator_id} (log_index/seq: {log_index}).")
                
                # Return a response structure matching what the API might expect
                return {"status": "success", "action": "room_created", "id": new_room.id, "name": new_room.name, "created_by_user_id": new_room.created_by_user_id, "sequence_number": new_room.sequence_number}

            elif command_type == 'SEND_MESSAGE':
                room_id = data.get('room_id')
                user_id = data.get('user_id')
                content = data.get('content')
                # msg_type is not stored in the DB schema provided, assume it's implied or handled elsewhere
                # time and id are generated by the database model

                # Validate required fields
                if room_id is None or user_id is None or content is None: # content can be empty string
                    logger_raft.error(f"SEND_MESSAGE: Missing room_id, user_id, or content in command data at log_index {log_index}. Data: {data}")
                    return {"status": "error", "message": "Invalid SEND_MESSAGE command data"}

                # Check if the room exists (optional but good practice)
                # room = db.query(Room).filter(Room.id == room_id).first()
                # if not room:
                #     logger_raft.warning(f"SEND_MESSAGE: Room ID {room_id} does not exist at log_index {log_index}. Skipping message.")
                #     return {"status": "skipped", "message": f"Room ID {room_id} does not exist"} # Or return error

                new_message = Message(
                    id=data.get('id'), # Pass the UUID generated by api_server
                    room_id=room_id,
                    user_id=user_id,
                    content=content,
                    sequence_number=log_index,
                    message_type=data.get('message_type'), # Pass message_type
                    created_at=data.get('timestamp') # Use timestamp as created_at
                )
                db.add(new_message)
                db.commit()
                db.refresh(new_message) # Refresh to get database-generated fields like id and timestamp
                logger_raft.info(f"Message recorded in room {room_id} by user {user_id} (log_index/seq: {log_index}, msg_id: {new_message.id}).")
                
                # Return a response structure matching what the API might expect
                return {
                    "status": "success", 
                    "action": "message_sent",
                    "id": new_message.id,
                    "room_id": new_message.room_id,
                    "user_id": new_message.user_id,
                    "content": new_message.content,
                    "sequence_number": new_message.sequence_number,
                    "timestamp": datetime.fromtimestamp(new_message.created_at).isoformat() if new_message.created_at else None
                }
            
            # Add other command handlers here (e.g., ADD_USER_TO_ROOM)
            # elif command_type == 'NO-OP':
            #     logger_raft.debug(f"Sequencer received NO-OP command at log_index: {log_index}")
            #     # NO-OP doesn't change state machine state, so no DB operation needed.
            #     return {"status": "success", "action": "no-op", "sequence_number": log_index}

            else:
                logger_raft.warning(f"Sequencer received unknown command type: {command_type} at log_index: {log_index}")
                return {"status": "error", "message": f"Unknown command type: {command_type}"}

        except IntegrityError as e:
            db.rollback()
            logger_raft.error(f"Database integrity error processing command {command_type} at log_index {log_index}: {e}. Data: {data}")
            return {"status": "error", "message": "Database integrity error"}
        except SQLAlchemyError as e:
            db.rollback()
            logger_raft.error(f"Database error processing command {command_type} at log_index {log_index}: {e}. Data: {data}")
            return {"status": "error", "message": "Database error"}
        except Exception as e:
            db.rollback()
            logger_raft.error(f"Unexpected error processing command {command_type} at log_index {log_index}: {e}. Data: {data}", exc_info=True)
            return {"status": "error", "message": "Internal sequencer error"}
        finally:
            db.close()

    def get_state(self) -> Optional[Dict[str, Any]]:
        """
        Returns a representation of the state machine's state for Raft snapshots.
        Since the state is persisted in the database, this doesn't need to dump the whole database.
        It can return minimal metadata indicating the state is in the DB.
        For a snapshot to be useful for a new follower, it needs to include the *actual* state.
        Dumping the relevant tables might be required, or using a DB-specific snapshot mechanism.
        
        For this implementation, let's assume we *do* dump essential data for the snapshot.
        This requires querying the database. This can be memory/CPU intensive for large states.
        """
        logger_raft.info("PersistentSequencer.get_state() called for snapshot.")
        db: Session = SessionLocal()
        try:
            # Example: Dump all rooms and messages. This might not scale for large datasets.
            # In a real system, consider streaming data or using incremental snapshots.
            all_rooms = [
                 {"id": r.id, "name": r.name, "created_by_user_id": r.created_by_user_id, "sequence_number": r.sequence_number} 
                 for r in db.query(Room).order_by(Room.sequence_number).all()
            ]
            all_messages = [
                 {"id": m.id, "room_id": m.room_id, "user_id": m.user_id, "content": m.content, "sequence_number": m.sequence_number, "timestamp": m.timestamp.isoformat() if m.timestamp else None}
                 for m in db.query(Message).order_by(Message.sequence_number).all()
            ]
            
            state_data = {
                "rooms": all_rooms,
                "messages": all_messages,
                "info": "Snapshot includes dump of Room and Message tables."
            }
            logger_raft.info(f"Snapshot state includes {len(all_rooms)} rooms and {len(all_messages)} messages.")
            return state_data

        except Exception as e:
            logger_raft.error(f"Error getting state from PersistentSequencer for snapshot: {e}", exc_info=True)
            # Return an empty state or error indicator if state retrieval fails
            return {"error": "Failed to retrieve state from database for snapshot"}
        finally:
            db.close()

    def load_state(self, state: Optional[Dict[str, Any]]):
        """
        Loads the state machine's state from a Raft snapshot.
        This method is called when applying a snapshot received via InstallSnapshot RPC
        or loading a snapshot at startup.
        It should replace the current state with the state from the snapshot.
        For a database-backed sequencer, this means clearing existing relevant data
        and loading the data from the snapshot.

        Args:
            state: The state dictionary from the snapshot's state_machine_state.
        """
        if state is None or 'rooms' not in state or 'messages' not in state:
            logger_raft.warning("PersistentSequencer.load_state() called with invalid or empty state. Skipping load.")
            return

        logger_raft.info("PersistentSequencer.load_state() called. Loading state from snapshot.")
        db: Session = SessionLocal()
        try:
            # IMPORTANT SAFETY: Clear existing relevant data before loading from snapshot.
            # This ensures the state machine is exactly as defined by the snapshot.
            # Be extremely careful with this in production - ensure you only delete
            # data managed by Raft's log.
            logger_raft.warning("Clearing existing Rooms and Messages tables before loading snapshot state.")
            db.query(Message).delete()
            db.query(Room).delete()
            db.commit()
            logger_raft.info("Existing data cleared.")

            # Load data from the snapshot state
            rooms_to_load = state.get('rooms', [])
            messages_to_load = state.get('messages', [])

            # Load Rooms
            for room_data in rooms_to_load:
                 # Ensure required fields exist and handle potential ID conflicts if loading into new DB
                 # If the snapshot includes DB primary keys (like 'id'), ensure they are handled correctly
                 # (e.g., let the DB assign new IDs or use UUIDs consistently).
                 # Using log_index as sequence_number is key for consistency.
                 try:
                      new_room = Room(
                           # id=room_data.get('id'), # Let DB handle auto-increment IDs unless UUIDs
                           name=room_data.get('name'),
                           created_by_user_id=room_data.get('created_by_user_id'),
                           sequence_number=room_data.get('sequence_number')
                      )
                      db.add(new_room)
                 except Exception as e:
                      logger_raft.error(f"Error loading room from snapshot: {room_data}: {e}")
                      # Decide error handling: abort load? skip entry?
                      # Skipping might lead to inconsistent state. Aborting is safer.
                      db.rollback()
                      logger_raft.error("Aborting snapshot load due to room loading error.")
                      return

            # Load Messages
            for msg_data in messages_to_load:
                 try:
                      # Ensure required fields exist and handle potential ID/timestamp conflicts
                      new_message = Message(
                           # id=msg_data.get('id'), # Let DB handle auto-increment IDs unless UUIDs
                           room_id=msg_data.get('room_id'),
                           user_id=msg_data.get('user_id'),
                           content=msg_data.get('content'),
                           sequence_number=msg_data.get('sequence_number'),
                           # timestamp=datetime.fromisoformat(msg_data['timestamp']) if msg_data.get('timestamp') else None # Convert string back to datetime if stored
                      )
                      db.add(new_message)
                 except Exception as e:
                      logger_raft.error(f"Error loading message from snapshot: {msg_data}: {e}")
                      db.rollback()
                      logger_raft.error("Aborting snapshot load due to message loading error.")
                      return

            db.commit()
            logger_raft.info("Successfully loaded state from snapshot into database.")

        except Exception as e:
            db.rollback()
            logger_raft.error(f"Unexpected error loading state into PersistentSequencer from snapshot: {e}", exc_info=True)
        finally:
            db.close()
