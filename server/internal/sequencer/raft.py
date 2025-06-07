import logging
import requests
from typing import List, Dict, Tuple, Any, Optional
import time
from datetime import datetime
import random
import json
import threading
import uuid
import traceback

from server.config import app_config
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError, SQLAlchemyError

from server.internal.discovery.etcd_client import EtcdClient
from server.internal.storage.models import Room, Message
from server.internal.storage.database import SessionLocal, init_db

logger_raft = logging.getLogger('raft')

LEADER = "LEADER"
FOLLOWER = "FOLLOWER"
CANDIDATE = "CANDIDATE"

class RaftNode:
    """
    Represents a single node participating in the Raft consensus algorithm.

    Manages the node's state (Leader, Follower, Candidate), term, log, and
    communication with peers for election and log replication. Interacts with
    an EtcdClient for service discovery and leader election/advertisement.
    Applies committed log entries to a provided state machine (sequencer_instance).
    """
    def __init__(self,
                 peer_clients: Dict[str, 'RaftRPCClient'],
                 persistent_state: Dict[str, Any],
                 etcd_client_instance: Optional['EtcdClient'],
                 node_api_address_str: Optional[str],
                 node_rpc_address_str: Optional[str],
                 sequencer_instance: Optional[Any] = None):
        """
        Initializes a Raft node.

        Args:
            peer_clients: A dictionary mapping peer node IDs to their RaftRPCClient instances.
            persistent_state: A dictionary containing the node's loaded persistent state
                              (current_term, voted_for, log, commit_index, last_applied).
            etcd_client_instance: An initialized EtcdClient instance for interacting with etcd.
            node_api_address_str: The HTTP API address of this node (for leader advertisement).
                                  e.g., "http://localhost:8001".
            node_rpc_address_str: The HTTP RPC address of this node (for etcd registration).
                                  e.g., "http://localhost:5001".
            sequencer_instance: An optional instance of the state machine (PersistentSequencer)
                                that committed log entries will be applied to.
        Raises:
            ValueError: If NODE_ID is not set or the current node's RPC address cannot be determined.
        """
        self.node_id = app_config.node_id
        if not self.node_id:
            logger_raft.critical("CRITICAL: NODE_ID is not set. RaftNode cannot initialize.")
            raise ValueError("NODE_ID is not set, cannot initialize RaftNode.")

        self.etcd_client = etcd_client_instance
        self.node_api_address = node_api_address_str
        self.node_rpc_address_for_etcd = node_rpc_address_str

        self.stop_event = threading.Event()

        self.peers = app_config.get_peer_node_ids()
        self.peer_clients = peer_clients

        self.current_term = int(persistent_state.get('current_term', 0))
        self.voted_for = persistent_state.get('voted_for', None)
        self.log = persistent_state.get('log', [])
        self.commit_index = int(persistent_state.get('commit_index', -1))
        self.last_applied = int(persistent_state.get('last_applied', -1))
        logger_raft.info(f"Node {self.node_id}: Initialized with term={self.current_term}, voted_for={self.voted_for}, log_len={len(self.log)}, commit_idx={self.commit_index}, last_applied={self.last_applied}")

        self.node_rpc_address_tuple = app_config.get_current_node_rpc_address()
        if not self.node_rpc_address_tuple:
            logger_raft.critical(f"CRITICAL: RPC address tuple for current node '{self.node_id}' could not be determined from config.")
            raise ValueError(f"RPC address tuple for current node '{self.node_id}' could not be determined from config.")

        self.all_node_rpc_addresses_map = app_config.get_all_node_addresses()

        self.state = FOLLOWER

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

        ETCD_LEASE_TTL = app_config.get('etcd', {}).get('ttl', 30)
        ETCD_HEARTBEAT_INTERVAL = app_config.get('etcd', {}).get('heartbeat_interval', 10)

        if self.etcd_client and self.node_rpc_address_for_etcd and self.node_api_address:
            try:
                logger_raft.info(f"Node {self.node_id}: Attempting to register with etcd as {self.state}. "
                                 f"RPC for Etcd: {self.node_rpc_address_for_etcd}, API for Etcd: {self.node_api_address}, "
                                 f"Lease TTL: {ETCD_LEASE_TTL}s, Etcd Heartbeat Interval: {ETCD_HEARTBEAT_INTERVAL}s")

                if not self.etcd_client.update_node_role(self.state):
                    logger_raft.info(f"Node {self.node_id}: Node not found in etcd during role update, attempting initial registration.")
                    self.etcd_client.register_node(
                        initial_role=self.state,
                        ttl=ETCD_LEASE_TTL,
                        heartbeat_interval=ETCD_HEARTBEAT_INTERVAL
                    )
                    logger_raft.info(f"Node {self.node_id}: Successfully initiated registration with etcd as {self.state}.")
                else:
                    logger_raft.info(f"Node {self.node_id}: Successfully updated role in etcd to {self.state}.")
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

    def get_last_log_index(self) -> int:
        """
        Gets the absolute index of the last entry in the log.

        Returns:
            The absolute index of the last log entry, or -1 if the log is empty.
        """
        return len(self.log) - 1 if self.log else -1

    def get_last_log_term(self) -> int:
        """
        Gets the term of the last entry in the log.

        Returns:
            The term of the last log entry, or 0 if the log is empty.
        """
        return self.log[-1]['term'] if self.log else 0

    def verify_peer_connections(self, timeout=30, initial_delay=0.1, max_delay=2.0) -> bool:
        """
        Verifies connectivity to peer RPC servers.

        Args:
            timeout: Total time to attempt connections.
            initial_delay: Initial delay between connection attempts.
            max_delay: Maximum delay between connection attempts.

        Returns:
            True if a majority of peers are reachable within the timeout, False otherwise.
        """
        logging.info(f"Node {self.node_id}: Verifying peer connections...")
        start_time = time.time()
        connected_peers = set()

        while time.time() - start_time < timeout:
            for peer_id in self.peers:
                if peer_id not in connected_peers:
                    try:
                        logging.debug(f"Node {self.node_id}: Attempting to connect to peer {peer_id} for verification.")
                        # Use a simple health check or a lightweight RPC like PreVote
                        # PreVote is used here as it's part of the Raft spec, but any successful RPC indicates connectivity
                        response = self.peer_clients[peer_id].pre_vote(
                            candidate_id=self.node_id,
                            term=self.current_term + 1,
                            last_log_index=self.get_last_log_index(),
                            last_log_term=self.get_last_log_term()
                        )
                        if response is not None:
                             connected_peers.add(peer_id)
                             logging.info(f"Node {self.node_id}: Successfully connected to peer {peer_id}.")
                    except Exception as e:
                        logging.debug(f"Node {self.node_id}: Failed to connect to peer {peer_id}: {e}")

            if len(connected_peers) >= (len(self.peers) // 2) + 1 or len(self.peers) == 0:
                logging.info(f"Node {self.node_id}: Majority of peers ({len(connected_peers)}/{len(self.peers)}) reachable. Proceeding.")
                return True

            time.sleep(initial_delay)
            initial_delay = min(initial_delay * 2, max_delay)

        logging.warning(f"Node {self.node_id}: Failed to connect to a majority of peers within {timeout} seconds. Only {len(connected_peers)}/{len(self.peers)} reachable.")
        return False

    def start_election_timer(self):
        """
        Starts or resets the election timer.

        If the timer expires while the node is a Follower or Candidate, it triggers
        the _handle_election_timeout method.
        """
        if hasattr(self, 'election_timer_thread') and self.election_timer_thread is not None:
            try:
                self.election_timer_thread.cancel()
                time.sleep(0.001)
            except Exception as e:
                logging.error(f"Node {self.node_id} error canceling election timer: {e}", exc_info=False)

        self.election_timeout = random.uniform(self.min_election_timeout_sec, self.max_election_timeout_sec)

        self.election_timer_thread = threading.Timer(self.election_timeout, self._handle_election_timeout)
        self.election_timer_thread.daemon = True
        self.election_timer_start_time = time.time()
        self.election_timer_thread.start()

        logging.debug(f"Node {self.node_id} (state: {self.state}, term: {self.current_term}) started election timer for {self.election_timeout:.3f}s")

    def _handle_election_timeout(self):
        """
        Handles the event when the election timer expires.

        Initiates a Pre-Vote round if the node is a Follower or Candidate and
        has not heard from a valid leader recently.
        """
        print(f"DEBUG: Node {self.node_id}: ENTERING _handle_election_timeout at {time.time()}", flush=True)
        if self.state == 'LEADER':
            return

        if self.leader_id is not None and \
           (time.time() - self.last_valid_leader_heartbeat_time) < self.election_timeout:
            logging.info(f"Node {self.node_id} (state: {self.state}, term: {self.current_term}) election timer expired, "
                         f"but deferring election due to recent heartbeat from leader {self.leader_id} (last seen {time.time() - self.last_valid_leader_heartbeat_time:.2f}s ago, timeout {self.election_timeout:.2f}s). Resetting timer.")
            self.start_election_timer()
            return

        logging.info(f"Node {self.node_id} (state: {self.state}, term: {self.current_term}) election timer expired. Initiating Pre-Vote round.")
        self._conduct_pre_vote_round()

    def pre_vote(self, candidate_id: str, candidate_proposed_term: int, candidate_last_log_index: int, candidate_last_log_term: int) -> Tuple[bool, int]:
        """
        Handles Pre-Vote RPC requests from other nodes.

        Grants a Pre-Vote if the candidate's proposed term is higher than or equal to
        the node's current term and its log is at least as up-to-date, provided the node
        is not a leader and hasn't heard from a valid leader recently.

        Args:
            candidate_id: The ID of the candidate requesting the Pre-Vote.
            candidate_proposed_term: The term the candidate is proposing to start.
            candidate_last_log_index: Index of candidate's last log entry.
            candidate_last_log_term: Term of candidate's last log entry.

        Returns:
            A tuple: (vote_granted, current_term) where vote_granted is a boolean
            indicating if the Pre-Vote was granted and current_term is the receiver's
            current term.
        """
        logging.info(f"Node {self.node_id} (term: {self.current_term}) received Pre-Vote request from {candidate_id} for term {candidate_proposed_term}")

        if candidate_proposed_term < self.current_term:
            logging.info(f"Node {self.node_id} rejected Pre-Vote for {candidate_id}: candidate term {candidate_proposed_term} < current term {self.current_term}")
            return False, self.current_term

        if self.state == 'LEADER':
            logging.info(f"Node {self.node_id} rejected Pre-Vote for {candidate_id}: I am already a LEADER for term {self.current_term}")
            return False, self.current_term;

        my_last_log_term = self.log[-1]['term'] if self.log else 0
        my_last_log_index = len(self.log) - 1 if self.log else -1

        log_ok = (candidate_last_log_term > my_last_log_term) or \
                (candidate_last_log_term == my_last_log_term and \
                 candidate_last_log_index >= my_last_log_index)

        has_valid_leader = (self.leader_id is not None and \
                          (time.time() - self.last_valid_leader_heartbeat_time) < self.election_timeout)

        recent_heartbeat = False
        if hasattr(self, 'election_timer_start_time') and self.election_timer_start_time > 0:
            time_since_timer_reset = time.time() - self.election_timer_start_time
            recent_heartbeat = time_since_timer_reset < (self.min_election_timeout_sec / 2)

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
        """
        Conducts a Pre-Vote round before potentially starting a full election.

        If a majority of peers grant the Pre-Vote, the node proceeds to a full election.
        Otherwise, it remains a Follower and resets its election timer.
        """
        proposed_term = self.current_term + 1
        logging.info(f"Node {self.node_id} starting Pre-Vote round for proposed term {proposed_term}.")

        my_last_log_term = self.log[-1]['term'] if self.log else 0
        my_last_log_index = len(self.log) - 1 if self.log else -1

        pre_vote_granted_count = 0

        if not self.peers:
            logging.info(f"Node {self.node_id} is a single-node cluster. Proceeding directly to election for term {proposed_term}.")
            self._start_actual_election(proposed_term)
            return

        total_nodes = len(self.peers) + 1
        required_pre_votes_from_others = total_nodes // 2

        for peer_id in self.peers:
            if peer_id == self.node_id: continue
            try:
                logging.debug(f"Node {self.node_id} sending PreVote to {peer_id} for term {proposed_term}")
                response = self.peer_clients[peer_id].pre_vote(
                    candidate_id=self.node_id,
                    term=proposed_term,
                    last_log_index=my_last_log_index,
                    last_log_term=my_last_log_term,
                    timeout=0.4
                )
                if response:
                    vote_granted, peer_term_response = response
                    if vote_granted:
                        pre_vote_granted_count += 1
                        logging.info(f"Node {self.node_id} received affirmative PreVote from {peer_id} for term {proposed_term}. Granted by others: {pre_vote_granted_count}/{required_pre_votes_from_others}.")

                    if peer_term_response > self.current_term:
                         logging.warning(f"Node {self.node_id} detected higher term {peer_term_response} from {peer_id} during PreVote response. Current term {self.current_term}. Will step down if an RPC with this term arrives.")
                else:
                    logging.warning(f"Node {self.node_id} received no PreVote response from {peer_id} for term {proposed_term}.")
            except Exception as e:
                logging.error(f"Node {self.node_id} error sending PreVote to {peer_id}: {e}", exc_info=False)

        logging.info(f"Node {self.node_id} Pre-Vote round for term {proposed_term} ended. Granted by others: {pre_vote_granted_count}, Required from others: {required_pre_votes_from_others} (Total nodes: {total_nodes})")

        if pre_vote_granted_count >= required_pre_votes_from_others:
            logging.info(f"Node {self.node_id} received sufficient PreVotes ({pre_vote_granted_count} >= {required_pre_votes_from_others}). Proceeding to actual election for term {proposed_term}.")
            self._start_actual_election(proposed_term)
        else:
            logging.info(f"Node {self.node_id} did not receive sufficient PreVotes for term {proposed_term} ({pre_vote_granted_count} < {required_pre_votes_from_others}). Remaining FOLLOWER. Resetting election timer.")
            self.start_election_timer()

    def _start_actual_election(self, election_term: int):
        """
        Starts a full Raft election for a given term.

        Called after a successful Pre-Vote round or directly in single-node mode.
        Transitions the node to the Candidate state, votes for itself, and sends
        RequestVote RPCs to peers.

        Args:
            election_term: The term for which the election is being held.
        """
        self.state = 'CANDIDATE'
        self.current_term = election_term
        self.voted_for = self.node_id
        self._ensure_persistence_for_term_update()

        if self.etcd_client:
            try:
                logger_raft.info(f"Node {self.node_id} updating role to CANDIDATE in etcd.")
                self.etcd_client.update_role(CANDIDATE)
            except Exception as e:
                logger_raft.error(f"Node {self.node_id} (CANDIDATE) failed to update etcd: {e}", exc_info=True)
        logging.info(f"Node {self.node_id} became CANDIDATE for term {self.current_term}. Voted for self, state persisted.")

        self.start_election_timer()

        my_last_log_term = self.log[-1]['term'] if self.log else 0
        my_last_log_index = len(self.log) - 1 if self.log else -1

        votes_obtained = 1
        total_nodes = len(self.peers) + 1
        required_for_majority = (total_nodes // 2) + 1

        logging.info(f"Node {self.node_id} (CANDIDATE, term {self.current_term}) sending RequestVote RPCs. Need {required_for_majority} votes from {total_nodes} total nodes.")

        for peer_id in self.peers:
            if peer_id == self.node_id: continue

            if self.state != 'CANDIDATE' or self.current_term != election_term:
                logging.info(f"Node {self.node_id} election for term {election_term} aborted. Current state: {self.state}, term: {self.current_term} (expected CANDIDATE, {election_term}).")
                return

            try:
                logging.debug(f"Node {self.node_id} sending RequestVote to {peer_id} for term {self.current_term}")
                response = self.peer_clients[peer_id].request_vote(
                    candidate_id=self.node_id,
                    term=self.current_term,
                    last_log_index=my_last_log_index,
                    last_log_term=my_last_log_term
                )
                if response:
                    vote_granted, peer_term_response = response
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
                            self.step_down(peer_term_response)
                            return
                else:
                    logging.warning(f"Node {self.node_id} received no RequestVote response from {peer_id} for term {self.current_term}.")
            except Exception as e:
                logging.error(f"Node {self.node_id} error sending RequestVote to {peer_id}: {e}", exc_info=False)


            if self.state != 'CANDIDATE' or self.current_term != election_term:
                logging.info(f"Node {self.node_id} election for term {election_term} aborted after RPC to {peer_id}. Current state: {self.state}, term: {self.current_term}.")
                return
            if votes_obtained >= required_for_majority:
                break

        if self.state == 'CANDIDATE' and self.current_term == election_term:
            if votes_obtained >= required_for_majority:
                logging.info(f"Node {self.node_id} ELECTED LEADER for term {self.current_term} with {votes_obtained} votes!")
                self.become_leader()
            else:
                logging.info(f"Node {self.node_id} did not win election for term {self.current_term}. Received {votes_obtained}/{required_for_majority} votes. Remaining CANDIDATE.")


    def _ensure_persistence_for_term_update(self):
        """
        Ensures the current_term and voted_for are marked for persistence.

        This method is a placeholder for interacting with the actual persistence
        layer to save critical Raft state before certain operations.
        """
        state_dict = self.get_raft_core_state_dict()

        logging.debug(f"Node {self.node_id} (simulated) persisting state: term={self.current_term}, voted_for={self.voted_for}")


    def get_raft_core_state_dict(self) -> Dict[str, Any]:
        """
        Returns a dictionary representation of the core Raft state.

        This dictionary is used for persisting the node's state.

        Returns:
            A dictionary containing the current term, voted for, log, commit index, and last applied index.
        """
        return {
            'current_term': self.current_term,
            'voted_for': self.voted_for,
            'log': self.log,
            'commit_index': self.commit_index,
            'last_applied': self.last_applied
        }

    def request_vote(self, candidate_id: str, term: int, last_log_index: int, last_log_term: int) -> Tuple[bool, int]:
        """
        Handles RequestVote RPC requests from other nodes.

        Grants a vote if the candidate's term is at least as high as the node's
        current term, the node has not already voted in this term (or voted for
        this candidate), and the candidate's log is at least as up-to-date.
        Includes leader stickiness logic.

        Args:
            candidate_id: The ID of the candidate requesting the vote.
            term: Candidate's term.
            last_log_index: Index of candidate's last log entry.
            last_log_term: Term of candidate's last log entry.

        Returns:
            A tuple: (vote_granted, current_term) where vote_granted is a boolean
            indicating if the vote was granted and current_term is the receiver's
            current term after processing the request.
        """
        try:
            candidate_term = int(term)
        except ValueError:
            logging.error(f"Node {self.node_id} received non-integer term '{term}' in request_vote from {candidate_id}. Rejecting vote.")
            return (False, int(self.current_term))

        local_current_term = int(self.current_term)
        vote_granted = False

        if candidate_term < local_current_term:
            logging.debug(f"Node {self.node_id} (term {local_current_term}) rejecting vote for {candidate_id} (term {candidate_term}): candidate term too low.")
            return (False, local_current_term)

        if candidate_term > local_current_term:
            logging.info(f"Node {self.node_id} (term {local_current_term}) received vote request from {candidate_id} with higher term {candidate_term}. Stepping down.")
            self.step_down(candidate_term)
            local_current_term = self.current_term

        if self.state == 'LEADER' and candidate_term == local_current_term:
            logging.warning(f"Node {self.node_id} is already LEADER for term {local_current_term}. "
                           f"Rejecting vote request from {candidate_id} in same term to prevent multiple leaders.")
            return (False, local_current_term)

        if candidate_term == local_current_term and \
           self.state == 'FOLLOWER' and \
           self.leader_id is not None and \
           hasattr(self, 'election_timer_start_time') and \
           self.election_timer_start_time > 0:

            time_since_timer_reset = time.time() - self.election_timer_start_time;
            if time_since_timer_reset < self.min_election_timeout_sec:
                logging.info(f"Node {self.node_id} (term {local_current_term}, current leader: {self.leader_id}, state: {self.state}) "
                             f"rejecting vote for {candidate_id} (term {candidate_term}) due to leader stickiness. "
                             f"Election timer started {time_since_timer_reset:.3f}s ago (stickiness threshold: {self.min_election_timeout_sec:.3f}s).")
                return (False, local_current_term)

        if self.voted_for is None or self.voted_for == candidate_id:
            my_last_log_term = self.log[-1]['term'] if self.log else 0
            my_last_log_index = len(self.log) - 1 if self.log else -1

            candidate_log_is_newer_or_same_term_longer = (
                last_log_term > my_last_log_term or
                (last_log_term == my_last_log_term and last_log_index >= my_last_log_index)
            )

            if candidate_log_is_newer_or_same_term_longer:
                if self.voted_for is None:
                    logging.info(f"Node {self.node_id} (term {local_current_term}, state: {self.state}) voting for {candidate_id} (term {candidate_term}). "
                                 f"My log: (idx={my_last_log_index}, term={my_last_log_term}). Cand log: (idx={last_log_index}, term={last_log_term}).")
                self.voted_for = candidate_id
                self._ensure_persistence_for_term_update()
                self.start_election_timer()
                vote_granted = True
            else:
                logging.info(f"Node {self.node_id} (term {local_current_term}, state: {self.state}) rejecting vote for {candidate_id} (term {candidate_term}): candidate log not up-to-date. "
                             f"My log: (idx={my_last_log_index}, term={my_last_log_term}). Cand log: (idx={last_log_index}, term={last_log_term}).")
        else:
            logging.info(f"Node {self.node_id} (term {local_current_term}, state: {self.state}) rejecting vote for {candidate_id} (term {candidate_term}): already voted for {self.voted_for} in this term.")

        return (vote_granted, int(local_current_term))

    def append_entries(self, leader_id: str, term: int, prev_log_index: int,
                      prev_log_term: int, entries: list, leader_commit: int) -> Tuple[bool, int]:
        """
        Handles AppendEntries RPC requests from the leader.

        Receives log entries (or heartbeats), performs log consistency checks,
        appends new entries, and updates the commit index if necessary.

        Args:
            leader_id: The ID of the leader.
            term: Leader's term.
            prev_log_index: Index of the log entry immediately preceding the new ones.
            prev_log_term: Term of the prev_log_index entry.
            entries: A list of log entries to store (empty for heartbeats).
            leader_commit: Leader's commit index.

        Returns:
            A tuple: (success, current_term) indicating if the request was successful
            and the receiver's current term.
        """
        try:
            leader_term = int(term)
            local_current_term = int(self.current_term)
            prev_log_index = int(prev_log_index)
            leader_commit = int(leader_commit)
        except (ValueError, TypeError) as e:
            logging.error(f"Type conversion error in append_entries: {e}, got term={term}, prev_log_index={prev_log_index}, leader_commit={leader_commit}")
            return False, int(self.current_term)

        if leader_term < local_current_term:
            logging.debug(f"Node {self.node_id}: Rejecting append_entries, leader term {leader_term} < current term {local_current_term}")
            return False, local_current_term

        if leader_term > local_current_term:
            logging.info(f"Node {self.node_id} received higher term {leader_term} from leader {leader_id} (current term: {local_current_term})")
            self.step_down(leader_term)
            local_current_term = self.current_term

        if leader_term >= local_current_term:
            if self.leader_id != leader_id:
                logging.info(f"Node {self.node_id} recognizing {leader_id} as leader for term {leader_term}")
            self.leader_id = leader_id
            self.last_heartbeat = time.time()
            self.last_valid_leader_heartbeat_time = time.time()
            self.start_election_timer()
        else:
            logging.warning(f"Node {self.node_id} rejecting append_entries from {leader_id} with term {leader_term} (current term: {local_current_term})")
            return False, local_current_term

        if prev_log_index > 0:
            if prev_log_index >= len(self.log):
                logging.debug(f"Node {self.node_id}: AppendEntries: prev_log_index ({prev_log_index}) out of bounds (log_len {len(self.log)})")
                return False, int(self.current_term)
            if self.log[prev_log_index]['term'] != prev_log_term:
                logging.debug(f"Node {self.node_id}: AppendEntries: term mismatch at prev_log_index ({prev_log_index}). Expected term {prev_log_term}, got {self.log[prev_log_index]['term']}")
                self.log = self.log[:prev_log_index]
                return False, int(self.current_term)

        if entries:
            append_start_index = prev_log_index + 1

            if append_start_index < len(self.log):
                i = 0
                while i < len(entries) and (append_start_index + i) < len(self.log):
                    if self.log[append_start_index + i]['term'] != entries[i]['term']:
                        self.log = self.log[:append_start_index + i]
                        logging.debug(f"Node {self.node_id}: AppendEntries: Truncating log at index {append_start_index + i} due to term conflict.")
                        break
                    i += 1
                self.log.extend(entries[i:])
                logging.debug(f"Node {self.node_id}: Appended {len(entries[i:])} entries starting from index {append_start_index + i}. New log length: {len(self.log)}")
            else:
                 self.log.extend(entries)
                 logging.debug(f"Node {self.node_id}: Appended {len(entries)} entries. New log length: {len(self.log)}")

        if leader_commit > self.commit_index:
            self.commit_index = min(leader_commit, len(self.log) - 1)
            logging.debug(f"Node {self.node_id}: Updated commit index to {self.commit_index}")
            self.apply_entries()

        return True, int(self.current_term)

    def take_snapshot(self):
        """
        Takes a snapshot of the state machine's state and the latest log metadata.

        Saves the snapshot to persistent storage and truncates the log up to the
        last applied index.
        """
        logger_raft.info(f"Node {self.node_id}: Attempting to take snapshot. Last applied index: {self.last_applied}")

        if self.last_applied < 0:
            logger_raft.warning(f"Node {self.node_id}: Cannot take snapshot, last_applied is {self.last_applied}. Log length: {len(self.log)}")
            return

        last_included_index = self.last_applied
        last_included_term = 0

        current_log_base_index = 0
        if self.last_applied >= current_log_base_index and (self.last_applied - current_log_base_index) < len(self.log):
            found_entry = self.log[self.last_applied - current_log_base_index]
            if 'term' in found_entry:
                 last_included_term = found_entry['term']
            else:
                 logger_raft.error(f"Node {self.node_id}: Log entry at index {self.last_applied} is missing 'term'. Cannot take accurate snapshot.")
                 return
        else:
            logger_raft.error(f"Node {self.node_id}: last_applied index {self.last_applied} is not found in current log segment (length {len(self.log)}, base index {current_log_base_index}). Cannot determine last_included_term for snapshot.")
            return

        state_machine_data = None
        if self.sequencer_instance:
            try:
                state_machine_data = self.sequencer_instance.get_state()
            except Exception as e:
                logger_raft.error(f"Node {self.node_id}: Error getting state from sequencer_instance: {e}", exc_info=True)
                state_machine_data = {"error": "Failed to get state machine state"}
        else:
            logger_raft.warning(f"Node {self.node_id}: No sequencer_instance, state_machine_state will be null in snapshot.")

        snapshot_data = {
            'last_included_index': last_included_index,
            'last_included_term': last_included_term,
            'state_machine_state': state_machine_data
        }

        try:
            import os
            snapshot_dir = os.path.dirname(self.snapshot_path)
            if snapshot_dir and not os.path.exists(snapshot_dir):
                os.makedirs(snapshot_dir)
                logger_raft.info(f"Created snapshot directory: {snapshot_dir}")

            with open(self.snapshot_path, 'w') as f:
                json.dump(snapshot_data, f)
            logger_raft.info(f"Node {self.node_id}: Snapshot saved to {self.snapshot_path} up to index {last_included_index}, term {last_included_term}.")
        except IOError as e:
            logger_raft.error(f"Node {self.node_id}: IO Error saving snapshot to {self.snapshot_path}: {e}", exc_info=True)
            return

        if self.log and self.last_applied >= self.log[0]['index']:
             truncation_point = -1
             for i, entry in enumerate(self.log):
                 if entry['index'] == self.last_applied:
                     truncation_point = i + 1
                     break

             if truncation_point != -1:
                 self.log = self.log[truncation_point:]
                 self.current_log_base_index = self.last_applied + 1
                 logger_raft.info(f"Node {self.node_id}: Log truncated after index {self.last_applied}. New log length: {len(self.log)}, new base index: {self.current_log_base_index}.")
             else:
                 logger_raft.warning(f"Node {self.node_id}: Could not find log entry with index {self.last_applied} in current log segment. Truncation skipped.")
        else:
             logger_raft.info(f"Node {self.node_id}: Log not truncated (empty log or last_applied < first log entry index).")

        self.last_included_index = last_included_index
        self.last_included_term = last_included_term

        self.persistent_state['commit_index'] = self.commit_index

    def load_snapshot(self):
        """
        Loads the latest snapshot from persistent storage if available.

        Updates the node's state (last_applied, commit_index, term, log base index)
        based on the snapshot and prepares the state machine data for application.
        """
        try:
            with open(self.snapshot_path, 'r') as f:
                snapshot_data = json.load(f)

            self.last_applied = snapshot_data['last_included_index']
            self.commit_index = max(self.commit_index, self.last_applied)
            self.current_term = max(self.current_term, snapshot_data.get('last_included_term', self.current_term))

            self.last_included_index = snapshot_data['last_included_index']
            self.last_included_term = snapshot_data['last_included_term']
            self.current_log_base_index = self.last_included_index + 1

            self._pending_state_machine_state = snapshot_data.get('state_machine_state')

            self.log = []
            logger_raft.info(f"Node {self.node_id}: Loaded snapshot. last_applied set to {self.last_applied}, term to {self.current_term}. Log cleared. Pending state machine state captured.")
        except FileNotFoundError:
            logger_raft.info(f"Node {self.node_id}: No snapshot found at {self.snapshot_path}, starting with empty/current state.")
            self._pending_state_machine_state = None
            self.last_included_index = -1
            self.last_included_term = 0
            self.current_log_base_index = 0
        except (IOError, json.JSONDecodeError, KeyError) as e:
            logger_raft.error(f"Node {self.node_id}: Error loading or parsing snapshot from {self.snapshot_path}: {e}", exc_info=True)
            self._pending_state_machine_state = None
            self.last_included_index = -1
            self.last_included_term = 0
            self.current_log_base_index = 0

    def become_leader(self) -> bool:
        """
        Transitions the node to the Leader state.

        Initializes leader-specific state, publishes leadership info to etcd,
        appends a no-op entry, and starts periodic AppendEntries RPCs.

        Returns:
            True if the transition was successful, False otherwise.
        """
        if self.state != 'CANDIDATE':
            logging.warning(f"Node {self.node_id} attempted to become leader while in {self.state} state (not CANDIDATE). Aborting leadership transition.")
            return False

        if self.leader_id is not None and self.leader_id != self.node_id:
            last_leader_contact = time.time() - self.last_valid_leader_heartbeat_time
            if last_leader_contact < self.election_timeout:
                logging.warning(f"Node {self.node_id} attempted to become leader but detected a recent leader {self.leader_id} "
                              f"({last_leader_contact:.2f}s ago, timeout: {self.election_timeout:.2f}s). Aborting leadership transition.")
                return False

        prev_state = self.state
        logging.info(f"Node {self.node_id} becoming leader for term {self.current_term} (previous state: {prev_state})")

        leader_status = {}
        for peer_id in self.peers:
            if peer_id == self.node_id: continue
            try:
                if hasattr(self.peer_clients[peer_id], 'is_connected'):
                    health_check = self.peer_clients[peer_clients[peer_id]].is_connected(timeout=0.2)
                    if health_check:
                        leader_status[peer_id] = "Connected"
                    else:
                        leader_status[peer_id] = "Not connected"
                else:
                    leader_status[peer_id] = "Connection status unknown"
            except Exception as e:
                leader_status[peer_id] = f"Error checking: {str(e)}"

        logging.info(f"Node {self.node_id} peer status before becoming leader: {leader_status}")

        self.state = 'LEADER'
        self.leader_id = self.node_id
        self.voted_for = None
        self._ensure_persistence_for_term_update()

        if self.etcd_client:
            try:
                logger_raft.info(f"Node {self.node_id} updating role to LEADER in etcd.")
                self.etcd_client.update_role(LEADER)
                logger_raft.info(f"Node {self.node_id} publishing leader info to etcd: API='{self.node_api_address}', RPC='{self.node_rpc_address_for_etcd}'.")
                max_publish_retries = 3
                publish_retries = 0
                publish_success = False
                ETCD_LEASE_TTL = app_config.get('etcd', {}).get('ttl', 30)
                while publish_retries < max_publish_retries and not publish_success:
                    try:
                        self.etcd_client.publish_leader_info(
                            leader_api_address=self.node_api_address,
                            ttl=ETCD_LEASE_TTL * 10
                        )
                        publish_success = True
                        logger_raft.info(f"Node {self.node_id} successfully published leader info to etcd.")
                    except Exception as publish_e:
                        publish_retries += 1
                        logger_raft.error(f"Node {self.node_id} (LEADER) failed to publish leader info to etcd (attempt {publish_retries}/{max_publish_retries}): {publish_e}", exc_info=True)
                        time.sleep(0.5 * publish_retries)
                if not publish_success:
                    logger_raft.error(f"Node {self.node_id} (LEADER) failed to publish leader info to etcd after {max_publish_retries} attempts. Leader key might not be set.")
            except Exception as e:
                logger_raft.error(f"Node {self.node_id} (LEADER) failed to update etcd: {e}", exc_info=True)

        self.next_index = {}
        self.match_index = {}

        leader_next_log_index = self.current_log_base_index + len(self.log)

        for peer in self.peers:
            if peer != self.node_id:
                self.next_index[peer] = leader_next_log_index
                self.match_index[peer] = self.last_included_index if hasattr(self, 'last_included_index') else -1

        total_nodes = len(self.peers) + 1
        is_configured_single_node = app_config.is_single_node_mode()
        self.single_node_mode = (total_nodes == 1) or is_configured_single_node

        if self.single_node_mode:
            logging.info(f"Single-node mode detected in become_leader (configured={is_configured_single_node})")
            if (self.current_log_base_index + len(self.log) - 1) > self.commit_index:
                 self.commit_index = self.current_log_base_index + len(self.log) - 1
                 self.apply_entries()

        self.last_heartbeat = time.time()

        no_op_entry_data = {'type': 'no-op', 'data': None}

        no_op_absolute_index = leader_next_log_index

        no_op_log_entry = {
             'index': no_op_absolute_index,
             'term': self.current_term,
             'entry': no_op_entry_data
        }
        self.log.append(no_op_log_entry)
        logging.info(f"Leader {self.node_id} appended no-op entry at index {no_op_absolute_index} for term {self.current_term}.")

        self.send_append_entries_to_peers()

        self._start_periodic_append_entries()
        return True

    def send_append_entries_to_peers(self):
        """
        Sends AppendEntries RPCs (including heartbeats or log entries) to all peers.

        This is typically called periodically by the leader's heartbeat timer
        or immediately after appending a new client command.
        """
        if self.state != 'LEADER':
            logging.warning(f"Node {self.node_id} tried to send AppendEntries but is not a leader (state: {self.state})")
            return

        if self.single_node_mode:
             return

        successful_acks_count = 0

        for peer_id in self.peers:
            if peer_id == self.node_id:
                continue

            if peer_id not in self.peer_clients or self.peer_clients[peer_id] is None:
                logging.error(f"Node {self.node_id}: No RPC client available for peer {peer_id}. Cannot send AppendEntries.")
                continue

            try:
                next_idx = self.next_index.get(peer_id, self.current_log_base_index + len(self.log))

                if next_idx < self.current_log_base_index:
                    logging.warning(f"Node {self.node_id}: Peer {peer_id} needs InstallSnapshot (next_idx {next_idx} < current_log_base_index {self.current_log_base_index}). InstallSnapshot not implemented, cannot replicate.")
                    continue

                entries_to_send = self.log[next_idx - self.current_log_base_index:]

                prev_log_index = next_idx - 1
                prev_log_term = 0

                if prev_log_index >= self.current_log_base_index:
                    prev_log_entry_in_log = self.log[prev_log_index - self.current_log_base_index]
                    prev_log_term = prev_log_entry_in_log.get('term', 0)
                elif prev_log_index == self.last_included_index:
                    prev_log_term = self.last_included_term

                logging.debug(f"Node {self.node_id} sending AppendEntries to {peer_id} (term {self.current_term}, prevIdx {prev_log_index}, prevTerm {prev_log_term}, {len(entries_to_send)} entries)")
                response = self.peer_clients[peer_id].append_entries(
                    leader_id=self.node_id,
                    term=self.current_term,
                    prev_log_index=prev_log_index,
                    prev_log_term=prev_log_term,
                    entries=entries_to_send,
                    leader_commit=self.commit_index,
                    timeout=0.4
                )

                if response is None:
                    logging.debug(f"No response from peer {peer_id} for AppendEntries")
                    continue

                success = False
                term = None

                if isinstance(response, tuple) and len(response) == 2:
                    success, term = response
                elif isinstance(response, dict) and 'success' in response and 'term' in response:
                    success = bool(response['success'])
                    term = int(response['term'])
                else:
                    logging.error(f"Could not parse append_entries response from {peer_id}: {response}")
                    continue

                if term is not None and term > self.current_term:
                    logging.info(f"Node {self.node_id} (LEADER) discovered higher term {term} from {peer_id}, stepping down.")
                    self.step_down(term)
                    return

                if success:
                    if entries_to_send:
                         new_match_index = next_idx + len(entries_to_send) - 1
                         self.match_index[peer_id] = new_match_index
                         self.next_index[peer_id] = new_match_index + 1
                         logging.debug(f"AppendEntries success to peer {peer_id}. MatchIndex updated to {self.match_index[peer_id]}, nextIndex to {self.next_index[peer_id]}.")
                    else:
                         self.next_index[peer_id] = max(self.next_index.get(peer_id, -1), prev_log_index + 1)
                         logging.debug(f"Heartbeat success to peer {peer_id}. NextIndex ensured at least {prev_log_index + 1}.")

                    successful_acks_count += 1

                else:
                    new_next_idx = max(self.current_log_base_index, self.next_index.get(peer_id, 1) - 1)
                    self.next_index[peer_id] = new_next_idx
                    logging.debug(f"AppendEntries failed for peer {peer_id}, decrementing nextIndex to {self.next_index[peer_id]}. Follower term: {term}.")

            except Exception as e:
                logging.error(f"Error sending AppendEntries to peer {peer_id}: {e}", exc_info=False)

        if self.state == 'LEADER':
             self.advance_commit_index()

             required_successful_peer_responses = (len(self.peers) + 1) // 2

             if successful_acks_count < required_successful_peer_responses:
                  logging.warning(f"Node {self.node_id} (Leader, term {self.current_term}) may be partitioned: only {successful_acks_count} successful AppendEntries responses from {len(self.peers)} peers. Required: {required_successful_peer_responses}.")


    def _start_periodic_append_entries(self):
        """
        Starts the periodic timer for sending AppendEntries RPCs (heartbeats and log replication).
        This is only run by the Leader.
        """
        if hasattr(self, 'append_entries_sender_thread') and self.append_entries_sender_thread and self.append_entries_sender_thread.is_alive():
            self.append_entries_sender_thread.cancel()

        def append_entries_task():
            if self.state == 'LEADER':
                try:
                    self.send_append_entries_to_peers()
                except Exception as e:
                    logging.error(f"Node {self.node_id} error during periodic AppendEntries task: {e}", exc_info=True)

                if self.state == 'LEADER':
                    self.append_entries_sender_thread = threading.Timer(self.heartbeat_interval_sec, append_entries_task)
                    self.append_entries_sender_thread.daemon = True
                    self.append_entries_sender_thread.start()
            else:
                logging.info(f"Node {self.node_id} periodic AppendEntries task running, but no longer LEADER. Stopping.")

        if self.state == 'LEADER':
            logging.info(f"Node {self.node_id} (LEADER) starting periodic AppendEntries every {self.heartbeat_interval_sec:.3f}s.")
            self.append_entries_sender_thread = threading.Timer(self.heartbeat_interval_sec, append_entries_task)
            self.append_entries_sender_thread.daemon = True
            self.append_entries_sender_thread.start()
        else:
            logging.warning(f"Node {self.node_id} attempted to start periodic AppendEntries but is not LEADER (state: {self.state}).")

    def step_down(self, term: int):
        """
        Transitions the node back to the Follower state.

        This happens when the node discovers a higher term or loses an election.

        Args:
            term: The term that caused the node to step down.
        """
        prev_state = self.state
        prev_term = self.current_term

        if term > self.current_term:
            self.current_term = int(term)
            self.state = 'FOLLOWER'
            self.voted_for = None
            self._ensure_persistence_for_term_update()

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

            if prev_state == 'LEADER' and hasattr(self, 'append_entries_sender_thread') and self.append_entries_sender_thread:
                if self.append_entries_sender_thread.is_alive():
                    self.append_entries_sender_thread.cancel()
                self.append_entries_sender_thread = None
                logging.info(f"Node {self.node_id} (was LEADER) cancelled periodic AppendEntries.")

            logging.info(f"Node {self.node_id} stepping down from {prev_state} (term {prev_term}) to FOLLOWER (term {self.current_term}) due to higher term {term}.")
        elif term < self.current_term:
            logging.warning(f"Node {self.node_id} received a step down request with a lower term {term} than current {self.current_term}. Ignoring.")
        else:
            if self.state in ('CANDIDATE', 'LEADER'):
                logging.info(f"Node {self.node_id} stepping down from {prev_state} (term {prev_term}) to FOLLOWER (term {self.current_term}) in the same term.")
                self.state = 'FOLLOWER'
                self.voted_for = None
                self._ensure_persistence_for_term_update()

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


    def replicate_log(self):
        """
        Triggers the sending of log entries (or heartbeats) to followers.
        """
        self.send_append_entries_to_peers()

    def propose_command(self, command: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        """
        Proposes a new command to the Raft cluster.

        Only the leader can propose commands. The command is appended to the
        leader's log and then replicated to followers.

        Args:
            command: A dictionary representing the command to propose.

        Returns:
            A tuple: (success, error_message). Success is True if the command
            was accepted by the leader, False otherwise. error_message is
            None on success, or a string describing the error.
        """
        if self.state != LEADER:
            logger_raft.warning(f"Node {self.node_id}: Received propose_command but not leader. Current state: {self.state}")
            return False, "NOT_LEADER"

        new_entry = {
            "term": self.current_term,
            "command": command,
            "index": self.current_log_base_index + len(self.log)
        }
        self.log.append(new_entry)
        logging.info(f"Node {self.node_id}: Appended new command to log at index {new_entry['index']} for term {new_entry['term']}. Log length: {len(self.log)}")

        self.send_append_entries_to_peers()

        return True, None

    def apply_entries(self):
        """
        Applies committed log entries to the state machine (sequencer_instance).

        Entries from last_applied + 1 up to commit_index are applied sequentially.
        Triggers snapshotting if the snapshot entry interval is reached.
        """
        while self.last_applied < self.commit_index:
            next_apply_absolute_index = self.last_applied + 1

            index_in_current_log = next_apply_absolute_index - self.current_log_base_index

            if index_in_current_log < 0 or index_in_current_log >= len(self.log):
                logger_raft.error(f"Node {self.node_id}: Cannot apply log entry at absolute index {next_apply_absolute_index}. Index {index_in_current_log} out of bounds for current log segment (length {len(self.log)}, base index {self.current_log_base_index}). Commit index: {self.commit_index}. This may indicate a bug.")
                break

            entry_to_apply = self.log[index_in_current_log]
            command_to_apply = entry_to_apply.get('command') or entry_to_apply.get('entry')

            if isinstance(command_to_apply, dict) and command_to_apply.get('type') == 'no-op':
                 logger_raft.debug(f"Node {self.node_id}: Skipping application of no-op entry at absolute index {next_apply_absolute_index} (log index {index_in_current_log}).")
                 self.last_applied = next_apply_absolute_index
                 continue

            if command_to_apply is None:
                logger_raft.warning(f"Node {self.node_id}: Log entry at absolute index {next_apply_absolute_index} (log index {index_in_current_log}) missing 'entry' or 'command' field: {entry_to_apply}. Skipping application.")
                self.last_applied = next_apply_absolute_index
                continue

            if self.sequencer_instance:
                logger_raft.info(f"Node {self.node_id}: Applying log entry at absolute index={next_apply_absolute_index} to state machine.")
                try:
                    result = self.sequencer_instance.record_entry(command_to_apply, next_apply_absolute_index)
                    if result is None or (isinstance(result, dict) and result.get('status') == 'error'):
                        error_detail = result.get('message', 'Unknown state machine error') if isinstance(result, dict) else 'None result from sequencer'
                        logger_raft.error(f"Node {self.node_id}: State machine failed to apply command at absolute log index {next_apply_absolute_index}. Command: {command_to_apply}. Error: {error_detail}. Halting further application in this cycle.")
                        break
                    else:
                        logger_raft.info(f"Node {self.node_id}: Successfully applied command at absolute log index {next_apply_absolute_index}. Result: {result}")
                        self.last_applied = next_apply_absolute_index
                except Exception as e:
                    logger_raft.error(f"Node {self.node_id}: Exception applying command at absolute log index {next_apply_absolute_index}: {e}. Command: {command_to_apply}. Halting further application.", exc_info=True)
                    break
            else:
                logger_raft.warning(f"Node {self.node_id}: No sequencer_instance. Cannot apply log entry at absolute index {next_apply_absolute_index}. Entries will accumulate until a sequencer is available.")
                break

            if self.snapshot_entry_interval > 0 and self.last_applied > 0 and (self.last_applied % self.snapshot_entry_interval == 0):
                logger_raft.info(f"Node {self.node_id}: Reached snapshot interval at last_applied={self.last_applied}. Triggering snapshot.")
                self.take_snapshot()

    def append_entry(self, entry_data: Dict[str, Any]) -> int:
        """
        Appends a new entry (command) to the leader's log.

        This method is called by the leader when a client request needs to be
        added to the replicated log.

        Args:
            entry_data: The data representing the command to append.

        Returns:
            The absolute index of the newly appended entry in the log.

        Raises:
            NotLeaderException: If called on a node that is not the leader.
        """
        if self.state != 'LEADER':
            raise NotLeaderException(f"Cannot append entry - not leader (current state: {self.state}, current leader: {self.leader_id})")

        new_absolute_index = self.current_log_base_index + len(self.log)

        log_entry = {
            'index': new_absolute_index,
            'term': self.current_term,
            'entry': entry_data
        }

        self.log.append(log_entry)

        logging.debug(f"Leader {self.node_id} appended entry at absolute index {new_absolute_index}, term {self.current_term}: {entry_data}")

        if self.single_node_mode:
            self.commit_index = new_absolute_index
            self.apply_entries()

        if isinstance(entry_data, dict) and entry_data.get('type') != 'no-op':
             logging.debug(f"Node {self.node_id} appended client request entry at index {new_absolute_index}, triggering immediate replication.")
             self.send_append_entries_to_peers()

        return new_absolute_index

    def advance_commit_index(self):
        """
        Attempts to advance the commit index based on the match indices of the peers.

        The commit index is advanced to the highest index for which a majority
        of nodes (including the leader) are known to have replicated the entry
        and whose term matches the leader's current term.
        """
        if self.state != 'LEADER':
            return

        if self.single_node_mode:
            last_log_absolute_index = self.current_log_base_index + len(self.log) - 1
            if last_log_absolute_index > self.commit_index:
                self.commit_index = last_log_absolute_index
                self.apply_entries()
            return

        leader_last_log_absolute_index = self.current_log_base_index + len(self.log) - 1 if self.log else self.current_log_base_index - 1
        match_indices = list(self.match_index.values()) + [leader_last_log_absolute_index]

        match_indices.sort()

        total_nodes = len(self.peers) + 1
        majority_count = (total_nodes // 2) + 1

        if len(match_indices) < majority_count:
             logging.debug(f"Cannot advance commit index: not enough match indices ({len(match_indices)} < {majority_count}).")
             return

        new_potential_commit_index = match_indices[len(match_indices) - majority_count]

        if new_potential_commit_index > self.commit_index:
            index_in_current_log = new_potential_commit_index - self.current_log_base_index

            if index_in_current_log >= 0 and index_in_current_log < len(self.log):
                 entry_at_potential_commit = self.log[index_in_current_log]
                 if entry_at_potential_commit.get('term') == self.current_term:
                      logging.info(f"Advancing commit index from {self.commit_index} to {new_potential_commit_index} (entry term {self.current_term})")
                      self.commit_index = new_potential_commit_index
                      self.apply_entries()
                 else:
                      logging.debug(f"Potential commit index {new_potential_commit_index} (term {entry_at_potential_commit.get('term')}) is not from current term {self.current_term}. Cannot commit yet.")
            elif new_potential_commit_index <= self.last_included_index:
                 logging.info(f"Advancing commit index from {self.commit_index} to {new_potential_commit_index} (index is within snapshot range).")
                 self.commit_index = new_potential_commit_index
                 self.apply_entries()
            else:
                logging.error(f"Node {self.node_id}: Potential commit index {new_potential_commit_index} is outside current log segment (base {self.current_log_base_index}, len {len(self.log)}) and after snapshot (last_included {self.last_included_index}). Cannot commit.")


    def get_leader(self) -> Optional[str]:
        """
        Returns the ID of the known leader.

        Returns:
            The ID of the leader node, or None if no leader is known.
        """
        return self.leader_id

    def forward_to_leader(self, room_id, user_id, content, msg_type):
        """
        Forwards a message-related client request to the current leader.

        Args:
            room_id: The ID of the room.
            user_id: The ID of the user.
            content: The message content.
            msg_type: The message type.

        Returns:
            A dictionary containing the response from the leader, or an error message.
        """
        leader_id = self.get_leader()
        if leader_id is None:
            logging.warning(f"Node {self.node_id}: Cannot forward request, no leader known.")
            return {"status": "error", "message": "No leader known"}

        if leader_id == self.node_id:
             logging.warning(f"Node {self.node_id}: Attempted to forward request to self (leader). Processing locally.")
             return self.record_entry({'type': 'SEND_MESSAGE', 'data': {'room_id': room_id, 'user_id': user_id, 'content': content, 'msg_type': msg_type}})

        leader_address = app_config.get_node_address(leader_id)
        if not leader_address:
            logging.error(f"Node {self.node_id}: Could not find address for leader {leader_id} in config.")
            return {"status": "error", "message": f"Could not find address for leader {leader_id}"}

        leader_client = self.peer_clients.get(leader_id)
        if leader_client is None:
             logging.error(f"Node {self.node_id}: No RPC client initialized for leader {leader_id}.")
             return {"status": "error", "message": f"No RPC client for leader {leader_id}"}

        logging.info(f"Node {self.node_id} forwarding request to leader {leader_id} at {leader_address}")
        try:
            command_data = {'type': 'SEND_MESSAGE', 'data': {'room_id': room_id, 'user_id': user_id, 'content': content, 'msg_type': msg_type}}
            response = leader_client._make_request("record_entry", command_data)

            if response is None:
                 logging.warning(f"Node {self.node_id}: Leader {leader_id} did not respond to forwarded request.")
                 return {"status": "error", "message": f"Leader {leader_id} did not respond"}

            if isinstance(response, dict) and 'status' in response:
                 logging.info(f"Node {self.node_id}: Forwarded request handled by leader {leader_id}. Response: {response['status']}")
                 return response
            else:
                 logging.error(f"Node {self.node_id}: Unexpected response format from leader {leader_id} for forwarded request: {response}")
                 return {"status": "error", "message": f"Unexpected response from leader {leader_id}"}

        except Exception as e:
            logging.error(f"Node {self.node_id} error forwarding request to leader {leader_id}: {e}", exc_info=True)
            return {"status": "error", "message": f"Failed to forward request to leader {leader_id}: {str(e)}"}

    def run(self):
        """
        Starts the main loop of the Raft node.

        This loop keeps the node alive and ensures that background timers (for
        elections and AppendEntries) are running and applying committed entries.
        """
        logging.info(f"Node {self.node_id} starting main Raft loop in state: {self.state}")

        while True:
            try:
                if self.state in ('FOLLOWER', 'CANDIDATE'):
                     if not hasattr(self, 'election_timer_thread') or not self.election_timer_thread or not self.election_timer_thread.is_alive():
                         logging.warning(f"Node {self.node_id} ({self.state}) detected election timer not running, restarting.")
                         self.start_election_timer()

                if self.state == 'LEADER' and not self.single_node_mode:
                     if not hasattr(self, 'append_entries_sender_thread') or not self.append_entries_sender_thread or not self.append_entries_sender_thread.is_alive():
                         logging.warning(f"Node {self.node_id} (LEADER) detected periodic AppendEntries sender not running, restarting.")
                         self._start_periodic_append_entries()

                self.apply_entries()

                time.sleep(0.1)

            except Exception as e:
                logging.error(f"Error in Raft main loop for node {self.node_id}: {e}")
                logging.error(traceback.format_exc())
                time.sleep(1.0)

    def close(self):
        """
        Gracefully shuts down the RaftNode.

        Stops all background threads and deregisters the node from etcd.
        """
        logger_raft.info(f"Node {self.node_id}: Initiating graceful shutdown.")

        if hasattr(self, 'election_timer_thread') and self.election_timer_thread is not None:
            logger_raft.debug(f"Node {self.node_id}: Cancelling election timer during shutdown.")
            self.election_timer_thread.cancel()
            try:
                self.election_timer_thread.join(timeout=0.2)
            except RuntimeError:
                logger_raft.debug(f"Node {self.node_id}: Election timer join failed, possibly already stopped or not started.")
            except Exception as e:
                logger_raft.error(f"Node {self.node_id}: Error joining election timer thread during shutdown: {e}", exc_info=True)


        if hasattr(self, 'heartbeat_sender_thread') and self.heartbeat_sender_thread is not None and self.heartbeat_sender_thread.is_alive():
            logger_raft.debug(f"Node {self.node_id}: Waiting for heartbeat sender thread to terminate...")
            self.heartbeat_sender_thread.join(timeout=max(1.0, self.heartbeat_interval_sec * 2))
            if self.heartbeat_sender_thread.is_alive():
                logger_raft.warning(f"Node {self.node_id}: Heartbeat sender thread did not terminate in time.")
            else:
                logger_raft.info(f"Node {self.node_id}: Heartbeat sender thread terminated.")

        if self.etcd_client:
            logger_raft.info(f"Node {self.node_id}: Shutting down EtcdClient component.")
            try:
                if hasattr(self.etcd_client, 'shutdown') and callable(getattr(self.etcd_client, 'shutdown')):
                    self.etcd_client.shutdown()
                    logger_raft.info(f"Node {self.node_id}: EtcdClient.shutdown() called.")
                else:
                    logger_raft.warning(f"Node {self.node_id}: EtcdClient does not have a 'shutdown' method. Calling deregister and stop_all_heartbeats manually.")
                    self.etcd_client.deregister_node(self.node_id)
                    self.etcd_client.stop_all_heartbeats()
                    if hasattr(self.etcd_client, 'close') and callable(getattr(self.etcd_client, 'close')):
                        self.etcd_client.close()
                    logger_raft.info(f"Node {self.node_id}: Manually handled EtcdClient cleanup (deregister, stop heartbeats, close).")
            except Exception as e:
                logger_raft.error(f"Node {self.node_id}: Error during EtcdClient shutdown/cleanup: {e}", exc_info=True)


        logger_raft.info(f"Node {self.node_id}: Shutdown sequence completed.")


class NotLeaderException(Exception):
    """Custom exception raised when a non-leader node receives a client request that must go to the leader."""
    pass

class PersistentSequencer:
    """
    A state machine implementation that uses a persistent database (via SQLAlchemy)
    to store the application state (rooms, messages).
    """
    def __init__(self):
        """
        Initializes the PersistentSequencer.

        Assumes that the database initialization (init_db) has been called
        globally before creating instances of this class.
        """
        logger_raft.info("PersistentSequencer initialized. Database tables should be pre-initialized.")

    def record_entry(self, command: Dict[str, Any], log_index: int) -> Optional[Dict[str, Any]]:
        """
        Applies a command from a committed Raft log entry to the database state.

        Ensures that commands are applied atomically and idempotently if possible.
        Uses the log_index as the authoritative sequence number for ordering.

        Args:
            command: A dictionary representing the command to apply.
                     Expected format: {'type': 'COMMAND_TYPE', 'data': {...}}
            log_index: The absolute Raft log index of this command, serving as
                       the sequence number for the applied state change.

        Returns:
            A dictionary representing the outcome or the created/modified resource,
            or None if the command failed or is unknown. The format should match
            what the client API might expect as a result.
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
                creator_id = data.get('created_by_user_id') # Corrected key to match Room model

                if not room_name or not creator_id:
                    logger_raft.error(f"CREATE_ROOM: Missing name or creator_id in command data at log_index {log_index}. Data: {data}")
                    return {"status": "error", "message": "Invalid CREATE_ROOM command data"}

                # Check if a room with this sequence number (log_index) already exists for idempotency
                existing_room = db.query(Room).filter(Room.sequence_number == log_index).first()
                if existing_room:
                    logger_raft.warning(f"CREATE_ROOM: Room with sequence number {log_index} already exists. Skipping application for command: {room_name}.")
                    return {"status": "skipped", "message": "Command already applied", "id": existing_room.id}


                new_room = Room(
                    id=data.get('id', str(uuid.uuid4())), # Use provided ID if available, otherwise generate
                    name=room_name,
                    created_by_user_id=creator_id,
                    sequence_number=log_index,
                    created_at=datetime.fromtimestamp(data.get('timestamp', time.time())) if data.get('timestamp') else datetime.utcnow() # Use provided timestamp or current time
                )
                db.add(new_room)
                db.commit()
                db.refresh(new_room)
                logger_raft.info(f"Room '{room_name}' created with ID {new_room.id} by user {creator_id} (log_index/seq: {log_index}).")

                return {"status": "success", "action": "room_created", "id": new_room.id, "name": new_room.name, "created_by_user_id": new_room.created_by_user_id, "sequence_number": new_room.sequence_number}

            elif command_type == 'SEND_MESSAGE':
                room_id = data.get('room_id')
                user_id = data.get('user_id')
                content = data.get('content')
                message_id = data.get('id') # Message ID generated by API server
                message_type = data.get('msg_type')
                timestamp = data.get('timestamp')

                if room_id is None or user_id is None or content is None or message_id is None or timestamp is None:
                    logger_raft.error(f"SEND_MESSAGE: Missing required fields in command data at log_index {log_index}. Data: {data}")
                    return {"status": "error", "message": "Invalid SEND_MESSAGE command data"}

                # Check for idempotency using the log_index (sequence_number)
                existing_message = db.query(Message).filter(Message.sequence_number == log_index).first()
                if existing_message:
                     logger_raft.warning(f"SEND_MESSAGE: Message with sequence number {log_index} already exists. Skipping application for message ID: {message_id}.")
                     return {"status": "skipped", "message": "Command already applied", "id": existing_message.id, "sequence_number": existing_message.sequence_number}

                # Check for idempotency using the message ID (UUID) if the above isn't sufficient
                # (e.g., if a command gets a new log_index during leader change but has the same original ID)
                # This requires careful thought about idempotency strategy. Using log_index as primary key
                # for idempotency is standard in Raft state machines.
                # Let's stick to log_index (sequence_number) for idempotency for simplicity here.


                new_message = Message(
                    id=message_id,
                    room_id=room_id,
                    user_id=user_id,
                    content=content,
                    sequence_number=log_index,
                    message_type=message_type,
                    created_at=datetime.fromtimestamp(timestamp) if timestamp else datetime.utcnow()
                )
                db.add(new_message)
                db.commit()
                db.refresh(new_message)
                logger_raft.info(f"Message recorded in room {room_id} by user {user_id} (log_index/seq: {log_index}, msg_id: {new_message.id}).")

                return {
                    "status": "success",
                    "action": "message_sent",
                    "id": new_message.id,
                    "room_id": new_message.room_id,
                    "user_id": new_message.user_id,
                    "content": new_message.content,
                    "sequence_number": new_message.sequence_number,
                    "timestamp": new_message.created_at.isoformat() if new_message.created_at else None
                }

            elif command_type == 'no-op':
                 logger_raft.debug(f"Sequencer received NO-OP command at log_index: {log_index}. No state change.")
                 return {"status": "success", "action": "no-op", "sequence_number": log_index}

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
        Returns a snapshot of the state machine's state from the database.

        This state is used by Raft for creating snapshots to send to followers
        or save for recovery. For this implementation, it dumps the contents
        of the 'rooms' and 'messages' tables.

        Returns:
            A dictionary containing the database state, or None if state retrieval fails.
        """
        logger_raft.info("PersistentSequencer.get_state() called for snapshot.")
        db: Session = SessionLocal()
        try:
            all_rooms = [
                 {"id": r.id, "name": r.name, "created_by_user_id": r.created_by_user_id, "sequence_number": r.sequence_number, "created_at": r.created_at.timestamp() if r.created_at else None}
                 for r in db.query(Room).order_by(Room.sequence_number).all()
            ]
            all_messages = [
                 {"id": m.id, "room_id": m.room_id, "user_id": m.user_id, "content": m.content, "sequence_number": m.sequence_number, "message_type": m.message_type, "created_at": m.created_at.timestamp() if m.created_at else None}
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
            return {"error": "Failed to retrieve state from database for snapshot"}
        finally:
            db.close()

    def load_state(self, state: Optional[Dict[str, Any]]):
        """
        Loads the state machine's state from a snapshot into the database.

        This method is called when applying a snapshot. It clears the existing
        database tables managed by the sequencer and populates them with data
        from the snapshot.

        Args:
            state: A dictionary containing the state to load, typically from
                   a snapshot's 'state_machine_state'. Expected to have 'rooms'
                   and 'messages' keys.
        """
        if state is None or 'rooms' not in state or 'messages' not in state:
            logger_raft.warning("PersistentSequencer.load_state() called with invalid or empty state. Skipping load.")
            return

        logger_raft.info("PersistentSequencer.load_state() called. Loading state from snapshot.")
        db: Session = SessionLocal()
        try:
            logger_raft.warning("Clearing existing Rooms and Messages tables before loading snapshot state.")
            db.query(Message).delete()
            db.query(Room).delete()
            db.commit()
            logger_raft.info("Existing data cleared.")

            rooms_to_load = state.get('rooms', [])
            messages_to_load = state.get('messages', [])

            for room_data in rooms_to_load:
                 try:
                      new_room = Room(
                           id=room_data.get('id'),
                           name=room_data.get('name'),
                           created_by_user_id=room_data.get('created_by_user_id'),
                           sequence_number=room_data.get('sequence_number'),
                           created_at=datetime.fromtimestamp(room_data.get('created_at')) if room_data.get('created_at') is not None else None
                      )
                      db.add(new_room)
                 except Exception as e:
                      logger_raft.error(f"Error loading room from snapshot: {room_data}: {e}")
                      db.rollback()
                      logger_raft.error("Aborting snapshot load due to room loading error.")
                      return

            for msg_data in messages_to_load:
                 try:
                      new_message = Message(
                           id=msg_data.get('id'),
                           room_id=msg_data.get('room_id'),
                           user_id=msg_data.get('user_id'),
                           content=msg_data.get('content'),
                           sequence_number=msg_data.get('sequence_number'),
                           message_type=msg_data.get('message_type'),
                           created_at=datetime.fromtimestamp(msg_data.get('created_at')) if msg_data.get('created_at') is not None else None
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


class DistributedSequencer(RaftNode):
    """
    Combines the Raft consensus algorithm with the PersistentSequencer state machine.

    This class inherits from RaftNode and integrates the state machine logic
    into the Raft process, ensuring that application commands are consistently
    ordered and applied across the cluster.
    """
    def __init__(self, node_id: str, peers: list, node_address: str, rpc_port: int, sequencer: PersistentSequencer):
        """
        Initializes the DistributedSequencer node.

        Sets up the underlying Raft node and integrates the provided sequencer
        instance as the state machine. Handles single-node cluster initialization.

        Args:
            node_id: The unique identifier for this node.
            peers: A list of the string identifiers of peer nodes.
            node_address: The API address (host:port) of this node.
            rpc_port: The RPC port of this node.
            sequencer: An instance of the PersistentSequencer state machine.
        """
        from server.internal.discovery.etcd_client import EtcdClient
        from server.internal.sequencer.raft_rpc_client import RaftRPCClient
        from server.config import app_config

        peer_clients = {}
        for peer_id in peers:
            _, _, peer_host, peer_port = app_config.get_all_node_addresses().get(peer_id)
            peer_clients[peer_id] = RaftRPCClient(peer_host, peer_port)

        persistent_state = {}

        api_endpoint = f"http://{node_address}"
        rpc_endpoint = f"http://{app_config.get_current_node_rpc_host()}:{rpc_port}"

        etcd_client_instance = EtcdClient(
            etcd_endpoints=app_config.get_etcd_endpoints(),
            node_id=node_id,
            api_address=api_endpoint,
            rpc_address=rpc_endpoint
        )

        super().__init__(
            peer_clients=peer_clients,
            persistent_state=persistent_state,
            etcd_client_instance=etcd_client_instance,
            node_api_address_str=api_endpoint,
            node_rpc_address_str=rpc_endpoint,
            sequencer_instance=sequencer
        )

        self.sequencer = sequencer
        print(f"Initialized DistributedSequencer on port {rpc_port}")

        total_nodes_in_config = len(app_config.get_all_node_addresses())
        self.single_node_mode = (total_nodes_in_config == 1) or app_config.is_single_node_mode()

        if self.single_node_mode and self.state != 'LEADER':
            logging.info(f"Node {self.node_id}: Single node mode detected (total nodes {total_nodes_in_config}, explicit config {app_config.is_single_node_mode()}), attempting to become leader.")
            if self.current_term == 0:
                self.current_term = 1

            self.become_leader()


    def get_raft_node(self):
        """
        Returns the underlying Raft node instance (self).
        """
        return self

    def run(self):
        """
        Starts the main loop for the DistributedSequencer node, inheriting from RaftNode.
        """
        logging.info(f"DistributedSequencer Node {self.node_id} starting Raft loop.")
        super().run()

    def apply_entries(self):
        """
        Applies committed entries by delegating to the PersistentSequencer instance.

        This method overrides the base RaftNode's apply_entries to specifically
        interact with the configured state machine.
        """
        super().apply_entries()

    def record_entry(self, room_id=None, user_id=None, content=None, msg_type=None, entry_data=None):
        """
        Client-facing method to record a new command entry.

        If the node is not the leader, it forwards the request to the current leader.
        If the node is the leader, it appends the command to its log and initiates replication.
        Waits for the command to be committed before returning a successful response
        (providing linearizable consistency).

        Args:
            room_id: The ID of the room (used for SEND_MESSAGE and CREATE_ROOM).
            user_id: The ID of the user (used for SEND_MESSAGE and CREATE_ROOM creator).
            content: The command-specific content (e.g., message text or room name).
            msg_type: The type of the command (e.g., "SEND_MESSAGE", "CREATE_ROOM").
            entry_data: A dictionary containing the command and its data (alternative to
                        individual parameters).

        Returns:
            A dictionary with status information and relevant details (e.g., message ID,
            room ID, sequence number) upon successful commitment and application, or
            an error/timeout status if the operation fails or times out.

        Raises:
            NotLeaderException: If the node is not the leader and cannot forward the request.
            ValueError: If required parameters are missing or invalid.
        """
        if entry_data is None and room_id is not None and user_id is not None and content is not None:
            command_type = msg_type or 'text'

            if command_type == 'CREATE_ROOM':
                entry_data = {
                    'type': 'CREATE_ROOM',
                    'id': str(uuid.uuid4()),
                    'room_id': str(room_id),
                    'created_by_user_id': str(user_id),
                    'name': content,
                    'timestamp': time.time()
                }
            else:
                entry_data = {
                    'type': 'SEND_MESSAGE',
                    'id': str(uuid.uuid4()),
                    'room_id': str(room_id),
                    'user_id': str(user_id),
                    'content': content,
                    'msg_type': command_type,
                    'timestamp': time.time()
                }

        if self.state != 'LEADER':
            logging.info(f"Node {self.node_id} received request, but not leader. Forwarding to {self.leader_id}.")
            try:
                leader_response = self.forward_command_to_leader(entry_data)
                return leader_response

            except NotLeaderException as e:
                 logging.error(f"Forwarding failed due to NotLeaderException: {e}")
                 return {"status": "error", "message": "Forwarding failed, leader status uncertain."}
            except Exception as e:
                logging.error(f"Node {self.node_id} failed to forward command to leader: {e}", exc_info=True)
                return {"status": "error", "message": f"Failed to forward command to leader: {str(e)}"}

        logging.info(f"Node {self.node_id} (LEADER) receiving and processing command locally: {entry_data.get('type')}")
        try:
            absolute_log_index = self.append_entry(entry_data)

            wait_timeout = 5.0
            start_wait_time = time.time()

            logging.debug(f"Leader {self.node_id} waiting for entry at index {absolute_log_index} to be committed (current commit: {self.commit_index}).")

            while self.commit_index < absolute_log_index and (time.time() - start_wait_time) < wait_timeout:
                 time.sleep(0.01)

            if self.commit_index < absolute_log_index:
                 logging.warning(f"Leader {self.node_id}: Timed out waiting for entry at index {absolute_log_index} to be committed (commit_index: {self.commit_index}). Request may or may not eventually succeed.")
                 return {"status": "timeout", "message": "Request accepted by leader but timed out waiting for commitment", "log_index": absolute_log_index}

            db: Session = SessionLocal()
            try:
                command_type = entry_data.get('type')
                # Data needed for querying the result after application
                command_id = entry_data.get('id') # Use the command's original ID if available
                room_id = entry_data.get('room_id') # For messages/rooms
                room_name = entry_data.get('name') # For rooms
                user_id = entry_data.get('user_id') # For messages
                creator_id = entry_data.get('created_by_user_id') # For rooms

                # Re-query the database for the result based on the applied log index or other identifiers
                # The sequencer should have applied the entry using log_index as sequence number.
                # We can query the DB using this sequence number.

                if command_type == 'SEND_MESSAGE':
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
                             "timestamp": message.created_at.isoformat() if message.created_at else None
                         }
                     else:
                         logging.error(f"Leader {self.node_id}: Entry at index {absolute_log_index} committed but corresponding message not found in DB.")
                         return {"status": "error", "message": "Entry committed but failed to find result in database."}

                elif command_type == 'CREATE_ROOM':
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

                return {"status": "error", "message": f"Unknown command type after commitment: {command_type}"}
            finally:
                db.close()
        except Exception as e:
            logger_raft.error(f"Node {self.node_id} (LEADER) encountered an error processing command: {e}", exc_info=True)
            return {"status": "error", "message": f"Leader failed to process command: {str(e)}"}

    def forward_command_to_leader(self, command_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Forwards a generic command dictionary to the current leader's RPC endpoint.

        Args:
            command_data: The command dictionary to forward.

        Returns:
            The response dictionary received from the leader's RPC endpoint.

        Raises:
            NotLeaderException: If no leader is known or forwarding fails after retries.
            ValueError: If this method is called on the leader node itself (indicating a logic error).
            requests.exceptions.RequestException: If the RPC request to the leader fails.
        """
        leader_id = self.get_leader()
        if leader_id is None:
            logging.warning(f"Node {self.node_id}: Cannot forward command, no leader known.")
            raise NotLeaderException(f"Node {self.node_id}: No leader known to forward command.")

        if leader_id == self.node_id:
             logging.warning(f"Node {self.node_id}: Attempted to forward command to self (leader). Processing locally.")
             raise ValueError(f"Node {self.node_id}: Called forward_command_to_leader when node is the leader.")

        leader_address_info = app_config.get_all_node_addresses().get(leader_id)
        if not leader_address_info:
            logging.error(f"Node {self.node_id}: Could not find address for leader {leader_id} in config for command forwarding.")
            raise NotLeaderException(f"Could not find address for leader {leader_id}")

        # Use the RPC address for forwarding commands
        _, _, leader_rpc_host, leader_rpc_port = leader_address_info
        leader_rpc_client = self.peer_clients.get(leader_id)

        if leader_rpc_client is None:
             logging.error(f"Node {self.node_id}: No RPC client initialized for leader {leader_id} for command forwarding.")
             raise NotLeaderException(f"No RPC client for leader {leader_id}")

        logging.info(f"Node {self.node_id} forwarding command {command_data.get('type')} to leader {leader_id} RPC at http://{leader_rpc_host}:{leader_rpc_port}")
        try:
            # Forward the entire command_data dictionary to the leader's record_entry RPC endpoint.
            # The leader's RPC server needs to accept this structure.
            response = leader_rpc_client._make_request("record_entry", command_data)

            if response is None:
                 logging.warning(f"Node {self.node_id}: Leader {leader_id} did not respond to forwarded command.")
                 raise requests.exceptions.RequestException(f"Leader {leader_id} did not respond")

            logging.info(f"Node {self.node_id}: Forwarded command handled by leader {leader_id}. Response status: {response.get('status')}")
            return response

        except requests.exceptions.RequestException as e:
            logging.error(f"Node {self.node_id} error forwarding command to leader {leader_id} RPC: {e}", exc_info=True)
            raise NotLeaderException(f"Failed to forward command to leader {leader_id}: {str(e)}") from e
        except Exception as e:
            logging.error(f"Node {self.node_id} unexpected error forwarding command to leader {leader_id} RPC: {e}", exc_info=True)
            raise NotLeaderException(f"Unexpected error forwarding command to leader {leader_id}: {str(e)}") from e

    def shutdown(self):
        """
        Initiates the shutdown process for the DistributedSequencer node.

        Calls the shutdown method of the base RaftNode class.
        """
        logger_raft.info(f"DistributedSequencer Node {self.node_id}: Initiating shutdown.")
        super().shutdown()

