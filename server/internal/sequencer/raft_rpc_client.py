import requests
import json
import logging
from typing import Dict, Any, Tuple, Optional

logger = logging.getLogger('raft.rpc.client')

class RaftRPCClient:
    """
    Client for making RPC calls to other Raft nodes.
    This class handles the communication with other nodes in the Raft cluster.
    """
    def __init__(self, host: str, port: int):
        """
        Initialize a new RaftRPCClient.
        
        Args:
            host: The hostname or IP address of the remote Raft node
            port: The port number of the remote Raft node's RPC server
        """
        self.host = host
        self.port = port
        self.base_url = f"http://{host}:{port}"
        logger.debug(f"Initialized RaftRPCClient for {self.base_url}")
    
    def pre_vote(self, candidate_id: str, term: int, last_log_index: int, last_log_term: int, timeout: float = 5.0) -> Tuple[bool, int]:
        """
        Send a Pre-Vote RPC to the remote node.
        
        Args:
            candidate_id: Candidate requesting pre-vote
            term: Proposed term for which the candidate is seeking pre-votes
            last_log_index: Index of candidate's last log entry
            last_log_term: Term of candidate's last log entry
            timeout: Timeout for the RPC call in seconds
            
        Returns:
            Tuple of (vote_granted, term)
        """
        try:
            url = f"{self.base_url}/pre_vote"
            data = {
                "candidate_id": candidate_id,
                "term": term,
                "last_log_index": last_log_index,
                "last_log_term": last_log_term
            }
            response = requests.post(url, json=data, timeout=timeout)
            if response.status_code == 200:
                result = response.json()
                return result.get("vote_granted", False), result.get("term", 0)
            else:
                logger.error(f"Failed to request pre-vote from {self.base_url}: {response.status_code}")
                return False, 0
        except Exception as e:
            logger.error(f"Error requesting pre-vote from {self.base_url}: {str(e)}")
            return False, 0
    
    def request_vote(self, term: int, candidate_id: str, last_log_index: int, last_log_term: int, timeout: float = 5.0) -> Tuple[bool, int]:
        """
        Send a RequestVote RPC to the remote node.
        
        Args:
            term: Candidate's term
            candidate_id: Candidate requesting vote
            last_log_index: Index of candidate's last log entry
            last_log_term: Term of candidate's last log entry
            
        Returns:
            Tuple of (vote_granted, term)
        """
        try:
            url = f"{self.base_url}/request_vote"
            data = {
                "term": term,
                "candidate_id": candidate_id,
                "last_log_index": last_log_index,
                "last_log_term": last_log_term
            }
            response = requests.post(url, json=data, timeout=timeout)
            if response.status_code == 200:
                result = response.json()
                return result.get("vote_granted", False), result.get("term", 0)
            else:
                logger.error(f"Failed to request vote from {self.base_url}: {response.status_code}")
                return False, 0
        except Exception as e:
            logger.error(f"Error requesting vote from {self.base_url}: {str(e)}")
            return False, 0
    
    def append_entries(self, term: int, leader_id: str, prev_log_index: int, prev_log_term: int, 
                      entries: list, leader_commit: int, timeout: float = 5.0) -> Tuple[bool, int]:
        """
        Send an AppendEntries RPC to the remote node.
        
        Args:
            term: Leader's term
            leader_id: Leader's ID
            prev_log_index: Index of log entry immediately preceding new ones
            prev_log_term: Term of prev_log_index entry
            entries: Log entries to store (empty for heartbeat)
            leader_commit: Leader's commit index
            
        Returns:
            Tuple of (success, term)
        """
        try:
            url = f"{self.base_url}/append_entries"
            data = {
                "term": term,
                "leader_id": leader_id,
                "prev_log_index": prev_log_index,
                "prev_log_term": prev_log_term,
                "entries": entries,
                "leader_commit": leader_commit
            }
            response = requests.post(url, json=data, timeout=timeout)
            if response.status_code == 200:
                result = response.json()
                return result.get("success", False), result.get("term", 0)
            else:
                logger.error(f"Failed to append entries to {self.base_url}: {response.status_code}")
                return False, 0
        except Exception as e:
            logger.error(f"Error appending entries to {self.base_url}: {str(e)}")
            return False, 0