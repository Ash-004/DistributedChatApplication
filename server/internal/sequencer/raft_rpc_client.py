import requests
import json
import logging
from typing import Dict, Any, Tuple, Optional

logger = logging.getLogger('raft.rpc.client')

class RaftRPCClient:
    """Client for making RPC calls to other Raft nodes in a distributed system.

    Attributes:
        host (str): The hostname or IP address of the remote Raft node.
        port (int): The port number of the remote Raft node's RPC server.
        base_url (str): The base URL for RPC requests (e.g., 'http://host:port').
    """

    def __init__(self, host: str, port: int):
        """Initialize a new RaftRPCClient.

        Args:
            host (str): The hostname or IP address of the remote Raft node.
            port (int): The port number of the remote Raft node's RPC server.
        """
        self.host = host
        self.port = port
        self.base_url = f"http://{host}:{port}"
        logger.debug(f"Initialized RaftRPCClient for {self.base_url}")

    def pre_vote(self, candidate_id: str, term: int, last_log_index: int, last_log_term: int, timeout: float = 5.0) -> Tuple[bool, int]:
        """Send a Pre-Vote RPC to the remote node.

        Args:
            candidate_id (str): Candidate requesting pre-vote.
            term (int): Proposed term for which the candidate is seeking pre-votes.
            last_log_index (int): Index of candidate's last log entry.
            last_log_term (int): Term of candidate's last log entry.
            timeout (float, optional): Timeout for the RPC call in seconds. Defaults to 5.0.

        Returns:
            Tuple[bool, int]: A tuple of (vote_granted, term).
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
        """Send a RequestVote RPC to the remote node.

        Args:
            term (int): Candidate's term.
            candidate_id (str): Candidate requesting vote.
            last_log_index (int): Index of candidate's last log entry.
            last_log_term (int): Term of candidate's last log entry.
            timeout (float, optional): Timeout for the RPC call in seconds. Defaults to 5.0.

        Returns:
            Tuple[bool, int]: A tuple of (vote_granted, term).
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
        """Send an AppendEntries RPC to the remote node.

        Args:
            term (int): Leader's term.
            leader_id (str): Leader's ID.
            prev_log_index (int): Index of log entry immediately preceding new ones.
            prev_log_term (int): Term of prev_log_index entry.
            entries (list): Log entries to store (empty for heartbeat).
            leader_commit (int): Leader's commit index.
            timeout (float, optional): Timeout for the RPC call in seconds. Defaults to 5.0.

        Returns:
            Tuple[bool, int]: A tuple of (success, term).
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

    def record_entry(self, room_id: str, user_id: str, content: str, msg_type: str, timeout: float = 5.0) -> dict:
        """Send a RecordEntry RPC to the remote node.

        Args:
            room_id (str): ID of the room where the message is being sent.
            user_id (str): ID of the user sending the message.
            content (str): Content of the message.
            msg_type (str): Type of the message (e.g., "text").
            timeout (float, optional): Timeout for the RPC call in seconds. Defaults to 5.0.

        Returns:
            dict: A dictionary with the result of the operation.
                  If successful, returns the response from the remote node.
                  If failed, returns {"status": "error", "message": error_message}.
        """
        try:
            url = f"{self.base_url}/record_entry"
            data = {
                "room_id": room_id,
                "user_id": user_id,
                "content": content,
                "msg_type": msg_type
            }
            response = requests.post(url, json=data, timeout=timeout)
            if response.status_code == 200:
                return response.json()
            else:
                logger.error(f"Failed to record entry at {self.base_url}: {response.status_code}")
                return {"status": "error", "message": f"Failed to record entry: HTTP {response.status_code}"}
        except Exception as e:
            logger.error(f"Error recording entry at {self.base_url}: {str(e)}")
            return {"status": "error", "message": f"Error recording entry: {str(e)}"}

    def _make_request(self, endpoint: str, data: dict, timeout: float = 5.0) -> dict:
        """Generic method to make an RPC request to the remote node.

        Args:
            endpoint (str): The endpoint to call (e.g., "record_entry").
            data (dict): The data to send in the request body.
            timeout (float, optional): Timeout for the RPC call in seconds. Defaults to 5.0.

        Returns:
            dict: A dictionary with the result of the operation.
                  If successful, returns the response from the remote node.
                  If failed, returns {"status": "error", "message": error_message}.
        """
        try:
            url = f"{self.base_url}/{endpoint}"
            response = requests.post(url, json=data, timeout=timeout)
            if response.status_code == 200:
                return response.json()
            else:
                logger.error(f"Failed to make request to {url}: {response.status_code}")
                return {"status": "error", "message": f"Failed to make request: HTTP {response.status_code}"}
        except Exception as e:
            logger.error(f"Error making request to {url}: {str(e)}")
            return {"status": "error", "message": f"Error making request: {str(e)}"}