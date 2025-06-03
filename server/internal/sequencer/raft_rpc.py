import json
import time
import logging
from http.server import BaseHTTPRequestHandler, HTTPServer
import threading
import requests
from socket import SOL_SOCKET, SO_REUSEADDR
from typing import Optional

from server.config import app_config

class RaftRPCServer(BaseHTTPRequestHandler):
    def do_POST(self):
        if self.path == "/request_vote":
            content_length = int(self.headers.get('Content-Length', 0))
            if content_length > 0:
                request_data = self.rfile.read(content_length)
                data = json.loads(request_data.decode('utf-8'))
                
                try:
                    # Correctly order parameters to match RaftNode.request_vote
                    result = self.node.request_vote(
                        candidate_id=data.get('candidate_id'),
                        term=int(data.get('term', 0)),
                        last_log_index=int(data.get('last_log_index', 0)),
                        last_log_term=int(data.get('last_log_term', 0))
                    )
                except Exception as e:
                    logging.error(f"Error in request_vote handler: {e}")
                    result = (False, 0)  # Default response on error
                self.send_response(200)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                
                # Format response as a proper JSON object with named fields
                if isinstance(result, tuple) and len(result) == 2:
                    response_data = {
                        "vote_granted": bool(result[0]),
                        "term": int(result[1])
                    }
                    logging.info(f"Node {self.node.node_id} sending vote response: {response_data} to {data.get('candidate_id')}")
                    self.wfile.write(json.dumps(response_data).encode())
                else:
                    # Fallback for unexpected response format
                    logging.warning(f"Unexpected result format from request_vote: {result}")
                    self.wfile.write(json.dumps({"vote_granted": False, "term": 0}).encode())
        elif self.path == "/append_entries":
            content_length = int(self.headers.get('Content-Length', 0))
            if content_length > 0:
                request_data = self.rfile.read(content_length)
                data = json.loads(request_data.decode('utf-8'))
                
                try:
                    # Correctly order parameters to match RaftNode.append_entries
                    result = self.node.append_entries(
                        leader_id=data.get('leader_id'),
                        term=int(data.get('term', 0)),
                        prev_log_index=int(data.get('prev_log_index', 0)),
                        prev_log_term=int(data.get('prev_log_term', 0)),
                        entries=data.get('entries', []),
                        leader_commit=int(data.get('leader_commit', 0))
                    )
                except Exception as e:
                    logging.error(f"Error in append_entries handler: {e}")
                    result = (False, self.node.current_term)  # Default response on error
                self.send_response(200)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                
                # Format response as a proper JSON object with named fields
                if isinstance(result, tuple) and len(result) == 2:
                    # If response is (success, term) tuple
                    response_data = {
                        "success": bool(result[0]),
                        "term": int(result[1])
                    }
                    self.wfile.write(json.dumps(response_data).encode())
                else:
                    # If response is just a boolean (old format)
                    # This branch should ideally not be hit if append_entries is consistent
                    logging.warning(f"Node {self.node.node_id} received unexpected result format from append_entries: {result}. Assuming success={result}, term={self.node.current_term}")
                    response_data = {
                        "success": bool(result), 
                        "term": int(self.node.current_term)
                    }
                    self.wfile.write(json.dumps(response_data).encode())
                
            logging.debug(f"Node {self.node.node_id} sent append_entries response to {data.get('leader_id')}: {response_data}")
        elif self.path == "/pre_vote":
            content_length = int(self.headers.get('Content-Length', 0))
            if content_length == 0:
                logging.warning(f"RPC Server: Received PreVote request with no body from {self.client_address}")
                self.send_response(400) # Bad Request
                self.send_header('Content-type', 'text/plain')
                self.end_headers()
                self.wfile.write(b"Request body required for /pre_vote.")
                return

            request_data_raw = self.rfile.read(content_length)
            try:
                request_data_str = request_data_raw.decode('utf-8')
                data = json.loads(request_data_str)
            except json.JSONDecodeError as e:
                logging.error(f"RPC Server: Failed to decode JSON for PreVote request from {self.client_address}. Data: '{request_data_raw.decode('utf-8', errors='replace')}'. Error: {e}")
                self.send_response(400) # Bad Request
                self.send_header('Content-type', 'text/plain')
                self.end_headers()
                self.wfile.write(b"Invalid JSON format in request body.")
                return
            except Exception as e: 
                logging.error(f"RPC Server: Error reading/decoding PreVote request from {self.client_address}. Error: {e}", exc_info=True)
                self.send_response(400) # Bad Request
                self.send_header('Content-type', 'text/plain')
                self.end_headers()
                self.wfile.write(b"Error processing request body.")
                return

            # Validate required fields
            candidate_id = data.get('candidate_id')
            term_str = data.get('term')
            last_log_index_str = data.get('last_log_index')
            last_log_term_str = data.get('last_log_term')

            if not all(val is not None for val in [candidate_id, term_str, last_log_index_str, last_log_term_str]):
                logging.warning(f"RPC Server: PreVote request from {self.client_address} missing required fields. Data: {data}")
                self.send_response(400) # Bad Request
                self.send_header('Content-type', 'application/json') # Send JSON error
                self.end_headers()
                self.wfile.write(json.dumps({"error": "Missing required fields: candidate_id, term, last_log_index, last_log_term"}).encode())
                return

            try:
                candidate_proposed_term = int(term_str)
                candidate_last_log_index = int(last_log_index_str)
                candidate_last_log_term = int(last_log_term_str)
            except ValueError:
                logging.warning(f"RPC Server: PreVote request from {candidate_id} has non-integer term/log_index/log_term. Data: {data}")
                self.send_response(400) # Bad Request
                self.send_header('Content-type', 'application/json') # Send JSON error
                self.end_headers()
                self.wfile.write(json.dumps({"error": "Fields term, last_log_index, last_log_term must be integers."}).encode())
                return
            
            logging.debug(f"RPC Server: Node {self.node.node_id if self.node else 'Unknown'} received PreVote request from {candidate_id} for term {candidate_proposed_term}, LLI {candidate_last_log_index}, LLT {candidate_last_log_term}")

            try:
                # Ensure self.node and self.node.pre_vote exist
                if not hasattr(self.node, 'pre_vote'):
                    logging.error(f"RPC Server: RaftNode.pre_vote method not found on node {self.node.node_id if self.node else 'Unknown'}.")
                    raise AttributeError("PreVote method not implemented on server-side RaftNode.")

                result = self.node.pre_vote(
                    candidate_id=str(candidate_id), # Ensure candidate_id is string
                    candidate_proposed_term=candidate_proposed_term,
                    candidate_last_log_index=candidate_last_log_index,
                    candidate_last_log_term=candidate_last_log_term
                )
            except AttributeError as ae: # Specifically catch if pre_vote is missing
                 logging.error(f"RPC Server: RaftNode.pre_vote method not found. Node: {self.node.node_id if self.node else 'Unknown'}. Error: {ae}", exc_info=True)
                 self.send_response(501) # Not Implemented
                 self.send_header('Content-type', 'application/json')
                 self.end_headers()
                 fallback_term = self.node.current_term if self.node and hasattr(self.node, 'current_term') else 0
                 self.wfile.write(json.dumps({"vote_granted": False, "term": int(fallback_term), "error": "PreVote not implemented on server"}).encode())
                 return
            except Exception as e:
                logging.error(f"RPC Server: Error in RaftNode.pre_vote call for candidate {candidate_id}. Error: {e}", exc_info=True)
                # Default response on internal error: pre-vote not granted, return current term
                fallback_term = self.node.current_term if self.node and hasattr(self.node, 'current_term') else 0
                result = (False, int(fallback_term)) 
            
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            
            if isinstance(result, tuple) and len(result) == 2:
                response_data = {
                    "vote_granted": bool(result[0]),
                    "term": int(result[1])
                }
                logging.info(f"RPC Server: Node {self.node.node_id if self.node else 'Unknown'} sending PreVote response: {response_data} to {candidate_id}")
                self.wfile.write(json.dumps(response_data).encode())
            else:
                logging.warning(f"RPC Server: Unexpected result format from RaftNode.pre_vote for candidate {candidate_id}: {result}. Expected (bool, int).")
                fallback_term = self.node.current_term if self.node and hasattr(self.node, 'current_term') else 0
                self.wfile.write(json.dumps({"vote_granted": False, "term": int(fallback_term), "error": "Internal server error processing PreVote response"}).encode())
        else:
            self.send_response(404)
            self.end_headers()
    def do_GET(self):
        """Handle GET requests, currently just for the health endpoint."""
        if self.path == "/health":
            # Simple health check endpoint
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            health_data = {
                "status": "ok",
                "node_id": self.node.node_id if self.node else "unknown",
                "state": self.node.state if self.node else "unknown"
            }
            self.wfile.write(json.dumps(health_data).encode())
        else:
            # Unknown GET endpoint
            self.send_response(404)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({"error": "Not found"}).encode())
            
    def __init__(self, *args, node=None, **kwargs):
        self.node = node
        super().__init__(*args, **kwargs)
    
class RaftRPCClient:
    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port
        self.base_url = f"http://{host}:{port}"
        self.logger = logging.getLogger(f"RaftRPCClient.{host}:{port}") # Initialize logger for this instance
        self.max_retries = app_config.get_raft_config('rpc_client_max_retries', 5)
        self.initial_backoff_s = app_config.get_raft_config('rpc_client_initial_backoff_s', 0.1)
        self.max_backoff_s = app_config.get_raft_config('rpc_client_max_backoff_s', 1.0)
        self.default_rpc_timeout_s = app_config.get_raft_config('rpc_default_timeout_s', 1.0)
        self._last_connection_status = False
        self._last_connection_check = 0  # timestamp of last check
        
    def is_connected(self, force_check=False, check_interval_sec=5.0):
        """
        Check if the peer node is reachable. Caches result for check_interval_sec to avoid
        excessive network calls.
        
        Args:
            force_check: If True, ignores cached status and checks immediately
            check_interval_sec: How long to cache the connection status (default: 5s)
        
        Returns:
            bool: True if peer is reachable, False otherwise
        """
        # Use cached result if check was recent and force_check is False
        current_time = time.time()
        if not force_check and (current_time - self._last_connection_check) < check_interval_sec:
            return self._last_connection_status
            
        # Perform a simple health check request to see if peer is alive
        try:
            # Using low timeout for quick health check
            health_timeout = min(0.3, self.default_rpc_timeout_s / 2)
            response = requests.get(f"{self.base_url}/health", timeout=health_timeout)
            self._last_connection_status = response.status_code == 200
        except Exception:
            # Any exception means peer is not reachable
            self._last_connection_status = False
            
        self._last_connection_check = current_time
        return self._last_connection_status
        
    def _request_with_retry(self, url, data, timeout: Optional[float] = None):
        actual_timeout = timeout if timeout is not None else self.default_rpc_timeout_s
        retries = 0
        backoff = self.initial_backoff_s
        while retries < self.max_retries:
            try:
                response = requests.post(url, json=data, timeout=actual_timeout)
                response.raise_for_status()  # Raise an HTTPError for bad responses (4xx or 5xx)
                return response
            except requests.exceptions.RequestException as e:
                self.logger.warning(f"RPC Client: Connection to {url} failed (attempt {retries+1}/{self.max_retries}), retrying in {backoff:.2f}s: {e}")
                time.sleep(backoff)
            backoff = min(self.max_backoff_s, backoff * 2)
            retries += 1
        self.logger.error(f"RPC Client: Failed to connect to {url} after {self.max_retries} attempts.")
        return None
    
    def request_vote(self, candidate_id, term, last_log_index, last_log_term, timeout: Optional[float] = None):
        url = f"{self.base_url}/request_vote"
        data = {
            'candidate_id': candidate_id,
            'term': int(term),
            'last_log_index': int(last_log_index),
            'last_log_term': int(last_log_term)
        }
        logging.debug(f"RPC Client: Sending RequestVote to {url} for candidate {candidate_id}, term {term}")
        request_vote_timeout = timeout if timeout is not None else app_config.get_raft_config('rpc_request_vote_timeout_s', self.default_rpc_timeout_s)
        response = self._request_with_retry(url, data, timeout=request_vote_timeout)
        if response is None:
            return None
            
        try:
            # Log the actual response text for debugging
            response_text = response.text
            logging.debug(f"Raw response from {url}: {response_text}")
            
            # Parse the response, handling different potential formats
            result = response.json()
            
            # Handle different response formats
            if isinstance(result, list) and len(result) == 2:
                # Format: [vote_granted, term]
                return (bool(result[0]), int(result[1]))
            elif isinstance(result, dict) and 'vote_granted' in result and 'term' in result:
                # Format: {"vote_granted": bool, "term": int}
                return (bool(result['vote_granted']), int(result['term']))
            else:
                # Unexpected format
                logging.error(f"Unexpected response format from {url}: {result}")
                return None
        except ValueError as e:
            # JSON decoding error
            logging.error(f"Invalid JSON response from {url}: {response.text} - Error: {e}")
            return None
        except Exception as e:
            # Other errors
            logging.error(f"Error parsing response from {url}: {e}")
            return None
        
    def append_entries(self, leader_id, term, prev_log_index, prev_log_term, entries, leader_commit, timeout: Optional[float] = None):
        url = f"{self.base_url}/append_entries"
        append_entries_timeout = timeout if timeout is not None else app_config.get_raft_config('rpc_append_entries_timeout_s', self.default_rpc_timeout_s)
        data = {
            'leader_id': leader_id,
            'term': int(term),  # Ensure term is sent as int
            'prev_log_index': int(prev_log_index),  # Ensure index is sent as int
            'prev_log_term': int(prev_log_term),  # Ensure term is sent as int
            'entries': entries,
            'leader_commit': int(leader_commit)  # Ensure index is sent as int
        }
        
        logging.debug(f"RPC Client: Sending AppendEntries to {url} from leader {leader_id} for term {term}")
        response = self._request_with_retry(url, data, timeout=append_entries_timeout)
        if response is None:
            return None
            
        try:
            # Log the actual response text for debugging
            response_text = response.text
            logging.debug(f"Raw response from {url}: {response_text}")
            
            # Parse the response, handling different potential formats
            result = response.json()
            
            # Handle different response formats
            if isinstance(result, list) and len(result) == 2:
                # Format: [success, term]
                return (bool(result[0]), int(result[1]))
            elif isinstance(result, dict) and 'success' in result and 'term' in result:
                # Format: {"success": bool, "term": int}
                return (bool(result['success']), int(result['term']))
            elif isinstance(result, bool):
                # Format: true/false (success only, no term)
                return result
            else:
                # For any other format, convert to boolean
                logging.warning(f"Unexpected response format from {url}: {result}")
                return bool(result)
        except ValueError as e: # Catches response.json() errors
            logging.error(f"RPC Client: Invalid JSON in append_entries response from {url}. Response text: '{response_text if 'response_text' in locals() else 'N/A'}'. Error: {e}")
            return (False, 0) # Default to failure, term 0
        except Exception as e: # Generic catch-all
            logging.error(f"RPC Client: Error processing append_entries response from {url}. Error: {e}", exc_info=True)
            return (False, 0) # Default to failure, term 0

    def pre_vote(self, candidate_id, term, last_log_index, last_log_term, timeout: Optional[float] = None):
        url = f"{self.base_url}/pre_vote"
        pre_vote_timeout = timeout if timeout is not None else app_config.get_raft_config('rpc_pre_vote_timeout_s', 0.5) # 0.5s is a common short timeout for pre-vote
        data = {
            'candidate_id': candidate_id,
            'term': int(term),
            'last_log_index': int(last_log_index),
            'last_log_term': int(last_log_term)
        }
        
        logging.debug(f"RPC Client: Sending PreVote request to {url} for candidate {candidate_id}, term {term}, last_log_index {last_log_index}, last_log_term {last_log_term}")
        
        response = self._request_with_retry(url, data, timeout=pre_vote_timeout)
        
        if response is None:
            self.logger.warning(f"RPC Client: No response received from {url} for PreVote request to candidate {candidate_id} after retries.")
            return (False, 0) # Consistent error response
            
        response_text = ""
        try:
            response_text = response.text # Store for logging in case of JSON error
            logging.debug(f"RPC Client: Raw PreVote response from {url} for candidate {candidate_id}: {response_text}")
            
            result = response.json() # This can raise ValueError if not JSON
            
            # Expected server response: {"vote_granted": bool, "term": int}
            if isinstance(result, dict) and 'vote_granted' in result and 'term' in result:
                vote_granted = bool(result['vote_granted'])
                response_term = int(result['term'])
                logging.debug(f"RPC Client: Parsed PreVote response from {url} for candidate {candidate_id}: vote_granted={vote_granted}, term={response_term}")
                return (vote_granted, response_term)
            else:
                logging.error(f"RPC Client: Unexpected PreVote response format from {url} for candidate {candidate_id}. Expected dict with 'vote_granted' and 'term', got: {result}")
                return (False, 0) # Consistent error response
        except ValueError as e: # Catches response.json() errors
            logging.error(f"RPC Client: Invalid JSON in PreVote response from {url} for candidate {candidate_id}. Response text: '{response_text}'. Error: {e}")
            return (False, 0) # Consistent error response
        except AttributeError as e: # Should not happen with requests.Response but good to keep
            logging.error(f"RPC Client: Attribute error while parsing PreVote response from {url} for candidate {candidate_id}. Error: {e}")
            return (False, 0) # Consistent error response
        except Exception as e: # Generic catch-all for other parsing/processing errors
            logging.error(f"RPC Client: Generic error parsing PreVote response from {url} for candidate {candidate_id}. Response text: '{response_text}'. Error: {e}", exc_info=True)
            return (False, 0) # Consistent error response

    def is_connected(self, timeout: float = 1.0) -> bool:
        """Checks if the RPC client can connect to the peer's health endpoint."""
        health_url = f"http://{self.host}:{self.port}/health"
        try:
            response = requests.get(health_url, timeout=timeout)
            return response.status_code == 200
        except requests.exceptions.RequestException as e:
            self.logger.debug(f"Failed to connect to {health_url} (is_connected check): {e}")
            return False

    # End of pre_vote method

# End of RaftRPCClient class

def start_rpc_server(node, host: str, port: int):
    def handler(*args):
        return RaftRPCServer(*args, node=node)
    
    try:
        # Create server with address reuse to avoid "Address already in use" errors
        server = HTTPServer((host, port), handler)
        server.socket.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
        
        logging.info(f"Starting RPC server for node {node.node_id} on {host}:{port}")
        thread = threading.Thread(target=server.serve_forever)
        thread.daemon = True
        thread.start()
        
        # Add a loop to wait for the server to be ready
        server_url = f"http://{host}:{port}/health"
        max_retries = 10
        retry_delay_sec = 0.5
        for i in range(max_retries):
            try:
                response = requests.get(server_url, timeout=0.1) # Short timeout for health check
                if response.status_code == 200:
                    logging.info(f"RPC server for node {node.node_id} on {host}:{port} is confirmed ready.")
                    return server
            except requests.exceptions.ConnectionError:
                logging.warning(f"RPC server for node {node.node_id} not yet ready at {host}:{port}, retrying ({i+1}/{max_retries})...")
                time.sleep(retry_delay_sec)
            except Exception as e:
                logging.error(f"Error during RPC server health check for node {node.node_id} at {host}:{port}: {e}")
                break # Exit loop on unexpected error
        
        logging.error(f"RPC server for node {node.node_id} on {host}:{port} failed to become ready after {max_retries} retries.")
        raise Exception(f"RPC server failed to start on {host}:{port}")
    except Exception as e:
        logging.error(f"Failed to start RPC server on {host}:{port}: {e}")
        raise

__all__ = ['start_rpc_server', 'RaftRPCClient']