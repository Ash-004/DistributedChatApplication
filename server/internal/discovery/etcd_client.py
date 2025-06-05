import etcd3
import json
import logging
import threading
import time

logger = logging.getLogger(__name__)

class EtcdClient:
    def __init__(self, etcd_endpoints, node_id=None, api_address=None, rpc_address=None):
        """
        Initializes the EtcdClient.
        Args:
            etcd_endpoints (list): List of etcd server endpoints (e.g., ['http://localhost:2379']).
            node_id (str, optional): The ID of this node. Required for registration.
            api_address (str, optional): The API address of this node. Required for registration.
            rpc_address (str, optional): The RPC address of this node. Required for registration.
        """
        self.etcd_endpoints = []
        for endpoint in etcd_endpoints:
            try:
                scheme, netloc = endpoint.split('//')
                host, port = netloc.split(':')
                self.etcd_endpoints.append((host, int(port)))
            except ValueError:
                logger.warning(f"Invalid etcd endpoint format: {endpoint}. Expected 'http://host:port'. Skipping.")
        
        if not self.etcd_endpoints:
            logger.error("No valid etcd endpoints provided. Client may not function correctly.")
            # Fallback to default if no valid endpoints were parsed
            self.etcd_endpoints.append(('localhost', 2379))

        self.node_id = node_id
        self.api_address = api_address
        self.rpc_address = rpc_address
        
        self.etcd = None
        self._connect()

        self.lease_id = None
        self.heartbeat_thread = None
        self.heartbeat_stop_event = threading.Event()
        self._heartbeat_interval = 10 # Default, can be overridden by register_node

        if self.node_id:
            self.node_key = f"/chat_app/nodes/{self.node_id}"
        self.leader_key = "/chat_app/service/leader_api_endpoint"
        self.leader_lease_id = None # Lease for the leader key, if we want it to expire

    def _connect(self):
        try:
            # etcd3.client can take a list of (host, port) tuples
            self.etcd = etcd3.client(host=self.etcd_endpoints[0][0], port=self.etcd_endpoints[0][1])
            # For multiple endpoints, you might want to iterate or use a load-balancing client if etcd3 supports it directly.
            # For now, we use the first valid endpoint.
            self.etcd.status() # Check connection
            logger.info(f"Successfully connected to etcd at {self.etcd_endpoints[0][0]}:{self.etcd_endpoints[0][1]}. Type of self.etcd: {type(self.etcd)}")
        except Exception as e:
            logger.error(f"Failed to connect to etcd at {self.etcd_endpoints[0][0]}:{self.etcd_endpoints[0][1]}: {e}")
            self.etcd = None
            logger.error("Etcd client connection failed. All etcd operations will fail.")

    def register_node(self, initial_role, ttl=30, heartbeat_interval=10):
        """
        Registers the node with etcd, creating a lease for automatic expiration.
        Args:
            initial_role (str): The initial role of the node (e.g., "FOLLOWER").
            ttl (int): Time-to-live for the lease in seconds.
            heartbeat_interval (int): How often to refresh the lease in seconds.
        Returns:
            bool: True if registration is successful, False otherwise.
        """
        if not self.etcd:
            logger.error("Etcd client not connected. Cannot perform etcd operation.")
            return False
        if not all([self.node_id, self.api_address, self.rpc_address]):
            logger.error("Node ID, API address, and RPC address must be set to register node.")
            return False

        node_data = {
            "id": self.node_id,
            "api_address": self.api_address,
            "rpc_address": self.rpc_address,
            "role": initial_role,
            "last_updated": time.time()
        }

        # Create a lease for this node's registration
        try:
            logger.debug(f"Attempting to create lease. Type of self.etcd: {type(self.etcd)}")
            self.lease_id = self.etcd.lease(ttl=ttl).id
        except Exception as e:
            logger.error(f"Failed to create lease for node {self.node_id}: {e}")
            return False

        try:
            self.etcd.put(self.node_key, json.dumps(node_data), lease=self.lease_id)
            logger.info(f"Node {self.node_id} registered with etcd. Key: {self.node_key}, Lease ID: {self.lease_id}")
            self._heartbeat_interval = heartbeat_interval
            self.heartbeat_stop_event.clear()
            self.heartbeat_thread = threading.Thread(target=self._keep_alive, args=(heartbeat_interval,), daemon=True)
            self.heartbeat_thread.start()
            return True
        except Exception as e:
            logger.error(f"Failed to register node {self.node_id} with etcd: {e}")
            return False

    def _keep_alive(self, heartbeat_interval):
        logger.info(f"Starting etcd lease keep-alive thread for node {self.node_id} with interval {heartbeat_interval}s.")
        while not self.heartbeat_stop_event.is_set():
            try:
                if not self.etcd:
                    logger.error("Etcd client not connected. Cannot perform etcd operation.")
                    break
                if self.lease_id:
                    self.etcd.refresh_lease(self.lease_id) # Corrected line
                else:
                    logger.warning(f"Node {self.node_id}: No lease_id found for keep-alive. Stopping heartbeat.")
                    break
            except Exception as e:
                logger.error(f"Error refreshing etcd lease for node {self.node_id}: {e}")
                break
            time.sleep(heartbeat_interval)
        logger.info(f"Etcd lease keep-alive thread for node {self.node_id} stopped.")

    def update_role(self, new_role):
        """
        Updates the role of the node in etcd.
        Args:
            new_role (str): The new role of the node.
        Returns:
            bool: True if update is successful, False otherwise.
        """
        if not self.etcd:
            logger.error("Etcd client not connected. Cannot perform etcd operation.")
            return False
        if not self.etcd or not self.node_id:
            logger.error("Etcd client not connected or node ID not set. Cannot update role.")
            return False
        try:
            # Get current node data to preserve other fields
            logger.debug(f"Attempting to get node data. Type of self.etcd: {type(self.etcd)}")
            value, _ = self.etcd.get(self.node_key)
            if value:
                node_data = json.loads(value.decode('utf-8'))
                node_data["role"] = new_role
                node_data["last_updated"] = time.time()
                self.etcd.put(self.node_key, json.dumps(node_data), lease=self.lease_id)
                logger.info(f"Node {self.node_id} role updated to {new_role} in etcd.")
                return True
            else:
                logger.warning(f"Node {self.node_id} not found in etcd. Cannot update role.")
                return False
        except Exception as e:
            logger.error(f"Failed to update role for node {self.node_id} in etcd: {e}")
            return False

    def publish_leader_info(self, leader_api_address, ttl=30):
        """
        Publishes the leader's API address to etcd with a lease.
        Args:
            leader_api_address (str): The API address of the current leader.
            ttl (int): Time-to-live for the leader key lease in seconds.
        Returns:
            bool: True if successful, False otherwise.
        """
        if not self.etcd:
            logger.error("Etcd client not connected. Cannot perform etcd operation.")
            return False
        if not self.etcd:
            logger.error("Etcd client not connected. Cannot publish leader info.")
            return False
        try:
            # Create a new lease for the leader key
            logger.debug(f"Attempting to publish leader info. Type of self.etcd: {type(self.etcd)}")
            self.leader_lease_id = self.etcd.lease(ttl=ttl).id
            self.etcd.put(self.leader_key, leader_api_address, lease=self.leader_lease_id)
            logger.info(f"Published leader info: {leader_api_address} with lease ID: {self.leader_lease_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to publish leader info to etcd: {e}")
            return False

    def clear_leader_info(self, previous_leader_id=None):
        """
        Clears the leader's API address from etcd.
        Args:
            previous_leader_id: Optional ID of the previous leader (not used in this implementation but accepted for compatibility)
        Returns:
            bool: True if successful, False otherwise.
        """
        if not self.etcd:
            logger.error("Etcd client not connected. Cannot perform etcd operation.")
            return False
        if not self.etcd:
            logger.error("Etcd client not connected. Cannot clear leader info.")
            return False
        try:
            self.etcd.delete(self.leader_key)
            logger.info("Cleared leader info from etcd.")
            self.leader_lease_id = None # Clear the lease ID as well
            return True
        except Exception as e:
            logger.error(f"Failed to clear leader info from etcd: {e}")
            return False

    def clear_leader_info_if_self(self, current_api_address):
        """
        Clears the leader info from etcd ONLY if the current leader is this node.
        This prevents a stepping-down leader from clearing another leader's entry.
        """
        if not self.etcd:
            logger.error("Etcd client not connected. Cannot perform etcd operation.")
            return False
        if not self.etcd:
            logger.error("Etcd client not connected. Cannot clear leader info conditionally.")
            return False
        try:
            value, _ = self.etcd.get(self.leader_key)
            if value and value.decode('utf-8') == current_api_address:
                logger.info(f"Current leader is {current_api_address}, which is self. Clearing leader info.")
                self.clear_leader_info()
            else:
                logger.info(f"Current leader is {value.decode('utf-8') if value else 'None'}, not self. Not clearing leader info.")
        except Exception as e:
            logger.error(f"Error checking and clearing leader info: {e}")

    def get_leader_api_endpoint(self):
        """
        Retrieves the leader's API endpoint from etcd.
        Returns:
            str or None: The leader's API endpoint if found, otherwise None.
        """
        if not self.etcd:
            logger.error("Etcd client not connected. Cannot perform etcd operation.")
            return None
        if not self.etcd:
            logger.error("Etcd client not connected. Cannot get leader API endpoint.")
            return None
        try:
            value, _ = self.etcd.get(self.leader_key)
            if value:
                return value.decode('utf-8')
            return None
        except Exception as e:
            logger.error(f"Failed to get leader API endpoint from etcd: {e}")
            return None

    def get_all_nodes(self):
        """
        Retrieves all registered nodes from etcd.
        Returns:
            list: A list of dictionaries, each representing a registered node.
        """
        if not self.etcd:
            logger.error("Etcd client not connected. Cannot perform etcd operation.")
            return []
        if not self.etcd:
            logger.error("Etcd client not connected. Cannot get all nodes.")
            return []
        nodes = []
        try:
            for value, metadata in self.etcd.get_prefix('/chat_app/nodes/'):
                node_data = json.loads(value.decode('utf-8'))
                nodes.append(node_data)
            return nodes
        except Exception as e:
            logger.error(f"Failed to get all nodes from etcd: {e}")
            return []

    def deregister_node(self):
        """
        Deregisters the node from etcd and stops the heartbeat thread.
        """
        if not self.etcd:
            logger.warning("Etcd client not connected. Cannot perform etcd operation.")
            return
        if not self.etcd or not self.node_id:
            logger.warning("Etcd client not connected or node ID not set. Cannot deregister node.")
            return

        logger.info(f"Attempting to deregister node {self.node_id}...")
        
        # Stop heartbeat thread first
        if self.heartbeat_thread and self.heartbeat_thread.is_alive():
            self.heartbeat_stop_event.set()
            self.heartbeat_thread.join(timeout=5) # Wait for thread to finish
            if self.heartbeat_thread.is_alive():
                logger.warning(f"Heartbeat thread for node {self.node_id} did not stop gracefully.")
            else:
                logger.info(f"Heartbeat thread for node {self.node_id} stopped.")

        # Delete node key from etcd
        try:
            if self.node_key:
                self.etcd.delete(self.node_key)
                logger.info(f"Node {self.node_id} key {self.node_key} deleted from etcd.")
        except Exception as e:
            logger.error(f"Failed to delete node key {self.node_key} for node {self.node_id}: {e}")
        
        if self.api_address:
             self.clear_leader_info_if_self(self.api_address)

        logger.info(f"Node {self.node_id} deregistration process complete.")

    def get_node_status(self, node_id: str):
        """
        Retrieves the full status (role, API address, RPC address) of a specific node from etcd.
        """
        try:
            node_key = f"/chat_app/nodes/{node_id}"
            value, metadata = self.etcd.get(node_key)
            if value:
                return json.loads(value.decode('utf-8'))
            return None
        except Exception as e:
            logger.error(f"Error retrieving status for node {node_id}: {e}")
            return None

    def get_all_registered_nodes(self):
        """
        Retrieves details (node_id, role, API address, RPC address) for all registered nodes.
        """
        nodes = {}
        try:
            for value, metadata in self.etcd.get_prefix('/chat_app/nodes/'):
                node_id = metadata.key.decode('utf-8').split('/')[-1]
                node_info = json.loads(value.decode('utf-8'))
                nodes[node_id] = node_info
            return nodes
        except Exception as e:
            logger.error(f"Error retrieving all registered nodes: {e}")
            return {}

    def close(self):
        if self.node_id:
            self.deregister_node()
        logger.info("EtcdClient closed for node_id: %s.", self.node_id if self.node_id else 'N/A (query client)')

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(threadName)s - %(name)s - %(message)s')

    etcd_server = 'http://localhost:2379'
    
    query_client = EtcdClient(etcd_endpoints=[etcd_server])
    leader = query_client.get_leader_api_endpoint()
    print(f"Initial leader from query client: {leader}")

    node1_client = EtcdClient(etcd_endpoints=[etcd_server], node_id='test_node1', 
                              api_address='http://localhost:9001', rpc_address='http://localhost:8001')
    
    if node1_client.register_node(initial_role="FOLLOWER", ttl=15, heartbeat_interval=5):
        print("Node1 registered.")
        time.sleep(2)
        node1_client.update_role("CANDIDATE")
        time.sleep(2)
        node1_client.update_role("LEADER")
        node1_client.publish_leader_info(leader_api_address='http://localhost:9001', ttl=15) # Leader key also with TTL
        
        leader = query_client.get_leader_api_endpoint()
        print(f"Leader after node1 became leader: {leader}")

        time.sleep(10) # Keep alive
        print("Shutting down Node1 client...")
        node1_client.close()
        print("Node1 client closed and deregistered.")

        time.sleep(2) # Give etcd time to reflect lease expiry if leader key had TTL
        leader = query_client.get_leader_api_endpoint()
        print(f"Leader after node1 deregistered: {leader}") # Should be None if leader key expired
    else:
        print("Failed to register Node1")

    query_client.close()