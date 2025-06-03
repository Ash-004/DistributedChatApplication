# server/config.py
import yaml
import os
import logging
from typing import Dict, List, Any, Optional, Tuple

CONFIG_FILE_NAME = 'raft_config.yaml'
DEFAULT_CONFIG_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), CONFIG_FILE_NAME)

logger = logging.getLogger(__name__)

class AppConfig:
    def __init__(self, config_path: Optional[str] = None):
        self._config_path = config_path or DEFAULT_CONFIG_PATH
        self._raw_config: Dict[str, Any] = self._load_config_from_file()

        # Core identifiers, primarily from environment variables
        self.node_id: str = os.getenv('NODE_ID')
        if not self.node_id:
            logger.warning("NODE_ID environment variable not set. Some configurations might not load correctly.")
            # Attempt to find a default or raise an error if critical
            # For now, let it be None and handle in specific getters

        self._configure_logging()
        self._process_raft_nodes()

    def _load_config_from_file(self) -> Dict[str, Any]:
        try:
            with open(self._config_path, 'r') as f:
                config_data = yaml.safe_load(f)
                logger.info(f"Successfully loaded configuration from {self._config_path}")
                return config_data or {}
        except FileNotFoundError:
            logger.error(f"Configuration file not found at {self._config_path}. Using default values or environment variables where possible.")
            return {}
        except yaml.YAMLError as e:
            logger.error(f"Error parsing YAML configuration file {self._config_path}: {e}")
            return {}

    def _configure_logging(self):
        log_level_str = os.getenv('LOG_LEVEL', self._raw_config.get('log_level', 'INFO')).upper()
        numeric_level = getattr(logging, log_level_str, logging.INFO)
        logging.basicConfig(level=numeric_level,
                            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        # Ensure this specific logger (server.config) is at DEBUG level for verbose config loading details
        logger.setLevel(logging.DEBUG)
        logger.info(f"Logging level set to {log_level_str} (server.config logger forced to DEBUG for this run)")

    def _process_raft_nodes(self):
        self.all_raft_nodes_config: List[Dict[str, Any]] = self._raw_config.get('raft', {}).get('nodes', [])
        self.current_node_raft_config: Optional[Dict[str, Any]] = None
        self.peer_raft_configs: List[Dict[str, Any]] = []

        if not self.all_raft_nodes_config:
            logger.warning("No Raft nodes defined in configuration file.")
            return

        if self.node_id:
            for node_conf in self.all_raft_nodes_config:
                if node_conf.get('node_id') == self.node_id:
                    self.current_node_raft_config = node_conf
                else:
                    self.peer_raft_configs.append(node_conf)
            
            if not self.current_node_raft_config:
                logger.warning(f"Current node_id '{self.node_id}' not found in Raft nodes configuration.")
        else:
            logger.warning("NODE_ID not set, cannot determine current node or peers from Raft configuration.")
            # If NODE_ID is not set, all nodes are effectively peers for some generic context, or none are current.
            self.peer_raft_configs = list(self.all_raft_nodes_config) 

    def get(self, key: str, default: Any = None) -> Any:
        """Get a top-level configuration value."""
        return self._raw_config.get(key, default)

    # --- API Server Config --- 
    def get_api_host(self) -> str:
        return os.getenv('API_HOST', self._raw_config.get('api_server', {}).get('default_host', '127.0.0.1'))

    def get_api_port(self) -> int:
        env_port = os.getenv('API_PORT')
        if env_port:
            try:
                return int(env_port)
            except ValueError:
                logger.warning(f"Invalid API_PORT environment variable: {env_port}. Falling back to config.")
        
        if self.node_id and self.current_node_raft_config: # Check if node_id is valid
            api_ports_map = self._raw_config.get('api_server', {}).get('ports', {})
            node_specific_port = api_ports_map.get(self.node_id)
            if node_specific_port is not None:
                return int(node_specific_port)
        
        # Fallback if no specific port found or NODE_ID not set for specific lookup
        # This part might need a more robust default or error if port is crucial and not found
        default_api_port = 9000 # A generic default if nothing else is found
        logger.warning(f"API port for node '{self.node_id}' not found in config, defaulting to {default_api_port}. Set API_PORT or configure in config.yaml.")
        return default_api_port

    # --- Raft Config --- 
    def get_raft_config(self, key: str, default: Any = None) -> Any:
        return self._raw_config.get('raft', {}).get(key, default)

    def get_current_node_rpc_host(self) -> Optional[str]:
        if self.current_node_raft_config:
            return os.getenv('RPC_HOST', self.current_node_raft_config.get('rpc_host'))
        return os.getenv('RPC_HOST') # Fallback to env if node_id not in config

    def get_current_node_rpc_port(self) -> Optional[int]:
        env_rpc_port = os.getenv('RPC_PORT')
        if env_rpc_port:
            try:
                return int(env_rpc_port)
            except ValueError:
                logger.warning(f"Invalid RPC_PORT environment variable: {env_rpc_port}. Falling back to config.")

        if self.current_node_raft_config:
            port = self.current_node_raft_config.get('rpc_port')
            return int(port) if port is not None else None
        return None

    def get_current_node_rpc_address(self) -> Optional[Tuple[str, int]]:
        host = self.get_current_node_rpc_host()
        port = self.get_current_node_rpc_port()
        if host and port is not None:
            return host, port
        logger.warning(f"RPC address for current node '{self.node_id}' could not be determined.")
        return None

    def get_peer_node_ids(self) -> List[str]:
        """Get the node_ids of all peer nodes."""
        return [peer['node_id'] for peer in self.peer_raft_configs if peer.get('node_id')]
        
    def is_single_node_mode(self) -> bool:
        """Check if the cluster is configured to run in single-node mode.
        
        Returns:
            bool: True if this is explicitly configured as a single-node cluster or if only one node exists in the config
        """
        # Check if single_node_mode is explicitly set in config
        explicit_single_mode = self._raw_config.get('raft', {}).get('single_node_mode', False)
        
        # If not explicitly set, check if there's only one node in the configuration
        if not explicit_single_mode and self.all_raft_nodes_config:
            return len(self.all_raft_nodes_config) == 1
        
        return explicit_single_mode

    def get_all_node_addresses(self) -> Dict[str, Tuple[str, int, str, int]]:
        """
        Returns a dictionary of all Raft node addresses, including API and RPC.
        Format: {node_id: (api_host, api_port, rpc_host, rpc_port)}
        """
        all_addresses = {}
        # Correctly access the list of nodes from the 'raft' section
        raft_nodes_list = self._raw_config.get('raft', {}).get('nodes', [])
        for config in raft_nodes_list: # Iterate over the list of node dictionaries
            node_id = config.get('node_id')
            if not node_id:
                logger.warning("Node configuration missing 'node_id'. Skipping.")
                continue

            api_host = config.get('api_host', '127.0.0.1')
            api_port = config.get('api_port')
            rpc_host = config.get('rpc_host', '127.0.0.1')
            rpc_port = config.get('rpc_port')

            if api_port and rpc_port:
                all_addresses[node_id] = (api_host, api_port, rpc_host, rpc_port)
            else:
                logger.warning(f"Incomplete address configuration for node {node_id}. Skipping.")
        logger.debug(f"get_all_node_addresses returning: {all_addresses}")
        return all_addresses

    def get_snapshot_path(self) -> str:
        snapshot_dir = self._raw_config.get('raft', {}).get('snapshot_dir', 'raft_persistent_state')
        os.makedirs(snapshot_dir, exist_ok=True)
        return os.path.join(snapshot_dir, f"raft_snapshot_{self.node_id}.json")
        
    def get_etcd_endpoints(self) -> List[str]:
        # Prioritize environment variable, then config file, then default
        etcd_env = os.getenv('ETCD_ENDPOINTS')
        if etcd_env:
            return [ep.strip() for ep in etcd_env.split(',')]
        
        etcd_config = self._raw_config.get('etcd', {}).get('endpoints')
        if etcd_config:
            return etcd_config

        logger.warning("ETCD_ENDPOINTS not found in environment or config. Using default: ['http://localhost:2379']")
        return ['http://localhost:2379']

    def get_database_config(self) -> Dict[str, Any]:
        """
        Get database configuration with defaults for SQLite if not specified.
        Returns a dictionary with database configuration including connection string.
        """
        # Get database config from YAML or use defaults
        db_config = self._raw_config.get('database', {})
        
        # Default to SQLite if no database type is specified
        db_type = db_config.get('type', 'sqlite')
        
        if db_type == 'sqlite':
            # For SQLite, use a file-based database in the current directory
            db_name = f"chat_app_{self.node_id or 'node'}.db"
            connection_string = f"sqlite:///{db_name}"
        else:
            # For other database types, expect a connection string or build from components
            connection_string = db_config.get('connection_string', '')
            if not connection_string and all(k in db_config for k in ['host', 'port', 'user', 'password', 'dbname']):
                # Build connection string from components if not provided directly
                connection_string = f"{db_type}://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['dbname']}"
        
        return {
            'type': db_type,
            'connection_string': connection_string,
            'echo': db_config.get('echo', False)  # SQLAlchemy echo parameter
        }

# Global config instance
# Load config when module is imported. Applications should import this instance.
app_config = AppConfig()

if __name__ == '__main__':
    # Example usage when running this module directly for testing
    print(f"Loaded configuration for NODE_ID: {app_config.node_id}")
    print(f"Log Level: {app_config.get('log_level')}")
    print(f"API Host: {app_config.get_api_host()}")
    print(f"API Port: {app_config.get_api_port()}")
    print(f"Raft Election Min Timeout: {app_config.get_raft_config('election_timeout_min_ms')}ms")
    print(f"Raft Heartbeat Interval: {app_config.get_raft_config('heartbeat_interval_ms')}ms")
    print(f"Current Node RPC Address: {app_config.get_current_node_rpc_address()}")
    print(f"Peer Node IDs: {app_config.get_peer_node_ids()}")
    print(f"All Node Addresses: {app_config.get_all_node_addresses()}")
    print(f"Snapshot Path: {app_config.get_snapshot_path()}")

    # Test with a specific NODE_ID if not set in env for testing
    if not os.getenv('NODE_ID'):
        print("\nTesting with NODE_ID='node1' (simulated)")
        os.environ['NODE_ID'] = 'node1'
        test_config_node1 = AppConfig() # Reload with NODE_ID
        print(f"  API Port (node1): {test_config_node1.get_api_port()}")
        print(f"  RPC Address (node1): {test_config_node1.get_current_node_rpc_address()}")
        print(f"  Peer IDs (for node1): {test_config_node1.get_peer_node_ids()}")
        print(f"  Snapshot Path (node1): {test_config_node1.get_snapshot_path()}")
        del os.environ['NODE_ID']
