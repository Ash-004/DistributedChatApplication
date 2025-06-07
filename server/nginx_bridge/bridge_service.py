import etcd3
import logging
import signal
import sys
import time
import threading

from . import config
from . import nginx_config

logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL.upper(), logging.INFO),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class NginxBridgeService:
    def __init__(self):
        self.etcd_client = None
        self.current_leader_address = None
        self.running = True
        self.stop_event = threading.Event()
        self._connect_etcd()

    def _connect_etcd(self):
        """Establishes a connection to etcd."""
        while not self.stop_event.is_set():
            try:
                endpoints = []
                for endpoint_str in config.ETCD_ENDPOINTS:
                    try:
                        # Assuming format like 'host:port' or 'http://host:port'
                        if '//' in endpoint_str:
                            scheme, netloc = endpoint_str.split('//', 1)
                            host, port_str = netloc.split(':', 1)
                        else:
                            host, port_str = endpoint_str.split(':', 1)
                        endpoints.append((host, int(port_str)))
                    except ValueError:
                        logger.warning(f"Invalid etcd endpoint format: {endpoint_str}. Skipping.")
                
                if not endpoints:
                    logger.error("No valid etcd endpoints configured after parsing. Cannot connect.")
                    raise ConnectionError("No valid etcd endpoints")

                # Use the first valid endpoint for simplicity, etcd3 client handles multiple if provided as host,port tuples
                # For etcd3.client, it expects host and port for the first server, or a list of (host,port) for 'hosts'
                # Let's try connecting to the first one directly.
                host, port = endpoints[0]
                self.etcd_client = etcd3.client(host=host, port=port)
                self.etcd_client.status()  # Check connection
                logger.info(f"Successfully connected to etcd at {host}:{port}")
                return
            except Exception as e:
                logger.error(f"Failed to connect to etcd: {e}. Retrying in {config.UPDATE_RETRY_INTERVAL} seconds...")
                self.stop_event.wait(config.UPDATE_RETRY_INTERVAL)
        logger.info("ETCD connection attempts stopped by stop_event.")

    def _get_initial_leader(self):
        """Gets the initial leader from etcd when the service starts."""
        if not self.etcd_client:
            logger.error("Etcd client not connected. Cannot get initial leader.")
            return None
        try:
            value, _ = self.etcd_client.get(config.ETCD_LEADER_KEY)
            if value:
                leader_address = value.decode('utf-8')
                logger.info(f"Initial leader found: {leader_address}")
                return leader_address
            else:
                logger.info("No initial leader found in etcd.")
                return None
        except Exception as e:
            logger.error(f"Error getting initial leader from etcd: {e}")
            return None

    def watch_leader_changes(self):
        """Watches for changes to the leader key in etcd and updates NGINX."""
        if not self.etcd_client:
            logger.error("Etcd client not connected. Cannot watch leader changes.")
            return

        # Get initial leader and update NGINX
        initial_leader = self._get_initial_leader()
        if initial_leader and initial_leader != self.current_leader_address:
            if nginx_config.update_nginx_upstream(initial_leader):
                self.current_leader_address = initial_leader
            else:
                logger.error(f"Failed to update NGINX with initial leader: {initial_leader}")
        elif not initial_leader:
            logger.info("No initial leader, NGINX not updated yet.")

        event_iterator, cancel = self.etcd_client.watch(config.ETCD_LEADER_KEY)
        logger.info(f"Watching etcd key '{config.ETCD_LEADER_KEY}' for leader changes...")

        try:
            while self.running and not self.stop_event.is_set():
                try:
                    for event in event_iterator:
                        if self.stop_event.is_set(): break
                        logger.debug(f"Received etcd event: {event}")
                        new_leader_address = None
                        if isinstance(event, etcd3.events.PutEvent):
                            new_leader_address = event.value.decode('utf-8')
                            logger.info(f"Leader changed (PutEvent): New leader is {new_leader_address}")
                        elif isinstance(event, etcd3.events.DeleteEvent):
                            logger.info(f"Leader key '{config.ETCD_LEADER_KEY}' deleted (DeleteEvent). Treating as no leader.")
                            new_leader_address = "" # Represent no leader
                        else:
                            logger.debug(f"Ignoring event type: {type(event)}")
                            continue

                        if new_leader_address is not None and new_leader_address != self.current_leader_address:
                            logger.info(f"Leader changed. Old: '{self.current_leader_address}', New: '{new_leader_address}'")
                            if new_leader_address: # If there's a new leader address
                                if nginx_config.update_nginx_upstream(new_leader_address):
                                    self.current_leader_address = new_leader_address
                                else:
                                    logger.error(f"Failed to update NGINX for leader {new_leader_address}. Will retry on next change.")
                            else: # Leader key was deleted or value is empty
                                logger.info("Leader information cleared. Attempting to update NGINX to reflect no leader.")
                                # How to handle no leader? For now, we might keep the last known good one or clear the upstream.
                                # The current template expects a leader. For simplicity, we'll log and not change NGINX if leader is empty.
                                # A more robust solution might involve a default 'no_leader_available.html' page or similar.
                                # logger.warning("No leader address provided by etcd event. NGINX not updated.") # Commented out old warning
                                if nginx_config.update_nginx_upstream(""): # Pass empty string for no leader
                                    logger.info("NGINX updated to reflect no leader.")
                                    self.current_leader_address = None # Reflect no current leader
                                else:
                                    logger.error("Failed to update NGINX to reflect no leader.")
                                    # self.current_leader_address remains unchanged, will retry on next event
                        elif new_leader_address is not None and new_leader_address == self.current_leader_address:
                            logger.debug(f"Leader address '{new_leader_address}' is the same as current. No NGINX update needed.")

                except etcd3.exceptions.ConnectionFailedError as e:
                    logger.error(f"Etcd connection failed during watch: {e}. Attempting to reconnect...")
                    self.etcd_client = None # Force reconnect
                    self._connect_etcd()
                    if self.etcd_client:
                        event_iterator, cancel = self.etcd_client.watch(config.ETCD_LEADER_KEY)
                        logger.info("Re-established watch on etcd key.")
                    else:
                        logger.error("Failed to reconnect to etcd. Watch loop will exit if stop_event is set.")
                        self.stop_event.wait(config.ETCD_WATCH_TIMEOUT) # Wait before retrying connection in outer loop
                except Exception as e:
                    logger.error(f"Error in etcd watch loop: {e}. Retrying watch setup...")
                    # Brief pause before trying to re-establish the watch
                    self.stop_event.wait(config.UPDATE_RETRY_INTERVAL)
                    if self.etcd_client:
                         try:
                            event_iterator, cancel = self.etcd_client.watch(config.ETCD_LEADER_KEY)
                         except Exception as watch_e:
                            logger.error(f"Failed to re-initiate watch: {watch_e}")
                            self.etcd_client = None # Trigger reconnect
                            self._connect_etcd()
                            if self.etcd_client:
                                event_iterator, cancel = self.etcd_client.watch(config.ETCD_LEADER_KEY)
                            else:
                                self.stop_event.wait(config.ETCD_WATCH_TIMEOUT)
                    else:
                        self._connect_etcd()
                        if self.etcd_client:
                            event_iterator, cancel = self.etcd_client.watch(config.ETCD_LEADER_KEY)
                        else:
                             self.stop_event.wait(config.ETCD_WATCH_TIMEOUT)
        finally:
            logger.info("Stopping etcd watch.")
            cancel.cancel()

    def start(self):
        logger.info("Starting NGINX Bridge Service...")
        signal.signal(signal.SIGINT, self.shutdown)
        signal.signal(signal.SIGTERM, self.shutdown)

        if not self.etcd_client:
            logger.error("Failed to connect to etcd. Bridge service cannot start.")
            return

        self.watch_thread = threading.Thread(target=self.watch_leader_changes, daemon=True)
        self.watch_thread.start()

        # Keep main thread alive until shutdown
        while self.running and not self.stop_event.is_set():
            try:
                time.sleep(1) # Keep main thread alive, periodically check running status
            except InterruptedError:
                break
        
        logger.info("Main loop finished. Waiting for watch thread to complete...")
        if self.watch_thread.is_alive():
            self.watch_thread.join(timeout=10) # Wait for watch thread to finish
        logger.info("NGINX Bridge Service stopped.")

    def shutdown(self, signum=None, frame=None):
        logger.info(f"Shutdown signal received ({signum if signum else 'programmatically'}). Stopping NGINX Bridge Service...")
        self.running = False
        self.stop_event.set() # Signal all loops and waits to stop
        
        # Attempt to clean up etcd client if it exists
        if hasattr(self.etcd_client, 'close'): # etcd3-py client might not have a close method
            try:
                # self.etcd_client.close() # etcd3 library doesn't have an explicit close method for the client object itself.
                # Connections are managed internally.
                pass
            except Exception as e:
                logger.error(f"Error closing etcd client: {e}")
        logger.info("Shutdown process initiated.")

if __name__ == '__main__':
    service = NginxBridgeService()
    try:
        service.start()
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received. Shutting down...")
    finally:
        if service.running:
             service.shutdown()