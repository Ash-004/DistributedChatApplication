import uuid
import time
import os
import json # Added for persistent state
import logging

logger = logging.getLogger(__name__)
from typing import List, Dict, Any, Optional, Tuple # Added Any, Optional, Tuple

from fastapi import FastAPI, HTTPException, Depends, Request, Response
from fastapi.responses import JSONResponse, RedirectResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from sqlalchemy.orm import Session
import uvicorn
import asyncio # Added to fix NameError in periodic_save_raft_state
import requests # Added for health checks

# Cascade: Assuming these imports are still correct relative to the new structure
# Change this import
# from server.internal.sequencer.raft_node import RaftNode

# To this import
from server.internal.sequencer.raft import RaftNode # Corrected import
from server.internal.sequencer.raft_rpc_client import RaftRPCClient
from server.internal.sequencer.raft_rpc import start_rpc_server
# from server.internal.sequencer import raft # Commenting out or removing if raft.py is not a package or not needed directly
from server.internal.storage.database import init_db, get_db, SessionLocal
from server.internal.storage.models import Room as RoomModel, Message as MessageModel, RoomParticipant
from server.internal.sequencer.persistent_sequencer import PersistentSequencer, DistributedSequencer

from server.config import app_config # Import the global AppConfig instance
from server.internal.discovery.etcd_client import EtcdClient # Import EtcdClient
from server.internal.domain.models import Room as DomainRoom, User as DomainUser # Renamed to avoid conflict with Pydantic models

# Define response models needed for API endpoints
class NodeInfo(BaseModel):
    id: str
    api_address: str
    rpc_address: str
    role: str
    term: Optional[int] = None

class LogEntry(BaseModel):
    index: int
    term: int
    command: Dict[str, Any]
    
    
class RaftState(BaseModel):
    node_id: str
    current_term: int
    voted_for: Optional[str] = None
    commit_index: int
    last_applied: int
    role: str
    leader_id: Optional[str] = None
    log_size: int

class MessageResponse(BaseModel):
    status: str
    message_id: str
    detail: Optional[str] = None

class RoomResponse(BaseModel):
    status: str
    room_id: str
    name: str
    created_at: float
    detail: Optional[str] = None

class RoomCreate(BaseModel):
    name: str

class MessageCreate(BaseModel):
    room_id: str
    user_id: str
    content: str


# Global instances (initialized later in __main__ or on app startup)
etcd_client: Optional[EtcdClient] = None
raft_node_instance: Optional[RaftNode] = None
raft_rpc_clients: Dict[str, RaftRPCClient] = {}

# Constants from app_config
NODE_ID = app_config.node_id
API_HOST = app_config.get_api_host()
API_PORT = app_config.get_api_port()
CURRENT_NODE_RPC_ADDRESS = app_config.get_current_node_rpc_address()
PEER_NODE_IDS = app_config.get_peer_node_ids()
ALL_RAFT_NODE_RPC_ADDRESSES = app_config.get_all_node_addresses() # This includes current node

# FastAPI app setup
app = FastAPI(
    title=f"Chat API Node {NODE_ID}",
    description="Distributed Chat Application API with Raft Consensus",
    version="0.1.0",
)

# CORS Middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Adjust as needed for your frontend
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Configure logging (app_config already does basicConfig, this ensures FastAPI also uses it)
# logging.getLogger().setLevel(app_config.get('log_level', 'INFO').upper())
# logging.basicConfig(level=app_config.get('log_level', 'INFO').upper(), format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

@app.on_event("startup")
async def startup_event():
    global etcd_client, raft_node_instance, raft_rpc_clients
    
    # Initialize EtcdClient
    etcd_endpoints = app_config.get_etcd_endpoints()
    # Ensure the EtcdClient is initialized with the node's specific addresses for registration
    # Check if CURRENT_NODE_RPC_ADDRESS is available
    rpc_address = None
    if CURRENT_NODE_RPC_ADDRESS is not None:
        rpc_address = f"http://{CURRENT_NODE_RPC_ADDRESS[0]}:{CURRENT_NODE_RPC_ADDRESS[1]}"
    else:
        # Fallback to using the RPC_HOST and RPC_PORT environment variables or configuration
        rpc_host = os.getenv('RPC_HOST', '127.0.0.1')
        rpc_port = os.getenv('RPC_PORT')
        
        # If RPC_PORT is not set in environment, try to get from the config based on NODE_ID
        if rpc_port is None:
            for node in app_config._raw_config.get('raft', {}).get('nodes', []):
                if node.get('node_id') == NODE_ID:
                    rpc_host = node.get('rpc_host', rpc_host)
                    rpc_port = node.get('rpc_port')
                    logger.info(f"Found RPC configuration for node {NODE_ID}: host={rpc_host}, port={rpc_port}")
                    break
        
        # If we still don't have a port, use a default based on the node ID
        if rpc_port is None:
            # Default RPC ports: node1=8004, node2=8005, node3=8006
            default_ports = {"node1": 8004, "node2": 8005, "node3": 8006}
            rpc_port = default_ports.get(NODE_ID)
            logger.warning(f"No RPC port configured for {NODE_ID}, using default: {rpc_port}")
        
        if rpc_port is not None:
            rpc_address = f"http://{rpc_host}:{rpc_port}"
            logger.info(f"Using RPC address: {rpc_address}")
        else:
            logger.error(f"Node {NODE_ID}: Could not determine RPC address. Please set RPC_HOST and RPC_PORT environment variables or configure them in raft_config.yaml.")
            raise ValueError("RPC address not configured")
    
    etcd_client = EtcdClient(
        etcd_endpoints=etcd_endpoints,
        node_id=NODE_ID,
        api_address=f"http://{API_HOST}:{API_PORT}",
        rpc_address=rpc_address
    )
    logger.info(f"Node {NODE_ID}: Initialized EtcdClient with endpoints: {etcd_endpoints}")

    # Initialize RaftRPCClients for peers
    for peer_id in PEER_NODE_IDS:
        # Retrieve RPC address from the comprehensive node addresses map
        # The tuple is (api_host, api_port, rpc_host, rpc_port)
        _, _, peer_host, peer_port = app_config.get_all_node_addresses().get(peer_id)
        raft_rpc_clients[peer_id] = RaftRPCClient(peer_host, peer_port)
        logger.info(f"Node {NODE_ID}: Initialized RaftRPCClient for peer {peer_id} at {peer_host}:{peer_port}")

    # Initialize RaftNode with correct parameters
    # Create persistent_state dict for RaftNode initialization
    persistent_state = {}
    # We need strings for the API and RPC addresses, not tuples
    api_endpoint = f"http://{API_HOST}:{API_PORT}"
    rpc_endpoint = f"http://{CURRENT_NODE_RPC_ADDRESS[0]}:{CURRENT_NODE_RPC_ADDRESS[1]}"

    # Initialize the PersistentSequencer
    # PersistentSequencer needs a WAL path and db_session_factory
    wal_path = app_config.get_raft_config('wal_path', 'raft_wal.log')
    sequencer_instance = PersistentSequencer(wal_path=wal_path, db_session_factory=SessionLocal)
    
    # Initialize DistributedSequencer with the correct parameters
    raft_node_instance = DistributedSequencer(
        node_id=NODE_ID,
        peers=PEER_NODE_IDS,
        node_address=api_endpoint,
        rpc_port=CURRENT_NODE_RPC_ADDRESS[1],
        sequencer=sequencer_instance
    )
    logger.info(f"Node {NODE_ID}: Initialized RaftNode.")
    logger.info(f"Node {NODE_ID}: Type of raft_node_instance: {type(raft_node_instance)}")
    logger.info(f"Node {NODE_ID}: Attributes of raft_node_instance: {dir(raft_node_instance)}")

    # API endpoint registration is handled during RaftNode initialization
    # with the node_api_address_str parameter we passed
    logger.info(f"Node {NODE_ID}: API endpoint {api_endpoint} passed to RaftNode for registration.")

    # Start the Raft RPC server (non-blocking)
    # This is already run in a separate thread inside the function
    try:
        rpc_server = start_rpc_server(raft_node_instance, CURRENT_NODE_RPC_ADDRESS[0], CURRENT_NODE_RPC_ADDRESS[1])
        logger.info(f"Node {NODE_ID}: RPC server started successfully.")
    except Exception as e:
        logger.error(f"Node {NODE_ID}: Failed to start RPC server: {e}")
        raise
    logger.info(f"Node {NODE_ID}: Started Raft RPC server on {CURRENT_NODE_RPC_ADDRESS[0]}:{CURRENT_NODE_RPC_ADDRESS[1]}")

    # Initialize database
    init_db()
    logger.info(f"Node {NODE_ID}: Database initialized.")

    # Wait for all RPC servers in the cluster to be ready
    # This block will be executed by each api_server.py instance.
    # Each instance will wait for ALL other instances' RPC servers to be ready.
    await wait_for_all_rpc_servers_ready(ALL_RAFT_NODE_RPC_ADDRESSES)

    # Start Raft election timer after all RPC servers are confirmed ready
    raft_node_instance.start_election_timer()
    logger.info(f"Node {NODE_ID}: Raft election timer initiated explicitly after all RPC servers are ready.")

@app.on_event("shutdown")
async def shutdown_event():
    logger.info(f"Node {NODE_ID}: Shutting down...")
    if etcd_client:
        await etcd_client.unregister_node(NODE_ID)
        logger.info(f"Node {NODE_ID}: Unregistered from Etcd.")
    if raft_node_instance:
        raft_node_instance.stop()
        logger.info(f"Node {NODE_ID}: RaftNode stopped.")


# Helper function to check if a specific RPC server is ready
async def check_rpc_server_ready(node_id: str, host: str, port: int, max_retries: int = 5, retry_delay_sec: float = 1.0):
    server_url = f"http://{host}:{port}/health"
    logger.info(f"Node {NODE_ID}: Checking RPC server readiness for node {node_id} at {server_url}")
    
    for i in range(max_retries):
        try:
            # Use aiohttp or httpx for async HTTP requests if available
            # For now, using requests in a synchronous way
            response = requests.get(server_url, timeout=1.0)
            if response.status_code == 200:
                logger.info(f"Node {NODE_ID}: RPC server for node {node_id} is ready at {host}:{port}")
                return True
        except Exception as e:
            logger.warning(f"Node {NODE_ID}: RPC server for node {node_id} not ready at {host}:{port}, retry {i+1}/{max_retries}: {e}")
        
        # Wait before retrying
        await asyncio.sleep(retry_delay_sec)
    
    logger.error(f"Node {NODE_ID}: Failed to connect to RPC server for node {node_id} at {host}:{port} after {max_retries} retries")
    return False

# Helper function to wait for all RPC servers to be ready
async def wait_for_all_rpc_servers_ready(all_rpc_addresses: Dict[str, Tuple[str, int, str, int]], timeout: int = 60):
    logger.info(f"Node {NODE_ID}: Waiting for all RPC servers to be ready...")
    tasks = []
    for node_id, (api_host, api_port, rpc_host, rpc_port) in all_rpc_addresses.items():
        if node_id != NODE_ID: # Don't check self
            tasks.append(asyncio.create_task(check_rpc_server_ready(node_id, rpc_host, rpc_port)))

    
    if not tasks: # Only one node in config
        logger.info(f"Node {NODE_ID}: No other RPC servers to wait for (single node configuration).")
        return

    try:
        # Use asyncio.wait to wait for all tasks to complete, with a timeout
        done, pending = await asyncio.wait(tasks, timeout=timeout)

        if pending:
            for task in pending:
                task.cancel() # Cancel tasks that are still pending
            logger.warning(f"Node {NODE_ID}: Timed out waiting for {len(pending)} RPC servers to become ready after {timeout} seconds.")
            # Depending on strictness, you might want to raise an exception here
            # raise HTTPException(status_code=504, detail="Timeout waiting for all RPC servers to be ready.")
        else:
            logger.info(f"Node {NODE_ID}: All {len(done)} peer RPC servers are ready.")
    except Exception as e:
        logger.error(f"Node {NODE_ID}: Error waiting for RPC servers: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Error during RPC server readiness check.")

async def check_rpc_server_ready(node_id: str, host: str, port: int, retries: int = 5, delay: int = 1):
    url = f"http://{host}:{port}/health"
    for i in range(retries):
        try:
            response = requests.get(url, timeout=delay) # Use a short timeout for the health check itself
            response.raise_for_status() # Raise an HTTPError for bad responses (4xx or 5xx)
            logger.info(f"Node {NODE_ID}: RPC server {node_id} at {host}:{port} is ready.")
            return True
        except requests.exceptions.RequestException as e:
            logger.warning(f"Node {NODE_ID}: RPC server {node_id} at {host}:{port} not ready yet (attempt {i+1}/{retries}): {e}")
            await asyncio.sleep(delay)
    logger.error(f"Node {NODE_ID}: RPC server {node_id} at {host}:{port} failed to become ready after {retries} attempts.")
    return False

# Dependency to ensure request is handled by the leader
async def redirect_if_not_leader(request: Request, max_retries: int = 3, retry_delay_sec: float = 0.5):
    if raft_node_instance is None:
        logger.warning(f"Node {NODE_ID}: Raft node not initialized. Cannot determine leader status.")
        raise HTTPException(status_code=503, detail="Raft node not initialized.")

    if not raft_node_instance.state == "LEADER":
        logger.info(f"Node {NODE_ID}: Current node is a {raft_node_instance.state}. Redirecting to leader.")
        # Attempt to get leader's API endpoint from Etcd with retries
        for attempt in range(max_retries):
            try:
                leader_api_url = etcd_client.get_leader_api_endpoint() # Removed await
                if leader_api_url:
                    # Construct the full redirect URL, preserving the original path and query parameters
                    # Ensure leader_api_url does not end with a slash if path starts with one, and vice-versa
                    full_redirect_url = f"{leader_api_url.rstrip('/')}{request.url.path}"
                    if request.url.query:
                        full_redirect_url += f"?{request.url.query}"
                    logger.info(f"Node {NODE_ID}: Redirecting to leader at {full_redirect_url} (attempt {attempt + 1})")
                    raise HTTPException(status_code=307, detail=full_redirect_url) # Use 307 Temporary Redirect
                else:
                    logger.warning(f"Node {NODE_ID}: Leader API endpoint not found in Etcd for redirection (attempt {attempt + 1}/{max_retries}). Retrying in {retry_delay_sec}s...")
                    await asyncio.sleep(retry_delay_sec) # Use asyncio.sleep for async context

            except HTTPException as http_exc:
                # Re-raise HTTPException to propagate redirects or other HTTP errors immediately
                raise http_exc
            except Exception as e:
                logger.error(f"Node {NODE_ID}: Error fetching leader info for redirection (attempt {attempt + 1}/{max_retries}): {e}", exc_info=True)
                if attempt < max_retries - 1:
                    await asyncio.sleep(retry_delay_sec)
                else:
                    raise HTTPException(status_code=500, detail="Error redirecting to leader after multiple attempts.")
        
        # If loop finishes without returning/raising, it means leader was not found after all retries
        logger.error(f"Node {NODE_ID}: Leader API endpoint not found after {max_retries} attempts.")
        raise HTTPException(status_code=503, detail="Leader API endpoint not found after multiple retries.")
    return True # Current node is leader, proceed

# --- FastAPI Routes ---
@app.get("/routes")
async def get_all_routes(request: Request):
    return {"routes": [route.path for route in request.app.routes]}

# Helper to get leader's API address for redirection
def get_leader_api_redirect_url(endpoint: str) -> Optional[str]:
    if raft_node_instance.leader_id and raft_node_instance.leader_id != NODE_ID:
        # The tuple is (api_host, api_port, rpc_host, rpc_port)
        leader_api_host, leader_api_port, _, _ = app_config.get_all_node_addresses().get(raft_node_instance.leader_id)
        if leader_api_host and leader_api_port:
            url = f"http://{leader_api_host}:{leader_api_port}{endpoint}"
            logger.info(f"Node {NODE_ID}: Not leader. Leader is {raft_node_instance.leader_id}. Redirecting to {url}")
            return url
        else:
            logger.warning(f"Node {NODE_ID}: Not leader. Leader is {raft_node_instance.leader_id}, but API address not found in config.")
    return None

@app.post("/send_message")
async def send_message(message: MessageCreate, _ = Depends(redirect_if_not_leader)):
    entry_data = {
        "type": "SEND_MESSAGE",
        "data": {
            "id": str(uuid.uuid4()),
            "room_id": message.room_id,
            "user_id": message.user_id,
            "content": message.content,
            "message_type": "chat_message",
            "timestamp": time.time()
        }
    }

    # Since redirect_if_not_leader handles redirection, we are on the leader here.
    result = raft_node_instance.record_entry(
        room_id=message.room_id,
        user_id=message.user_id,
        content=message.content,
        msg_type="chat_message"
    )

    if result and result.get('status') == 'success':
        logger.info(f"Node {NODE_ID}: Message command processed successfully: {entry_data['data']['id']}")
        # Assuming the 'id' in entry_data is the one to return, or result might contain a specific one.
        return JSONResponse(content={"status": "success", "message_id": entry_data['data']['id'], "detail": "Message command processed."}, status_code=200)
    else:
        error_detail = result.get('message', 'Failed to process command by Raft') if result else 'No response from Raft'
        logger.error(f"Node {NODE_ID}: Failed to process message command: {error_detail}")
        raise HTTPException(status_code=500, detail=error_detail)

@app.post("/messages", response_model=MessageResponse)
async def create_message(
    message: MessageCreate,
    _ = Depends(redirect_if_not_leader), # Ensure request goes to leader
    db: Session = Depends(get_db)
):
    try:
        entry_data = {
            "id": str(uuid.uuid4()), # Message ID generated by API server
            "room_id": message.room_id,
            "user_id": message.user_id,
            "content": message.content,
            "message_type": "text", 
            "timestamp": time.time()
        }
        
        result = raft_node_instance.record_entry(
            room_id=message.room_id,
            user_id=message.user_id,
            content=message.content,
            msg_type="text"
        )
        if result and result.get('status') == 'success':
            logger.info(f"Message entry {entry_data['id']} accepted by Raft leader {NODE_ID}.")
            return {"status": "success", "message_id": entry_data['id'], "detail": "Message accepted by leader, pending replication."}
        else:
            err_msg = result.get('message', 'Unknown error') if result else 'No response from Raft'
            logger.error(f"Raft leader {NODE_ID} failed to accept message entry: {err_msg}")
            raise HTTPException(status_code=500, detail=f"Raft leader failed to accept message: {err_msg}")
    except Exception as e:
        logger.error(f"Error in /messages on leader {NODE_ID}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/rooms", response_model=RoomResponse)
async def create_room(
    room_create: RoomCreate,
    _ = Depends(redirect_if_not_leader), # Ensure request goes to leader
    db: Session = Depends(get_db)
):
    try:
        # Generate a UUID for the room
        room_id = str(uuid.uuid4())
        # Generate a UUID for the creator (using a placeholder for now)
        creator_id = str(uuid.uuid4())  # In a real app, this would come from authentication
        created_at = time.time()
        
        logger.info(f"Node {NODE_ID}: Processing room creation command via DistributedSequencer: {room_create.name}")
        
        # Call record_entry with the parameters expected by DistributedSequencer
        result = raft_node_instance.record_entry(
            room_id=room_id,
            user_id=creator_id,
            content=room_create.name,  # Using content field for room name
            msg_type="CREATE_ROOM"  # Using msg_type to indicate this is a room creation
        )
        if 'error' in result or not result.get('id'):
            err_msg = result.get('error', 'Unknown error')
            logger.error(f"Node {NODE_ID}: Failed to create room '{room_create.name}': {err_msg}")
            return RoomResponse(
                status="error",
                room_id=room_id,
                name=room_create.name,
                created_at=created_at,
                error=err_msg
            )
        else:
            logger.info(f"Node {NODE_ID}: Room '{room_create.name}' created successfully with ID {room_id}")
            return RoomResponse(
                status="success",
                room_id=room_id,
                name=room_create.name,
                created_at=created_at
            )
        
    except Exception as e:
        logger.error(f"Node {NODE_ID}: Error creating room '{room_create.name}': {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/health")
async def health_check():
    return {"status": "ok", "node_id": NODE_ID, "raft_state": raft_node_instance.state if raft_node_instance else "uninitialized"}

@app.get("/raft_state", response_model=RaftState)
async def get_raft_state():
    if not raft_node_instance:
        raise HTTPException(status_code=503, detail="Raft node not initialized.")
    
    return RaftState(
        node_id=raft_node_instance.node_id,
        current_term=raft_node_instance.current_term,
        voted_for=raft_node_instance.voted_for,
        commit_index=raft_node_instance.commit_index,
        last_applied=raft_node_instance.last_applied,
        role=raft_node_instance.state,
        leader_id=raft_node_instance.leader_id,
        log_size=len(raft_node_instance.log)
    )

@app.get("/nodes", response_model=List[NodeInfo])
async def get_cluster_nodes():
    nodes = []
    # Include current node
    nodes.append(NodeInfo(
        id=NODE_ID,
        api_address=f"http://{API_HOST}:{API_PORT}",
        rpc_address=f"http://{CURRENT_NODE_RPC_ADDRESS[0]}:{CURRENT_NODE_RPC_ADDRESS[1]}",
        role=raft_node_instance.state if raft_node_instance else "unknown",
        term=raft_node_instance.current_term if raft_node_instance else None
    ))
    # Include peers
    for peer_id in PEER_NODE_IDS:
        peer_host, peer_port = app_config.get_node_address(peer_id)
        # For API address, we assume a convention or fetch from Etcd if available
        # For simplicity, let's assume API port is same as RPC port for now, or fetch from Etcd
        # Here, we'll just use the RPC address as a placeholder for peer info
        nodes.append(NodeInfo(
            id=peer_id,
            api_address="N/A", # Or fetch from Etcd if registered
            rpc_address=f"http://{peer_host}:{peer_port}",
            role="unknown", # Can't know peer's role directly from here without another RPC
            term=None
        ))
    return nodes

@app.get("/leader_api_address")
async def get_leader_api_address():
    if not etcd_client:
        raise HTTPException(status_code=503, detail="Etcd client not available.")
    try:
        leader_api_url = etcd_client.get_leader_api_endpoint()  # Removed await as this is a synchronous method
        if leader_api_url:
            return {"leader_api_address": leader_api_url}
        else:
            raise HTTPException(status_code=404, detail="Leader API address not found.")
    except Exception as e:
        logger.error(f"Node {NODE_ID}: Error fetching leader API address from etcd: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Error fetching leader API address from etcd.")

@app.get("/leader_info")
async def get_leader_information():
    logger.info(f"Node {NODE_ID}: Received request for /leader_info")
    if not etcd_client:
        logger.error(f"Node {NODE_ID}: Etcd client not initialized. Cannot fetch leader info.")
        raise HTTPException(status_code=503, detail="Etcd client not available.")
    try:
        leader_info = etcd_client.get_leader_info()
        if leader_info:
            logger.info(f"Node {NODE_ID}: Successfully fetched leader info from etcd: {leader_info}")
            return leader_info
        else:
            logger.warning(f"Node {NODE_ID}: No leader information currently available in etcd.")
            raise HTTPException(status_code=404, detail="Leader information not found.")
    except Exception as e:
        logger.error(f"Node {NODE_ID}: Error fetching leader information from etcd: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Error fetching leader information from etcd.")

@app.get("/rooms/{room_id}/messages")
async def get_messages(room_id: str, db: Session = Depends(get_db)):
    try:
        # Query messages for the specified room, ordered by sequence number
        messages = db.query(MessageModel).filter(MessageModel.room_id == room_id).order_by(MessageModel.sequence_number).all()
        
        # Convert database models to response models
        response = [
            {
                "message_id": message.id,
                "room_id": message.room_id,
                "user_id": message.user_id,
                "content": message.content,
                "message_type": message.message_type,
                "created_at": message.created_at,
                "sequence_number": message.sequence_number
            } for message in messages
        ]
        
        return response
    except Exception as e:
        logger.error(f"Node {NODE_ID}: Error retrieving messages for room {room_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/health")
async def health_check():
    return {"status": "ok", "node_id": NODE_ID, "raft_state": raft_node_instance.state if raft_node_instance else "uninitialized"}

@app.get("/raft_state", response_model=RaftState)
async def get_raft_state():
    if not raft_node_instance:
        raise HTTPException(status_code=503, detail="Raft node not initialized.")
    
    return RaftState(
        node_id=raft_node_instance.node_id,
        current_term=raft_node_instance.current_term,
        voted_for=raft_node_instance.voted_for,
        commit_index=raft_node_instance.commit_index,
        last_applied=raft_node_instance.last_applied,
        role=raft_node_instance.state,
        leader_id=raft_node_instance.leader_id,
        log_size=len(raft_node_instance.log)
    )

@app.get("/nodes", response_model=List[NodeInfo])
async def get_cluster_nodes():
    nodes = []
    # Include current node
    nodes.append(NodeInfo(
        id=NODE_ID,
        api_address=f"http://{API_HOST}:{API_PORT}",
        rpc_address=f"http://{CURRENT_NODE_RPC_ADDRESS[0]}:{CURRENT_NODE_RPC_ADDRESS[1]}",
        role=raft_node_instance.state if raft_node_instance else "unknown",
        term=raft_node_instance.current_term if raft_node_instance else None
    ))
    # Include peers
    for peer_id in PEER_NODE_IDS:
        peer_host, peer_port = app_config.get_node_address(peer_id)
        # For API address, we assume a convention or fetch from Etcd if available
        # For simplicity, let's assume API port is same as RPC port for now, or fetch from Etcd
        # Here, we'll just use the RPC address as a placeholder for peer info
        nodes.append(NodeInfo(
            id=peer_id,
            api_address="N/A", # Or fetch from Etcd if registered
            rpc_address=f"http://{peer_host}:{peer_port}",
            role="unknown", # Can't know peer's role directly from here without another RPC
            term=None
        ))
    return nodes

@app.get("/leader_api_address")
async def get_leader_api_address():
    if not etcd_client:
        raise HTTPException(status_code=503, detail="Etcd client not available.")
    try:
        leader_api_url = etcd_client.get_leader_api_endpoint()
        if leader_api_url:
            return {"leader_api_address": leader_api_url}
        else:
            raise HTTPException(status_code=404, detail="Leader API address not found.")
    except Exception as e:
        logger.error(f"Node {NODE_ID}: Error fetching leader API address from etcd: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Error fetching leader API address from etcd.")

@app.get("/leader_info")
async def get_leader_information():
    logger.info(f"Node {NODE_ID}: Received request for /leader_info")
    if not etcd_client:
        logger.error(f"Node {NODE_ID}: Etcd client not initialized. Cannot fetch leader info.")
        raise HTTPException(status_code=503, detail="Etcd client not available.")
    try:
        leader_info = etcd_client.get_leader_info()
        if leader_info:
            logger.info(f"Node {NODE_ID}: Successfully fetched leader info from etcd: {leader_info}")
            return leader_info
        else:
            logger.warning(f"Node {NODE_ID}: No leader information currently available in etcd.")
            raise HTTPException(status_code=404, detail="Leader information not found.")
    except Exception as e:
        logger.error(f"Node {NODE_ID}: Error fetching leader information from etcd: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Error fetching leader information from etcd.")

@app.get("/rooms")
async def get_rooms(db: Session = Depends(get_db)):
    try:
        # Check if we need to redirect to leader
        redirect_url = get_leader_api_redirect_url("/rooms")
        if redirect_url:
            return RedirectResponse(url=redirect_url, status_code=307)
            
        # Query all rooms from the database
        rooms = db.query(RoomModel).all()
        
        # Convert database models to response format expected by frontend
        response = [
            {
                "id": room.id,
                "name": room.name,
                "created_by": "Unknown",  # This field isn't stored in our model, using placeholder
                "created_at": str(room.created_at)
            } for room in rooms
        ]
        
        logger.info(f"Node {NODE_ID}: Successfully retrieved {len(rooms)} rooms")
        return response
    except Exception as e:
        logger.error(f"Node {NODE_ID}: Error retrieving rooms: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/leader_info")
async def get_leader_information():
    logger.info(f"Node {NODE_ID}: Received request for /leader_info")
    if not etcd_client:
        logger.error(f"Node {NODE_ID}: Etcd client not initialized. Cannot fetch leader info.")
        raise HTTPException(status_code=503, detail="Etcd client not available.")
    try:
        leader_info = etcd_client.get_leader_info()
        if leader_info:
            logger.info(f"Node {NODE_ID}: Successfully fetched leader info from etcd: {leader_info}")
            return leader_info
        else:
            logger.warning(f"Node {NODE_ID}: No leader information currently available in etcd.")
            raise HTTPException(status_code=404, detail="Leader information not found.")
    except Exception as e:
        logger.error(f"Node {NODE_ID}: Error fetching leader information from etcd: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Error fetching leader information from etcd.")

# --- Main Execution ---
if __name__ == "__main__":


    logger.info(f"Starting Uvicorn API server for node {NODE_ID} on {API_HOST}:{API_PORT}")
    uvicorn.run("server.cmd.api_server:app", host=API_HOST, port=API_PORT, log_config=None, reload=True)