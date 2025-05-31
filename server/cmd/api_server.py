import uuid
import time
from fastapi import FastAPI, HTTPException, Depends, Request, Response
from fastapi.responses import JSONResponse, RedirectResponse
from pydantic import BaseModel
from sqlalchemy.orm import Session
from server.internal.sequencer.persistent_sequencer import DistributedSequencer
from server.internal.sequencer.raft_rpc import start_rpc_server
from server.internal.sequencer import raft
from server.internal.storage.database import init_db, get_db
from server.internal.storage.models import Room as RoomModel, Message as MessageModel, RoomParticipant
import os
import logging
from typing import List, Dict

app = FastAPI()

# Initialize distributed sequencer
node_id = "node1"
peers = ["node2:8000", "node3:8000"]
node_address = "localhost:8000"
rpc_port = int(os.getenv("RPC_PORT", 8001))
node_addresses = {
    "node1": ("localhost", 12935),
    "node2": ("localhost", 12936),
    "node3": ("localhost", 12937)
}
sequencer = DistributedSequencer(
    node_id=node_id,
    peers=peers,
    rpc_port=rpc_port,
    node_addresses=node_addresses
)
# Start RPC server for Raft
start_rpc_server(sequencer, rpc_port)

# Initialize database
@app.on_event("startup")
def on_startup():
    init_db()

class RoomCreate(BaseModel):
    name: str

class MessageCreate(BaseModel):
    room_id: str
    user_id: str
    content: str

class Message(BaseModel):
    room_id: str
    user_id: str
    content: str
    sequence_number: int = 0
    created_at: float = 0.0
    message_type: str = "text"

@app.post("/send_message")
async def send_message(request: Request, message: Message):
    try:
        # If this node is the leader, record the entry
        if sequencer.state == 'leader':
            # Generate a unique ID for the message
            message_id = str(uuid.uuid4())
            # Record the entry in the log
            result = sequencer.record_entry(
                room_id=message.room_id,
                user_id=message.user_id,
                content=message.content,
                msg_type="text"
            )
            sequence_number = result['sequence_number']
            # Create domain message
            msg = MessageModel(
                id=result['id'],
                room_id=message.room_id,
                user_id=message.user_id,
                sequence_number=sequence_number,
                content=message.content,
                message_type="text",
                created_at=time.time()
            )
            # Store message in database
            db = next(get_db())
            db.add(msg)
            db.commit()
            return JSONResponse(content={"message_id": str(msg.id), "sequence": sequence_number}, status_code=200)
        else:
            # If not the leader, redirect to the leader
            leader_host, leader_port = sequencer.node_addresses[sequencer.leader_id]
            leader_url = f"http://{leader_host}:{leader_port}/send_message"
            return RedirectResponse(url=leader_url, status_code=307)
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)

@app.get("/messages/{room_id}")
async def get_messages(room_id: str, db: Session = Depends(get_db)):
    try:
        room_uuid = room_id
        messages = db.query(MessageModel).filter(MessageModel.room_id == room_uuid).order_by(MessageModel.sequence_number).all()
        return {"messages": [{
            "id": str(msg.id),
            "room_id": str(msg.room_id),
            "user_id": str(msg.user_id),
            "content": msg.content,
            "sequence_number": msg.sequence_number,
            "created_at": msg.created_at
        } for msg in messages]}
    except Exception as e:
        logging.error(f"Error retrieving messages: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve messages")

@app.post("/rooms")
async def create_room(room: RoomCreate, db: Session = Depends(get_db)):
    room_id = str(uuid.uuid4())
    room_model = RoomModel(
        id=room_id,
        name=room.name,
        created_at=time.time()
    )
    db.add(room_model)
    db.commit()
    return {"room_id": room_id}

@app.post('/private-chats')
async def create_private_chat(user1: str, user2: str, db: Session = Depends(get_db)):
    """Create a private chat between two users"""
    # Create private room without a name
    room_id = str(uuid.uuid4())
    room = RoomModel(id=room_id, is_private=True, created_at=time.time())
    db.add(room)
    
    # Add participants
    db.add(RoomParticipant(room_id=room_id, user_id=user1))
    db.add(RoomParticipant(room_id=room_id, user_id=user2))
    db.commit()
    return {'room_id': room_id}

@app.get("/rooms")
async def list_rooms(db: Session = Depends(get_db)):
    rooms = db.query(RoomModel).all()
    return [{"id": str(room.id), "name": room.name} for room in rooms]

@app.post("/messages")
async def create_message(message: MessageCreate):
    """Create a new message in the specified room"""
    try:
        # If this node is the leader, record the entry
        if sequencer.state == 'leader':
            result = sequencer.record_entry(
                room_id=message.room_id,
                user_id=message.user_id,
                content=message.content,
                msg_type="text"
            )
            
            db = next(get_db())
            try:
                # Create and save the message
                msg = MessageModel(
                    id=result['id'],
                    room_id=message.room_id,
                    user_id=message.user_id,
                    content=message.content,
                    sequence_number=result['sequence_number'],
                    message_type="text",
                    created_at=time.time()
                )
                db.add(msg)
                db.commit()
                return {"status": "success", "message_id": result['id']}
            except Exception as e:
                db.rollback()
                logging.error(f"Database error: {e}")
                raise HTTPException(status_code=500, detail="Failed to save message")
        else:
            # If not the leader, redirect to the leader
            leader_host, leader_port = sequencer.node_addresses[sequencer.leader_id]
            leader_url = f"http://{leader_host}:{leader_port}/messages"
            return RedirectResponse(url=leader_url, status_code=307)
    except Exception as e:
        logging.error(f"Error creating message: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/messages")
async def get_messages():
    """Endpoint to get all messages"""
    try:
        # In a real implementation, fetch from shared storage
        return {"messages": messages_store["global"]}
    except Exception as e:
        logging.error(f"Error retrieving messages: {e}")
        raise HTTPException(status_code=500, detail="Message retrieval failed")

@app.get("/raft_state")
async def get_raft_state():
    """Endpoint to get Raft state information"""
    try:
        # Placeholder - replace with actual Raft state
        return {
            "state": "leader",
            "term": 1,
            "leader_id": "node1",
            "nodes": ["node1", "node2", "node3"]
        }
    except Exception as e:
        logging.error(f"Error getting Raft state: {e}")
        raise HTTPException(status_code=500, detail="Raft state unavailable")

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
