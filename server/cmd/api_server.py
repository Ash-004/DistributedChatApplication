import uuid
import time
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from server.internal.sequencer.persistent_sequencer import PersistentSequencer
from server.internal.domain.models import Message

app = FastAPI()
sequencer = PersistentSequencer("wal.log")

# In-memory storage (to be replaced with database)
messages = {}
rooms = {}

class RoomCreate(BaseModel):
    name: str

class MessageCreate(BaseModel):
    room_id: str
    user_id: str
    content: str

@app.post("/messages")
async def submit_message(message: MessageCreate):
    try:
        room_uuid = uuid.UUID(message.room_id)
        user_uuid = uuid.UUID(message.user_id)
        
        # Get next sequence number and record in WAL
        entry = sequencer.record_entry(room_uuid, user_uuid, message.content, "text")
        
        # Create domain message
        msg = Message(
            id=entry.id,
            room_id=room_uuid,
            user_id=user_uuid,
            sequence_number=entry.sequence_number,
            content=message.content,
            message_type="text",
            created_at=time.time() # Use time.time() as a workaround
        )
        
        # Store message (temporary in-memory storage)
        if message.room_id not in messages:
            messages[message.room_id] = []
        messages[message.room_id].append(msg)
        
        return {"message_id": str(msg.id), "sequence_number": msg.sequence_number}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/messages/{room_id}")
async def get_messages(room_id: str):
    if room_id not in messages:
        return []
    return [{
        "id": str(msg.id),
        "user_id": str(msg.user_id),
        "content": msg.content,
        "sequence_number": msg.sequence_number,
        "created_at": msg.created_at # No isoformat for float
    } for msg in messages[room_id]]

@app.post("/rooms")
async def create_room(room: RoomCreate):
    room_id = uuid.uuid4()
    rooms[str(room_id)] = {
        "id": room_id,
        "name": room.name,
        "created_at": time.time() # Use time.time() as a workaround
    }
    return {"room_id": str(room_id)}

@app.get("/rooms")
async def list_rooms():
    return [{"id": str(rid), "name": data["name"]} for rid, data in rooms.items()]

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
