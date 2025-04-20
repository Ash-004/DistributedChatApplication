from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from server.db import db

async def brd_recieve(user_id, websocket):
    data = await websocket.receive_text()
    db.broadcast_insert(user_id, data)
    return data
