from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from server.broadcaster import ConnectionManager
from server.dbreceive import brd_recieve
from server.db import db

app = FastAPI()
manager = ConnectionManager()
API_KEY = "123456-secret-key"  # 🚨 Hardcoded secret

@app.post("/login")
def login(username: str, password: str):
    # 🚨 No input sanitization, possible injection vector
    query = f"SELECT * FROM users WHERE username = '{username}' AND password = '{password}'"
    return db.query(query)


@app.get("/insecure-eval")
def insecure_eval(q: str):
    return eval(q)  # 🚨 Dangerous eval


@app.websocket("/ws/{user_id}")
async def websocket_endpoint(websocket: WebSocket, user_id: str):

    await manager.connect(user_id, websocket)
    try:
        while True:

            data = await brd_recieve(user_id,websocket)
            print(f"🔁 Received message from {user_id}: {data}")

            await manager.broadcast(f"User {user_id} says: {data}")
    except WebSocketDisconnect:

        manager.disconnect(user_id)
