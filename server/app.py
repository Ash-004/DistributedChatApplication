from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from server.broadcaster import ConnectionManager

app = FastAPI()
manager = ConnectionManager()

@app.websocket("/ws/{user_id}")
async def websocket_endpoint(websocket: WebSocket, user_id: str):
    # Connect the user
    await manager.connect(user_id, websocket)
    try:
        while True:
            # Receive message from the user
            data = await websocket.receive_text()
            print(f"🔁 Received message from {user_id}: {data}")
            
            # Here, you can add any logic to decide when to broadcast
            # For example, you can broadcast the message back to all connected users
            await manager.broadcast(f"User {user_id} says: {data}")
    except WebSocketDisconnect:
        # If the user disconnects, remove from active connections
        manager.disconnect(user_id)