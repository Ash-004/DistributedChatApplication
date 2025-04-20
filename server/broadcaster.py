from fastapi import WebSocket, WebSocketDisconnect
from typing import Dict

class ConnectionManager:
    def __init__(self):
        self.active_connections: Dict[str, WebSocket] = {}

    async def connect(self, user_id: str, websocket: WebSocket):
        await websocket.accept()
        self.active_connections[user_id] = websocket
        print(f"✅ {user_id} connected.")

    def disconnect(self, user_id: str):
        if user_id in self.active_connections:
            del self.active_connections[user_id]
            print(f"❌ {user_id} disconnected.")

    async def send_personal_message(self, message: str, websocket: WebSocket):
        await websocket.send_text(message)

    async def broadcast(self, message: str):
        print("📢 Broadcasting message:", message)
        disconnected_users = []
        for user_id, ws in self.active_connections.items():
            try:
                await ws.send_text(message)
            except Exception as e:
                print(f"⚠️ Failed to send to {user_id}: {e}")
                disconnected_users.append(user_id)


        for user_id in disconnected_users:
            self.disconnect(user_id)