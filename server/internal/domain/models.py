from uuid import UUID


class Message:
    def __init__(self, id: UUID, room_id: UUID, user_id: UUID, sequence_number: int, content: str, message_type: str, created_at: float, delivered_at: float = None, read_at: float = None):
        self.id = id
        self.room_id = room_id
        self.user_id = user_id
        self.sequence_number = sequence_number
        self.content = content
        self.message_type = message_type
        self.created_at = created_at
        self.delivered_at = delivered_at
        self.read_at = read_at


class Room:
    def __init__(self, id: UUID, name: str, created_at: float, last_active_at: float, participants: list[UUID]):
        self.id = id
        self.name = name
        self.created_at = created_at
        self.last_active_at = last_active_at
        self.participants = participants


class User:
    def __init__(self, id: UUID, username: str, created_at: float, last_seen_at: float):
        self.id = id
        self.username = username
        self.created_at = created_at
        self.last_seen_at = last_seen_at
