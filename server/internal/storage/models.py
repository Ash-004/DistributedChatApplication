import uuid
from sqlalchemy import Column, String, Integer, Float, ForeignKey, Boolean
from sqlalchemy.orm import declarative_base

Base = declarative_base()

class Room(Base):
    __tablename__ = 'rooms'
    id = Column(String, primary_key=True)  # Changed to String
    name = Column(String, nullable=True)  # Nullable for private chats
    is_private = Column(Boolean, default=False)
    created_at = Column(Float, nullable=False)

class RoomParticipant(Base):
    __tablename__ = 'room_participants'
    room_id = Column(String, ForeignKey('rooms.id'), primary_key=True)  # Changed to String
    user_id = Column(String, primary_key=True)  # Changed to String

class Message(Base):
    __tablename__ = 'messages'
    id = Column(String, primary_key=True)  # Changed to String
    room_id = Column(String, ForeignKey('rooms.id'), nullable=False)  # Changed to String
    user_id = Column(String, nullable=False)  # Changed to String
    sequence_number = Column(Integer, nullable=False)
    content = Column(String, nullable=False)
    message_type = Column(String, nullable=False)
    created_at = Column(Float, nullable=False)

class User(Base):
    __tablename__ = 'users'
    id = Column(String, primary_key=True)  # Changed to String
    username = Column(String, unique=True, nullable=False)
    created_at = Column(Float, nullable=False)

class PrivateChat(Base):
    __tablename__ = 'private_chats'
    id = Column(String, primary_key=True)  # Changed to String
    user1_id = Column(String, nullable=False)  # Changed to String
    user2_id = Column(String, nullable=False)  # Changed to String
    created_at = Column(Float, nullable=False)
