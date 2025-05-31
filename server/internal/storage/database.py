import os
import time
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from .models import Base

# Load environment variables from .env file
load_dotenv()

# Database URL configuration (to be loaded from environment variables)
DATABASE_URL = os.getenv("DATABASE_URL")
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def init_db():
    Base.metadata.create_all(bind=engine)
    
    # Create a database session
    db = SessionLocal()
    try:
        # Import here to avoid circular imports
        from .models import Room
        
        # Check if global room exists, if not create it
        global_room = db.query(Room).filter(Room.id == "00000000-0000-0000-0000-000000000000").first()
        if not global_room:
            global_room = Room(
                id="00000000-0000-0000-0000-000000000000",
                name="Global Chat",
                is_private=False,
                created_at=time.time()
            )
            db.add(global_room)
            db.commit()
    except Exception as e:
        db.rollback()
        print(f"Error initializing global room: {e}")
        raise
    finally:
        db.close()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
