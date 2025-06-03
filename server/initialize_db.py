import os
from dotenv import load_dotenv
from internal.storage.database import init_db

# Load environment variables from .env file
load_dotenv()

# Ensure DATABASE_URL is set
if not os.getenv("DATABASE_URL"):
    print("Error: DATABASE_URL environment variable is not set.")
    print("Please set it to your PostgreSQL connection string, e.g., 'postgresql://user:password@host:port/database_name'")
    exit(1)

print("Initializing database and creating tables...")
try:
    init_db()
    print("Database initialized successfully.")
except Exception as e:
    print(f"Failed to initialize database: {e}")
    exit(1)
