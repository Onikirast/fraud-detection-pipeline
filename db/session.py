"""Engine/connection helper shared by the consumer and the API.

Centralizing this in one place means the consumer, the FastAPI app, and any
one-off scripts all read the same environment configuration instead of each
hardcoding a connection string.
"""
import os
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()


def get_engine():
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    db = os.getenv("POSTGRES_DB", "frauddb")
    user = os.getenv("POSTGRES_USER", "fraud")
    password = os.getenv("POSTGRES_PASSWORD", "fraud")
    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
    return create_engine(url, pool_pre_ping=True)
