"""SQLAlchemy engine, session, get_db. Schema is managed by Alembic."""
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from db.config import DATABASE_URL, DB_PATH

engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

DB_PATH.parent.mkdir(parents=True, exist_ok=True)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
