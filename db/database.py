from sqlalchemy import create_engine, event
from sqlalchemy.orm import Session, sessionmaker

from db.models import Base

_engine = None
_SessionLocal = None


def get_engine(database_url: str | None = None):
    global _engine
    if _engine is None:
        if database_url is None:
            from config import settings
            database_url = settings.DATABASE_URL
        connect_args = {}
        if database_url.startswith("sqlite"):
            connect_args["check_same_thread"] = False
        _engine = create_engine(database_url, connect_args=connect_args, echo=False)
        # Enable WAL mode for SQLite (concurrent reads during writes)
        if database_url.startswith("sqlite"):
            @event.listens_for(_engine, "connect")
            def set_sqlite_pragma(dbapi_conn, connection_record):
                cursor = dbapi_conn.cursor()
                cursor.execute("PRAGMA journal_mode=WAL")
                cursor.execute("PRAGMA foreign_keys=ON")
                cursor.close()
    return _engine


def get_session_factory(database_url: str | None = None) -> sessionmaker[Session]:
    global _SessionLocal
    if _SessionLocal is None:
        engine = get_engine(database_url)
        _SessionLocal = sessionmaker(bind=engine)
    return _SessionLocal


def get_session(database_url: str | None = None) -> Session:
    factory = get_session_factory(database_url)
    return factory()


def init_database(database_url: str | None = None) -> None:
    engine = get_engine(database_url)
    Base.metadata.create_all(bind=engine)
