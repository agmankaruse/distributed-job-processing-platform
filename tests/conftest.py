import os
from collections.abc import Generator

os.environ["DATABASE_URL"] = "sqlite+pysqlite:///:memory:"
os.environ["REDIS_URL"] = "redis://unused:6379/0"

import fakeredis
import pytest
from fastapi.testclient import TestClient
from redis import Redis
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from app.database import Base, get_db
from app.main import app
from app.queue import JobQueue, get_queue


@pytest.fixture
def session_factory():
    database_url = os.environ.get("JOBS_TEST_DATABASE_URL")
    if database_url:
        engine = create_engine(database_url)
    else:
        engine = create_engine(
            "sqlite+pysqlite:///:memory:",
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
    Base.metadata.create_all(engine)
    factory = sessionmaker(bind=engine, expire_on_commit=False)
    yield factory
    Base.metadata.drop_all(engine)
    engine.dispose()


@pytest.fixture
def queue():
    redis_url = os.environ.get("JOBS_TEST_REDIS_URL")
    client = (
        Redis.from_url(redis_url, decode_responses=True)
        if redis_url
        else fakeredis.FakeRedis(decode_responses=True)
    )
    client.flushdb()
    yield JobQueue(client, lease_seconds=5)
    client.flushdb()
    client.close()


@pytest.fixture
def client(session_factory, queue) -> Generator[TestClient, None, None]:
    def override_db():
        db: Session = session_factory()
        try:
            yield db
        finally:
            db.close()

    app.dependency_overrides[get_db] = override_db
    app.dependency_overrides[get_queue] = lambda: queue
    with TestClient(app) as test_client:
        yield test_client
    app.dependency_overrides.clear()
