from __future__ import annotations

import uuid
from typing import Generator

import pytest
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker
from surrealdb import RecordID


@pytest.fixture(scope="session")
def surrealdb_url() -> str:
    return "surrealdb://root:root@127.0.0.1:5070/test/test"


@pytest.fixture(scope="session")
def engine(surrealdb_url: str) -> Engine:
    return create_engine(
        surrealdb_url,
        connect_args={"confirm_deleted_rows": False},
    )


@pytest.fixture(scope="function", autouse=True)
def setup_and_teardown(engine: Engine) -> Generator[None, None, None]:
    from sqlalchemy import text
    from tests.models import Base

    with engine.connect() as conn:
        try:
            conn.execute(text("REMOVE TABLE posts"))
        except Exception:
            pass
        try:
            conn.execute(text("REMOVE TABLE users"))
        except Exception:
            pass
        try:
            conn.commit()
        except Exception:
            pass

    Base.metadata.create_all(engine)

    yield


@pytest.fixture(scope="function")
def session(setup_and_teardown: None, engine: Engine) -> Generator[Session, None, None]:
    SessionLocal = sessionmaker(
        bind=engine,
        autocommit=False,
        autoflush=True,
        expire_on_commit=False,
    )
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()


def generate_record_id(table: str) -> RecordID:
    return RecordID(table, uuid.uuid4().hex)
