from __future__ import annotations

from typing import Optional

from sqlalchemy import Index, Integer, String
from sqlalchemy.orm import Mapped, declarative_base, mapped_column
from sqlalchemy_surrealdb.types import SurrealRecordID
from surrealdb import RecordID

from tests.conftest import generate_record_id


Base = declarative_base()


class User(Base):
    __tablename__ = "users"
    __table_args__ = (Index("email_idx", "email", unique=True),)

    id: Mapped[RecordID] = mapped_column(
        SurrealRecordID, primary_key=True, default=lambda: generate_record_id("users")
    )
    username: Mapped[str] = mapped_column(String(50), nullable=False)
    email: Mapped[str] = mapped_column(String(120), nullable=False)
    age: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)


class Post(Base):
    __tablename__ = "posts"
    __table_args__ = (Index("title_idx", "title", unique=False),)

    id: Mapped[RecordID] = mapped_column(
        SurrealRecordID, primary_key=True, default=lambda: generate_record_id("posts")
    )
    title: Mapped[str] = mapped_column(String(200), nullable=False)
    content: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    user_id: Mapped[Optional[str]] = mapped_column(String, nullable=True)
