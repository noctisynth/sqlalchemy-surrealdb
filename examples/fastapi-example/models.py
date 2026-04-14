from typing import Optional
from uuid import uuid4

from pydantic import BaseModel
from sqlalchemy import ForeignKey, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from surrealdb import RecordID

from sqlalchemy_surrealdb.types import SurrealRecordID


class Base(DeclarativeBase):
    pass


class UserModel(BaseModel):
    name: str
    fullname: str


class User(Base):
    __tablename__ = "user_account"
    id: Mapped[int] = mapped_column(
        SurrealRecordID,
        primary_key=True,
        default=lambda: RecordID("user_account", uuid4().hex),
    )
    name: Mapped[str] = mapped_column(String(30))
    fullname: Mapped[Optional[str]]

    def __repr__(self) -> str:
        return f"User(id={self.id!r}, name={self.name!r}, fullname={self.fullname!r})"


class PostModel(BaseModel):
    title: str
    content: str


class Post(Base):
    __tablename__ = "posts"
    id: Mapped[int] = mapped_column(
        SurrealRecordID,
        primary_key=True,
        default=lambda: RecordID("posts", uuid4().hex),
    )
    title: Mapped[str] = mapped_column(String(30))
    content: Mapped[str] = mapped_column(String(100))
    user_id: Mapped[int] = mapped_column(ForeignKey("user_account.id"))

    def __repr__(self) -> str:
        return f"Post(id={self.id!r}, title={self.title!r}, content={self.content!r})"
