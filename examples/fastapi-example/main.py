from surrealdb import RecordID
from sqlalchemy.orm import sessionmaker, Session
from models import Base, User, Post, UserModel, PostModel
from sqlalchemy import create_engine
from contextlib import asynccontextmanager
from typing import Optional
from fastapi import FastAPI


session: Session


@asynccontextmanager
async def lifespan(_app: FastAPI):
    global session
    engine = create_engine(
        "surrealdb://root:root@127.0.0.1:5070/test",
        echo=True,
    )
    Base.metadata.create_all(bind=engine)
    SessionLocal = sessionmaker(
        bind=engine, autocommit=False, autoflush=False, expire_on_commit=False
    )
    session = SessionLocal()
    yield
    session.close()
    engine.dispose()


app = FastAPI(lifespan=lifespan)


@app.post("/register")
def register(user: UserModel):
    global session
    user_data = User(**user.model_dump())
    session.add(user_data)
    session.commit()
    return {"data": user_data, "message": "User registered successfully"}


@app.get("/posts")
def get_posts():
    global session
    posts = session.query(Post).all()
    return posts


@app.post("/posts")
def create_post(user_id: str, post: PostModel):
    global session
    user = session.get(User, RecordID.parse(user_id))
    if not user:
        return {"message": "User not found"}
    post_data = Post(user_id=user.id, **post.model_dump())
    session.add(post_data)
    session.commit()
    return {"post": post_data, "message": "Post created successfully"}


@app.delete("/posts/{post_id}")
def delete_post(post_id: str):
    global session
    post = session.get(Post, RecordID.parse(post_id))
    if not post:
        return {"message": "Post not found"}
    session.delete(post)
    session.commit()
    return {"message": "Post deleted successfully"}
