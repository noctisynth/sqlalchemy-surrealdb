from typing import Union
from surrealdb import RecordID
from sqlalchemy_surrealdb.types import SurrealRecordID
from sqlalchemy import Column, String, DateTime, ForeignKey, create_engine, Integer
from sqlalchemy.orm import declarative_base, sessionmaker
from datetime import datetime, timezone
import uuid


Base = declarative_base()


class User(Base):
    __tablename__ = "users"

    id: Union[Column[RecordID], RecordID] = Column(
        SurrealRecordID,
        primary_key=True,
        default=lambda: RecordID("users", uuid.uuid4().hex),
    )
    username = Column(String(50), unique=True, nullable=False, index=True)
    email = Column(String(120), unique=True, nullable=False)
    age = Column(Integer, nullable=True)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))

    def __repr__(self):
        return f"<User(id={self.id}, username='{self.username}', email='{self.email}')>"


class Post(Base):
    __tablename__ = "posts"

    id: Column[RecordID] = Column(
        SurrealRecordID,
        primary_key=True,
        default=lambda: RecordID("posts", uuid.uuid4().hex),
    )
    title: Column[str] = Column(String(200), nullable=False)
    content: Column[str] = Column(String)
    user_id = Column(SurrealRecordID, ForeignKey("users.id"))
    created_at = Column(DateTime, default=datetime.now)


def test_crud():
    engine = create_engine(
        "surrealdb://root:root@127.0.0.1:5070/test",
        connect_args={"expire_on_commit": False},
    )
    Base.metadata.create_all(bind=engine)

    SessionLocal = sessionmaker(
        bind=engine, autocommit=False, autoflush=False, expire_on_commit=False
    )

    session = SessionLocal()
    try:
        print("=== 清理数据 ===")
        session.query(Post).delete()
        session.query(User).delete()
        session.commit()
        print("数据已清理\n")

        print("=== Create (批量插入) ===")
        users_data = [
            {"username": "alice", "email": "alice@example.com", "age": 25},
            {"username": "bob", "email": "bob@example.com", "age": 30},
            {"username": "charlie", "email": "charlie@example.com", "age": 35},
            {"username": "david", "email": "david@example.com", "age": 28},
            {"username": "eve", "email": "eve@example.com", "age": 22},
        ]
        for u in users_data:
            user = User(**u)
            session.add(user)
        session.commit()
        print(f"创建了 {len(users_data)} 个用户")

        posts_data = [
            {"title": "Alice的第一篇文章", "content": "Hello World!", "user_id": None},
            {"title": "Bob的第一篇文章", "content": "大家好!", "user_id": None},
            {
                "title": "Alice的第二篇文章",
                "content": "SQLAlchemy真好玩",
                "user_id": None,
            },
        ]

        all_users = session.query(User).all()
        for i, p in enumerate(posts_data):
            user_id = all_users[i % len(all_users)].id
            p["user_id"] = user_id  # type: ignore
            post = Post(**p)
            session.add(post)
        session.commit()
        print(f"创建了 {len(posts_data)} 篇文章\n")

        print("=== Read (查询) ===")
        print("\n--- 通过 get 查询 ---")
        first_user = session.query(User).first()
        if not first_user:
            print("没有用户")
        else:
            user = session.get(User, first_user.id)
            print(f"get查询: {user}")

        print("\n--- 通过 filter 精确查询 ---")
        user = session.query(User).filter(User.username == "alice").first()
        print(f"filter精确查询: {user}")

        print("\n--- 通过 filter 多条件查询 ---")
        user = (
            session.query(User).filter(User.username == "alice", User.age >= 20).first()
        )
        print(f"filter多条件: {user}")

        print("\n--- 通过 filter + or_ 查询 ---")
        from sqlalchemy import or_

        users = (
            session
            .query(User)
            .filter(or_(User.username == "alice", User.username == "bob"))
            .all()
        )
        print(f"or查询(alice OR bob): {users}")

        print("\n--- order_by 排序 ---")
        users = session.query(User).order_by(User.age.desc()).all()
        print(f"按年龄降序: {[u.username for u in users]}")

        print("\n--- limit ---")
        users = session.query(User).order_by(User.created_at).limit(3).all()
        print(f"limit(3): {[u.username for u in users]}")

        print("\n--- all 查询 ---")
        all_users = session.query(User).all()
        print(f"用户总数: {len(all_users)}")

        print("\n--- 关联查询 (通过外键) ---")
        user = session.query(User).filter(User.username == "alice").first()
        if not user:
            print("用户不存在")
            return
        user_id_str = str(user.id)
        posts = session.query(Post).filter(Post.user_id == user_id_str).all()
        print(f"Alice的文章数: {len(posts)}")
        for p in posts:
            print(f"  - {p.title}")

        print("\n=== Update (更新) ---")
        print("\n--- 单条更新 ---")
        user = session.query(User).filter(User.username == "alice").first()
        if not user:
            print("用户不存在")
            return
        user.age = 26
        session.commit()
        print(f"更新年龄: {user.username} -> {user.age}")

        print("\n=== Delete (删除) ---")
        print("\n--- 单条删除 ---")
        user = session.query(User).filter(User.username == "eve").first()
        if not user:
            print("用户不存在")
            return
        session.delete(user)
        session.commit()
        print(f"删除用户: {user.username}")

        print("\n=== 最终数据 ===")
        all_users = session.query(User).order_by(User.age).all()
        print("用户列表:")
        for u in all_users:
            print(f"  - {u.username}, {u.email}, age={u.age}")

        all_posts = session.query(Post).all()
        print("\n文章列表:")
        for p in all_posts:
            print(f"  - {p.title}")

        print("\n=== 测试完成 ===")
    finally:
        session.close()


if __name__ == "__main__":
    test_crud()
