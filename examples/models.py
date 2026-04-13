from sqlalchemy_surrealdb.types import RecordIDType
from sqlalchemy import Column, String, DateTime, ForeignKey, create_engine
from sqlalchemy.orm import relationship, declarative_base, sessionmaker
from datetime import datetime
import uuid


Base = declarative_base()


class User(Base):
    __tablename__ = "users"

    id = Column(RecordIDType, primary_key=True, default=lambda: f"users:{uuid.uuid4()}")
    username = Column(String(50), unique=True, nullable=False, index=True)
    email = Column(String(120), unique=True, nullable=False)
    created_at = Column(DateTime, default=datetime.now)

    posts = relationship("Post", back_populates="author")

    def __repr__(self):
        return f"<User(id={self.id}, username='{self.username}')>"


class Post(Base):
    __tablename__ = "posts"

    id = Column(String, primary_key=True, default=lambda: f"posts:{uuid.uuid4()}")
    title = Column(String(200), nullable=False)
    content = Column(String)
    user_id = Column(String, ForeignKey("users.id"))
    created_at = Column(DateTime, default=datetime.now)

    author = relationship("User", back_populates="posts")


engine = create_engine(
    "surrealdb://root:root@127.0.0.1:5070/test",
    connect_args={"expire_on_commit": False},
)
Base.metadata.create_all(bind=engine)

SessionLocal = sessionmaker(
    bind=engine, autocommit=False, autoflush=False, expire_on_commit=False
)


def test_crud():
    session = SessionLocal()
    try:
        # Create - 插入数据
        print("=== Create (插入) ===")
        new_user = User(username="alice", email="alice@example.com")
        session.add(new_user)
        session.commit()
        print(f"创建用户: {new_user.username}")

        new_post = Post(title="Hello World", content="First post!", user_id=new_user.id)
        session.add(new_post)
        session.commit()
        print(f"创建文章: {new_post.title}")

        # Read - 查询数据
        print("\n=== Read (查询) ===")
        # 查询单个用户
        user = session.get(User, new_user.id)
        if not user:
            print("未找到用户")
            return
        print(f"查询用户: {user.id}, 用户id类型 {isinstance(user.id, str)}")

        # 过滤单个用户
        # user = session.query(User).filter(User.username == "alice").first()
        # print(f"查询用户: {user}")
        # if not user:
        #     print("未找到用户")
        #     return

        # # 查询所有用户
        # all_users = session.query(User).all()
        # print(f"所有用户: {all_users}")

        # # 查询用户的文章
        # posts = session.query(Post).filter(Post.user_id == user.id).all()
        # print(f"用户的文章: {posts}")

        # # Update - 更新数据
        # print("\n=== Update (更新) ===")
        # user.email = "alice_new@example.com"
        # session.commit()
        # print(f"更新后邮箱: {user.email}")

        # post = session.get(Post).first()
        # post.title = "Updated Title"
        # session.commit()
        # print(f"更新后标题: {post.title}")

        # # Delete - 删除数据
        # print("\n=== Delete (删除) ===")
        # session.delete(post)
        # session.commit()
        # print(f"删除文章: {post.title}")

        # session.delete(user)
        # session.commit()
        # print(f"删除用户: {user.username}")

        # # 验证删除
        # remaining_users = session.query(User).all()
        # print(f"剩余用户: {remaining_users}")

    finally:
        session.close()


if __name__ == "__main__":
    test_crud()
