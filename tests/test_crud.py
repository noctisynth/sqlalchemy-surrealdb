from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.orm import Session

from tests.models import Post, User


class TestUserCRUD:
    def test_create_user(self, session: Session) -> None:
        user = User(username="user_create", email="create@test.com", age=25)
        session.add(user)
        session.commit()
        assert user.id is not None
        assert user.username == "user_create"

    def test_read_user_by_id(self, session: Session) -> None:
        user = User(username="user_by_id", email="by_id@test.com", age=25)
        session.add(user)
        session.commit()

        retrieved = session.get(User, user.id)
        assert retrieved is not None
        assert retrieved.username == "user_by_id"

    def test_update_user(self, session: Session) -> None:
        user = User(username="user_update", email="update@test.com", age=25)
        session.add(user)
        session.commit()

        user.age = 26
        session.commit()

        retrieved = session.get(User, user.id)
        assert retrieved is not None
        assert retrieved.age == 26

    def test_delete_user(self, session: Session) -> None:
        user = User(username="user_delete", email="delete@test.com")
        session.add(user)
        session.commit()

        session.delete(user)
        session.commit()

        retrieved = session.get(User, user.id)
        assert retrieved is None

    def test_user_with_none_age(self, session: Session) -> None:
        user = User(username="user_no_age", email="no_age@test.com")
        session.add(user)
        session.commit()
        assert user.age is None


class TestPostCRUD:
    def test_create_post(self, session: Session) -> None:
        post = Post(title="Test Post", content="Test Content")
        session.add(post)
        session.commit()
        assert post.id is not None
        assert post.title == "Test Post"

    def test_read_post_by_id(self, session: Session) -> None:
        post = Post(title="Read Post", content="Read Content")
        session.add(post)
        session.commit()

        retrieved = session.get(Post, post.id)
        assert retrieved is not None
        assert retrieved.title == "Read Post"


class TestComplexQueries:
    def test_filter_by_id(self, session: Session) -> None:
        user = User(username="filter_id", email="filter_id@test.com")
        session.add(user)
        session.commit()

        retrieved = session.get(User, user.id)
        assert retrieved is not None
        assert retrieved.username == "filter_id"

    def test_filter_with_and(self, session: Session) -> None:
        user = User(username="and_user", email="and@test.com", age=30)
        session.add(user)
        session.commit()

        user_retrieved = session.get(User, user.id)
        assert user_retrieved is not None
        assert user_retrieved.age is not None
        assert user_retrieved.age >= 20

    def test_query_with_none_result(self, session: Session) -> None:
        user = User(username="none_query", email="none@test.com")
        session.add(user)
        session.commit()

        result = session.execute(
            select(User).where(User.username == "nonexistent_user_xyz")
        ).scalar_one_or_none()
        assert result is None
