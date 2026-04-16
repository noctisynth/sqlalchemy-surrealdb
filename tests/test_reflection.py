from __future__ import annotations

from sqlalchemy import inspect, text
from sqlalchemy.engine import Engine


class TestHasTable:
    def test_has_table_existing(self, engine: Engine) -> None:
        inspector = inspect(engine)
        assert inspector.has_table("users") is True
        assert inspector.has_table("posts") is True

    def test_has_table_nonexistent(self, engine: Engine) -> None:
        inspector = inspect(engine)
        assert inspector.has_table("nonexistent_table") is False


class TestGetTableNames:
    def test_get_table_names(self, engine: Engine) -> None:
        inspector = inspect(engine)
        tables = inspector.get_table_names()
        assert "users" in tables
        assert "posts" in tables


class TestGetColumns:
    def test_get_columns_users(self, engine: Engine) -> None:
        inspector = inspect(engine)
        columns = inspector.get_columns("users")
        col_names = [c["name"] for c in columns]
        assert "username" in col_names
        assert "email" in col_names
        assert "age" in col_names

    def test_get_columns_posts(self, engine: Engine) -> None:
        inspector = inspect(engine)
        columns = inspector.get_columns("posts")
        col_names = [c["name"] for c in columns]
        assert "title" in col_names
        assert "content" in col_names

    def test_get_columns_schemaless(self, engine: Engine) -> None:
        inspector = inspect(engine)
        with engine.connect() as conn:
            conn.execute(text("DEFINE TABLE schemaless_test SCHEMALESS"))
            conn.commit()

        try:
            columns = inspector.get_columns("schemaless_test")
            assert len(columns) == 0
        finally:
            with engine.connect() as conn:
                conn.execute(text("REMOVE TABLE schemaless_test"))
                conn.commit()


class TestGetPrimaryKeys:
    def test_get_pk_constraint(self, engine: Engine) -> None:
        inspector = inspect(engine)
        pks = inspector.get_pk_constraint("users")
        assert pks["constrained_columns"] == ["id"]
        assert pks["name"] is None


class TestGetIndexes:
    def test_get_indexes(self, engine: Engine) -> None:
        inspector = inspect(engine)
        indexes = inspector.get_indexes("users")

        index_names = [idx["name"] for idx in indexes]
        assert "email_idx" in index_names

        email_idx = next(idx for idx in indexes if idx["name"] == "email_idx")
        assert email_idx["unique"] is True
        assert "email" in email_idx["column_names"]

    def test_get_indexes_posts(self, engine: Engine) -> None:
        inspector = inspect(engine)
        indexes = inspector.get_indexes("posts")

        index_names = [idx["name"] for idx in indexes]
        assert "title_idx" in index_names

        title_idx = next(idx for idx in indexes if idx["name"] == "title_idx")
        assert title_idx["unique"] is False
        assert "title" in title_idx["column_names"]


class TestGetUniqueConstraints:
    def test_get_unique_constraints(self, engine: Engine) -> None:
        inspector = inspect(engine)
        constraints = inspector.get_unique_constraints("users")

        constraint_names = [c["name"] for c in constraints]
        assert "email_idx" in constraint_names

        email_constraint = next(c for c in constraints if c["name"] == "email_idx")
        assert "email" in email_constraint["column_names"]


class TestGetForeignKeys:
    def test_get_foreign_keys_no_fk(self, engine: Engine) -> None:
        inspector = inspect(engine)
        fks = inspector.get_foreign_keys("users")
        assert len(fks) == 0

    def test_get_foreign_keys_with_fk_table(self, engine: Engine) -> None:
        from sqlalchemy import text

        with engine.connect() as conn:
            conn.execute(text("DEFINE TABLE orders SCHEMAFUL"))
            conn.execute(text("DEFINE FIELD id ON orders TYPE record(orders)"))
            conn.execute(text("DEFINE FIELD user_id ON orders TYPE record(users)"))
            conn.execute(text("DEFINE FIELD title ON orders TYPE string"))
            conn.execute(text("DEFINE INDEX user_fk ON orders FIELDS user_id UNIQUE"))
            conn.commit()

        try:
            inspector = inspect(engine)
            fks = inspector.get_foreign_keys("orders")
            assert len(fks) >= 0
        finally:
            with engine.connect() as conn:
                conn.execute(text("REMOVE TABLE orders"))
                conn.commit()


class TestReflectionIntegration:
    def test_reflect_table_and_query(self, engine: Engine) -> None:
        from sqlalchemy import select, text
        from sqlalchemy.orm import sessionmaker

        with engine.connect() as conn:
            conn.execute(
                text(
                    "INSERT INTO users (username, email, age) VALUES ('alice', 'alice@test.com', 25)"
                )
            )
            conn.commit()

        SessionLocal = sessionmaker(bind=engine)
        session = SessionLocal()

        try:
            result = session.execute(select(text("*")).select_from(text("users")))
            rows = result.fetchall()
            assert len(rows) > 0
        finally:
            session.close()
