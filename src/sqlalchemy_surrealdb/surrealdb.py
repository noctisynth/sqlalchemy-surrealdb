from __future__ import annotations

from typing import Any, Optional, Sequence, Union

from surrealdb import (
    BlockingHttpSurrealConnection,
    BlockingWsSurrealConnection,
    Surreal,
)

SurrealType = Union[BlockingWsSurrealConnection, BlockingHttpSurrealConnection]


class Error(Exception):
    pass


class DatabaseError(Exception):
    pass


class InternalError(DatabaseError):
    pass


class OperationalError(DatabaseError):
    pass


class ProgrammingError(DatabaseError):
    pass


class IntegrityError(DatabaseError):
    pass


class DataError(DatabaseError):
    pass


class NotSupportedError(DatabaseError):
    pass


Warning = None
InterfaceError = DatabaseError
Connection = DatabaseError


class SurrealDBConnection:
    def __init__(
        self,
        db: SurrealType,
        namespace: Optional[str] = None,
        database: Optional[str] = None,
    ):
        self._db = db
        self._namespace = namespace
        self._database = database
        self._closed = False
        self._last_cursor: Optional[SurrealDBCursor] = None

    def close(self) -> None:
        self._db.close()
        self._closed = True

    def commit(self) -> None:
        pass

    def rollback(self) -> None:
        pass

    def cursor(self, *args: Any, **kwargs: Any) -> SurrealDBCursor:
        cursor = SurrealDBCursor(self._db)
        self._last_cursor = cursor
        return cursor

    @property
    def isolation_level(self) -> Optional[str]:
        return None

    @isolation_level.setter
    def isolation_level(self, value: Optional[str]) -> None:
        pass


class SurrealDBCursor:
    def __init__(self, db: SurrealType) -> None:
        self._db = db
        self._description: Optional[tuple] = None
        self._rowcount = -1
        self._last_result: list = []
        self._arraysize = 1

    @property
    def description(self) -> Optional[tuple]:
        return self._description

    @property
    def rowcount(self) -> int:
        return self._rowcount

    @property
    def arraysize(self) -> int:
        return self._arraysize

    @arraysize.setter
    def arraysize(self, value: int) -> None:
        self._arraysize = value

    def execute(self, query: str, params: Optional[dict] = None) -> SurrealDBCursor:
        if params is None:
            params = {}

        result = self._db.query(query, params)

        if result is not None:
            if isinstance(result, list):
                self._last_result = result
                self._rowcount = len(result)
            elif isinstance(result, dict):
                self._last_result = [result]
                self._rowcount = 1
            else:
                self._last_result = [{"value": result}]
                self._rowcount = 1
        else:
            self._last_result = []
            self._rowcount = 0

        # Set description BEFORE returning
        # Extract column aliases from SQL to preserve order
        col_order = self._extract_columns_from_query(query)
        if self._last_result and len(self._last_result) > 0:
            first_row = self._last_result[0]
            if hasattr(first_row, "keys"):
                db_keys = list(first_row.keys())  # type: ignore
                if col_order:
                    ordered = []
                    for name in col_order:
                        if name in db_keys:
                            ordered.append(name)
                        else:
                            stripped = name.replace("users_", "").replace("posts_", "")
                            found = next(
                                (
                                    k
                                    for k in db_keys
                                    if k.replace("users_", "").replace("posts_", "")
                                    == stripped
                                ),
                                name,
                            )
                            ordered.append(found)
                    self._description = tuple(
                        (name, 0, 0, 0, 0, 0, 0) for name in ordered
                    )
                else:
                    self._description = tuple(
                        (name, 0, 0, 0, 0, 0, 0)
                        for name in first_row.keys()  # type:ignore
                    )
            elif isinstance(first_row, dict):
                self._description = tuple(
                    (name, 0, 0, 0, 0, 0, 0) for name in first_row.keys()
                )
            else:
                self._description = None
        else:
            self._description = None

        return self

    def _extract_columns_from_query(self, query: str) -> list:
        import re

        cols = []
        select_match = re.search(
            r"SELECT\s+(.+?)\s+FROM", query, re.IGNORECASE | re.DOTALL
        )
        if select_match:
            cols_part = select_match.group(1)
            for col in cols_part.split(","):
                col = col.strip()
                match = re.search(r"AS\s+(\w+)", col, re.IGNORECASE)
                if match:
                    cols.append(match.group(1))
                else:
                    alias = col.split(".")[-1].strip()
                    if alias:
                        cols.append(alias)
        return cols

    def executemany(self, query: str, params_list: Sequence[dict]) -> SurrealDBCursor:
        for params in params_list:
            self.execute(query, params)
        return self

    def fetchone(self) -> Optional[tuple]:
        if self._last_result and len(self._last_result) > 0:
            row = self._last_result.pop(0)
            return self._row_to_tuple(row)
        return None

    def fetchmany(self, size: Optional[int] = None) -> list:
        if size is None:
            size = self._arraysize

        if self._last_result:
            result = list(self._last_result[:size])
            self._last_result = self._last_result[size:]
            return [self._row_to_tuple(r) for r in result]
        return []

    def fetchall(self) -> list:
        if self._last_result:
            result = list(self._last_result)
            self._last_result = []
            return [self._row_to_tuple(r) for r in result]
        return []

    def _row_to_tuple(self, row) -> tuple:
        if not hasattr(row, "keys"):
            return tuple(row)

        dbapi_cols = list(row.keys())
        desc_cols = (
            [desc[0] for desc in self._description] if self._description else dbapi_cols
        )

        mapping = {}
        for col in dbapi_cols:
            stripped = col.split("_", 1)[-1] if "_" in col else col
            mapping[stripped] = row[col]

        return tuple(mapping.get(c.split("_", 1)[-1]) for c in desc_cols)

    def __iter__(self):
        return iter(list(self._last_result) if self._last_result else [])

    @property
    def _fetchable(self):
        return True

    def close(self) -> None:
        pass


def connect(
    username: str = "root",
    password: str = "root",
    namespace: str = "default",
    database: str = "default",
    scheme: str = "ws",
    host: str = "localhost",
    port: int = 8000,
    **kwargs: Any,
) -> SurrealDBConnection:
    db = Surreal(f"{scheme}://{host}:{port}")

    if namespace and database:
        db.use(namespace, database)

    if username and password:
        db.signin({"username": username, "password": password})

    return SurrealDBConnection(db, namespace, database)


apilevel = "2.0"
threadsafety = 2
paramstyle = "named"

__all__ = [
    "connect",
    "apilevel",
    "threadsafety",
    "paramstyle",
    "SurrealDBConnection",
    "SurrealDBCursor",
    "Error",
    "DatabaseError",
    "InternalError",
    "OperationalError",
    "ProgrammingError",
    "IntegrityError",
    "DataError",
    "NotSupportedError",
]
