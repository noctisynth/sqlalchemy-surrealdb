from __future__ import annotations
from surrealdb import (
    BlockingWsSurrealConnection,
    Surreal,
    BlockingHttpSurrealConnection,
)

from typing import Any, Optional, Sequence, Union

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

    def cursor(self) -> SurrealDBCursor:
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
        if self._last_result and len(self._last_result) > 0:
            first_row = self._last_result[0]
            if hasattr(first_row, "keys"):
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

    def executemany(self, query: str, params_list: Sequence[dict]) -> SurrealDBCursor:
        for params in params_list:
            self.execute(query, params)
        return self

    def fetchone(self) -> Optional[tuple]:
        if self._last_result and len(self._last_result) > 0:
            row = self._last_result.pop(0)
            return tuple(row.values()) if hasattr(row, "values") else tuple(row)
        return None

    def fetchmany(self, size: Optional[int] = None) -> list:
        if size is None:
            size = self._arraysize

        if self._last_result:
            result = list(self._last_result[:size])
            self._last_result = self._last_result[size:]
            return [
                tuple(r.values()) if hasattr(r, "values") else tuple(r) for r in result
            ]
        return []

    def fetchall(self) -> list:
        if self._last_result:
            result = list(self._last_result)
            self._last_result = []
            return [
                tuple(r.values()) if hasattr(r, "values") else tuple(r) for r in result
            ]
        return []

    def __iter__(self):
        return iter(list(self._last_result) if self._last_result else [])

    @property
    def _fetchable(self):
        return True

    def close(self) -> None:
        pass


def connect(
    url: str,
    username: Optional[str] = None,
    password: Optional[str] = None,
    namespace: Optional[str] = None,
    database: Optional[str] = None,
    **kwargs: Any,
) -> SurrealDBConnection:
    db = Surreal(url)

    if namespace and database:
        db.use(namespace, database)

    if username and password:
        db.signin({"username": username, "password": password})

    return SurrealDBConnection(db, namespace, database)


apilevel = "2.0"
threadsafety = 2
paramstyle = "named"

version_info = (1, 0, 0)
__version__ = "0.1.0"

__all__ = [
    "connect",
    "apilevel",
    "threadsafety",
    "paramstyle",
    "version_info",
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
