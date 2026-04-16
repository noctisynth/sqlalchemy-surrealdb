from __future__ import annotations

from typing import Any, Optional, Sequence

from surrealdb import Surreal

SurrealConnection = Any


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
ConnectionError = DatabaseError


class SurrealDBConnection:
    def __init__(
        self,
        db: Any,
        namespace: Optional[str] = None,
        database: Optional[str] = None,
    ):
        self._db = db
        self._namespace = namespace
        self._database = database
        self._closed = False
        self._in_transaction = False

    def close(self) -> None:
        if not self._closed:
            self._db.close()
            self._closed = True

    def begin(self) -> None:
        if not self._in_transaction:
            self._in_transaction = True

    def commit(self) -> None:
        if self._in_transaction:
            try:
                self._db.query("COMMIT")
            finally:
                self._in_transaction = False

    def rollback(self) -> None:
        if self._in_transaction:
            try:
                self._db.query("CANCEL")
            finally:
                self._in_transaction = False

    def cursor(self) -> SurrealDBCursor:
        return SurrealDBCursor(self._db)

    @property
    def isolation_level(self) -> Optional[str]:
        return "READ COMMITTED"

    @isolation_level.setter
    def isolation_level(self, value: Optional[str]) -> None:
        pass

    def __del__(self):
        if not self._closed:
            try:
                self.close()
            except Exception:
                pass


class SurrealDBCursor:
    def __init__(self, db: Any) -> None:
        self._db = db
        self._description: Optional[tuple] = None
        self._rowcount = -1
        self._last_result: list = []
        self._arraysize = 1
        self._current_position = 0
        self._result_columns: Optional[list] = None

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

        self._process_result(result, query)

        return self

    def _parse_select_columns(self, query: str) -> list:
        import re

        cols = []
        select_match = re.search(
            r"SELECT\s+(.+?)\s+FROM", query, re.IGNORECASE | re.DOTALL
        )
        if select_match:
            cols_part = select_match.group(1)
            for col in cols_part.split(","):
                col = col.strip()
                as_match = re.search(r"\s+AS\s+(\w+)", col, re.IGNORECASE)
                if as_match:
                    cols.append(as_match.group(1))
                else:
                    alias = col.split(".")[-1].strip()
                    if alias == "*":
                        cols = []
                        break
                    if alias:
                        cols.append(alias)
        return cols

    def _process_result(self, result: Any, query: str) -> None:
        if result is None:
            self._last_result = []
            self._rowcount = 0
            self._description = None
            self._result_columns = None
            return

        expected_cols = self._parse_select_columns(query)

        if isinstance(result, list):
            self._last_result = result
            self._rowcount = len(result)
        elif isinstance(result, dict):
            self._last_result = [result]
            self._rowcount = 1
        else:
            self._last_result = [{"value": result}]
            self._rowcount = 1

        if self._last_result:
            first_row = self._last_result[0]
            self._description = self._extract_description(first_row, expected_cols)
            if expected_cols:
                self._result_columns = expected_cols
            else:
                self._result_columns = None
        elif expected_cols:
            self._description = tuple(
                (name, None, None, None, None, None, None) for name in expected_cols
            )
            self._result_columns = expected_cols
        else:
            self._description = None
            self._result_columns = None

    def _extract_description(
        self, row: Any, expected_cols: Optional[list] = None
    ) -> Optional[tuple]:
        if expected_cols:
            return tuple(
                (name, None, None, None, None, None, None) for name in expected_cols
            )
        if hasattr(row, "keys"):
            keys = list(row.keys())
            return tuple((name, None, None, None, None, None, None) for name in keys)
        elif isinstance(row, dict):
            return tuple(
                (name, None, None, None, None, None, None) for name in row.keys()
            )
        elif isinstance(row, (list, tuple)):
            return tuple(
                (f"col_{i}", None, None, None, None, None, None)
                for i in range(len(row))
            )
        return None

    def executemany(self, query: str, params_list: Sequence[dict]) -> SurrealDBCursor:
        for params in params_list:
            self.execute(query, params)
        return self

    def fetchone(self) -> Optional[tuple]:
        if self._last_result and self._current_position < len(self._last_result):
            row = self._last_result[self._current_position]
            self._current_position += 1
            return self._row_to_tuple(row)
        return None

    def fetchmany(self, size: Optional[int] = None) -> list:
        if size is None:
            size = self._arraysize

        results = []
        for _ in range(size):
            row = self.fetchone()
            if row is None:
                break
            results.append(row)
        return results

    def fetchall(self) -> list:
        results = []
        while True:
            row = self.fetchone()
            if row is None:
                break
            results.append(row)
        return results

    def _row_to_tuple(self, row: Any) -> tuple:
        if not row:
            return ()

        if hasattr(row, "values"):
            values = list(row.values())
        elif isinstance(row, dict):
            values = list(row.values())
        elif isinstance(row, (list, tuple)):
            values = list(row)
        else:
            return (row,)

        if self._result_columns and isinstance(row, dict):
            col_map = dict(zip(row.keys(), values))
            ordered_values = [col_map.get(col) for col in self._result_columns]
            return (
                tuple(ordered_values)
                if all(
                    v is not None or k in col_map
                    for k, v in zip(self._result_columns, ordered_values)
                )
                else tuple(values)
            )

        return tuple(values)

    def __iter__(self):
        return self

    def __next__(self):
        row = self.fetchone()
        if row is None:
            raise StopIteration
        return row

    @property
    def _fetchable(self):
        return True

    def close(self) -> None:
        self._last_result = []
        self._current_position = 0

    def __del__(self):
        self.close()


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
    url = f"{scheme}://{host}:{port}"
    db = Surreal(url)

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
