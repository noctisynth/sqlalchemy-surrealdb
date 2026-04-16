from __future__ import annotations

from typing import Any, Optional
from datetime import timedelta

from sqlalchemy import Dialect, TypeDecorator
from sqlalchemy.types import (
    BIGINT,
    BINARY,
    BOOLEAN,
    DATE,
    DATETIME,
    DECIMAL,
    FLOAT,
    INTEGER,
    JSON,
    SMALLINT,
    TEXT,
    TIMESTAMP,
    TypeEngine,
    VARCHAR,
)
from surrealdb import RecordID, Geometry


class SurrealRecordID(TypeDecorator):
    impl = VARCHAR(512)
    cache_ok = True

    def process_bind_param(
        self, value: Optional[Any], dialect: Dialect
    ) -> Optional[RecordID]:
        if value is None:
            return None
        if isinstance(value, RecordID):
            return value
        if isinstance(value, str):
            try:
                return RecordID.parse(value)
            except (ValueError, AttributeError):
                return None
        return None

    def process_result_value(
        self, value: Optional[Any], dialect: Dialect
    ) -> Optional[RecordID]:
        if value is None:
            return None
        if isinstance(value, RecordID):
            return value
        if isinstance(value, str):
            try:
                return RecordID.parse(value)
            except (ValueError, AttributeError):
                return None
        return None

    def coerce_compared_value(self, op: Any, value: Any) -> TypeEngine:
        if isinstance(value, RecordID):
            return self
        return self


class SurrealArray(TypeDecorator):
    impl = JSON
    cache_ok = True

    def process_bind_param(
        self, value: Optional[Any], dialect: Dialect
    ) -> Optional[list]:
        return value

    def process_result_value(
        self, value: Optional[Any], dialect: Dialect
    ) -> Optional[list]:
        if value is None:
            return None
        if isinstance(value, list):
            return value
        return value

    def coerce_compared_value(self, op: Any, value: Any) -> TypeEngine:
        return JSON()


class SurrealObject(TypeDecorator):
    impl = JSON
    cache_ok = True

    def process_bind_param(
        self, value: Optional[Any], dialect: Dialect
    ) -> Optional[dict]:
        return value

    def process_result_value(
        self, value: Optional[Any], dialect: Dialect
    ) -> Optional[dict]:
        if value is None:
            return None
        if isinstance(value, dict):
            return value
        return value

    def coerce_compared_value(self, op: Any, value: Any) -> TypeEngine:
        return JSON()


class SurrealUUID(TypeDecorator):
    impl = VARCHAR(36)
    cache_ok = True

    def process_bind_param(
        self, value: Optional[Any], dialect: Dialect
    ) -> Optional[str]:
        if value is None:
            return None
        return str(value)

    def process_result_value(
        self, value: Optional[Any], dialect: Dialect
    ) -> Optional[str]:
        if value is None:
            return None
        return str(value)


class SurrealDuration(TypeDecorator):
    impl = VARCHAR(50)
    cache_ok = True

    def process_bind_param(
        self, value: Optional[Any], dialect: Dialect
    ) -> Optional[str]:
        if value is None:
            return None
        if isinstance(value, timedelta):
            seconds = value.total_seconds()
            return f"{seconds}s"
        return str(value)

    def process_result_value(
        self, value: Optional[Any], dialect: Dialect
    ) -> Optional[timedelta]:
        if value is None:
            return None
        if isinstance(value, timedelta):
            return value
        if isinstance(value, str):
            if value.endswith("ns"):
                return timedelta(microseconds=int(value[:-2]) / 1000)
            elif value.endswith("µs") or value.endswith("us"):
                return timedelta(microseconds=int(value[:-2]))
            elif value.endswith("ms"):
                return timedelta(milliseconds=int(value[:-2]))
            elif value.endswith("s"):
                return timedelta(seconds=float(value[:-1]))
            elif value.endswith("m"):
                return timedelta(minutes=int(value[:-1]))
            elif value.endswith("h"):
                return timedelta(hours=int(value[:-1]))
            elif value.endswith("d"):
                return timedelta(days=int(value[:-1]))
            elif value.endswith("w"):
                return timedelta(weeks=int(value[:-1]))
            elif value.endswith("y"):
                return timedelta(days=int(value[:-1]) * 365)
        return value


class SurrealGeometry(TypeDecorator):
    impl = JSON
    cache_ok = True

    def process_bind_param(
        self, value: Optional[Any], dialect: Dialect
    ) -> Optional[Any]:
        if value is None:
            return None
        if isinstance(value, Geometry):
            return value
        return value

    def process_result_value(
        self, value: Optional[Any], dialect: Dialect
    ) -> Optional[Any]:
        if value is None:
            return None
        if isinstance(value, Geometry):
            return value
        return value

    def coerce_compared_value(self, op: Any, value: Any) -> TypeEngine:
        return JSON()


SURREAL_TYPE_MAPPING = {
    "int": INTEGER(),
    "integer": INTEGER(),
    "bigint": BIGINT(),
    "smallint": SMALLINT(),
    "float": FLOAT(),
    "double": FLOAT(),
    "decimal": DECIMAL(),
    "string": TEXT(),
    "varchar": VARCHAR(),
    "text": TEXT(),
    "bool": BOOLEAN(),
    "boolean": BOOLEAN(),
    "datetime": DATETIME(),
    "date": DATE(),
    "timestamp": TIMESTAMP(),
    "binary": BINARY(),
    "array": SurrealArray(),
    "object": SurrealObject(),
    "uuid": SurrealUUID(),
    "duration": SurrealDuration(),
    "record": SurrealRecordID(),
    "geometry": SurrealGeometry(),
    "option::int": INTEGER(),
    "option::float": FLOAT(),
    "option::string": TEXT(),
    "option::bool": BOOLEAN(),
    "option::datetime": DATETIME(),
    "option::array": SurrealArray(),
    "option::object": SurrealObject(),
    "option::record": SurrealRecordID(),
}


def parse_surrealdb_type(type_str: str) -> TypeEngine:
    type_lower = type_str.lower().strip()

    if type_lower in SURREAL_TYPE_MAPPING:
        return SURREAL_TYPE_MAPPING[type_lower]

    if type_lower.startswith("option<"):
        inner = type_lower[7:-1]
        return parse_surrealdb_type(inner)

    if type_lower.startswith("array<"):
        return SurrealArray()
    if type_lower.startswith("object<"):
        return SurrealObject()

    return JSON()
