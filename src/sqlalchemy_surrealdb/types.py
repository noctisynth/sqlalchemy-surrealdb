from sqlalchemy import Dialect, TypeDecorator, String
from sqlalchemy.types import TypeEngine
from typing import Any, Optional

from surrealdb import RecordID


class SurrealRecordID(TypeDecorator):
    impl = String
    cache_ok = False

    def process_bind_param(
        self, value: Optional[Any], dialect: Dialect
    ) -> Optional[RecordID]:
        if value is None:
            return None

        if isinstance(value, RecordID):
            return value

        try:
            return RecordID.parse(value)
        except (ValueError, AttributeError):
            return value

    def process_result_value(
        self, value: Optional[RecordID], dialect: Dialect
    ) -> Optional[str]:
        if value is None:
            return None

        return str(value)

    def coerce_compared_value(self, op: Any, value: Any) -> TypeEngine:
        if value is not None:
            if isinstance(value, RecordID):
                return self
        return super().coerce_compared_value(op, value)
