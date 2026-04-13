from sqlalchemy.types import UserDefinedType
from surrealdb import RecordID
from sqlalchemy import types

class RecordIDType(UserDefinedType):
    cache_ok = True

    def __init__(self, table_name=None, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def bind_processor(self, dialect):
        def process(value: str) -> RecordID:
            return RecordID.parse(value)

        return process

    def result_processor(self, dialect, coltype):
        def process(value):
            return str(value)

        return process
