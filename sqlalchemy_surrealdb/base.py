from __future__ import annotations
from sqlalchemy.sql.compiler import SQLCompiler, DDLCompiler, TypeCompiler

from typing import Any, Tuple
from sqlalchemy.engine import default, reflection
from sqlalchemy import exc
from sqlalchemy.dialects import registry
from sqlalchemy.sql import compiler


class SurrealDBTypeCompiler(TypeCompiler):
    def visit_INTEGER(self, type_: Any, **kwargs: Any) -> str:
        return "INT"

    def visit_BIGINT(self, type_: Any, **kwargs: Any) -> str:
        return "BIGINT"

    def visit_SMALLINT(self, type_: Any, **kwargs: Any) -> str:
        return "INT"

    def visit_FLOAT(self, type_: Any, **kwargs: Any) -> str:
        return "FLOAT"

    def visit_DOUBLE(self, type_: Any, **kwargs: Any) -> str:
        return "FLOAT"

    def visit_NUMERIC(self, type_: Any, **kwargs: Any) -> str:
        return "FLOAT"

    def visit_DECIMAL(self, type_: Any, **kwargs: Any) -> str:
        return "FLOAT"

    def visit_STRING(self, type_: Any, **kwargs: Any) -> str:
        return "STRING"

    def visit_VARCHAR(self, type_: Any, **kwargs: Any) -> str:
        return "STRING"

    def visit_TEXT(self, type_: Any, **kwargs: Any) -> str:
        return "STRING"

    def visit_BOOLEAN(self, type_: Any, **kwargs: Any) -> str:
        return "BOOL"

    def visit_DATE(self, type_: Any, **kwargs: Any) -> str:
        return "DATETIME"

    def visit_DATETIME(self, type_: Any, **kwargs: Any) -> str:
        return "DATETIME"

    def visit_TIMESTAMP(self, type_: Any, **kwargs: Any) -> str:
        return "DATETIME"

    def visit_BINARY(self, type_: Any, **kwargs: Any) -> str:
        return "BINARY"

    def visit_BLOB(self, type_: Any, **kwargs: Any) -> str:
        return "BINARY"

    def visit_JSON(self, type_: Any, **kwargs: Any) -> str:
        return "OBJECT"


class SurrealDBIdentifierPreparer(compiler.IdentifierPreparer):
    reserved_words = {
        "ALL",
        "AND",
        "ANY",
        "AS",
        "ASC",
        "BETWEEN",
        "BREAK",
        "CASE",
        "CATCH",
        "COLUMN",
        "CONCAT",
        "CONTINUE",
        "CREATE",
        "DATABASE",
        "DEFINE",
        "DELETE",
        "DESC",
        "DISTINCT",
        "ELSE",
        "END",
        "EXPLAIN",
        "FETCH",
        "FIELD",
        "FLEX",
        "FOR",
        "FOREIGN",
        "FROM",
        "FUNCTION",
        "GRANT",
        "GROUP",
        "IF",
        "INDEX",
        "INSERT",
        "INTO",
        "LIMIT",
        "MATH",
        "MERGE",
        "NOT",
        "NULL",
        "ON",
        "ONLY",
        "OPTION",
        "ORDER",
        "PASSWORD",
        "PERMISSIONS",
        "PRECISION",
        "RETURN",
        "SCHEMA",
        "SELECT",
        "SET",
        "SHOW",
        "SORT",
        "SQL",
        "START",
        "TABLE",
        "THEN",
        "TYPE",
        "UNION",
        "UNIQUE",
        "UPDATE",
        "USE",
        "USER",
        "VALUES",
        "WHEN",
        "WHERE",
    }


class SurrealDBCompiler(SQLCompiler):
    def __init__(self, dialect, statement, **kwargs: Any) -> None:
        super().__init__(dialect, statement, **kwargs)
        self.bindtemplate = "$%(name)s"
        self.compilation_bindtemplate = "$%(name)s"

    def bindparam_string(self, name, *args, **kwargs):
        return "$" + name

    def visit_textclause(self, textclause, *args, **kwargs) -> str:
        text = textclause.text
        text_upper = text.upper().strip()

        if text_upper.startswith("SELECT") and "FROM" not in text_upper:
            import re

            text = re.sub(r"\s+as\s+\w+", "", text, flags=re.IGNORECASE)
            text = "RETURN " + text[6:].strip()

        return text

    def visit_select(self, *args, **kwargs: Any) -> str:
        import re

        sql = super().visit_select(*args, **kwargs)

        if "FROM" not in sql.upper():
            sql = re.sub(r"\s+as\s+\w+", "", sql, flags=re.IGNORECASE)
            sql = "RETURN " + sql.replace("SELECT ", "").strip()
        else:
            # SurrealDB doesn't support table prefix in column names
            # Convert "table.column" to just "column"
            sql = re.sub(r"\b(\w+)\.(\w+)\b", r"\2", sql)

        return sql


class SurrealDBDDLCompiler(DDLCompiler):
    def get_column_specification(self, column: Any, **kwargs: Any) -> str:
        colspec = self.preparer.format_column(column)

        if column.autoincrement:
            colspec += " FLEX"

        if column.server_default is not None:
            default = self.get_column_default_string(column)
            if default is not None:
                colspec += " DEFAULT " + default

        if not column.nullable:
            colspec += " NOT NULL"

        return colspec


class SurrealDBExecutionContext(default.DefaultExecutionContext):
    def post_exec(self) -> None:
        pass


class SurrealDBInspector(reflection.Inspector):
    dialect: SurrealDBDialect


class SurrealDBDialect(default.DefaultDialect):
    name = "surrealdb"
    driver = "surrealdb"

    supports_statement_cache = True
    supports_alter = True
    supports_comments = False
    supports_constraint_comments = False
    supports_native_boolean = True
    supports_native_decimal = False
    supports_native_json = False
    supports_schemas = False
    supports_sequences = False
    supports_views = False

    max_identifier_length = 1024

    statement_compiler = SurrealDBCompiler
    ddl_compiler = SurrealDBDDLCompiler
    type_compiler_cls = SurrealDBTypeCompiler
    preparer = SurrealDBIdentifierPreparer
    execution_ctx_cls = SurrealDBExecutionContext
    inspector = SurrealDBInspector

    insert_returning = False
    update_returning = False
    delete_returning = False

    supports_empty_insert = False
    supports_default_values = False
    supports_default_metavalue = False
    supports_multivalues_insert = False

    colspecs: dict = {}
    ischema_names: dict = {}

    @property
    def dbapi_exception(self) -> Any:
        from sqlalchemy_surrealdb import surrealdb as dbapi

        return dbapi.DatabaseError

    @classmethod
    def get_pool_class(cls, url: Any):
        from sqlalchemy.pool import NullPool

        return NullPool

    @classmethod
    def import_dbapi(cls) -> Any:
        from sqlalchemy_surrealdb import surrealdb as surrealdb_module

        return surrealdb_module

    def create_connect_args(self, url: Any) -> Tuple[Tuple[()], dict]:
        opts = url.translate_connect_args()

        host = url.host or "localhost"
        port = url.port or 8000
        scheme = "ws"

        if url.drivername and url.drivername.startswith("surrealdb+"):
            scheme = "ws"
        else:
            scheme = "ws"

        full_url = f"{scheme}://{host}:{port}"

        return (
            (),
            {
                "url": full_url,
                "username": opts.get("username"),
                "password": opts.get("password"),
                "namespace": url.database,
                "database": url.database,
            },
        )

    def do_execute(
        self, cursor: Any, statement: Any, parameters: Any, context: Any = None
    ) -> None:
        try:
            cursor.execute(statement, parameters or {})
        except Exception as e:
            raise exc.DBAPIError.instance(
                statement, parameters, e, self.dbapi_exception
            )

    def do_executemany(
        self, cursor: Any, statement: Any, parameters: Any, context: Any = None
    ) -> None:
        try:
            cursor.executemany(statement, parameters or [])
        except Exception as e:
            raise exc.DBAPIError.instance(
                statement, parameters, e, self.dbapi_exception
            )

    def do_begin(self, dbapi_connection: Any) -> None:
        pass

    def do_commit(self, dbapi_connection: Any) -> None:
        pass

    def do_rollback(self, dbapi_connection: Any) -> None:
        pass

    def do_close(self, dbapi_connection: Any) -> None:
        try:
            dbapi_connection.close()
        except Exception:
            pass

    @reflection.cache
    def get_table_names(self, connection, schema=None, **kw):
        result = connection.exec_driver_sql("INFO FOR DB")
        tables = []

        row = result.fetchone()
        if row and len(row) > 10:
            tables_dict = row[10]
            if isinstance(tables_dict, dict):
                tables = list(tables_dict.keys())

        return tables

    @reflection.cache
    def get_view_names(self, connection, schema=None, **kw):
        result = connection.exec_driver_sql("SELECT name FROM SCHEMALESS TYPE view")
        views = []
        for row in result:
            if hasattr(row, "name"):
                views.append(row.name)
            elif isinstance(row, dict):
                views.append(row.get("name"))
        return views

    @reflection.cache
    def get_columns(self, connection, table_name, schema=None, **kw):
        result = connection.exec_driver_sql(f"INFO FOR TABLE {table_name}")

        row = result.fetchone()

        if not row or len(row) < 2:
            return []

        fields_info = row[1]

        if not isinstance(fields_info, dict):
            return []

        columns = []
        for field_name, field_def in fields_info.items():
            col = self._parse_column_info(field_name, field_def)
            columns.append(col)

        return columns

    def _parse_column_info(self, field_name: str, field_def: str) -> dict:
        from sqlalchemy import Integer, Float, String, Boolean, DateTime, TEXT

        col_type = String()

        if isinstance(field_def, str):
            if "TYPE int" in field_def or "TYPE integer" in field_def:
                col_type = Integer()
            elif "TYPE float" in field_def or "TYPE number" in field_def:
                col_type = Float()
            elif "TYPE bool" in field_def or "TYPE boolean" in field_def:
                col_type = Boolean()
            elif "TYPE datetime" in field_def or "TYPE timestamp" in field_def:
                col_type = DateTime()
            elif "TYPE array" in field_def:
                col_type = TEXT()
            elif "TYPE object" in field_def:
                col_type = TEXT()

            nullable = "NOT NULL" not in field_def

            default = None
            if "DEFAULT" in field_def:
                parts = field_def.split("DEFAULT")
                if len(parts) > 1:
                    default_val = parts[1].split()[0] if parts[1].strip() else None
                    default = default_val
        else:
            nullable = True
            default = None

        return {
            "name": field_name,
            "type": col_type,
            "nullable": nullable,
            "default": default,
            "autoincrement": False,
        }

    @reflection.cache
    def get_indexes(self, connection, table_name, schema=None, **kw):
        result = connection.exec_driver_sql(f"INFO FOR TABLE {table_name}")

        row = result.fetchone()

        if not row or len(row) < 3:
            return []

        indexes_info = row[2]

        if not isinstance(indexes_info, dict):
            return []

        indexes = []
        for idx_name, idx_def in indexes_info.items():
            idx = {
                "name": idx_name,
                "column_names": [],
                "unique": False,
            }

            if isinstance(idx_def, str):
                if "UNIQUE" in idx_def:
                    idx["unique"] = True

            indexes.append(idx)

        return indexes

    def get_multi_pk_constraint(self, connection, **kw):
        return {}

    @reflection.cache
    def get_primary_keys(self, connection, table_name, schema=None, **kw):
        result = connection.exec_driver_sql(f"INFO FOR TABLE {table_name}")

        row = result.fetchone()

        if not row or len(row) < 2:
            return [{"constrained_columns": ["id"]}]

        return [{"constrained_columns": ["id"]}]

    @reflection.cache
    def get_foreign_keys(self, connection, table_name, schema=None, **kw):
        return []

    @reflection.cache
    def get_unique_constraints(self, connection, table_name, schema=None, **kw):
        return []

    @reflection.cache
    def get_check_constraints(self, connection, table_name, schema=None, **kw):
        return []

    @reflection.cache
    def get_table_comment(self, connection, table_name, schema=None, **kw):
        return None

    def get_schema_names(self, connection, **kw):
        return ["public"]

    @reflection.cache
    def has_table(self, connection, table_name, schema=None, **kw):
        result = connection.exec_driver_sql(
            f"SELECT name FROM SCHEMAFULL WHERE name = '{table_name}'"
        )
        return result.fetchone() is not None


registry.register("surrealdb", "sqlalchemy_surrealdb.base", "SurrealDBDialect")

__dialect__ = SurrealDBDialect
