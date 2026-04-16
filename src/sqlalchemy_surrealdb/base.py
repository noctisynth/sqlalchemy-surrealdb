from __future__ import annotations

from typing import Any, Optional, Type

import sqlalchemy
from sqlalchemy import URL, Pool, exc
from sqlalchemy.dialects import registry
from sqlalchemy.engine import ConnectArgsType, default, reflection
from sqlalchemy.schema import CreateIndex, CreateTable, DropTable
from sqlalchemy.sql import compiler
from sqlalchemy.sql.compiler import DDLCompiler, GenericTypeCompiler, SQLCompiler


class SurrealDBTypeCompiler(GenericTypeCompiler):
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
        return "DECIMAL" if type_.precision else "FLOAT"

    def visit_DECIMAL(self, type_: Any, **kwargs: Any) -> str:
        return "DECIMAL"

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

    def visit_VARBINARY(self, type_: Any, **kwargs: Any) -> str:
        return "BINARY"

    def visit_BLOB(self, type_: Any, **kwargs: Any) -> str:
        return "BINARY"

    def visit_JSON(self, type_: Any, **kwargs: Any) -> str:
        return "OBJECT"

    def visit_UUID(self, type_: Any, **kwargs: Any) -> str:
        return "STRING"


class SurrealDBIdentifierPreparer(compiler.IdentifierPreparer):
    reserved_words = {
        "ALL",
        "AND",
        "ANY",
        "AS",
        "ASC",
        "BEGIN",
        "BETWEEN",
        "BREAK",
        "BY",
        "CASE",
        "CATCH",
        "COLUMNS",
        "CONCAT",
        "CONTENT",
        "CONTINUE",
        "CREATE",
        "DATABASE",
        "DATETIME",
        "DEFINE",
        "DELETE",
        "DESC",
        "DISTINCT",
        "ELSE",
        "END",
        "EVENT",
        "EXISTS",
        "EXPLAIN",
        "FETCH",
        "FIELD",
        "FIELDS",
        "FLEX",
        "FOR",
        "FROM",
        "FUNCTION",
        "GRANT",
        "GROUP",
        "IF",
        "INDEX",
        "INFO",
        "INSERT",
        "INTO",
        "KILL",
        "LET",
        "LIMIT",
        "LIVE",
        "MERGE",
        "NO",
        "NOT",
        "NULL",
        "ON",
        "OPTION",
        "ORDER",
        "OVER",
        "PARALLEL",
        "PASSWORD",
        "PERMISSIONS",
        "PRECISION",
        "RANGE",
        "READONLY",
        "RETURN",
        "SCHEMA",
        "SELECT",
        "SET",
        "SHOW",
        "SIGNED",
        "SLEEP",
        "SNAPSHOT",
        "SQL",
        "START",
        "TABLE",
        "THEN",
        "TYPE",
        "UNIQUE",
        "UNLESS",
        "UPDATE",
        "USE",
        "USER",
        "VALUES",
        "VERSION",
        "WHEN",
        "WHERE",
        "WITH",
        "WRITABLE",
    }

    def quote(self, ident: str, force: bool = False) -> str:
        if not ident:
            return ident

        if self._requires_quotes(ident):
            if not force and ident in self.reserved_words:
                return ident
            return f"`{ident}`"
        return ident


class SurrealDBCompiler(SQLCompiler):
    def __init__(self, dialect, statement, **kwargs: Any) -> None:
        super().__init__(dialect, statement, **kwargs)
        self.bindtemplate = "$%(name)s"
        self.compilation_bindtemplate = "$%(name)s"

    def bindparam_string(
        self,
        name: str,
        post_compile: bool = False,
        expanding: bool = False,
        escaped_from: Optional[str] = None,
        bindparam_type: Any = None,
        accumulate_bind_names: Any = None,
        visited_bindparam: Any = None,
        **kw: Any,
    ) -> str:
        return "$" + name

    def visit_column(
        self,
        column: Any,
        add_to_result_map: Any = None,
        include_table: bool = False,
        result_map_targets: Any = (),
        ambiguous_table_name_map: Any = None,
        **kwargs: Any,
    ) -> str:
        return super().visit_column(
            column,
            add_to_result_map=add_to_result_map,
            include_table=False,
            result_map_targets=result_map_targets,
            ambiguous_table_name_map=ambiguous_table_name_map,
            **kwargs,
        )

    def limit_clause(self, select: Any, **kw: Any) -> str:
        text = ""
        if select._limit_clause is not None:
            text += "\n LIMIT " + self.process(select._limit_clause, **kw)
        if select._offset_clause is not None:
            if select._limit_clause is None:
                text += "\n LIMIT MATH::INF"
            text += " START " + self.process(select._offset_clause, **kw)
        return text

    def visit_select(
        self,
        select_stmt: Any,
        asfrom: bool = False,
        insert_into: bool = False,
        fromhints: Any = None,
        compound_index: Any = None,
        select_wraps_for: Any = None,
        lateral: bool = False,
        from_linter: Any = None,
        **kwargs: Any,
    ) -> str:
        sql = super().visit_select(
            select_stmt,
            asfrom=asfrom,
            insert_into=insert_into,
            fromhints=fromhints,
            compound_index=compound_index,
            select_wraps_for=select_wraps_for,
            lateral=lateral,
            from_linter=from_linter,
            **kwargs,
        )

        sql_upper = sql.upper()

        if "FROM" not in sql_upper:
            sql = "RETURN " + sql.replace("SELECT ", "").strip()

        return sql

    def visit_insert(
        self,
        insert_stmt: Any,
        visited_bindparam: Any = None,
        visiting_cte: Any = None,
        **kw: Any,
    ) -> str:
        sql = super().visit_insert(
            insert_stmt,
            visited_bindparam=visited_bindparam,
            visiting_cte=visiting_cte,
            **kw,
        )
        return sql

    def visit_update(
        self, update_stmt: Any, visiting_cte: Any = None, **kw: Any
    ) -> str:
        sql = super().visit_update(update_stmt, visiting_cte=visiting_cte, **kw)
        return sql

    def visit_delete(
        self, delete_stmt: Any, visiting_cte: Any = None, **kw: Any
    ) -> str:
        sql = super().visit_delete(delete_stmt, visiting_cte=visiting_cte, **kw)
        if "RETURN" not in sql.upper():
            sql = sql + " RETURN BEFORE"
        return sql

    def visit_textclause(
        self, textclause: Any, add_to_result_map: Any = None, **kw: Any
    ) -> str:
        text = textclause.text

        text_upper = text.upper().strip()

        if text_upper.startswith("SELECT") and "FROM" not in text_upper:
            parts = text[6:].strip().split()
            cleaned_parts = []
            skip_next = False
            for i, part in enumerate(parts):
                if skip_next:
                    skip_next = False
                    continue
                if part.upper() == "AS":
                    skip_next = True
                    continue
                cleaned_parts.append(part)
            text = "RETURN " + " ".join(cleaned_parts)

        return text

    def returning_clause(self, stmt: Any, returning_cols: Any, **kw: Any) -> str:
        columns = [self.preparer.format_column(col) for col in returning_cols]
        return "RETURN " + ", ".join(columns)


class SurrealDBDDLCompiler(DDLCompiler):
    def get_column_specification(
        self, column: sqlalchemy.schema.Column, **kwargs: Any
    ) -> str:
        colspec = self.preparer.format_column(column)

        col_type = self.dialect.type_compiler.process(column.type)
        colspec += f" TYPE {col_type}"

        if column.autoincrement:
            colspec += " FLEX"

        if column.server_default is not None:
            default = self.get_column_default_string(column)
            if default is not None:
                colspec += f" DEFAULT {default}"

        if not column.nullable:
            colspec += " ASSERT $value != null"

        return colspec

    def visit_create_table(self, create: CreateTable, **kwargs: Any) -> str:
        table = create.element
        preparer = self.preparer

        parts = []

        table_name = preparer.format_table(table)
        parts.append(f"DEFINE TABLE {table_name} SCHEMAFUL")

        for column in table.columns:
            col_name = preparer.format_column(column)
            col_type = self.dialect.type_compiler.process(column.type)

            if column.nullable:
                col_type = self._make_option_type(col_type)

            field_def = f"DEFINE FIELD {col_name} ON {table_name} TYPE {col_type}"

            if column.server_default is not None:
                default = self.get_column_default_string(column)
                if default is not None:
                    field_def += f" DEFAULT {default}"

            if not column.nullable:
                field_def += " ASSERT $value != null"

            parts.append(field_def)

        for index in table.indexes:
            idx_name = index.name
            if idx_name is None:
                continue
            idx_cols = [preparer.format_column(c) for c in index.columns]
            unique = index.unique
            if unique:
                parts.append(
                    f"DEFINE INDEX {idx_name} ON {table_name} FIELDS {', '.join(idx_cols)} UNIQUE"
                )
            else:
                parts.append(
                    f"DEFINE INDEX {idx_name} ON {table_name} FIELDS {', '.join(idx_cols)}"
                )

        return "; ".join(parts)

    def visit_drop_table(self, drop: DropTable, **kwargs: Any) -> str:
        return f"REMOVE TABLE {self.preparer.format_table(drop.element)}"

    def visit_create_index(
        self,
        create: CreateIndex,
        include_schema: bool = False,
        include_table_schema: bool = True,
        **kwargs: Any,
    ) -> str:
        index = create.element
        preparer = self.preparer

        if index.table is None:
            return ""
        table_name = preparer.format_table(index.table, use_schema=include_table_schema)

        idx_name = index.name
        if idx_name is None:
            return ""

        idx_cols = [preparer.format_column(c) for c in index.columns]
        unique = index.unique

        if unique:
            return f"DEFINE INDEX {idx_name} ON {table_name} FIELDS {', '.join(idx_cols)} UNIQUE"
        return f"DEFINE INDEX {idx_name} ON {table_name} FIELDS {', '.join(idx_cols)}"

    def _make_option_type(self, type_str: str) -> str:
        type_map = {
            "INT": "option<int>",
            "BIGINT": "option<int>",
            "FLOAT": "option<float>",
            "DECIMAL": "option<decimal>",
            "STRING": "option<string>",
            "BOOL": "option<bool>",
            "DATETIME": "option<datetime>",
            "BINARY": "option<binary>",
            "OBJECT": "option<object>",
            "ARRAY": "option<array>",
        }
        upper_type = type_str.upper()
        return type_map.get(upper_type, f"option<{type_str.lower()}>")


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
    supports_comments = True
    supports_constraint_comments = False
    supports_native_boolean = True
    supports_native_decimal = True
    supports_native_json = True
    supports_schemas = False
    supports_sequences = False
    supports_views = True

    max_identifier_length = 1024

    statement_compiler = SurrealDBCompiler
    ddl_compiler = SurrealDBDDLCompiler
    type_compiler_cls = SurrealDBTypeCompiler

    preparer = SurrealDBIdentifierPreparer

    execution_ctx_cls = SurrealDBExecutionContext
    inspector = SurrealDBInspector

    insert_returning = True
    update_returning = True
    delete_returning = True

    supports_empty_insert = True
    supports_default_values = True
    supports_default_metavalue = True
    supports_multivalues_insert = True

    colspecs: dict = {}
    ischema_names: dict = {}

    @property
    def dbapi_exception(self) -> Any:
        from sqlalchemy_surrealdb import surrealdb as dbapi

        return dbapi.DatabaseError

    @classmethod
    def get_pool_class(cls, url: URL) -> Type[Pool]:
        from sqlalchemy.pool import NullPool

        return NullPool

    @classmethod
    def import_dbapi(cls) -> Any:
        from sqlalchemy_surrealdb import surrealdb as surrealdb_module

        return surrealdb_module

    def create_connect_args(self, url: URL) -> ConnectArgsType:
        opts = {}

        scheme = url.query.get("scheme", "ws")
        opts["scheme"] = scheme

        if url.username:
            opts["username"] = url.username
        if url.password:
            opts["password"] = url.password

        host = url.host or "localhost"
        port = url.port or 8000
        opts["host"] = host
        opts["port"] = port

        path = url.database or ""
        if "/" in path:
            db_parts = path.split("/")
            opts["database"] = db_parts[0] if len(db_parts) > 0 else "surrealdb"
            opts["namespace"] = db_parts[1] if len(db_parts) > 1 else "default"
        else:
            opts["database"] = path or "surrealdb"
            opts["namespace"] = url.query.get("namespace", "default")

        return ((), opts)

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
        dbapi_connection.begin()

    def do_commit(self, dbapi_connection: Any) -> None:
        dbapi_connection.commit()

    def do_rollback(self, dbapi_connection: Any) -> None:
        dbapi_connection.rollback()

    def do_close(self, dbapi_connection: Any) -> None:
        dbapi_connection.close()

    @reflection.cache
    def get_table_names(self, connection, schema=None, **kw):
        result = connection.exec_driver_sql("INFO FOR DB")
        tables = []

        row = result.fetchone()
        if row:
            tables_dict = None

            if isinstance(row, dict):
                tables_dict = row.get("tables", {})
            elif hasattr(row, "_mapping"):
                tables_dict = row._mapping.get("tables", {})
            elif isinstance(row, (list, tuple)) and len(row) > 0:
                tables_dict = row[0]

            if isinstance(tables_dict, dict):
                tables = list(tables_dict.keys())

        return tables

    @reflection.cache
    def get_view_names(self, connection, schema=None, **kw):
        result = connection.exec_driver_sql(
            "SELECT name FROM SCHEMALESS WHERE type = 'view'"
        )
        views = []
        for row in result:
            if hasattr(row, "name"):
                views.append(row.name)
            elif isinstance(row, dict):
                views.append(row.get("name"))
            elif isinstance(row, (list, tuple)) and len(row) > 0:
                views.append(row[0])
        return views

    @reflection.cache
    def get_columns(self, connection, table_name, schema=None, **kw):
        result = connection.exec_driver_sql(f"INFO FOR TABLE {table_name}")

        row = result.fetchone()

        if not row:
            return []

        fields_info = None
        if isinstance(row, dict):
            fields_info = row.get("fields", {})
        elif hasattr(row, "_mapping"):
            fields_info = row._mapping.get("fields", {})
        elif isinstance(row, (list, tuple)) and len(row) > 1:
            fields_info = row[1] if len(row) > 1 else None

        if not isinstance(fields_info, dict):
            return []

        columns = []
        for field_name, field_def in fields_info.items():
            col = self._parse_column_info(field_name, field_def)
            columns.append(col)

        return columns

    def _parse_column_info(self, field_name: str, field_def: Any) -> dict:
        from sqlalchemy import TEXT, Boolean, DateTime, Float, Integer, String

        if isinstance(field_def, str):
            type_str = field_def.upper()

            if "INT" in type_str or "INTEGER" in type_str:
                col_type = Integer()
            elif "FLOAT" in type_str or "NUMBER" in type_str or "DECIMAL" in type_str:
                col_type = Float()
            elif "BOOL" in type_str:
                col_type = Boolean()
            elif "DATETIME" in type_str or "TIMESTAMP" in type_str:
                col_type = DateTime()
            elif "ARRAY" in type_str:
                col_type = TEXT()
            elif "OBJECT" in type_str:
                col_type = TEXT()
            else:
                col_type = String()

            nullable = "NOT NULL" not in field_def.upper()

            default = None
            if "DEFAULT" in field_def.upper():
                parts = field_def.upper().split("DEFAULT")
                if len(parts) > 1:
                    default_val = parts[1].split()[0] if parts[1].strip() else None
                    default = default_val
        else:
            col_type = String()
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

        if not row:
            return []

        indexes_info = None
        if isinstance(row, dict):
            indexes_info = row.get("indexes", {})
        elif hasattr(row, "_mapping"):
            indexes_info = row._mapping.get("indexes", {})
        elif isinstance(row, (list, tuple)) and len(row) > 2:
            indexes_info = row[2] if len(row) > 2 else None

        if not isinstance(indexes_info, dict):
            return []

        indexes = []
        for idx_name, idx_def in indexes_info.items():
            idx = {
                "name": idx_name,
                "column_names": self._extract_index_columns(idx_def),
                "unique": False,
            }

            if isinstance(idx_def, str):
                if "UNIQUE" in idx_def.upper():
                    idx["unique"] = True

            indexes.append(idx)

        return indexes

    def _extract_index_columns(self, idx_def: str) -> list:
        import re

        if not isinstance(idx_def, str):
            return []

        match = re.search(r"FIELDS\s+([\w\s,]+?)(?:\s+UNIQUE|\s*$)", idx_def, re.I)
        if match:
            fields_str = match.group(1).strip()
            columns = [f.strip().split(".")[-1] for f in fields_str.split(",")]
            return [c for c in columns if c]
        return []

    def get_multi_pk_constraint(self, connection, **kw):
        return {}

    @reflection.cache
    def get_pk_constraint(self, connection, table_name, schema=None, **kw):
        result = connection.exec_driver_sql(f"INFO FOR TABLE {table_name}")

        row = result.fetchone()

        if not row:
            return {"constrained_columns": ["id"], "name": None}

        fields_info = None
        if isinstance(row, dict):
            fields_info = row.get("fields", {})
        elif hasattr(row, "_mapping"):
            fields_info = row._mapping.get("fields", {})
        elif isinstance(row, (list, tuple)) and len(row) > 1:
            fields_info = row[1] if len(row) > 1 else None

        if isinstance(fields_info, dict) and "id" in fields_info:
            field_def = fields_info["id"]
            if isinstance(field_def, str) and "TYPE record" in field_def.upper():
                return {"constrained_columns": ["id"], "name": None}

        return {"constrained_columns": ["id"], "name": None}

    @reflection.cache
    def get_foreign_keys(self, connection, table_name, schema=None, **kw):
        result = connection.exec_driver_sql(f"INFO FOR TABLE {table_name}")

        row = result.fetchone()

        if not row:
            return []

        import re

        foreign_keys = []

        fields_info = None
        if isinstance(row, dict):
            fields_info = row.get("fields", {})
        elif hasattr(row, "_mapping"):
            fields_info = row._mapping.get("fields", {})
        elif isinstance(row, (list, tuple)) and len(row) > 1:
            fields_info = row[1] if len(row) > 1 else None

        if isinstance(fields_info, dict):
            for col_name, col_def in fields_info.items():
                if not isinstance(col_def, str):
                    continue

                match = re.search(r"TYPE\s+RECORD\s*\(\s*(\w+)\s*\)", col_def, re.I)
                if match:
                    ref_table = match.group(1)
                    if col_name != "id":
                        foreign_keys.append(
                            {
                                "name": f"fk_{col_name}",
                                "constrained_columns": [col_name],
                                "referred_table": ref_table,
                                "referred_columns": ["id"],
                            }
                        )

        indexes_info = None
        if isinstance(row, dict):
            indexes_info = row.get("indexes", {})
        elif hasattr(row, "_mapping"):
            indexes_info = row._mapping.get("indexes", {})
        elif isinstance(row, (list, tuple)) and len(row) > 2:
            indexes_info = row[2] if len(row) > 2 else None

        if isinstance(indexes_info, dict):
            for idx_name, idx_def in indexes_info.items():
                if not isinstance(idx_def, str):
                    continue

                idx_upper = idx_def.upper()
                if "UNIQUE" not in idx_upper and "INDEX" not in idx_upper:
                    continue

                col_match = re.search(
                    r"FIELDS\s+([\w\s,]+?)(?:\s+UNIQUE|\s*$)", idx_def, re.I
                )
                if not col_match:
                    continue

                fields_str = col_match.group(1).strip()
                for field_part in fields_str.split(","):
                    field_name = field_part.strip().split(".")[-1]

                    if field_name.startswith("id_") or field_name.endswith("_id"):
                        ref_table_match = re.search(
                            r"TYPE\s+RECORD\s*\(\s*(\w+)\s*\)", idx_def, re.I
                        )
                        if ref_table_match:
                            ref_table = ref_table_match.group(1)
                        else:
                            ref_table = field_name.replace("id_", "").replace("_id", "")

                        fk_exists = any(
                            fk["constrained_columns"] == [field_name]
                            for fk in foreign_keys
                        )
                        if not fk_exists:
                            foreign_keys.append(
                                {
                                    "name": f"fk_{field_name}",
                                    "constrained_columns": [field_name],
                                    "referred_table": ref_table,
                                    "referred_columns": ["id"],
                                }
                            )

        return foreign_keys

    @reflection.cache
    def get_unique_constraints(self, connection, table_name, schema=None, **kw):
        result = connection.exec_driver_sql(f"INFO FOR TABLE {table_name}")

        row = result.fetchone()

        if not row:
            return []

        indexes_info = None
        if isinstance(row, dict):
            indexes_info = row.get("indexes", {})
        elif hasattr(row, "_mapping"):
            indexes_info = row._mapping.get("indexes", {})
        elif isinstance(row, (list, tuple)) and len(row) > 2:
            indexes_info = row[2] if len(row) > 2 else None

        if not isinstance(indexes_info, dict):
            return []

        constraints = []
        for idx_name, idx_def in indexes_info.items():
            if isinstance(idx_def, str) and "UNIQUE" in idx_def.upper():
                constraints.append(
                    {
                        "name": idx_name,
                        "column_names": self._extract_index_columns(idx_def),
                    }
                )

        return constraints

    @reflection.cache
    def get_check_constraints(self, connection, table_name, schema=None, **kw):
        return []

    @reflection.cache
    def get_table_comment(self, connection, table_name, schema=None, **kw):
        return {"text": None}

    def get_schema_names(self, connection, **kw):
        return ["default"]

    @reflection.cache
    def has_table(self, connection, table_name, schema=None, **kw):
        result = connection.exec_driver_sql("INFO FOR DB")
        row = result.fetchone()

        if not row:
            return False

        row_dict = None
        if isinstance(row, dict):
            row_dict = row
        elif hasattr(row, "_mapping"):
            row_dict = row._mapping

        if row_dict:
            tables_info = row_dict.get("tables", {})
            if isinstance(tables_info, dict):
                return table_name in tables_info

        return False


registry.register("surrealdb", "sqlalchemy_surrealdb.base", "SurrealDBDialect")

__dialect__ = SurrealDBDialect
