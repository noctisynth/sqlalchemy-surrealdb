# SQLAlchemy SurrealDB Dialect

## Setup

```bash
# Install with dev dependencies (uses uv-style dependency groups)
uv sync --dev
```

## Connection String

```
surrealdb://username:password@host:port/database
```

Example: `surrealdb://root:root@127.0.0.1:5070/test`

## Testing

No automated tests exist. Run the example to verify functionality:

```bash
# Requires a running SurrealDB instance
python examples/models.py
```

## Architecture

- Entry point registered in `pyproject.toml` under `[project.entry-points."sqlalchemy.dialects"]`
- Main dialect: `sqlalchemy_surrealdb.base:SurrealDBDialect`
- DBAPI wrapper: `sqlalchemy_surrealdb.surrealdb` (wraps `surrealdb` package)
- Custom type: `SurrealRecordID` in `types.py` for SurrealDB RecordID handling

## Key Constraints

- SurrealDB doesn't support table-prefixed columns in some contexts; the compiler strips `table.column` → `column`
- Uses `NullPool` (no connection pooling)
- No schema support (SurrealDB uses namespaces/databases differently)
- `RETURNING` clauses not supported for INSERT/UPDATE/DELETE
