# SQLAlchemy SurrealDB Dialect

English | [简体中文](./README.zh.md)

> ⚠️ **Experimental Stage**: This project is in an experimental stage. Breaking changes may occur at any time. Use in production at your own risk.

A SQLAlchemy dialect for [SurrealDB](https://surrealdb.com/).

## Installation

```bash
uv add sqlalchemy-surrealdb
# or
pip install sqlalchemy-surrealdb
```

## Connection String

```
surrealdb://username:password@host:port/database
```

Example:

```python
from sqlalchemy import create_engine

engine = create_engine("surrealdb://root:root@127.0.0.1:5070/test")
```

## Usage

```python
from sqlalchemy import Column, String, Integer, create_engine
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy_surrealdb.types import SurrealRecordID
from surrealdb import RecordID
import uuid

Base = declarative_base()


class User(Base):
    __tablename__ = "users"

    id = Column(
        SurrealRecordID,
        primary_key=True,
        default=lambda: RecordID("users", uuid.uuid4().hex),
    )
    username = Column(String(50), unique=True, nullable=False)
    email = Column(String(120), unique=True, nullable=False)
    age = Column(Integer, nullable=True)


# Create tables
engine = create_engine("surrealdb://root:root@127.0.0.1:5070/test")
Base.metadata.create_all(bind=engine)

# Use session
SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)
session = SessionLocal()

# CRUD operations
user = User(username="alice", email="alice@example.com", age=25)
session.add(user)
session.commit()
```

## Features

- Basic CRUD operations
- SQLAlchemy ORM support
- Custom `SurrealRecordID` type for SurrealDB RecordID handling

## Limitations

- Complex expressions have not yet been fully tested
- Asynchronous operations are not yet supported
- SQL statement construction currently inherits from SQLAlchemy and modifies the results, which may lead to unexpected issues and result in a performance overhead

## Requirements

- Python >= 3.9
- SQLAlchemy >= 2.0
- SurrealDB server running (tested with SurrealDB 3.0.4)

## License

AGPL-v3.0
