# SQLAlchemy SurrealDB 方言

[English](./README.md) | 简体中文

> ⚠️ **实验性阶段**：本项目处于实验性阶段，可能随时发生破坏性变更。生产环境使用请自行承担风险。

[SQLAlchemy](https://www.sqlalchemy.org/) 的 [SurrealDB](https://surrealdb.com/) 方言驱动。

## 安装

```bash
uv add sqlalchemy-surrealdb
# 或
pip install sqlalchemy-surrealdb
```

## 连接字符串

```
surrealdb://用户名:密码@主机:端口/数据库
```

示例：

```python
from sqlalchemy import create_engine

engine = create_engine("surrealdb://root:root@127.0.0.1:5070/test")
```

## 使用方法

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


# 创建表
engine = create_engine("surrealdb://root:root@127.0.0.1:5070/test")
Base.metadata.create_all(bind=engine)

# 使用会话
SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)
session = SessionLocal()

# CRUD 操作
user = User(username="alice", email="alice@example.com", age=25)
session.add(user)
session.commit()
```

## 功能特性

- 基本的 CRUD 操作
- SQLAlchemy ORM 支持
- 自定义 `SurrealRecordID` 类型，用于处理 SurrealDB 的 RecordID

## 限制

- 复杂表达式暂时未被全部测试
- 尚不支持异步
- SQL语句构建目前先继承于SQLAlchemy并在结果上进行修改，这可能导致无法预料的问题，且会导致性能开销

## 环境要求

- Python >= 3.9
- SQLAlchemy >= 2.0
- 运行中的 SurrealDB 服务器（测试于 SurrealDB 3.0.4）

## 许可证

AGPL-v3.0
