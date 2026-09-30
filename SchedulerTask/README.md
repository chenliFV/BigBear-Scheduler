# SchedulerTask

定时任务业务子包，负责任务注册与数据库公共方法封装。

## 模块说明

| 文件 | 用途 |
| --- | --- |
| [RegisterTask.py](RegisterTask.py) | 任务注册入口，由 Worker 启动时调用；含 5 个装饰器示例与 2 个脚本文件任务示例 |
| [Utils/db_utils.py](Utils/db_utils.py) | 数据库公共方法，提供连接与增删改查能力 |
| [db_demo.py](db_demo.py) | `db_utils` 的使用示例 |
| [tasks/](tasks/) | 脚本文件任务示例目录（hello_script_task.py / dir_size_report_task.py） |

> 脚本文件任务集中在 [tasks/](tasks/) 目录下，由 `RegisterTask.py` 中的
> `_register_script_file_examples()` 按绝对路径注册。

---

## 数据库公共方法（db_utils）

统一封装 SQLAlchemy，提供连接数据库、查询、写入以及多库协作的通用能力，支持 MySQL / PostgreSQL / SQLite 等多种后端。

### 核心特性

- **统一封装**：`Database` 类提供 `query` / `query_one` / `execute` / `insert_batch` 等方法
- **多库协作**：`multi_connect` 一次性连接多个数据库，业务逻辑由调用方自行处理
- **配置驱动**：连接串统一由 [Config/database.json](../Config/database.json) 集中管理，通过 `Config.get_database_url(db_name)` 生成；也可显式传入
- **上下文管理**：`with` 语法自动关闭连接池
- **批量写入**：使用 `executemany` 高效导入大数据集
- **显式事务**：`transaction()` 上下文支持多条语句原子提交

### API

#### `Database` 类

单库封装，通过 `connect()` 工厂创建，推荐用 `with` 语法管理生命周期。

| 方法 | 说明 |
| --- | --- |
| `query(sql, params=None)` | 执行 SELECT，返回 `list[dict]` |
| `query_one(sql, params=None)` | 执行 SELECT，返回首行 `dict` 或 `None` |
| `query_scalar(sql, params=None)` | 执行查询并返回首个标量值（如 `COUNT(*)`） |
| `execute(sql, params=None)` | 执行单条 INSERT/UPDATE/DELETE（自动提交），返回受影响行数 |
| `execute_many(sql, params_list)` | 批量执行同一 SQL，返回总受影响行数 |
| `insert_batch(table, rows, batch_size=1000)` | 字典列表批量插入指定表，返回总插入行数 |
| `transaction()` | 显式事务上下文，多条语句原子提交 |
| `close()` | 释放连接池 |
| `url` / `engine` | 暴露底层连接串与 Engine，供高级场景使用 |

#### 模块级函数

| 函数 | 说明 |
| --- | --- |
| `connect(url=None, config_name="scheduler", echo=False)` | 创建 `Database`。`url` 为 `None` 时通过 `Config.get_database_url(config_name)` 从 [Config/database.json](../Config/database.json) 统一生成 |
| `get_db_url(config_name="scheduler")` | 通过 `Config.get_database_url(config_name)` 生成连接串，`config_name` 为数据库逻辑名（如 `scheduler` / `business`） |
| `multi_connect(*db_specs)` | 一次性连接多个数据库的上下文管理器，业务逻辑自行处理 |

### 支持的数据库连接串

> 推荐通过 [Config/database.json](../Config/database.json) 统一管理连接参数，使用 `connect()` / `get_db_url()` 时传入逻辑名即可。下表仅列出 SQLAlchemy 连接串格式，用于显式传入 `url` 参数或调试。

| 数据库 | 连接串示例 |
| --- | --- |
| MySQL | `mysql+pymysql://user:pass@localhost/dbname` |
| PostgreSQL | `postgresql://user:pass@localhost:5432/dbname` |
| SQLite | `sqlite:///path/to/db.sqlite` |

### 使用示例

#### 1. 单库操作

```python
from SchedulerTask.Utils.db_utils import connect

# 显式指定连接串
with connect("mysql+pymysql://user:pass@localhost/db") as db:
    # 查询
    rows = db.query("SELECT * FROM users WHERE age > :age", {"age": 18})
    one  = db.query_one("SELECT * FROM users WHERE id = :id", {"id": 10})
    cnt  = db.query_scalar("SELECT COUNT(*) FROM users")

    # 写入（自动提交）
    affected = db.execute(
        "UPDATE users SET active = 1 WHERE id = :id", {"id": 10}
    )

    # 批量插入
    rows = [
        {"name": "alice", "age": 20},
        {"name": "bob",   "age": 25},
    ]
    inserted = db.insert_batch("users", rows, batch_size=500)
```

#### 2. 从项目配置读取连接串

```python
from SchedulerTask.Utils.db_utils import connect

# 不传 url，默认读取 Config/database.json 中的 scheduler 数据库
with connect() as db:
    cnt = db.query_scalar("SELECT COUNT(*) FROM scheduler_jobs")

# 也可指定其他逻辑数据库名（如 business）
with connect(config_name="business") as db:
    ...
```

#### 3. 显式事务

```python
from sqlalchemy import text
from SchedulerTask.Utils.db_utils import connect

with connect("mysql+pymysql://user:pass@localhost/db") as db:
    with db.transaction() as conn:
        conn.execute(text("INSERT INTO orders (id, amount) VALUES (:id, :amt)"),
                     {"id": 1, "amt": 100})
        conn.execute(text("UPDATE inventory SET stock = stock - 1 WHERE sku = :sku"),
                     {"sku": "A001"})
        # 任一语句失败则整体回滚
```

#### 4. 多库操作（跨库数据迁移）

`multi_connect` 只负责一次性建立多个连接，具体的读取、处理、写入逻辑由调用方自行实现。

**方式 A：按位置返回（适合两个库）**

```python
from SchedulerTask.Utils.db_utils import multi_connect

source_url = "mysql+pymysql://user:pass@host-a/src_db"
dest_url   = "mysql+pymysql://user:pass@host-b/dst_db"

with multi_connect(source_url, dest_url) as (src, dst):
    # 1. 从源库读取
    rows = src.query("SELECT id, name, created_at FROM users WHERE active = 1")

    # 2. 自行处理（字段重命名 / 增删列 / 过滤行）
    archived = [{**row, "archived_at": "2026-07-06"} for row in rows]

    # 3. 写入目标库
    dst.insert_batch("archived_users", archived, batch_size=2000)
```

**方式 B：按名字返回（适合多个库，避免顺序混淆）**

```python
with multi_connect(
    ("src", source_url),
    ("dst", dest_url),
) as dbs:
    rows = dbs["src"].query("SELECT * FROM users")
    dbs["dst"].insert_batch("archived_users", rows)
```

**方式 C：混合配置（连接串 + 数据库逻辑名）**

```python
with multi_connect(
    {"name": "scheduler", "config": "scheduler"},          # 从 Config/database.json 生成
    {"name": "business",  "config": "business"},           # 从 Config/database.json 生成
) as dbs:
    jobs = dbs["scheduler"].query("SELECT * FROM scheduler_jobs")
    dbs["business"].insert_batch("job_snapshot", jobs)
```

> `multi_connect` 在 `with` 块结束时统一释放所有连接池，即使中间抛异常也能保证全部释放。

### 在脚本任务中使用

脚本任务以子进程方式运行，需在文件开头将项目根目录加入 `sys.path`：

```python
import sys
from pathlib import Path

# 动态向上查找包含 SchedulerTask 和 Config 子目录的项目根
_PROJECT_ROOT = Path(__file__).resolve().parent
while _PROJECT_ROOT != _PROJECT_ROOT.parent:
    if (_PROJECT_ROOT / "SchedulerTask").is_dir() and (
        _PROJECT_ROOT / "Config"
    ).is_dir():
        break
    _PROJECT_ROOT = _PROJECT_ROOT.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from SchedulerTask.Utils.db_utils import connect, multi_connect

def my_task():
    with connect() as db:
        ...
```

参考 [db_demo.py](db_demo.py) 查看完整示例。

### 运行示例

```bash
# 直接运行示例脚本
python SchedulerTask/db_demo.py
```
