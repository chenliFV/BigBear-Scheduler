# BigBear-Scheduler

任务调度器，支持异步、时区处理、cron 表达式和美观的实时仪表板。采用 **Worker + API 双进程架构**，将任务调度执行与 Web 监控完全解耦，可独立重启、独立扩展。

## 功能特性

- 🎯 简洁的装饰器 API - 一行代码即可注册任务
- ⚡ 异步支持 - 原生 async/await 函数支持
- 🕐 时区支持 - 可在任意时区调度任务
- 📅 Cron 表达式 - 通过 cron 语法实现复杂调度
- 💾 状态持久化 - 重启后恢复状态，处理错过的任务
- 🗄️ 数据库支持 - 通过 SQLModel 支持 SQLite、PostgreSQL、MySQL
- 🎨 FastAPI 仪表板 - 美观的实时监控界面
- 🔄 自动重试 - 可配置重试次数，采用指数退避策略
- ⏱️ 任务超时 - 自动终止长时间运行的任务
- ⏸️ 暂停/恢复 - 无需删除即可控制任务执行
- 📋 死信队列 - 追踪并调试失败的任务
- 🛠️ 动态建表 - 通过 JSON 配置手动创建/迁移业务数据表（CLI 或代码调用）
- 🗃️ 数据库公共方法 - 统一封装多数据库连接、查询、批量写入与跨库操作

## 架构概览

![架构概览](docs/architecture.svg)

- **Worker 进程**（`SchedulerRun.py`）：负责调度循环与任务执行，不启动 FastAPI。可独立运行、独立重启。
- **API 进程**（`FastApiRun.py`）：只负责 Dashboard 展示与 REST API 接口，不启动调度循环、不执行任务。可独立重启，不影响 Worker。
- **共享存储**：两个进程通过同一存储后端（默认 MySQL）同步任务状态、历史记录、死信队列。

## 目录结构

```
Python-Scheduler/
├── Config/                     # 配置文件目录
│   ├── __init__.py
│   ├── loader.py               # 配置加载器（含日志初始化、get_database_url）
│   ├── database.json           # 数据库连接集中配置（connections + databases）
│   ├── scheduler.json          # Worker 进程配置
│   └── fastapi.json            # API 进程配置
├── Log/                        # 运行日志根目录（运行时自动创建，按组件分子目录，按天轮转保留 30 天）
│   ├── worker/                 # Worker 进程日志
│   ├── api/                    # API 进程日志
│   └── table_generator/        # 建表/迁移日志
├── Scheduler/                  # Scheduler 调度库（核心引擎）
│   ├── __init__.py
│   ├── main.py                 # Scheduler 主类（调度引擎）
│   ├── models.py               # 数据模型（Job / JobHistory / JobStatus）
│   ├── schedulers.py           # 各调度模式（interval/daily/weekly/cron/once）
│   ├── fastapi_integration.py  # FastAPI 集成（Dashboard + REST API）
│   ├── storage/                # 存储后端（JSON / SQLModel）
│   ├── dashboard/              # Web Dashboard 前端资源（HTML/CSS/JS/i18n）
│   └── tests/                  # 调度器单元测试
├── SchedulerTable/             # 动态数据表生成模块
│   ├── TableGenerator.py       # 建表/迁移引擎（JSON 配置驱动，支持配置目录）
│   ├── RollbackSchema.py       # 结构回滚脚本（按 batch_id 回滚，支持 SQLite/MySQL/PostgreSQL）
│   ├── tables_config.md        # 配置字段说明文档
│   ├── tables_config_test.json # 示例建表配置（演示用，可按需修改）
│   └── tests/                  # 建表/回滚单元测试
├── SchedulerTask/              # 业务任务注册中心
│   ├── __init__.py
│   ├── RegisterTask.py         # 任务注册入口（当前内置 5 个装饰器示例 + 2 个脚本文件示例）
│   ├── db_demo.py              # 数据库公共方法使用示例
│   ├── README.md               # SchedulerTask 使用说明
│   ├── Utils/                  # 公共工具目录
│   │   └── db_utils.py         # 数据库公共方法（连接/查询/写入/多库协作）
│   └── tasks/                  # 脚本文件任务示例目录
│       ├── hello_script_task.py        # 最小脚本示例（调度上下文环境变量）
│       └── dir_size_report_task.py     # 带参数脚本示例（script_args + 退出码约定）
├── docs/                       # 文档资源（架构图、Dashboard 截图）
├── docker/                     # Docker 部署目录（完整说明见 docker/README.md）
│   ├── README.md               # Docker 部署指南（快速启动 / 环境变量 / 运维命令）
│   ├── Dockerfile              # 调度器镜像构建文件（Worker / API 共用同一镜像）
│   ├── Dockerfile.dockerignore # 构建排除清单（BuildKit 变体，与 Dockerfile 同目录即生效）
│   ├── docker-compose.yml      # Docker Compose 编排（MySQL + Worker + API）
│   ├── database.json           # Docker 数据库配置（${DB_HOST} 等环境变量占位）
│   ├── initdb/
│   │   └── 01-init.sql         # MySQL 首启初始化脚本（预建 business 库并授权）
│   └── log/                    # 容器运行日志挂载目录（worker/ api/，运行时生成）
├── SchedulerRun.py             # Worker 进程入口
├── FastApiRun.py               # API 进程入口
├── start.sh                    # 一键启停脚本
├── Requirements.txt            # Python 依赖
└── README.md
```

## 快速开始

### 1. 环境准备

```bash
# 创建虚拟环境
python -m venv .venv
source .venv/bin/activate

# 安装依赖
pip install -r Requirements.txt
```

### 2. 配置数据库

所有数据库连接串统一由 [Config/database.json](Config/database.json) 集中管理，通过 `Config.get_database_url(db_name)` 生成。修改连接参数只需编辑这一个文件：

```json
{
    "connections": {
        "default": {
            "driver": "mysql+pymysql",
            "host": "localhost",
            "port": 3306,
            "user": "stkuser",
            "password": "stkuser",
            "charset": "utf8mb4"
        }
    },
    "databases": {
        "scheduler": {
            "connection": "default",
            "name": "scheduler"
        },
        "business": {
            "connection": "default",
            "name": "test"
        }
    }
}
```

- `connections`：连接参数（驱动、主机、端口、账号等），可定义多组连接
- `databases`：数据库逻辑名到物理库名的映射，每个库引用一个 `connection`

代码中通过逻辑名获取连接串：

```python
from Config import get_database_url

url = get_database_url("scheduler")   # 调度器存储库
url = get_database_url("business")    # 业务库（动态建表的默认目标）
```

支持的 `driver`（对应 SQLAlchemy 连接串前缀）：

| 数据库        | driver 示例                            |
| ---------- | ------------------------------------ |
| MySQL      | `mysql+pymysql`                      |
| SQLite     | `sqlite`（无需 user/password/port）      |
| PostgreSQL | `postgresql` / `postgresql+psycopg2` |

#### MySQL 版本说明（默认数据库）

| 项目 | 要求 |
| --- | --- |
| 推荐版本 | **MySQL 8.0+**（调度器存储、动态建表、结构回滚全功能可用） |
| 最低版本 | MySQL 5.7（`json` 列类型需 5.7.8+；utf8mb4 字符集需 5.5.3+） |
| MariaDB | 10.x 系列兼容（动态建表与结构回滚均已按 `mariadb` 方言适配） |
| 驱动 | PyMySQL 1.2.0（`mysql+pymysql`），支持 MySQL 5.7+ / 8.0+ 与 MariaDB |

注意事项：

- **认证插件**：MySQL 8.0 默认使用 `caching_sha2_password`，PyMySQL 需额外安装 `cryptography` 才能连接（`pip install cryptography`，未列入 [Requirements.txt](Requirements.txt)，按需安装）；也可将账号改回 `mysql_native_password` 规避
- **字符集**：自动建库默认使用 `utf8mb4`（完整支持中文与 emoji），可在连接串 `charset` 参数覆盖，非法值会自动回退 `utf8mb4`
- **JSON 列类型**：建表配置 `"type": "json"` 使用原生 `JSON` 类型，需 MySQL 5.7.8+
- **删除列/索引**：`TableGenerator.py --drop`（`drop_missing=true`）的 `DROP COLUMN` 依赖 MySQL 8.0+，5.7 及以下版本会执行失败
- **并发安全**：Worker 与 API 双进程共用存储时，SQLModel 后端在 MySQL/PostgreSQL 上通过 `SELECT ... FOR UPDATE` 行锁同步任务状态，SQLite 无行锁、不建议生产使用

### 3. 注册任务

在 [SchedulerTask/RegisterTask.py](SchedulerTask/RegisterTask.py) 中注册定时任务，支持两种方式（当前该文件内置了 interval / daily / hourly / weekly / cron 五种调度类型的装饰器示例，以及 2 个脚本文件任务示例，可直接运行体验）：

```python
from Scheduler import Scheduler
from SchedulerTask import register_tasks

def register_tasks(scheduler: Scheduler) -> None:
    # 方式一：装饰器注册函数任务（源码会被序列化入库，重启自动恢复）
    @scheduler.every(5).seconds
    def background_task():
        print("每 5 秒执行")

    # 方式二：注册脚本文件任务（以子进程方式执行指定 .py 文件）
    scheduler.register_script_file(
        script_path="/path/to/your/script.py",
        schedule_type="interval",   # interval / daily / cron / once
        interval=20,
        func_name="my_script_task",
    )
```

### 4. 启动服务

```bash
# 一键启动 Worker + API 两个进程（自动检测 Python：scheduler_env → .venv → python3）
./start.sh start

# 指定 Python 解释器
./start.sh --python python3 start

# 查看运行状态
./start.sh status
```

启动后访问 Dashboard：<http://localhost:8000/scheduler/>

## 启停管理

[start.sh](start.sh) 提供完整的服务管理命令：

```
用法: ./start.sh [--python <解释器路径>] <命令>

参数:
  --python   指定 Python 解释器路径（如 python3、/usr/bin/python3、.venv/bin/python）
             未指定时按优先级自动检测：scheduler_env/bin/python → .venv/bin/python → PATH 中的 python3
```

| 命令                   | 说明                             |
| -------------------- | ------------------------------ |
| `./start.sh start`   | 启动 Worker + API 两个进程（默认命令，可省略） |
| `./start.sh worker`  | 仅启动 Worker                     |
| `./start.sh api`     | 仅启动 API                        |
| `./start.sh stop`    | 停止所有进程（优雅关闭）                   |
| `./start.sh restart` | 重启所有进程                         |
| `./start.sh status`  | 查看运行状态                         |

示例：

```bash
# 自动检测 Python 启动
./start.sh start

# 指定 Python 解释器
./start.sh --python python3 start
./start.sh --python .venv/bin/python start
./start.sh --python /usr/bin/python3 start

# 其他命令（status/stop/restart 同样支持 --python）
./start.sh status
```

停止时发送 `SIGTERM` 信号，Worker 会等待正在执行的任务完成后再退出（最长等待 `worker.shutdown_timeout` 秒，默认 60 秒），超时后强制终止。

也可单独运行：

```bash
# 直接运行 Worker
python SchedulerRun.py

# 直接运行 API
python FastApiRun.py
```

## 配置说明

配置文件位于 [Config/](Config/) 目录，使用 JSON 格式，支持环境变量和 `~` 路径展开。

> **数据库连接串统一管理**：所有 `database_url` 均由 [Config/database.json](Config/database.json) 集中定义，通过 `Config.get_database_url(db_name)` 生成。`scheduler.json` / `fastapi.json` 及建表 JSON 配置中不再硬编码连接串，仅保留 `storage` 等非连接参数。

### database.json（数据库连接配置）

集中管理所有数据库的连接参数和逻辑名映射，详见 [配置数据库](#2-配置数据库) 小节。

### scheduler.json（Worker 进程）

```json
{
    "database": {
        "storage": "sqlmodel"            // 存储后端，连接串由 Config/database.json 统一提供
    },
    "scheduler": {
        "max_history": 10000,            // 历史记录最大保留条数
        "max_workers": 20,               // 任务并发线程数
        "history_retention_days": 8,     // 历史记录保留天数
        "max_dead_letters": 500,         // 死信队列最大长度
        "quiet": false                   // 是否抑制 info 日志
    },
    "worker": {
        "shutdown_timeout": 60           // 优雅关闭超时秒数
    },
    "logging": { ... }                   // 日志配置（见下文）
}
```

### fastapi.json（API 进程）

```json
{
    "database": { ... },                 // 与 scheduler.json 保持一致
    "scheduler": { ... },                // 与 scheduler.json 保持一致
    "api": {
        "host": "0.0.0.0",
        "port": 8000,
        "reload": true                   // 开发模式热重载
    },
    "logging": { ... }
}
```

### 日志配置

日志按天轮转（`midnight`），默认保留 30 天，全部收集到项目根目录 `Log/` 下（运行时自动创建），并按组件分子目录：

```
2026-07-03 14:30:00 [INFO] worker - Scheduler started
```

| 进程/组件 | 日志目录 | 日志文件 |
| ------ | ------------- | ------------ |
| Worker | `Log/worker/` | `worker.log` |
| API | `Log/api/` | `api.log` |
| 建表/迁移 | `Log/table_generator/` | `table_generator.log` |

可通过 `logging.level` 调整日志级别（`INFO` / `DEBUG` / `WARNING`），通过 `logging.console` 控制是否输出到控制台。

## 任务注册

任务注册与 Worker 进程解耦，所有任务在 [SchedulerTask/RegisterTask.py](SchedulerTask/RegisterTask.py) 中统一注册。新增任务只需在该文件添加，无需修改 `SchedulerRun.py`。当前内置的 7 个示例任务：

### 函数任务（装饰器方式）

| 任务名 | 调度方式 | 节奏 |
| --- | --- | --- |
| `heartbeat_task` | `every(5).seconds` | 每 5 秒 |
| `daily_report_task` | `daily.at("01:00")` | 每天 01:00 |
| `hourly_summary_task` | `hourly.at(":30")` | 每小时第 30 分钟 |
| `weekly_review_task` | `weekly.monday.at("09:00")` | 每周一 09:00 |
| `workday_close_task` | `cron("0 18 * * 1-5")` | 工作日 18:00 |

### 脚本文件任务（[SchedulerTask/tasks/](SchedulerTask/tasks/)）

| 任务名 | 脚本 | 调度 | 说明 |
| --- | --- | --- | --- |
| `hello_script_task` | `hello_script_task.py` | 每 10 分钟 | 最小结构，读取调度上下文环境变量 |
| `dir_size_report_task` | `dir_size_report_task.py` | 每天 08:30 | 演示 `script_args` 传参与退出码约定 |

```python
@scheduler.every(10).seconds                    # 每 10 秒
@scheduler.every(5).minutes                     # 每 5 分钟
@scheduler.daily.at("09:00")                    # 每天早上 9 点
@scheduler.weekly.monday.at("10:00")            # 每周一 10 点
@scheduler.cron("*/15 * * * *")                 # 每 15 分钟（cron 表达式）
@scheduler.once(60).seconds                     # 60 秒后执行一次
```

#### 基于区间

```python
@scheduler.every(10).seconds
@scheduler.every(5).minutes
@scheduler.every(2).hours
@scheduler.every(1).days
```

#### 基于时间

```python
@scheduler.daily.at("09:00")              # Daily at 9 AM
@scheduler.hourly.at(":30")               # Every hour at :30
@scheduler.weekly.monday.at("10:00")      # Every Monday at 10 AM
@scheduler.weekly.weekdays.at("09:00")    # Weekdays at 9 AM
@scheduler.weekly.weekends.at("12:00")    # Weekends at noon
```

#### Cron 表达式

支持标准 cron 表达式，可在任意时区调度任务。

```python
@scheduler.cron("0 9 * * MON-FRI")        # 9 AM on weekdays
def market_open():
    ...

@scheduler.cron("*/15 * * * *")           # Every 15 minutes
def frequent_check():
    ...

@scheduler.cron("0 0 1 * *")              # First day of each month
def monthly_report():
    ...
```

#### 一次性任务

```python
@scheduler.once(60)                        # Run once after 60 seconds
def delayed_task():
    ...

@scheduler.at("2024-12-25 00:00:00")      # Run at specific datetime
def christmas_task():
    ...
```

#### 时区支持

支持在任意时区调度任务，默认使用 `Asia/Shanghai` 时区。

```python
# Using the tz parameter
@scheduler.daily.at("09:00", tz="Asia/Shanghai")
def shanghai_morning():
    print("Good morning, Beijing!")

# Using the .tz() method (chainable)
@scheduler.weekly.monday.tz("Europe/London").at("09:00")
def london_standup():
    print("Monday standup")

# With cron expressions
@scheduler.cron("0 9 * * MON-FRI").tz("Asia/Shanghai")
def tokyo_market():
    print("Beijing market open")
```

常用时区: `UTC`, `America/New_York`, `America/Los_Angeles`, `Europe/London`, `Europe/Paris`, `Asia/Tokyo`, `Asia/Shanghai`, `Australia/Sydney`

链式修饰符：

| 修饰符                 | 说明            |
| ------------------- | ------------- |
| `.timeout(seconds)` | 单次执行最大耗时，超时终止 |
| `.retries(n)`       | 失败重试次数（指数退避）  |
| `.no_catch_up()`    | 跳过停机期间错过的执行   |
| `.tz("时区")`         | 指定时区          |

> 函数任务的源码会被提取并序列化到数据库，Worker 重启后自动恢复执行，无需重新注册。

### 脚本文件任务

适合复杂业务逻辑，以子进程方式执行独立的 `.py` 文件：

```python
scheduler.register_script_file(
    script_path="/abs/path/to/task.py",
    schedule_type="daily",          # interval / daily / cron / once
    interval=20,                    # interval/once 类型必填（秒）
    schedule_time="02:00",          # daily 类型必填（HH:MM）
    cron_expression="0 2 * * *",    # cron 类型必填
    script_args=["--env", "prod"],  # 传给脚本的命令行参数（可选）
    func_name="cleanup_task",
    max_retries=3,
    timeout=3600,
    timezone="Asia/Shanghai",
)
```

脚本文件路径不限，项目内的示例脚本集中放在 [SchedulerTask/tasks/](SchedulerTask/tasks/) 目录便于管理。脚本以 `python <script> <args...>` 子进程方式执行，退出码非 0 视为失败并触发重试；调度器会向脚本注入 `SCHEDULER_JOB_ID` / `SCHEDULER_JOB_RETRY_COUNT` / `SCHEDULER_JOB_MAX_RETRIES` 环境变量供感知调度上下文。

## 动态建表

通过 JSON 配置文件定义表结构，自动在数据库中创建/迁移业务数据表，无需手写 SQL DDL。底层基于 SQLAlchemy Core，与调度器自身的 SQLModel 存储后端共用同一个 engine。

### 核心特性

- **声明式建表**：在 JSON 配置文件中定义表结构（完整示例见 [SchedulerTable/tables\_config\_test.json](SchedulerTable/tables_config_test.json)），通过 `--config` 指定文件或目录后手动执行创建
- **自动建库**：数据库不存在时先自动 `CREATE DATABASE` 再建表（`create_database=true`，默认启用；SQLite 库文件由驱动自动创建）
- **手动触发**：通过 CLI 或代码显式调用，便于在部署/升级阶段单独运行
- **自动迁移**：表已存在时，自动添加缺失的列和索引；已有列类型不修改（避免数据丢失）
- **可选清理**：`drop_missing=true` 时同步删除配置中已不存在的列和索引
- **变更留痕**：每次执行自动写入变更记录表 `table_schema_change_log`（含 `batch_id`），可据此回滚结构（见下文「变更记录表」「结构回滚」）
- **独立数据库**：配置目录内多份配置可分别通过顶层 `database` 字段指向不同逻辑库（默认 `business`），未在 [Config/database.json](Config/database.json) 中配置的逻辑库会被自动跳过
- **丰富类型**：支持 `int`/`str`/`text`/`float`/`double`/`numeric`/`bool`/`json`/`datetime`/`bytes` 等

### 使用方式

支持 **CLI 命令行**和 **代码调用**两种方式，均为手动触发，Worker 启动时不会自动建表。

#### 方式一：CLI 命令行（推荐，部署/升级时单独运行）

```bash
# 用示例配置建表（默认落到 Config/database.json 的 business 库）
python SchedulerTable/TableGenerator.py --config SchedulerTable/tables_config_test.json

# 指定配置目录：逐份执行目录内全部 JSON 配置，便于按目标库拆分管理
python SchedulerTable/TableGenerator.py --config path/to/config_dir

# 命令行指定数据库（覆盖统一配置）
python SchedulerTable/TableGenerator.py --config SchedulerTable/tables_config_test.json --db sqlite:///./biz.db

# 启用删除配置中已不存在的列和索引
python SchedulerTable/TableGenerator.py --config SchedulerTable/tables_config_test.json --drop

# 数据库不存在时不自动建库
python SchedulerTable/TableGenerator.py --config SchedulerTable/tables_config_test.json --no-create-db

# 打印 SQL 日志（调试用）
python SchedulerTable/TableGenerator.py --config SchedulerTable/tables_config_test.json --echo
```

CLI 参数说明：

| 参数         | 说明                                                               |
| ---------- | ---------------------------------------------------------------- |
| `--config` | 配置入口：配置文件路径或配置目录（未显式指定时使用 TableGenerator.py 同目录的 `tables_config/` 目录） |
| `--db`     | 数据库连接字符串（覆盖 [Config/database.json](Config/database.json) 中的统一配置） |
| `--drop`   | 删除配置中已不存在的列和索引（谨慎使用）                                             |
| `--create-db` / `--no-create-db` | 数据库不存在时是否自动创建（默认启用）                    |
| `--echo`   | 打印 SQLAlchemy SQL 日志                                             |

#### 方式二：代码调用

```python
from SchedulerTable.TableGenerator import generate_tables

# 使用示例配置建表
generate_tables(config_path="SchedulerTable/tables_config_test.json")

# 指定配置目录（逐份执行目录内全部 JSON 配置）
generate_tables(config_path="path/to/config_dir")

# 使用调度器自身的存储后端（业务表与调度器表同库）
generate_tables(scheduler)

# 指定独立数据库（覆盖统一配置）
generate_tables(database_url="mysql+pymysql://user:pwd@host/biz_db")

# 删除配置中已不存在的列和索引（谨慎使用）
generate_tables(drop_missing=True)
```

> 注：`generate_tables` 不会在 `SchedulerRun.py` 启动时被调用。如需在 Worker 启动时自动建表，需自行在 `SchedulerRun.py` 中插入调用。

### 配置文件示例

摘自 [SchedulerTable/tables\_config\_test.json](SchedulerTable/tables_config_test.json)：

```json
{
    "drop_missing": false,
    "tables": [
        {
            "name": "example_user_logs",
            "comment": "示例表：演示通过 JSON 配置创建数据表",
            "columns": [
                {
                    "name": "id",
                    "type": "int",
                    "primary_key": true,
                    "autoincrement": true,
                    "nullable": false
                },
                {
                    "name": "user_name",
                    "type": "str",
                    "length": 64,
                    "nullable": false,
                    "comment": "用户名"
                },
                {
                    "name": "score",
                    "type": "float",
                    "nullable": true
                },
                {
                    "name": "is_active",
                    "type": "bool",
                    "default": true
                },
                {
                    "name": "extra",
                    "type": "json",
                    "nullable": true
                },
                {
                    "name": "created_at",
                    "type": "datetime",
                    "default": "now"
                }
            ],
            "indexes": [
                {
                    "name": "idx_user_logs_user_name",
                    "columns": ["user_name"],
                    "unique": false
                },
                {
                    "name": "idx_user_logs_created_at",
                    "columns": ["created_at"],
                    "unique": false
                }
            ]
        }
    ]
}
```

### 数据库来源优先级

`generate_tables()` 对每份配置分别按以下优先级解析数据库 engine（从高到低）：

1. 显式传入的 `engine` 参数（SQLAlchemy Engine 对象）
2. 显式传入的 `database_url` 参数（连接字符串）
3. JSON 配置文件顶层的 `database_url` 字段（向后兼容）
4. [Config/database.json](Config/database.json) 中由该份配置顶层 `database` 逻辑名生成的 URL（默认 `business`）（推荐）
5. `scheduler._storage.engine`（调度器自身的存储后端）

> 第 4 项按各份配置的 `database` 逻辑名分别解析，因此目录内多份配置可以落到不同数据库。若该逻辑名未在 `Config/database.json` 中定义且未提供 1–3 任一来源，则该份配置被跳过（只记日志，不报错），其余配置照常执行。

### 迁移策略

| 场景            | 行为                                             |
| ------------- | ---------------------------------------------- |
| 数据库不存在        | `create_database=true`（默认）时先 `CREATE DATABASE` 再建表 |
| 表不存在          | `CREATE TABLE`（含所有列、约束、索引）                     |
| 表已存在 + 配置新增列  | `ALTER TABLE ADD COLUMN`（NOT NULL 列需有 DEFAULT） |
| 表已存在 + 配置新增索引 | `CREATE INDEX`                                 |
| 表已存在 + 配置删除列  | `drop_missing=true` 时 `DROP COLUMN`，否则保留       |
| 表已存在 + 配置删除索引 | `drop_missing=true` 时 `DROP INDEX`，否则保留        |
| 已有列的类型修改      | 不修改（需手动迁移，避免数据丢失）                              |
| 主键列           | 永远不会被删除                                        |

完整的字段类型、列属性、索引配置说明详见 [SchedulerTable/tables\_config.md](SchedulerTable/tables_config.md)。

### 变更记录表

每次执行 `generate_tables()`（CLI 或代码调用）都会在**目标数据库**中自动维护一张变更记录表 `table_schema_change_log`，记录本次执行产生的全部 DDL 变更，供后续审计与结构回滚使用。

同一次执行中的所有变更共享一个 `batch_id`（UUID4），主要字段如下：

| 字段              | 说明                                                                                     |
| --------------- | -------------------------------------------------------------------------------------- |
| `batch_id`      | 本次执行的批次标识（UUID4），同一次执行的所有变更共享                                                           |
| `executed_at`   | 变更执行时间                                                                                 |
| `database_name` | 目标数据库名                                                                                 |
| `table_name`    | 目标表名（建库事件为空）                                                                           |
| `operation`     | `create_database` / `create_table` / `add_column` / `drop_column` / `add_index` / `drop_index` / `error` |
| `object_type`   | 对象类型：`database` / `table` / `column` / `index`                                          |
| `object_name`   | 对象名（库名/表名/列名/索引名）                                                                      |
| `ddl_sql`       | 对应的 DDL 语句（由配置重建，供回滚参考）                                                                 |
| `success`       | 是否执行成功                                                                                 |
| `note`          | 备注（失败原因、随表创建等）                                                                         |
| `operator`      | 执行者（操作系统用户名）                                                                           |

### 结构回滚

[SchedulerTable/RollbackSchema.py](SchedulerTable/RollbackSchema.py) 读取上述变更记录表，按 `batch_id` 把数据库结构恢复到「该批次执行之前」的状态。

- **回滚语义**：`--batch-id X` 表示回滚批次 X **及其之后**的所有批次，即恢复到 X 执行之前的结构。
- **支持数据库**：SQLite / MySQL（MariaDB）/ PostgreSQL。脚本按 `--db` 连接串的方言自动适配反向 DDL（如 MySQL 用 `DROP INDEX idx ON tbl`，SQLite/PostgreSQL 用 `DROP INDEX idx`）。
- **回滚范围折叠**：回滚范围内若某表将被 `DROP TABLE`，则该表的逐列 / 逐索引变更会被跳过（避免对已删表重复操作）。

反向 DDL 映射（按记录 id 倒序执行）：

| 正向操作                       | 反向操作                                        |
| -------------------------- | ------------------------------------------- |
| `create_table`             | `DROP TABLE`                                |
| `add_column`               | `ALTER TABLE ... DROP COLUMN`               |
| `add_index`                | `DROP INDEX`                                |
| `drop_column`              | `ALTER TABLE ... ADD COLUMN`（列定义从历史记录还原）     |
| `drop_index`               | `CREATE INDEX`（索引定义从历史记录还原）                 |
| `create_database` / `error` | 不自动处理，仅给出提示                                 |

#### 使用方式

```bash
# 1) 列出所有批次，拿到要回滚到的 batch_id（不做任何修改）
python SchedulerTable/RollbackSchema.py --db <url> --list

# 2) 预演：只打印将要执行的反向 SQL，不落库（默认行为，--dry-run 可显式声明）
python SchedulerTable/RollbackSchema.py --db <url> --batch-id <uuid> --dry-run

# 3) 实际执行回滚
python SchedulerTable/RollbackSchema.py --db <url> --batch-id <uuid> --execute
```

`--db` 支持各数据库的连接串，例如：

```bash
python SchedulerTable/RollbackSchema.py --db sqlite:////tmp/tg_mock_demo/demo.db --list
python SchedulerTable/RollbackSchema.py --db mysql+pymysql://user:pwd@host/biz_db --list
python SchedulerTable/RollbackSchema.py --db postgresql+psycopg2://user:pwd@host/biz_db --list
```

CLI 参数说明：

| 参数           | 说明                                                                                      |
| ------------ | --------------------------------------------------------------------------------------- |
| `--db`       | 数据库连接字符串（支持 SQLite / MySQL / PostgreSQL）                                                 |
| `--batch-id` | 回滚到此批次执行之前（含该批次及其之后的所有批次）                                                               |
| `--list`     | 仅列出所有批次，不做回滚                                                                            |
| `--dry-run`  | 仅预演并打印将要执行的 SQL，不落库（**默认行为**）                                                           |
| `--execute`  | 实际执行回滚                                                                                  |
| `--echo`     | 打印 SQL 日志                                                                                |

> `--dry-run` 与 `--execute` 互斥，不能同时指定。
>
> **不可逆提醒**：回滚只能恢复「结构」，无法恢复数据。`drop_column` 删除的列数据、`drop_table` 删除的表数据在回滚后**无法找回**（`drop_index` 仅重建索引，不丢数据）；`create_database` 不会被自动 `DROP DATABASE`，需人工确认。

## 数据库公共方法

[SchedulerTask/Utils/db\_utils.py](SchedulerTask/Utils/db_utils.py) 统一封装 SQLAlchemy，提供连接数据库、查询、写入以及跨库操作的通用能力，支持 MySQL / PostgreSQL / SQLite。业务任务和脚本任务均可复用，避免重复编写连接与事务代码。

### 核心能力

| 能力      | 说明                                                                                                     |
| ------- | ------------------------------------------------------------------------------------------------------ |
| 单库 CRUD | `query` / `query_one` / `query_scalar` / `execute` / `execute_many`                                    |
| 批量写入    | `insert_batch(table, rows, batch_size)` 自动构造 INSERT 语句并分批提交                                            |
| 显式事务    | `transaction()` 上下文，多条语句原子提交，失败回滚                                                                      |
| 多库协作    | `multi_connect(*specs)` 一次性连接多个库，统一释放连接池                                                               |
| 配置驱动    | `connect()` 不传 url 时通过 `Config.get_database_url()` 从 [Config/database.json](Config/database.json) 统一生成 |

### 单库操作

```python
from SchedulerTask.Utils.db_utils import connect

# 显式指定连接串
with connect("mysql+pymysql://user:pass@localhost/db") as db:
    rows = db.query("SELECT * FROM users WHERE age > :age", {"age": 18})
    cnt  = db.query_scalar("SELECT COUNT(*) FROM users")
    db.execute("UPDATE users SET active = 1 WHERE id = :id", {"id": 10})

# 不传 url，从 Config/database.json 统一生成（默认 scheduler 库）
with connect() as db:
    ...

# 指定其他逻辑数据库名
with connect(config_name="business") as db:
    ...
```

### 跨库操作

`multi_connect` 只负责一次性建立多个连接，具体的读取、处理、写入逻辑由调用方自行实现：

```python
from SchedulerTask.Utils.db_utils import multi_connect

source_url = "mysql+pymysql://user:pass@host-a/src_db"
dest_url   = "mysql+pymysql://user:pass@host-b/dst_db"

with multi_connect(source_url, dest_url) as (src, dst):
    rows = src.query("SELECT * FROM users WHERE active = 1")
    archived = [{**row, "archived_at": "2026-07-06"} for row in rows]
    dst.insert_batch("archived_users", archived, batch_size=2000)

# 按名字访问（适合多个库，避免顺序混淆）
with multi_connect(
    ("src", source_url),
    ("dst", dest_url),
) as dbs:
    rows = dbs["src"].query("SELECT * FROM users")
    dbs["dst"].insert_batch("archived_users", rows)

# 混合配置：连接串 + 数据库逻辑名（从 Config/database.json 生成）
with multi_connect(
    {"name": "scheduler", "config": "scheduler"},
    {"name": "business",  "config": "business"},
) as dbs:
    jobs = dbs["scheduler"].query("SELECT * FROM scheduler_jobs")
    dbs["business"].insert_batch("job_snapshot", jobs)
```

### 在脚本任务中使用

脚本任务以子进程方式运行，需在文件开头将项目根目录加入 `sys.path`（详见 [SchedulerTask/README.md](SchedulerTask/README.md)）：

```python
import sys
from pathlib import Path

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
```

完整的 API 说明、字段类型、使用示例详见 [SchedulerTask/README.md](SchedulerTask/README.md)。

## Dashboard 与 API

### Web Dashboard

访问地址：<http://localhost:8000/scheduler/>

页面基于 SSE（Server-Sent Events）每秒推送状态实时刷新，连接状态显示在导航栏右侧；支持中英双语切换，语言偏好持久化保存。以下截图来自示例任务的实际运行画面。

![Dashboard 总览](docs/dashboard.png)

#### 页面结构

导航栏包含三个主视图切换标签，右侧依次为调度器运行状态、SSE 连接状态与语言切换（中 / English）：

| 标签 | 内容 | 说明 |
| --- | --- | --- |
| **Dashboard** | 统计卡片 + 任务列表 | 默认视图，总览与日常操作 |
| **History** | 执行历史 | 全量执行记录查询 |
| **Failed** | 死信队列 | 重试耗尽的失败任务，标签上带数量角标 |

#### 任务总览（Dashboard 视图）

顶部为 4 个统计卡片：**Active Jobs**（活跃任务数）、**Total Runs**（累计执行）、**Failures**（失败数及成功率）、**Uptime**（运行时长）。

任务列表每行展示：任务名、调度描述、状态、**最近 5 次执行**（状态圆点，悬停显示时间与结果）、下次执行时间（含倒计时，暂停任务显示 `—`）以及操作按钮。

工具栏支持：状态筛选（All / Scheduled / Paused）、任务名搜索、Reset 重置条件、Query 查询，以及 **Add Task** 新增任务；表格底部支持分页与每页条数调整（默认 20 条）。

任务操作按钮：

| 按钮 | 说明 |
| --- | --- |
| 立即执行（Run Now） | 立即触发一次执行（仅对已暂停任务生效） |
| 暂停 / 恢复（Pause / Resume） | 切换任务暂停状态，无需删除任务 |
| 取消（Cancel） | 确认后从调度器移除任务 |
| 查看详情（View Details） | 打开任务详情弹窗 |
| 编辑任务（Edit Task） | 仅已暂停任务显示，可修改调度参数后重新保存 |

#### 任务详情弹窗

点击「查看详情」查看单个任务的完整元信息：

![任务详情弹窗](docs/dashboard_job_detail.png)

内容包括：Job ID、函数名、状态、暂停标记、调度描述、下次执行时间与倒计时、上次执行时间、执行/重试计数、最大重试、超时时间；脚本任务额外显示脚本路径、命令行参数或内联代码。

#### 新增任务（Add Task）

点击工具栏 **Add Task** 打开新增任务表单（编辑已暂停任务时复用同一表单）：

![新增任务弹窗（Cron 可视化配置）](docs/dashboard_add_task.png)

| 配置项 | 说明 |
| --- | --- |
| 任务来源 | **Script File**（执行指定路径的 .py 脚本）或 **Inline Code**（在线编写 Python 代码，写入临时文件执行） |
| 任务名 | 可留空，默认取脚本文件名或 `inline_code` |
| 调度类型 | interval（间隔秒数）/ daily（每天 HH:MM）/ cron（表达式）/ once（一次性延迟） |
| Cron 可视化配置 | 分钟/小时/日/月/周五个维度按「每 / 每 N / 指定 / 范围」组合自动生成表达式，提供 7 个常用预设按钮并实时预览结果 |
| 脚本参数 | 空格分隔的命令行参数，如 `--debug --port 8080` |
| 重试 / 超时 / 时区 | 最大重试次数（默认 3）、执行超时秒数、16 个常用时区下拉选择 |
| Catch up | 是否补偿调度器停机期间错过的执行（默认勾选） |

> 同名任务规则：已暂停的同名任务允许更新（进入编辑模式），未暂停的同名任务会被拒绝，避免意外覆盖。

#### 执行历史（History 视图）

![执行历史](docs/dashboard_history.png)

支持按状态（All / Completed / Failed / Missed）、任务名下拉与关键字组合筛选，Reset / Query 重查，底部支持分页。每行「查看详情」打开执行详情弹窗，展示任务名、状态、时间、耗时、执行序号、重试次数，失败记录附带完整错误信息。

#### 失败任务（Failed 视图 / 死信队列）

![失败任务（死信队列）](docs/dashboard_failed.png)

重试次数耗尽仍然失败的任务会进入死信队列，导航栏 **Failed** 标签实时显示数量角标。支持关键字搜索定位问题任务；每行「查看详情」可查看完整错误信息用于排查；右上角 **Clear All** 在确认后清空整个死信队列。

### REST API

| 端点                                    | 方法     | 说明           |
| ------------------------------------- | ------ | ------------ |
| `/scheduler/`                         | GET    | Dashboard 页面 |
| `/scheduler/api/status`               | GET    | 调度器状态        |
| `/scheduler/api/jobs`                 | GET    | 所有任务列表       |
| `/scheduler/api/jobs/{job_id}`        | GET    | 指定任务详情       |
| `/scheduler/api/jobs/{job_id}`        | PUT    | 更新已暂停任务      |
| `/scheduler/api/jobs/{job_id}/pause`  | POST   | 暂停任务         |
| `/scheduler/api/jobs/{job_id}/resume` | POST   | 恢复任务         |
| `/scheduler/api/jobs/{job_id}/run`    | POST   | 立即执行任务(仅限已暂停任务) |
| `/scheduler/api/jobs/{job_id}/cancel` | POST   | 取消任务         |
| `/scheduler/api/scripts`              | POST   | 添加脚本任务(同名未暂停任务拒绝更新) |
| `/scheduler/api/history`              | GET    | 执行历史         |
| `/scheduler/api/dead-letters`         | GET    | 死信队列         |
| `/scheduler/api/dead-letters`         | DELETE | 清空死信队列       |
| `/scheduler/events`                   | GET    | SSE 事件流      |

## 状态持久化

采用 SQLModel 数据库存储（默认 MySQL），自动创建以下表：

| 表名                       | 用途           |
| ------------------------ | ------------ |
| `scheduler_jobs`         | 活跃任务定义       |
| `scheduler_history`      | 执行历史记录       |
| `scheduler_dead_letters` | 失败任务记录（死信队列） |
| `scheduler_metadata`     | 任务计数器、统计数据   |

Worker 重启后自动：

1. 从数据库恢复所有任务
2. 计算停机期间错过的执行
3. 执行补偿任务（除非任务设置了 `.no_catch_up()`）

## 部署建议

### 生产环境

1. **关闭热重载**：将 [Config/fastapi.json](Config/fastapi.json) 中 `api.reload` 设为 `false`
2. **调整并发**：根据机器配置调整 `scheduler.max_workers`
3. **日志级别**：生产环境建议 `INFO` 或 `WARNING`
4. **进程守护**：建议配合 `supervisor` 或 `systemd` 管理 Worker 进程，确保异常退出后自动拉起
5. **数据库**：生产环境推荐 MySQL 8.0+ 或 PostgreSQL，避免使用 SQLite（版本要求见 [MySQL 版本说明](#mysql-版本说明默认数据库)）

### 信号处理

Worker 进程支持优雅关闭：

- `SIGTERM` / `SIGINT`：等待正在执行的任务完成后退出
- 适合 `supervisor` / `systemd` 的停止流程

## Docker 部署

容器化部署文件全部集中在 [docker/](docker/) 目录（Dockerfile、docker-compose.yml、数据库配置、initdb 脚本），完整部署指南单独见 [docker/README.md](docker/README.md)。

```bash
cd docker
docker compose up -d --build      # 一键拉起 MySQL + Worker + API
# Dashboard: http://localhost:8000/scheduler/
```

- **服务拓扑**：mysql（MySQL 8.0，utf8mb4 + `mysql_native_password` 认证，数据持久化于 `mysql-data` 命名卷）→ worker / api（等待健康检查通过后启动，API 映射 `${API_PORT}` 默认 8000）
- **连接参数注入**：[docker/database.json](docker/database.json) 以 `${DB_HOST}` 等占位符复用 [Config/loader.py](Config/loader.py) 既有的环境变量展开机制，账号 / 密码 / 端口通过 `.env` 或环境变量覆盖（`MYSQL_ROOT_PASSWORD` / `MYSQL_USER` / `MYSQL_PASSWORD` / `API_PORT`）
- **首次初始化**：[docker/initdb/01-init.sql](docker/initdb/01-init.sql) 自动预建 `business` 业务库并授权（`scheduler` 库由 `MYSQL_DATABASE` 自动创建）
- **日志持久化**：worker / api 日志挂载到 `docker/log/`（按天轮转，保留 30 天），同时输出到容器 stdout 可用 `docker compose logs` 查看
- **构建排除清单**：[docker/Dockerfile.dockerignore](docker/Dockerfile.dockerignore) 采用 BuildKit 的按 Dockerfile 命名变体（与 Dockerfile 同目录即生效，优先级高于根目录 `.dockerignore`），Docker 相关文件已全部集中于 `docker/` 目录；仅 `DOCKER_BUILDKIT=0` 的老构建器会忽略该清单（构建仍成功，仅上下文变大变慢）

## 依赖

详见 [Requirements.txt](Requirements.txt)。下表列出核心依赖（传递依赖见文件注释）：

| 依赖         | 版本      | 用途                                      |
| ---------- | ------- | --------------------------------------- |
| fastapi    | 0.138.2 | Web 框架（Dashboard 与 REST API）            |
| uvicorn    | 0.49.0  | ASGI 服务器                                |
| croniter   | 6.2.2   | cron 表达式解析                              |
| sqlmodel   | 0.0.39  | SQLModel 存储后端                           |
| SQLAlchemy | 2.0.51  | SQLModel 底层依赖，同时被 db\_utils 直接使用        |
| PyMySQL    | 1.2.0   | MySQL 驱动（`mysql+pymysql://`）            |
| psycopg2   | 2.9.12  | PostgreSQL 驱动（`postgresql+psycopg2://`） |
| pytest     | 9.1.1   | 测试框架（仅开发环境）                             |
