"""TableGenerator 自动建库（CREATE DATABASE）行为测试用例（真实 MySQL）。

覆盖建表前的"确保数据库存在"逻辑：
    1. 目标库不存在 → 先 CREATE DATABASE，再建表
    2. 目标库已存在 → 只做存在性检查，不重复建库（幂等）
    3. create_database=False（函数参数 / JSON 字段）→ 不自动建库
    4. 建库失败 → 抛出 RuntimeError，并提示可手动执行的 SQL
    5. 无法确认库是否存在（服务器不可达）→ 告警放行，不阻断建库流程
    6. SQLite → 库文件由驱动自动创建，不做任何服务器连接
    7. CREATE DATABASE 语句与服务器连接串的生成规则（含字符集、标识符转义）

测试数据库: 本地 MySQL，连接参数（账号/密码/主机/端口/字符集）取自
Config/database.json 的 default 连接，仅替换库名以构造测试库。
每个用例使用唯一的临时库名，结束后统一 DROP DATABASE，互不影响、可重复执行。
"""
import json
import logging
import sys
import uuid
from pathlib import Path

import pytest
import sqlalchemy
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import OperationalError

# 将项目根目录加入 sys.path，便于导入 SchedulerTable
# 动态查找：从本文件向上逐级查找，直到找到包含 SchedulerTable 子目录的目录
_PROJECT_ROOT = Path(__file__).resolve().parent
while _PROJECT_ROOT != _PROJECT_ROOT.parent:
    if (_PROJECT_ROOT / "SchedulerTable").is_dir():
        break
    _PROJECT_ROOT = _PROJECT_ROOT.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from Config import get_database_url  # noqa: E402
from SchedulerTable.TableGenerator import (  # noqa: E402
    _build_create_database_sql,
    _server_url,
    generate_tables,
)

# 真实 MySQL 连接参数（含账号密码）统一由 Config/database.json 生成
_BASE_URL = make_url(get_database_url("business"))

# 无库名的服务器连接 URL：用于建库/删库/查库等服务器级管理操作
_SERVER_URL = _server_url(_BASE_URL, None)


def _target_url(db_name: str) -> str:
    """构造指向指定库的连接串（复用 Config 中的账号与连接参数）。"""
    return _BASE_URL.set(database=db_name).render_as_string(hide_password=False)


def _connect_server():
    """连接服务器（不指定库），用于执行服务器级 SQL。"""
    engine = create_engine(_SERVER_URL)
    return engine


def _database_exists(db_name: str) -> bool:
    """通过 INFORMATION_SCHEMA 检查目标库是否真实存在。"""
    engine = _connect_server()
    try:
        with engine.connect() as conn:
            row = conn.execute(
                text(
                    "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA "
                    "WHERE SCHEMA_NAME = :name"
                ),
                {"name": db_name},
            ).first()
            return row is not None
    finally:
        engine.dispose()


def _default_charset(db_name: str) -> str:
    """返回目标库的默认字符集。"""
    engine = _connect_server()
    try:
        with engine.connect() as conn:
            row = conn.execute(
                text(
                    "SELECT DEFAULT_CHARACTER_SET_NAME "
                    "FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = :name"
                ),
                {"name": db_name},
            ).first()
            return row[0] if row else None
    finally:
        engine.dispose()


def _drop_database(db_name: str):
    """强制删除测试库（AUTOCOMMIT 下执行，保证幂等清理）。"""
    engine = create_engine(_SERVER_URL).execution_options(
        isolation_level="AUTOCOMMIT"
    )
    try:
        with engine.connect() as conn:
            conn.execute(text(f"DROP DATABASE IF EXISTS `{db_name}`"))
    finally:
        engine.dispose()


@pytest.fixture
def test_db():
    """生成唯一的测试库名，并在用例结束后强制 DROP，保证幂等。"""
    db_name = f"tg_auto_{uuid.uuid4().hex[:10]}"
    yield db_name
    _drop_database(db_name)


def _write_config(tmp_path: Path, table_name: str, **extra) -> str:
    """写入一份最小可用的表结构配置，返回文件路径。"""
    config = {
        "tables": [
            {
                "name": table_name,
                "columns": [
                    {"name": "id", "type": "int", "primary_key": True,
                     "autoincrement": True},
                    {"name": "user_name", "type": "str", "length": 64,
                     "nullable": False},
                    {"name": "score", "type": "float"},
                    {"name": "is_active", "type": "bool", "default": True},
                    {"name": "created_at", "type": "datetime", "default": "now"},
                ],
                "indexes": [
                    {"name": f"idx_{table_name}_user", "columns": ["user_name"]},
                ],
            }
        ],
    }
    config.update(extra)
    path = tmp_path / "config.json"
    path.write_text(json.dumps(config, ensure_ascii=False, indent=2), encoding="utf-8")
    return str(path)


def _table_names(engine) -> set:
    return set(inspect(engine).get_table_names())


# ==================== 场景 1: 库不存在 → 先建库再建表 ====================

def test_create_database_then_create_tables(test_db, tmp_path, caplog):
    """目标库不存在时先 CREATE DATABASE 再建表；再次执行不重复建库。"""
    assert not _database_exists(test_db)                # 前置：库确实不存在
    table_name = f"tg_mock_{uuid.uuid4().hex[:8]}"
    config_path = _write_config(tmp_path, table_name)
    database_url = _target_url(test_db)
    # DEBUG 级别可同时覆盖 info 与 debug 两条分支日志
    caplog.set_level(logging.DEBUG, logger="table_generator")

    # ---- 第 1 次运行：库不存在，应自动建库 ----
    created = generate_tables(config_path=config_path, database_url=database_url)

    assert created == [table_name]
    assert _database_exists(test_db)                    # 数据库真实建成
    # 建库使用了连接串中的字符集
    assert _default_charset(test_db) == "utf8mb4"
    assert _build_create_database_sql(
        create_engine(database_url), test_db
    ) in caplog.text
    assert "正在创建" in caplog.text
    assert "已自动创建" in caplog.text

    # 表确实创建在新建的库中（DDL 由真实 MySQL 执行）
    engine = create_engine(database_url)
    try:
        assert table_name in _table_names(engine)
    finally:
        engine.dispose()

    # ---- 第 2 次运行：库已存在，幂等 ----
    caplog.clear()
    created_again = generate_tables(config_path=config_path, database_url=database_url)

    assert created_again == [table_name]
    assert "正在创建" not in caplog.text
    assert "已自动创建" not in caplog.text
    assert f"数据库 '{test_db}' 已存在，无需创建" in caplog.text


# ==================== 场景 2: 关闭自动建库 ====================

@pytest.mark.parametrize(
    "mode",
    [pytest.param("param", id="function-arg"), pytest.param("config", id="json-field")],
)
def test_create_database_can_be_disabled(test_db, tmp_path, mode):
    """create_database=False 时不自动建库，目标库缺失会在建表阶段报错。"""
    assert not _database_exists(test_db)
    table_name = f"tg_mock_{uuid.uuid4().hex[:8]}"

    config_extra = {"create_database": False} if mode == "config" else {}
    config_path = _write_config(tmp_path, table_name, **config_extra)
    call_kwargs = {"create_database": False} if mode == "param" else {}

    # 库不存在且不自动创建 → 连接目标库时失败
    with pytest.raises(OperationalError):
        generate_tables(
            config_path=config_path,
            database_url=_target_url(test_db),
            quiet=True,
            **call_kwargs,
        )

    # 关键断言：确实没有自动建库
    assert not _database_exists(test_db)


# ==================== 场景 3: 建库失败 → RuntimeError ====================

def test_create_database_failure_raises_runtime_error(tmp_path):
    """已确认库不存在但建库失败时，应抛出 RuntimeError 并提示可手动执行的 SQL。

    用超过 MySQL 标识符长度上限（64）的库名触发真实的建库失败。
    """
    long_name = "tg_" + "x" * 70
    table_name = f"tg_mock_{uuid.uuid4().hex[:8]}"
    config_path = _write_config(tmp_path, table_name)

    with pytest.raises(RuntimeError) as exc_info:
        generate_tables(
            config_path=config_path,
            database_url=_target_url(long_name),
            quiet=True,
        )

    message = str(exc_info.value)
    assert long_name in message
    assert "自动创建失败" in message
    assert "CREATE DATABASE" in message                  # 提示手动执行的 SQL


# ==================== 场景 4: 无法确认库是否存在 → 告警放行 ====================

def test_check_failure_is_tolerated(tmp_path, caplog):
    """服务器不可达导致无法确认库是否存在时，仅告警放行，不抛建库异常。

    换用未监听的端口模拟"无法与服务器建立连接"，验证检查阶段的容错分支。
    """
    unreachable_url = _BASE_URL.set(
        port=13999, database="tg_unreachable"
    ).render_as_string(hide_password=False)
    table_name = f"tg_mock_{uuid.uuid4().hex[:8]}"
    config_path = _write_config(tmp_path, table_name)
    caplog.set_level(logging.WARNING, logger="table_generator")

    # 建库阶段被容忍，异常在建表阶段以连接失败的形式抛出
    with pytest.raises(OperationalError):
        generate_tables(config_path=config_path, database_url=unreachable_url, quiet=False)

    assert "无法确认数据库" in caplog.text


# ==================== 场景 5: SQLite 不做建库操作 ====================

def test_sqlite_skips_create_database(tmp_path, monkeypatch):
    """SQLite 的库文件由驱动自动创建，不应产生任何服务器连接。"""
    engine = create_engine(f"sqlite:///{tmp_path / 'plain.db'}")
    table_name = f"tg_sqlite_{uuid.uuid4().hex[:8]}"
    config_path = _write_config(tmp_path, table_name)

    def fail_if_called(*args, **kwargs):
        raise AssertionError("SQLite 场景不应新建任何 engine（即不连服务器）")

    monkeypatch.setattr(sqlalchemy, "create_engine", fail_if_called)

    created = generate_tables(config_path=config_path, engine=engine, quiet=True)

    assert created == [table_name]
    assert table_name in _table_names(engine)


# ==================== 场景 6: CREATE DATABASE 语句生成规则 ====================

@pytest.mark.parametrize(
    "dialect_name, url, db_name, expected",
    [
        # MySQL：默认 utf8mb4
        ("mysql", "mysql+pymysql://u:p@h:3306/demo", "demo",
         "CREATE DATABASE `demo` CHARACTER SET utf8mb4"),
        # MySQL：字符集取自连接串
        ("mysql", "mysql+pymysql://u:p@h:3306/demo?charset=utf8", "demo",
         "CREATE DATABASE `demo` CHARACTER SET utf8"),
        # MySQL：非法字符集（可能被注入）→ 回退 utf8mb4
        ("mysql", "mysql+pymysql://u:p@h:3306/demo?charset=utf8mb4%3Bdrop", "demo",
         "CREATE DATABASE `demo` CHARACTER SET utf8mb4"),
        # MySQL：库名中的反引号需转义
        ("mysql", "mysql+pymysql://u:p@h:3306/demo", "back`tick",
         "CREATE DATABASE `back``tick` CHARACTER SET utf8mb4"),
        # PostgreSQL：带连字符的库名需加双引号
        ("postgresql", "postgresql+psycopg2://u:p@h:5432/demo-db", "demo-db",
         'CREATE DATABASE "demo-db"'),
        # PostgreSQL：库名中的双引号需转义
        ("postgresql", "postgresql+psycopg2://u:p@h:5432/demo", 'my"db',
         'CREATE DATABASE "my""db"'),
    ],
)
def test_build_create_database_sql(dialect_name, url, db_name, expected):
    engine = create_engine(url)
    try:
        assert engine.dialect.name == dialect_name
        assert _build_create_database_sql(engine, db_name) == expected
    finally:
        engine.dispose()


# ==================== 场景 7: 服务器连接串生成规则 ====================

def test_server_url_for_mysql_drops_database_and_keeps_query():
    """MySQL 需去掉库名才能连服务器，同时保留 charset 等连接参数。"""
    engine = create_engine("mysql+pymysql://u:p@h:3306/demo?charset=utf8mb4")
    url = _server_url(engine.url, None)
    try:
        assert url.database is None
        assert url.query["charset"] == "utf8mb4"
        assert url.render_as_string(hide_password=False) == \
            "mysql+pymysql://u:p@h:3306?charset=utf8mb4"
    finally:
        engine.dispose()


def test_server_url_for_postgres_uses_maintenance_db():
    """PostgreSQL 需改连维护库 postgres（CREATE DATABASE 不能在目标库上执行）。"""
    engine = create_engine("postgresql+psycopg2://u:p@h:5432/demo")
    try:
        assert _server_url(engine.url, "postgres").database == "postgres"
    finally:
        engine.dispose()
