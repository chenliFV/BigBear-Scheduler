"""TableGenerator 变更记录表（table_schema_change_log）测试用例（真实 MySQL）。

覆盖目标:
    1. 新建表 → 记录 create_table + 每个列/索引各一条 add_column/add_index
    2. 模式迁移新增列与索引 → 记录 add_column / add_index
    3. drop_missing=True 删除列与索引 → 记录 drop_column / drop_index
    4. 无变更的重复执行 → 不产生任何记录
    5. 自动建库 → 记录 create_database 事件
    6. 记录字段完整性（batch_id/executed_at/database_name/ddl_sql 等）

记录表由 TableGenerator 自动创建在目标数据库中，不参与配置驱动迁移。
每个用例使用唯一表名，结束后清理测试表与本次产生的记录，互不影响。

测试数据库: 本地 MySQL（Config/database.json 的 business 连接）。
"""
import json
import sys
import uuid
from pathlib import Path

import pytest
from sqlalchemy import (
    Column,
    Index,
    Integer,
    MetaData,
    String,
    Table,
    create_engine,
    inspect,
    text,
)
from sqlalchemy.engine import make_url

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
    _CHANGE_LOG_TABLE_NAME,
    _server_url,
    generate_tables,
)

DB_URL = get_database_url("business")
_BASE_URL = make_url(DB_URL)
_SERVER_URL = _server_url(_BASE_URL, None)


# ==================== 公共工具 ====================

@pytest.fixture(scope="session")
def engine():
    """会话级 engine，所有用例共用一个连接池。"""
    eng = create_engine(DB_URL, echo=False)
    yield eng
    eng.dispose()


@pytest.fixture
def table_name():
    """每个用例生成唯一表名，避免相互冲突。"""
    return f"tg_log_{uuid.uuid4().hex[:12]}"


def _fetch_change_log(engine, table_name: str) -> list:
    """查询指定表的全部变更记录（按写入顺序）。"""
    if not inspect(engine).has_table(_CHANGE_LOG_TABLE_NAME):
        return []
    with engine.begin() as conn:
        result = conn.execute(
            text(
                f"SELECT id, batch_id, executed_at, database_name, table_name, "
                f"operation, object_type, object_name, ddl_sql, success, note, "
                f"operator FROM `{_CHANGE_LOG_TABLE_NAME}` "
                f"WHERE table_name = :t ORDER BY id"
            ),
            {"t": table_name},
        )
        return [dict(r._mapping) for r in result]


@pytest.fixture
def change_log(engine, table_name):
    """返回查询函数，并负责清理该表名下的变更记录。"""
    def fetch(name: str = None):
        return _fetch_change_log(engine, name or table_name)

    yield fetch

    if inspect(engine).has_table(_CHANGE_LOG_TABLE_NAME):
        with engine.begin() as conn:
            conn.execute(
                text(
                    f"DELETE FROM `{_CHANGE_LOG_TABLE_NAME}` WHERE table_name = :t"
                ),
                {"t": table_name},
            )


@pytest.fixture
def cleanup_table(engine):
    """测试结束后自动 DROP 测试表，保证幂等。"""
    _to_clean = []

    def register(name: str):
        _to_clean.append(name)

    yield register

    with engine.begin() as conn:
        for name in _to_clean:
            conn.execute(text(f"DROP TABLE IF EXISTS `{name}`"))


def _write_config(tmp_path: Path, config: dict) -> str:
    path = tmp_path / "config.json"
    path.write_text(json.dumps(config, ensure_ascii=False, indent=2), encoding="utf-8")
    return str(path)


def _make_config(table_name: str, columns: list, indexes: list = None,
                 drop_missing: bool = False, database_url: str = DB_URL) -> dict:
    return {
        "database_url": database_url,
        "drop_missing": drop_missing,
        "tables": [{"name": table_name, "columns": columns, "indexes": indexes or []}],
    }


def _create_table_raw(engine, table_name: str, columns: list, indexes: list = None):
    """绕过 generate_tables 直接建表，用于准备迁移前置状态。"""
    metadata = MetaData()
    if indexes:
        table = Table(table_name, metadata, *columns, *indexes)
    else:
        table = Table(table_name, metadata, *columns)
    table.create(engine)


def _ops(rows: list) -> set:
    """把记录压缩为 {(operation, object_type, object_name)} 集合。"""
    return {(r["operation"], r["object_type"], r["object_name"]) for r in rows}


# ==================== 场景 1: 新建表 ====================

def test_create_table_records_columns_and_indexes(
    engine, table_name, change_log, cleanup_table, tmp_path
):
    """新建表时，应记录 create_table 以及每个列/索引的新增记录。"""
    cleanup_table(table_name)
    config_path = _write_config(
        tmp_path,
        _make_config(
            table_name,
            columns=[
                {"name": "id", "type": "int", "primary_key": True, "autoincrement": True},
                {"name": "user_name", "type": "str", "length": 64},
            ],
            indexes=[
                {"name": f"idx_{table_name}_user", "columns": ["user_name"]},
            ],
        ),
    )

    generate_tables(config_path=config_path, quiet=True)

    rows = change_log()
    assert _ops(rows) == {
        ("create_table", "table", table_name),
        ("add_column", "column", "id"),
        ("add_column", "column", "user_name"),
        ("add_index", "index", f"idx_{table_name}_user"),
    }
    # 同一批次的记录共享 batch_id、时间与库名
    assert len({r["batch_id"] for r in rows}) == 1
    assert all(r["executed_at"] is not None for r in rows)
    assert all(r["database_name"] == _BASE_URL.database for r in rows)
    # 经 text() 查询，MySQL 的 BOOLEAN 会以 0/1 返回，故用 bool() 归一
    assert all(bool(r["success"]) for r in rows)
    # 建表记录的 DDL 应可读
    create_row = next(r for r in rows if r["operation"] == "create_table")
    assert "CREATE TABLE" in create_row["ddl_sql"]
    assert table_name in create_row["ddl_sql"]


# ==================== 场景 2: 迁移新增列与索引 ====================

def test_migration_add_records_column_and_index(
    engine, table_name, change_log, cleanup_table, tmp_path
):
    """表已存在时新增列/索引，应分别记入变更记录表。"""
    cleanup_table(table_name)
    _create_table_raw(
        engine, table_name,
        [Column("id", Integer, primary_key=True, autoincrement=True),
         Column("name", String(64))],
    )
    config_path = _write_config(
        tmp_path,
        _make_config(
            table_name,
            columns=[
                {"name": "id", "type": "int", "primary_key": True, "autoincrement": True},
                {"name": "name", "type": "str", "length": 64},
                {"name": "status", "type": "str", "length": 20, "default": "active"},
            ],
            indexes=[{"name": f"idx_{table_name}_status", "columns": ["status"]}],
        ),
    )

    generate_tables(config_path=config_path, quiet=True)

    rows = change_log()
    assert ("add_column", "column", "status") in _ops(rows)
    assert ("add_index", "index", f"idx_{table_name}_status") in _ops(rows)
    column_row = next(r for r in rows if r["object_name"] == "status")
    assert "ADD COLUMN status" in column_row["ddl_sql"]


# ==================== 场景 3: drop_missing 删除列与索引 ====================

def test_drop_missing_records_dropped_column_and_index(
    engine, table_name, change_log, cleanup_table, tmp_path
):
    """drop_missing=True 时，被删除的列与索引应记入变更记录表。"""
    cleanup_table(table_name)
    metadata = MetaData()
    legacy_idx_name = f"idx_{table_name}_legacy"
    Table(
        table_name, metadata,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("name", String(64)),
        Column("legacy_col", String(32)),
        Index(legacy_idx_name, "legacy_col"),
    ).create(engine)

    config_path = _write_config(
        tmp_path,
        _make_config(
            table_name,
            columns=[
                {"name": "id", "type": "int", "primary_key": True, "autoincrement": True},
                {"name": "name", "type": "str", "length": 64},
            ],
            drop_missing=True,
        ),
    )

    generate_tables(config_path=config_path, quiet=True)

    ops = _ops(change_log())
    assert ("drop_index", "index", legacy_idx_name) in ops
    assert ("drop_column", "column", "legacy_col") in ops


# ==================== 场景 4: 无变更不写记录 ====================

def test_no_change_writes_no_record(
    engine, table_name, change_log, cleanup_table, tmp_path
):
    """表结构未发生变化时，重复执行不应产生新的变更记录。"""
    cleanup_table(table_name)
    config_path = _write_config(
        tmp_path,
        _make_config(
            table_name,
            columns=[
                {"name": "id", "type": "int", "primary_key": True, "autoincrement": True},
                {"name": "name", "type": "str", "length": 64},
            ],
        ),
    )

    generate_tables(config_path=config_path, quiet=True)
    rows_after_create = change_log()
    assert rows_after_create                       # 首次建表必然有记录

    generate_tables(config_path=config_path, quiet=True)
    rows_after_rerun = change_log()

    assert len(rows_after_rerun) == len(rows_after_create)
    assert max(r["id"] for r in rows_after_rerun) == max(
        r["id"] for r in rows_after_create
    )


# ==================== 场景 5: 自动建库 ====================

def _drop_database(db_name: str):
    """强制删除测试库（AUTOCOMMIT 下执行，保证幂等清理）。"""
    engine = create_engine(_SERVER_URL).execution_options(isolation_level="AUTOCOMMIT")
    try:
        with engine.connect() as conn:
            conn.execute(text(f"DROP DATABASE IF EXISTS `{db_name}`"))
    finally:
        engine.dispose()


def test_create_database_is_recorded(tmp_path):
    """目标库不存在时自动建库，应记录 create_database 事件。"""
    db_name = f"tg_log_{uuid.uuid4().hex[:10]}"
    table_name = f"tg_log_{uuid.uuid4().hex[:8]}"
    database_url = _BASE_URL.set(database=db_name).render_as_string(
        hide_password=False
    )
    config_path = _write_config(
        tmp_path,
        _make_config(
            table_name,
            columns=[
                {"name": "id", "type": "int", "primary_key": True, "autoincrement": True},
            ],
            database_url=database_url,
        ),
    )

    engine = create_engine(database_url)
    try:
        generate_tables(config_path=config_path, database_url=database_url, quiet=True)

        with engine.begin() as conn:
            rows = [
                dict(r._mapping)
                for r in conn.execute(
                    text(
                        f"SELECT operation, object_type, object_name, database_name "
                        f"FROM `{_CHANGE_LOG_TABLE_NAME}` ORDER BY id"
                    )
                )
            ]
        assert ("create_database", "database", db_name, db_name) in [
            (r["operation"], r["object_type"], r["object_name"], r["database_name"])
            for r in rows
        ]
    finally:
        engine.dispose()
        _drop_database(db_name)
