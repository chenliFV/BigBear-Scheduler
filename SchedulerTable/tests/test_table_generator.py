"""TableGenerator 迁移策略测试用例。

覆盖 README 中描述的 7 种迁移场景：
    1. 表不存在 → CREATE TABLE
    2. 表已存在 + 配置新增列 → ADD COLUMN（NOT NULL 需 DEFAULT）
    3. 表已存在 + 配置新增索引 → CREATE INDEX
    4. 表已存在 + 配置删除列 → drop_missing 控制
    5. 表已存在 + 配置删除索引 → drop_missing 控制
    6. 已有列的类型修改 → 不修改
    7. 主键列 → 永远不会被删除

测试数据库: 本地 MySQL (test)
每个测试使用唯一表名，并在结束后清理，互不影响。
"""
import os
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
from SchedulerTable.TableGenerator import generate_tables  # noqa: E402

# 数据库连接串统一由 Config/database.json 生成
DB_URL = get_database_url("business")


# ==================== 公共 fixture ====================

@pytest.fixture(scope="session")
def engine():
    """会话级 engine，所有测试共用一个连接池。"""
    eng = create_engine(DB_URL, echo=False)
    yield eng
    eng.dispose()


@pytest.fixture
def table_name():
    """每个测试生成唯一表名，避免相互冲突。"""
    return f"tg_test_{uuid.uuid4().hex[:12]}"


@pytest.fixture
def cleanup_table(engine):
    """测试结束后自动 DROP 测试表，保证幂等。

    返回一个 register 函数，测试中调用 register(table_name) 注册需要清理的表。
    """
    _to_clean = []

    def register(name: str):
        _to_clean.append(name)

    yield register

    # teardown：依次 DROP
    with engine.begin() as conn:
        for name in _to_clean:
            conn.execute(text(f"DROP TABLE IF EXISTS `{name}`"))


def _make_config(table_name: str, columns: list, indexes: list = None,
                 drop_missing: bool = False, database_url: str = DB_URL) -> dict:
    """构造 generate_tables 所需的完整 JSON 配置字典。"""
    cfg = {
        "database_url": database_url,
        "drop_missing": drop_missing,
        "tables": [
            {
                "name": table_name,
                "columns": columns,
                "indexes": indexes or [],
            }
        ],
    }
    return cfg


def _write_config(tmp_path: Path, config: dict, name: str = "config.json") -> str:
    """将配置字典写入临时 JSON 文件，返回文件路径。"""
    p = tmp_path / name
    import json
    p.write_text(json.dumps(config, ensure_ascii=False, indent=2), encoding="utf-8")
    return str(p)


def _get_columns(engine, table_name: str) -> dict:
    """返回 {列名: 列信息} 字典。"""
    inspector = inspect(engine)
    return {c["name"]: c for c in inspector.get_columns(table_name)}


def _get_index_names(engine, table_name: str) -> set:
    """返回表的索引名集合（不含主键索引）。"""
    inspector = inspect(engine)
    return {i["name"] for i in inspector.get_indexes(table_name)}


def _get_pk_columns(engine, table_name: str) -> set:
    """返回主键列名集合。"""
    inspector = inspect(engine)
    pk = inspector.get_pk_constraint(table_name)
    return set(pk.get("constrained_columns", []))


def _create_table_raw(engine, table_name: str, columns: list, indexes: list = None):
    """直接用 SQLAlchemy Core 创建一张表（绕过 generate_tables），用于准备测试前置状态。

    indexes 应为 Index 对象列表，需直接传入 Table 构造函数，否则不会随 table.create() 创建。
    """
    metadata = MetaData()
    if indexes:
        table = Table(table_name, metadata, *columns, *indexes)
    else:
        table = Table(table_name, metadata, *columns)
    table.create(engine)


# ==================== 场景 1: 表不存在 → CREATE TABLE ====================

def test_1_create_table_when_not_exists(engine, table_name, cleanup_table, tmp_path):
    """表不存在时，generate_tables 应完整创建表（含列、主键、索引、注释）。"""
    cleanup_table(table_name)

    config = _make_config(
        table_name,
        columns=[
            {"name": "id", "type": "int", "primary_key": True,
             "autoincrement": True, "nullable": False, "comment": "主键"},
            {"name": "user_name", "type": "str", "length": 64, "nullable": False,
             "comment": "用户名"},
            {"name": "score", "type": "float", "nullable": True},
            {"name": "is_active", "type": "bool", "default": True},
            {"name": "created_at", "type": "datetime", "default": "now"},
        ],
        indexes=[
            {"name": f"idx_{table_name}_user", "columns": ["user_name"], "unique": False},
            {"name": f"idx_{table_name}_score", "columns": ["score"], "unique": False},
        ],
    )
    config_path = _write_config(tmp_path, config)

    created = generate_tables(config_path=config_path, quiet=True)
    assert table_name in created

    # 验证列全部存在
    cols = _get_columns(engine, table_name)
    assert set(cols.keys()) == {"id", "user_name", "score", "is_active", "created_at"}

    # 验证主键
    assert _get_pk_columns(engine, table_name) == {"id"}

    # 验证索引
    idx_names = _get_index_names(engine, table_name)
    assert f"idx_{table_name}_user" in idx_names
    assert f"idx_{table_name}_score" in idx_names


# ==================== 场景 2: 配置新增列 → ADD COLUMN ====================

def test_2_add_column_with_default(engine, table_name, cleanup_table, tmp_path):
    """新增列带 DEFAULT 时，应成功添加并保留 NOT NULL 约束。"""
    cleanup_table(table_name)
    # 预先创建一张只有 id, name 的表
    _create_table_raw(
        engine, table_name,
        [
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("name", String(64), nullable=False),
        ],
    )
    # 插入一条数据，验证 ADD COLUMN 后已有行能填上 default
    with engine.begin() as conn:
        conn.execute(text(f"INSERT INTO `{table_name}` (name) VALUES ('alice')"))

    config = _make_config(
        table_name,
        columns=[
            {"name": "id", "type": "int", "primary_key": True, "autoincrement": True},
            {"name": "name", "type": "str", "length": 64, "nullable": False},
            # 新增列：带 default，NOT NULL
            {"name": "status", "type": "str", "length": 20, "nullable": False,
             "default": "active"},
        ],
    )
    config_path = _write_config(tmp_path, config)

    generate_tables(config_path=config_path, quiet=True)

    cols = _get_columns(engine, table_name)
    assert "status" in cols
    # MySQL 下 NOT NULL 列应报告 nullable=False
    assert cols["status"]["nullable"] is False

    # 验证已有行被填上 default 值
    with engine.begin() as conn:
        row = conn.execute(
            text(f"SELECT status FROM `{table_name}` WHERE name='alice'")
        ).fetchone()
        assert row[0] == "active"


def test_2b_add_notnull_column_without_default_downgrades_to_nullable(
    engine, table_name, cleanup_table, tmp_path
):
    """新增 NOT NULL 列但未指定 default 时，应自动放宽为 nullable 以避免迁移失败。

    对应 _migrate_table 中的警告分支：'NOT NULL 列未指定 default，将允许 NULL'。
    """
    cleanup_table(table_name)
    _create_table_raw(
        engine, table_name,
        [
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("name", String(64), nullable=False),
        ],
    )
    # 先插一条数据，确保已有行存在
    with engine.begin() as conn:
        conn.execute(text(f"INSERT INTO `{table_name}` (name) VALUES ('bob')"))

    config = _make_config(
        table_name,
        columns=[
            {"name": "id", "type": "int", "primary_key": True, "autoincrement": True},
            {"name": "name", "type": "str", "length": 64, "nullable": False},
            # 新增列：NOT NULL 但无 default
            {"name": "remark", "type": "str", "length": 50, "nullable": False},
        ],
    )
    config_path = _write_config(tmp_path, config)

    # 不应抛异常
    generate_tables(config_path=config_path, quiet=True)

    cols = _get_columns(engine, table_name)
    assert "remark" in cols
    # 应被自动放宽为 nullable=True
    assert cols["remark"]["nullable"] is True


# ==================== 场景 3: 配置新增索引 → CREATE INDEX ====================

def test_3_add_index(engine, table_name, cleanup_table, tmp_path):
    """表已存在且配置新增索引时，应 CREATE INDEX。"""
    cleanup_table(table_name)
    _create_table_raw(
        engine, table_name,
        [
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("email", String(128), nullable=False),
            Column("age", Integer, nullable=True),
        ],
    )

    config = _make_config(
        table_name,
        columns=[
            {"name": "id", "type": "int", "primary_key": True, "autoincrement": True},
            {"name": "email", "type": "str", "length": 128, "nullable": False},
            {"name": "age", "type": "int", "nullable": True},
        ],
        indexes=[
            {"name": f"idx_{table_name}_email", "columns": ["email"], "unique": True},
            {"name": f"idx_{table_name}_age", "columns": ["age"], "unique": False},
        ],
    )
    config_path = _write_config(tmp_path, config)

    generate_tables(config_path=config_path, quiet=True)

    idx_names = _get_index_names(engine, table_name)
    assert f"idx_{table_name}_email" in idx_names
    assert f"idx_{table_name}_age" in idx_names

    # 验证唯一索引生效（插入重复 email 应失败）
    with engine.begin() as conn:
        conn.execute(text(f"INSERT INTO `{table_name}` (email) VALUES ('a@x.com')"))
    with pytest.raises(Exception):
        with engine.begin() as conn:
            conn.execute(text(f"INSERT INTO `{table_name}` (email) VALUES ('a@x.com')"))


# ==================== 场景 4: 配置删除列 → drop_missing 控制 ====================

def test_4a_drop_column_when_drop_missing_true(
    engine, table_name, cleanup_table, tmp_path
):
    """drop_missing=True 时，配置中已删除的列应被 DROP。"""
    cleanup_table(table_name)
    _create_table_raw(
        engine, table_name,
        [
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("name", String(64), nullable=False),
            Column("legacy_field", String(100), nullable=True),  # 待删除
        ],
    )

    # 配置中不再包含 legacy_field
    config = _make_config(
        table_name,
        columns=[
            {"name": "id", "type": "int", "primary_key": True, "autoincrement": True},
            {"name": "name", "type": "str", "length": 64, "nullable": False},
        ],
        drop_missing=True,
    )
    config_path = _write_config(tmp_path, config)

    generate_tables(config_path=config_path, quiet=True)

    cols = _get_columns(engine, table_name)
    assert "legacy_field" not in cols
    assert set(cols.keys()) == {"id", "name"}


def test_4b_keep_column_when_drop_missing_false(
    engine, table_name, cleanup_table, tmp_path
):
    """drop_missing=False（默认）时，配置中已删除的列应保留。"""
    cleanup_table(table_name)
    _create_table_raw(
        engine, table_name,
        [
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("name", String(64), nullable=False),
            Column("legacy_field", String(100), nullable=True),
        ],
    )

    config = _make_config(
        table_name,
        columns=[
            {"name": "id", "type": "int", "primary_key": True, "autoincrement": True},
            {"name": "name", "type": "str", "length": 64, "nullable": False},
        ],
        drop_missing=False,  # 默认行为
    )
    config_path = _write_config(tmp_path, config)

    generate_tables(config_path=config_path, quiet=True)

    cols = _get_columns(engine, table_name)
    assert "legacy_field" in cols  # 仍保留


# ==================== 场景 5: 配置删除索引 → drop_missing 控制 ====================

def test_5a_drop_index_when_drop_missing_true(
    engine, table_name, cleanup_table, tmp_path
):
    """drop_missing=True 时，配置中已删除的索引应被 DROP。"""
    cleanup_table(table_name)
    _create_table_raw(
        engine, table_name,
        [
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("name", String(64), nullable=False),
            Column("city", String(64), nullable=True),
        ],
        indexes=[
            Index(f"idx_{table_name}_name", "name"),
            Index(f"idx_{table_name}_city", "city"),  # 待删除
        ],
    )
    # 确认索引确实存在
    assert f"idx_{table_name}_city" in _get_index_names(engine, table_name)

    # 配置中只保留 name 索引
    config = _make_config(
        table_name,
        columns=[
            {"name": "id", "type": "int", "primary_key": True, "autoincrement": True},
            {"name": "name", "type": "str", "length": 64, "nullable": False},
            {"name": "city", "type": "str", "length": 64, "nullable": True},
        ],
        indexes=[
            {"name": f"idx_{table_name}_name", "columns": ["name"], "unique": False},
        ],
        drop_missing=True,
    )
    config_path = _write_config(tmp_path, config)

    generate_tables(config_path=config_path, quiet=True)

    idx_names = _get_index_names(engine, table_name)
    assert f"idx_{table_name}_city" not in idx_names
    assert f"idx_{table_name}_name" in idx_names


def test_5b_keep_index_when_drop_missing_false(
    engine, table_name, cleanup_table, tmp_path
):
    """drop_missing=False 时，配置中已删除的索引应保留。"""
    cleanup_table(table_name)
    _create_table_raw(
        engine, table_name,
        [
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("name", String(64), nullable=False),
            Column("city", String(64), nullable=True),
        ],
        indexes=[
            Index(f"idx_{table_name}_name", "name"),
            Index(f"idx_{table_name}_city", "city"),
        ],
    )

    # 配置中只保留 name 索引，drop_missing=False
    config = _make_config(
        table_name,
        columns=[
            {"name": "id", "type": "int", "primary_key": True, "autoincrement": True},
            {"name": "name", "type": "str", "length": 64, "nullable": False},
            {"name": "city", "type": "str", "length": 64, "nullable": True},
        ],
        indexes=[
            {"name": f"idx_{table_name}_name", "columns": ["name"], "unique": False},
        ],
        drop_missing=False,
    )
    config_path = _write_config(tmp_path, config)

    generate_tables(config_path=config_path, quiet=True)

    idx_names = _get_index_names(engine, table_name)
    assert f"idx_{table_name}_city" in idx_names  # 仍保留
    assert f"idx_{table_name}_name" in idx_names


# ==================== 场景 6: 已有列的类型修改 → 不修改 ====================

def test_6_existing_column_type_not_modified(
    engine, table_name, cleanup_table, tmp_path
):
    """已有列的类型声明变化时，generate_tables 不应修改其类型（避免数据丢失）。"""
    cleanup_table(table_name)
    # 预先创建 score 列为 Integer
    _create_table_raw(
        engine, table_name,
        [
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("score", Integer, nullable=True),  # 数据库中是 INT
        ],
    )

    # 配置中把 score 改成 str/VARCHAR(64)
    config = _make_config(
        table_name,
        columns=[
            {"name": "id", "type": "int", "primary_key": True, "autoincrement": True},
            {"name": "score", "type": "str", "length": 64, "nullable": True},  # 试图改类型
        ],
    )
    config_path = _write_config(tmp_path, config)

    generate_tables(config_path=config_path, quiet=True)

    # 验证 score 仍是 INT，未被改成 VARCHAR
    cols = _get_columns(engine, table_name)
    score_type = str(cols["score"]["type"]).upper()
    assert "INT" in score_type, f"score 类型不应被修改，实际: {score_type}"
    assert "VARCHAR" not in score_type and "CHAR" not in score_type


# ==================== 场景 7: 主键列永远不会被删除 ====================

def test_7_primary_key_column_never_dropped(
    engine, table_name, cleanup_table, tmp_path
):
    """drop_missing=True 时，主键列也不应被删除。

    对应 _migrate_table 中：'跳过删除主键列' 的保护分支。
    """
    cleanup_table(table_name)
    _create_table_raw(
        engine, table_name,
        [
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("name", String(64), nullable=False),
        ],
    )

    # 配置中只保留 name，不包含 id（试图删除主键列）
    config = _make_config(
        table_name,
        columns=[
            {"name": "name", "type": "str", "length": 64, "nullable": False},
        ],
        drop_missing=True,
    )
    config_path = _write_config(tmp_path, config)

    generate_tables(config_path=config_path, quiet=True)

    # 验证 id 列仍然存在（主键保护）
    cols = _get_columns(engine, table_name)
    assert "id" in cols, "主键列 id 不应被删除"
    assert _get_pk_columns(engine, table_name) == {"id"}
    assert "name" in cols


def test_7b_non_pk_columns_still_dropped_when_pk_protected(
    engine, table_name, cleanup_table, tmp_path
):
    """主键保护只针对主键列，其他普通列在 drop_missing=True 时仍应被删除。

    作为场景 7 的补充：确认主键保护不会让所有列都保留。
    """
    cleanup_table(table_name)
    _create_table_raw(
        engine, table_name,
        [
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("name", String(64), nullable=False),
            Column("legacy", String(50), nullable=True),  # 普通列，应被删除
        ],
    )

    # 配置只保留 name（id 是主键会被保护，但配置没列 id 也不会被删）
    config = _make_config(
        table_name,
        columns=[
            {"name": "name", "type": "str", "length": 64, "nullable": False},
        ],
        drop_missing=True,
    )
    config_path = _write_config(tmp_path, config)

    generate_tables(config_path=config_path, quiet=True)

    cols = _get_columns(engine, table_name)
    assert "id" in cols          # 主键保护
    assert "name" in cols        # 配置中保留
    assert "legacy" not in cols  # 普通列已删除
