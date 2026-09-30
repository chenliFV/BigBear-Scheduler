"""TableGenerator 并发（多实例同时运行）DDL 行为测试用例（真实 MySQL）。

验证目标:
    多个实例（用线程模拟多 worker / 多副本）在同一时刻执行建库、建表与
    模式迁移时——
        - 最终表结构始终一致（列/索引数量与内容正确，无重复、无丢失）
        - 不抛未捕获异常
        - "列/索引/表已存在"这类良性幂等竞态不会被记为 ERROR

覆盖场景:
    1. 并发 CREATE TABLE（同名表、多列、多索引）
    2. 并发 CREATE DATABASE（目标库不存在，多实例同时建库 + 建表）
    3. 并发模式迁移（ADD COLUMN + CREATE INDEX）
    4. 并发删除迁移（drop_missing=True，DROP COLUMN + DROP INDEX）
    5. 良性竞态判定 _is_benign_ddl_race 的单元用例（不依赖数据库）
    6. 收尾自校验：确认上述并发用例确实命中过竞态（避免"没发生竞态"导致的空转通过）

为提升竞态命中率，集成用例会在多轮（_ROUNDS）中各自使用唯一表名，
每轮都用 _THREADS 个线程通过 Barrier 同时起跑。

测试数据库: 本地 MySQL（Config/database.json 的 business 连接）。
每个用例使用唯一表名/库名，结束后统一清理，互不影响、可重复执行。
"""
import json
import logging
import sys
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest
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
    _is_benign_ddl_race,
    _server_url,
    generate_tables,
)

DB_URL = get_database_url("business")
_BASE_URL = make_url(DB_URL)
_SERVER_URL = _server_url(_BASE_URL, None)

# 并发规模
_THREADS = 8      # 同时起跑的实例数
_ROUNDS = 6       # 表相关用例的轮次（每轮唯一表名，制造多次竞态机会）
_DB_THREADS = 6   # 建库用例的实例数
_DB_ROUNDS = 3    # 建库用例轮次


# ==================== 公共工具 ====================

class _ErrorCollector(logging.Handler):
    """线程安全地收集 table_generator 的 ERROR 日志，用于断言"并发无冲突"。"""

    def __init__(self):
        super().__init__(level=logging.ERROR)
        self.messages = []
        self._lock = threading.Lock()

    def emit(self, record):
        with self._lock:
            self.messages.append(record.getMessage())


# 竞态证据：TableGenerator 把"已由其他实例完成"的良性竞态记为 info 日志。
# 这里在模块级别累计，供收尾用例断言"并发确实发生过"。
BENIGN_RACE_MARKER = "已由其他实例"
_benign_races: list = []
_benign_lock = threading.Lock()


class _BenignRaceCollector(logging.Handler):
    """线程安全地收集良性竞态 info 日志，作为"并发确实发生"的证据。"""

    def __init__(self):
        super().__init__(level=logging.INFO)

    def emit(self, record):
        if BENIGN_RACE_MARKER in record.getMessage():
            with _benign_lock:
                _benign_races.append(record.getMessage())


@pytest.fixture
def error_logs():
    """挂载 ERROR / 竞态收集器，返回 ERROR 消息列表（用例结束后自动卸载）。"""
    error_handler = _ErrorCollector()
    race_handler = _BenignRaceCollector()
    lg = logging.getLogger("table_generator")
    old_level = lg.level
    lg.addHandler(error_handler)
    lg.addHandler(race_handler)
    lg.setLevel(logging.DEBUG)
    try:
        yield error_handler.messages
    finally:
        lg.removeHandler(error_handler)
        lg.removeHandler(race_handler)
        lg.setLevel(old_level)


@pytest.fixture(scope="session")
def engine():
    """会话级 engine，所有用例共用一个连接池。"""
    eng = create_engine(DB_URL, echo=False)
    yield eng
    eng.dispose()


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


@pytest.fixture
def cleanup_database():
    """测试结束后强制 DROP 测试库，保证幂等。"""
    names = []

    def register(name: str):
        names.append(name)

    yield register

    eng = create_engine(_SERVER_URL).execution_options(isolation_level="AUTOCOMMIT")
    try:
        with eng.connect() as conn:
            for name in names:
                conn.execute(text(f"DROP DATABASE IF EXISTS `{name}`"))
    finally:
        eng.dispose()


def _write_config(tmp_path: Path, config: dict, name: str) -> str:
    path = tmp_path / name
    path.write_text(json.dumps(config, ensure_ascii=False, indent=2), encoding="utf-8")
    return str(path)


def _get_columns(engine, table_name: str) -> set:
    return {c["name"] for c in inspect(engine).get_columns(table_name)}


def _get_index_names(engine, table_name: str) -> set:
    return {i["name"] for i in inspect(engine).get_indexes(table_name)}


def _target_url(db_name: str) -> str:
    return _BASE_URL.set(database=db_name).render_as_string(hide_password=False)


def _run_concurrently(n_threads: int, worker) -> list:
    """用 Barrier 让 n 个线程尽量同时起跑，返回各线程结果。

    worker 抛出的异常会在此处向上传递，从而断言"并发下无未捕获异常"。
    """
    barrier = threading.Barrier(n_threads)

    def wrap(i):
        barrier.wait(timeout=30)
        return worker(i)

    with ThreadPoolExecutor(max_workers=n_threads) as ex:
        futures = [ex.submit(wrap, i) for i in range(n_threads)]
        return [f.result() for f in futures]


def _make_worker(config_path: str, table_name: str, engine):
    """构造一个"运行一次 generate_tables"的实例工作函数。

    注意：quiet 必须为 False，否则良性竞态只记 info 会被抑制，
    收尾用例就无法判断并发是否真的发生。
    """
    def worker(i):
        processed = generate_tables(config_path=config_path, engine=engine, quiet=False)
        assert processed == [table_name]
        return table_name
    return worker


def _base_columns(extra: list = None) -> list:
    cols = [
        {"name": "id", "type": "int", "primary_key": True, "autoincrement": True},
        {"name": "name", "type": "str", "length": 64, "nullable": False},
    ]
    return cols + (extra or [])


# ==================== 场景 1: 并发 CREATE TABLE ====================

def test_concurrent_create_table_has_no_conflict(
    engine, tmp_path, cleanup_table, error_logs
):
    """多实例同时对同一张不存在的表建表，最终结构正确且无冲突。

    竞态点: 多个实例各自的 checkfirst 检查都判定"表不存在"，
    于是都执行 CREATE TABLE，后到者会报 1050 Table already exists。
    """
    for r in range(_ROUNDS):
        table_name = f"tg_conc_{uuid.uuid4().hex[:10]}"
        cleanup_table(table_name)
        idx_a = f"idx_{table_name}_name"
        idx_b = f"idx_{table_name}_score"
        config = {
            "tables": [{
                "name": table_name,
                "columns": _base_columns([
                    {"name": "score", "type": "float"},
                ]),
                "indexes": [
                    {"name": idx_a, "columns": ["name"]},
                    {"name": idx_b, "columns": ["score"]},
                ],
            }],
        }
        config_path = _write_config(tmp_path, config, f"{table_name}.json")

        results = _run_concurrently(_THREADS, _make_worker(config_path, table_name, engine))
        # 每个实例都应在不抛异常的前提下完成
        assert results == [table_name] * _THREADS

        # 最终结构：列与索引有且仅有一份
        assert _get_columns(engine, table_name) == {"id", "name", "score"}
        assert _get_index_names(engine, table_name) == {idx_a, idx_b}

    # 整个并发过程不应出现任何 ERROR（竞态均为良性幂等）
    assert error_logs == [], f"并发建表出现冲突日志: {error_logs}"


# ==================== 场景 2: 并发 CREATE DATABASE ====================

def test_concurrent_create_database_has_no_conflict(
    tmp_path, cleanup_database, error_logs
):
    """多实例同时对同一不存在的库执行建库+建表，库只建一次且无冲突。

    竞态点: 多个实例都判定"库不存在"，都执行 CREATE DATABASE，
    后到者会报 1007 database exists。
    """
    for r in range(_DB_ROUNDS):
        db_name = f"tg_auto_{uuid.uuid4().hex[:10]}"
        cleanup_database(db_name)
        table_name = f"tg_conc_{uuid.uuid4().hex[:10]}"
        config = {
            "tables": [{
                "name": table_name,
                "columns": _base_columns(),
                "indexes": [{"name": f"idx_{table_name}_name", "columns": ["name"]}],
            }],
        }
        config_path = _write_config(tmp_path, config, f"{db_name}.json")
        database_url = _target_url(db_name)

        def worker(i, p=config_path, u=database_url, t=table_name):
            # 每个实例使用独立 engine（模拟多进程），用完释放连接
            eng = create_engine(u)
            try:
                generate_tables(config_path=p, engine=eng, quiet=False)
                return t
            finally:
                eng.dispose()

        results = _run_concurrently(_DB_THREADS, worker)
        # 任何实例都不应因"库已存在"而抛 RuntimeError
        assert results == [table_name] * _DB_THREADS

        # 库与表都真实存在，且表结构正确
        conn_engine = create_engine(database_url)
        try:
            assert table_name in set(inspect(conn_engine).get_table_names())
            assert _get_columns(conn_engine, table_name) == {"id", "name"}
        finally:
            conn_engine.dispose()
        cleanup_database(db_name)

    assert error_logs == [], f"并发建库出现冲突日志: {error_logs}"


# ==================== 场景 3: 并发模式迁移（ADD COLUMN + CREATE INDEX）====================

def test_concurrent_migration_has_no_conflict(
    engine, tmp_path, cleanup_table, error_logs
):
    """表已存在时并发迁移（新增列 + 新增索引），最终结构正确且无冲突。

    竞态点: 多个实例反射到的旧结构一致，都判定"缺列/缺索引"，
    于是都执行 ADD COLUMN / CREATE INDEX，后到者报 1060 / 1061。
    """
    for r in range(_ROUNDS):
        table_name = f"tg_conc_{uuid.uuid4().hex[:10]}"
        cleanup_table(table_name)
        # 前置：只有 id、name 两列
        with engine.begin() as conn:
            conn.execute(text(
                f"CREATE TABLE `{table_name}` "
                f"(id INT PRIMARY KEY AUTO_INCREMENT, name VARCHAR(64) NOT NULL)"
            ))

        idx_name = f"idx_{table_name}_name"
        config = {
            "tables": [{
                "name": table_name,
                "columns": _base_columns([
                    {"name": "status", "type": "str", "length": 20, "default": "active"},
                ]),
                "indexes": [{"name": idx_name, "columns": ["name"]}],
            }],
        }
        config_path = _write_config(tmp_path, config, f"{table_name}.json")

        results = _run_concurrently(
            _THREADS, _make_worker(config_path, table_name, engine)
        )
        assert results == [table_name] * _THREADS

        assert _get_columns(engine, table_name) == {"id", "name", "status"}
        assert _get_index_names(engine, table_name) == {idx_name}

    assert error_logs == [], f"并发迁移出现冲突日志: {error_logs}"


# ==================== 场景 4: 并发删除迁移（drop_missing=True）====================

def test_concurrent_drop_missing_has_no_conflict(
    engine, tmp_path, cleanup_table, error_logs
):
    """drop_missing=True 时并发删除多余列/索引，最终结构正确且无冲突。

    竞态点: 多个实例都判定"某列/索引多余"并执行 DROP，
    后到者报 1091（对象已不存在）。
    """
    for r in range(_ROUNDS):
        table_name = f"tg_conc_{uuid.uuid4().hex[:10]}"
        cleanup_table(table_name)
        legacy_idx = f"idx_{table_name}_legacy"
        # 前置：多出一列 legacy 及其索引
        with engine.begin() as conn:
            conn.execute(text(
                f"CREATE TABLE `{table_name}` "
                f"(id INT PRIMARY KEY AUTO_INCREMENT, name VARCHAR(64) NOT NULL, "
                f"legacy VARCHAR(50), INDEX {legacy_idx} (legacy))"
            ))

        config = {
            "drop_missing": True,
            "tables": [{
                "name": table_name,
                "columns": _base_columns(),          # 不再包含 legacy
                "indexes": [],                        # 不再包含 legacy 索引
            }],
        }
        config_path = _write_config(tmp_path, config, f"{table_name}.json")

        results = _run_concurrently(
            _THREADS, _make_worker(config_path, table_name, engine)
        )
        assert results == [table_name] * _THREADS

        assert _get_columns(engine, table_name) == {"id", "name"}
        assert legacy_idx not in _get_index_names(engine, table_name)

    assert error_logs == [], f"并发删除迁移出现冲突日志: {error_logs}"


# ==================== 场景 5: 良性竞态判定（单元用例，不依赖数据库）====================

class _OrigError(Exception):
    """模拟驱动原始异常：args[0] 为错误码或带错误码的文案。

    pgcode 用于模拟 PostgreSQL 的 5 位 SQLSTATE（psycopg2 的 exc.orig.pgcode）；
    非 PG 驱动没有该属性（默认 None）。
    """

    def __init__(self, *args, pgcode=None):
        super().__init__(*args)
        self.pgcode = pgcode


class _Diag:
    def __init__(self, sqlstate):
        self.sqlstate = sqlstate


class _PgDiagError(Exception):
    """模拟仅暴露 diag.sqlstate（无 pgcode）的 PG 驱动异常。"""

    def __init__(self, message, sqlstate):
        super().__init__(message)
        self.diag = _Diag(sqlstate)


def _wrap(orig: Exception) -> OperationalError:
    """把驱动异常包装成 SQLAlchemy 的 OperationalError（模拟真实调用栈）。"""
    return OperationalError("ALTER TABLE t ADD COLUMN c INT", {}, orig)


@pytest.mark.parametrize(
    "orig, dropping, expected",
    [
        # MySQL: 表已存在 / 重复列 / 重复索引 → 创建类良性
        (_OrigError(1050, "Table 't' already exists"), False, True),
        (_OrigError(1060, "Duplicate column name 'status'"), False, True),
        (_OrigError(1061, "Duplicate key name 'idx_x'"), False, True),
        # MySQL: 库已存在 → 创建类良性
        (_OrigError(1007, "Can't create database 'db'; database exists"), False, True),
        # SQLite: 仅凭文案也应识别（无错误码）
        (_OrigError("index idx_x already exists"), False, True),
        # PostgreSQL: duplicate object
        (_OrigError("relation \"t\" already exists"), False, True),
        # MySQL: 删除时对象已不存在（1091）→ 删除类良性，创建类不算
        (_OrigError(1091, "Can't DROP 'idx_x'; check that column/key exists"),
         True, True),
        (_OrigError(1091, "Can't DROP 'idx_x'; check that column/key exists"),
         False, False),
        # SQLite: 删除时对象不存在
        (_OrigError("no such index: idx_x"), True, True),
        # PostgreSQL: 5 位 SQLSTATE（pgcode）——创建类良性。
        # 文案故意用中文，模拟服务端 lc_messages 本地化后英文匹配失效的场景；
        # 必须靠 SQLSTATE 判定，否则会漏判并误记 ERROR。
        (_OrigError("错误: 关系 \"t\" 已经存在", pgcode="42P07"), False, True),
        (_OrigError("错误: 数据库 \"db\" 已经存在", pgcode="42P04"), False, True),
        (_OrigError("错误: 字段 \"status\" 已经存在", pgcode="42701"), False, True),
        (_OrigError("错误: 对象已经存在", pgcode="42710"), False, True),
        # PostgreSQL: SQLSTATE 删除类良性（对象/列/表不存在）
        (_OrigError("错误: 对象 \"idx_x\" 不存在", pgcode="42704"), True, True),
        (_OrigError("错误: 字段 \"legacy\" 不存在", pgcode="42703"), True, True),
        (_OrigError("错误: 表 \"t\" 不存在", pgcode="42P01"), True, True),
        # PostgreSQL: 仅暴露 diag.sqlstate（无 pgcode）时也应识别
        (_PgDiagError("relation \"t\" already exists", "42P07"), False, True),
        # PostgreSQL: 创建类 SQLSTATE 出现在删除类操作中 → 非良性
        (_OrigError("错误: 关系 \"t\" 已经存在", pgcode="42P07"), True, False),
        # 收紧后的删除文案：必须"对象关键字 + does not exist"同时出现
        (_OrigError("index \"idx_x\" does not exist"), True, True),      # 索引不存在 → 良性
        (_OrigError("column \"legacy\" does not exist"), True, True),    # 列不存在 → 良性
        # 裸 "does not exist" 不得再误吞以下真实故障
        (_OrigError("type \"mytype\" does not exist"), True, False),     # 类型不存在
        (_OrigError("schema \"myschema\" does not exist"), True, False),  # schema 不存在
        (_OrigError("table \"t\" does not exist"), True, False),         # MySQL 1146 表不存在
        # 真实故障：标识符过长 → 绝不能当作良性
        (_OrigError(1059, "Identifier name 'tg_xxx' is too long"), False, False),
        (_OrigError(1059, "Identifier name 'tg_xxx' is too long"), True, False),
        # 真实故障：语法错误
        (_OrigError(1064, "You have an error in your SQL syntax"), False, False),
    ],
)
def test_is_benign_ddl_race(orig, dropping, expected):
    """良性竞态判定：仅"已存在/已不存在"类错误被忽略，真实故障必须暴露。"""
    assert _is_benign_ddl_race(_wrap(orig), dropping=dropping) is expected


# ==================== 场景 6: 收尾自校验 ====================

# 依赖默认的"按定义顺序执行"，本用例必须位于文件最后。
def test_concurrency_actually_exercised():
    """确认前面的并发用例确实命中过良性竞态，避免"未真正并发"导致的空转通过。

    error_logs == [] 在并发未发生时天然成立；此处用竞态证据反向校验：
    只有真正发生了竞态，才说明"良性竞态被降级为 info 而非 error"得到验证。
    """
    assert _benign_races, (
        "并发用例未命中任何良性幂等竞态，本轮并发未真正竞争，测试可能空转；"
        "可适当提高 _THREADS / _ROUNDS 后重跑"
    )
