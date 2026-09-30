"""数据库公共方法模块。

提供连接不同数据库、执行查询、写入数据的通用能力，
支持 MySQL / PostgreSQL / SQLite 等多种后端（通过 SQLAlchemy 连接串区分）。

核心特性:
    - 统一的 Database 封装：query / query_one / execute / execute_many / insert_batch
    - SQLAlchemy Core 语句入口：fetch_all / fetch_one / fetch_scalar / execute_stmt
      / execute_many_stmt，由编译期按方言渲染标识符与语法，天然兼容 MySQL 与 PG
    - 多数据库协作：multi_connect 一次性连接多个库，由调用方自行处理跨库逻辑
    - 配置驱动：连接串可显式传入，也可从 Config/database.json 统一生成
    - 上下文管理：with 语法自动关闭连接池
    - 批量写入：使用 executemany 高效导入大数据集

典型用法:
    # 1. 单库操作
    from SchedulerTask.db_utils import connect
    with connect("mysql+pymysql://user:pass@localhost/db") as db:
        rows = db.query("SELECT * FROM users WHERE age > :age", {"age": 18})
        db.execute("UPDATE users SET active = 1 WHERE id = :id", {"id": 10})

    # 2. Core 语句（跨方言，推荐新代码使用）
    from sqlalchemy import select
    with connect() as db:
        rows = db.fetch_all(select(user_table.c.id, user_table.c.name))

    # 3. 从项目配置读取连接串
    from SchedulerTask.db_utils import connect
    with connect() as db:  # 默认读 Config/database.json 的 scheduler 数据库
        rows = db.query("SELECT 1")

    # 4. 多库操作（读取源库 → 自行处理 → 写入目标库）
    from SchedulerTask.db_utils import multi_connect
    with multi_connect(source_url, dest_url) as (src, dst):
        rows = src.query("SELECT * FROM users WHERE active = 1")
        dst.insert_batch("archived_users", rows)
"""
import logging
import os
import sys
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterable, Optional

from sqlalchemy import column as sa_column
from sqlalchemy import create_engine
from sqlalchemy import insert as sa_insert
from sqlalchemy import table as sa_table
from sqlalchemy import text
from sqlalchemy.engine import Engine

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

logger = logging.getLogger("worker")


class Database:
    """单个数据库的封装，基于 SQLAlchemy Engine。

    通过 connect() 工厂创建，推荐使用 with 语法确保连接池释放。
    """

    def __init__(self, url: str, echo: bool = False):
        self._url = url
        self._engine: Engine = self._build_engine(url, echo)

    @staticmethod
    def _build_engine(url: str, echo: bool) -> Engine:
        """根据连接串创建 Engine，SQLite 需关闭线程检查。"""
        if url.startswith("sqlite"):
            return create_engine(
                url, echo=echo, connect_args={"check_same_thread": False}
            )
        return create_engine(url, echo=echo)

    @property
    def url(self) -> str:
        """返回数据库连接串（用于日志/调试）。"""
        return self._url

    @property
    def engine(self) -> Engine:
        """暴露底层 Engine，供高级场景使用。"""
        return self._engine

    @property
    def dialect(self) -> str:
        """返回当前连接的方言名，如 "mysql" / "mariadb" / "postgresql"。

        用于需要按后端分派语法的场景（如 upsert 语句的构造）。
        """
        return self._engine.dialect.name

    # ---------- Core 语句执行（跨方言）----------
    # 以下入口接收 SQLAlchemy Core 语句（select/insert/update/delete），
    # 由编译器按当前方言渲染标识符引用与专有语法，因此同一份代码可在
    # MySQL 与 PostgreSQL 下运行。

    def fetch_all(self, stmt, params: Optional[dict] = None) -> list[dict]:
        """执行 Core 语句并返回所有行（每行为 dict）。

        Args:
            stmt: SQLAlchemy Core 可执行语句
            params: 额外的绑定参数字典（语句内已绑定的参数可省略）

        Returns:
            行列表，每行是一个 {列名: 值} 字典
        """
        with self._engine.connect() as conn:
            result = conn.execute(stmt, params or {})
            return [dict(row._mapping) for row in result.fetchall()]

    def fetch_one(self, stmt, params: Optional[dict] = None) -> Optional[dict]:
        """执行 Core 语句并返回首行（dict）或 None。"""
        with self._engine.connect() as conn:
            result = conn.execute(stmt, params or {})
            row = result.fetchone()
            return dict(row._mapping) if row else None

    def fetch_scalar(self, stmt, params: Optional[dict] = None) -> Any:
        """执行 Core 语句并返回首个标量值。"""
        with self._engine.connect() as conn:
            result = conn.execute(stmt, params or {})
            row = result.fetchone()
            return row[0] if row else None

    def execute_stmt(self, stmt, params: Optional[dict] = None) -> int:
        """执行单条 Core 写语句（自动提交）。

        Args:
            stmt: SQLAlchemy Core 的 insert/update/delete 语句
            params: 绑定参数字典

        Returns:
            受影响行数
        """
        with self._engine.begin() as conn:
            result = conn.execute(stmt, params or {})
            return result.rowcount or 0

    def execute_many_stmt(self, stmt, params_list: Iterable[dict]) -> int:
        """用同一 Core 语句批量执行参数列表（自动提交）。

        Args:
            stmt: SQLAlchemy Core 的 insert/update/delete 语句
            params_list: 参数字典的可迭代对象

        Returns:
            总受影响行数
        """
        params_list = list(params_list)
        if not params_list:
            return 0
        with self._engine.begin() as conn:
            result = conn.execute(stmt, params_list)
            return result.rowcount or 0

    # ---------- 查询（SELECT）----------

    def query(self, sql: str, params: Optional[dict] = None) -> list[dict]:
        """执行 SELECT，返回所有行（每行为 dict）。

        Args:
            sql: SQL 语句，使用 :name 命名参数
            params: 参数字典

        Returns:
            行列表，每行是一个 {列名: 值} 字典
        """
        with self._engine.connect() as conn:
            result = conn.execute(text(sql), params or {})
            return [dict(row._mapping) for row in result.fetchall()]

    def query_one(self, sql: str, params: Optional[dict] = None) -> Optional[dict]:
        """执行 SELECT，返回首行（dict）或 None。"""
        with self._engine.connect() as conn:
            result = conn.execute(text(sql), params or {})
            row = result.fetchone()
            return dict(row._mapping) if row else None

    def query_scalar(self, sql: str, params: Optional[dict] = None) -> Any:
        """执行查询并返回首个标量值（如 COUNT(*)）。"""
        with self._engine.connect() as conn:
            result = conn.execute(text(sql), params or {})
            row = result.fetchone()
            return row[0] if row else None

    # ---------- 写入（INSERT / UPDATE / DELETE）----------

    def execute(self, sql: str, params: Optional[dict] = None) -> int:
        """执行单条非查询语句（自动提交）。

        Args:
            sql: SQL 语句
            params: 参数字典

        Returns:
            受影响行数
        """
        with self._engine.begin() as conn:
            result = conn.execute(text(sql), params or {})
            return result.rowcount or 0

    def execute_many(
        self, sql: str, params_list: Iterable[dict]
    ) -> int:
        """批量执行同一 SQL（自动提交），适合批量 INSERT/UPDATE。

        Args:
            sql: SQL 语句（带命名参数）
            params_list: 参数字典的可迭代对象

        Returns:
            总受影响行数
        """
        params_list = list(params_list)
        if not params_list:
            return 0
        with self._engine.begin() as conn:
            result = conn.execute(text(sql), params_list)
            return result.rowcount or 0

    def insert_batch(
        self, table: str, rows: list[dict], batch_size: int = 1000
    ) -> int:
        """将字典列表批量插入指定表。

        自动根据首行字典的 key 构造 INSERT 语句，分批提交以控制内存。
        语句由 SQLAlchemy Core 构造，标识符引用由编译器按方言渲染，
        因此在 MySQL 与 PostgreSQL 下均可用。

        Args:
            table: 目标表名
            rows: 行数据列表，每个元素为 {列名: 值}
            batch_size: 每批最大行数

        Returns:
            总插入行数
        """
        if not rows:
            return 0

        columns = list(rows[0].keys())
        stmt = sa_insert(
            sa_table(table, *[sa_column(column) for column in columns])
        )

        total = 0
        with self._engine.begin() as conn:
            for i in range(0, len(rows), batch_size):
                chunk = rows[i : i + batch_size]
                # 补齐缺失列，避免 KeyError
                params_chunk = [
                    {c: row.get(c) for c in columns} for row in chunk
                ]
                result = conn.execute(stmt, params_chunk)
                total += result.rowcount or len(params_chunk)
        return total

    # ---------- 事务 ----------

    @contextmanager
    def transaction(self):
        """显式事务上下文。

        在 with 块内执行多条语句，全部成功才提交，任一失败回滚。
        返回的连接对象可直接 execute(text(...), params)。

        示例:
            with db.transaction() as conn:
                conn.execute(text("INSERT ..."), {...})
                conn.execute(text("UPDATE ..."), {...})
        """
        conn = self._engine.connect()
        trans = conn.begin()
        try:
            yield conn
            trans.commit()
        except Exception:
            trans.rollback()
            raise
        finally:
            conn.close()

    # ---------- 生命周期 ----------

    def close(self) -> None:
        """释放连接池。"""
        if self._engine is not None:
            self._engine.dispose()
            self._engine = None

    def __enter__(self) -> "Database":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    def __repr__(self) -> str:
        return f"Database(url={self._url!r})"


# ==================== 模块级工厂函数 ====================


def _mask_url(url: str) -> str:
    """脱敏 URL 中的密码段，用于日志输出。"""
    if "@" in url and "://" in url:
        prefix, suffix = url.split("@", 1)
        if ":" in prefix:
            scheme_user = prefix.split(":", 1)[0]
            return f"{scheme_user}:***@{suffix}"
    return url


def get_db_url(config_name: str = "scheduler") -> str:
    """从项目配置读取数据库连接串。

    连接串统一由 Config.get_database_url() 生成，参数定义在
    Config/database.json 中。本函数保留以兼容既有任务脚本调用。

    Args:
        config_name: 数据库逻辑名，如 "scheduler" 或 "business"

    Returns:
        连接 URL 字符串
    """
    from Config import get_database_url

    return get_database_url(config_name)


def connect(
    url: Optional[str] = None,
    config_name: str = "scheduler",
    echo: bool = False,
) -> Database:
    """创建 Database 实例。

    优先使用显式传入的 url；未提供时通过 Config.get_database_url(config_name)
    从 Config/database.json 统一生成。

    Args:
        url: 数据库连接串。为 None 时从统一配置生成
        config_name: 当 url 为 None 时使用的数据库逻辑名，如 "scheduler" 或 "business"
        echo: 是否回显 SQL（调试用）

    Returns:
        Database 实例（建议用 with 语法管理生命周期）
    """
    if url is None:
        url = get_db_url(config_name)
    logger.info("连接数据库: %s", _mask_url(url))
    return Database(url, echo=echo)


# ==================== 多数据库连接 ====================


@contextmanager
def multi_connect(*db_specs):
    """一次性连接多个数据库的上下文管理器。

    所有连接在 with 块结束时统一释放，便于跨库操作。
    具体的读取、处理、写入逻辑由调用方自行实现。

    Args:
        *db_specs: 数据库规格，每个可以是:
            - str: 直接作为连接串
            - tuple (name, url): 给连接起名字，便于通过返回字典按名访问
            - dict {"name": ..., "url": ...} 或 {"name": ..., "config": ...}

    Returns:
        若所有规格都带名字，返回 dict {name: Database}；
        否则返回 list[Database]，顺序与传入规格一致。

    示例:
        # 方式 1：按位置返回（适合两个库之间迁移）
        with multi_connect(source_url, dest_url) as (src, dst):
            rows = src.query("SELECT * FROM users")
            dst.insert_batch("archived_users", rows)

        # 方式 2：按名字返回（适合多个库，避免顺序混淆）
        with multi_connect(
            ("src", source_url),
            ("dst", dest_url),
        ) as dbs:
            rows = dbs["src"].query("SELECT * FROM users")
            dbs["dst"].insert_batch("archived_users", rows)

        # 方式 3：从配置文件读取
        with multi_connect(
            {"name": "scheduler", "config": "scheduler"},
            {"name": "dwd", "url": "mysql+pymysql://..."},
        ) as dbs:
            jobs = dbs["scheduler"].query("SELECT * FROM scheduler_jobs")
            dbs["dwd"].insert_batch("job_snapshot", jobs)
    """
    # 解析规格
    specs = []
    named = True
    for spec in db_specs:
        if isinstance(spec, str):
            specs.append((None, spec))
            named = False
        elif isinstance(spec, dict):
            name = spec.get("name")
            if "url" in spec:
                specs.append((name, spec["url"]))
            elif "config" in spec:
                specs.append((name, get_db_url(spec["config"])))
            else:
                raise ValueError(
                    f"规格字典必须包含 'url' 或 'config' 字段: {spec}"
                )
        elif isinstance(spec, tuple) and len(spec) == 2:
            specs.append(spec)
        else:
            raise TypeError(
                f"不支持的规格类型 {type(spec)}，应为 str/tuple/dict"
            )

    # 创建所有连接
    dbs: list[Database] = []
    try:
        for name, url in specs:
            dbs.append(connect(url))
        # 按需返回：全命名则返回 dict，否则返回 list
        if named and len(dbs) == len(specs) and all(n for n, _ in specs):
            yield {name: db for (name, _), db in zip(specs, dbs)}
        else:
            yield dbs
    finally:
        # 统一释放所有连接池（逆序关闭）
        for db in reversed(dbs):
            db.close()


__all__ = [
    "Database",
    "connect",
    "get_db_url",
    "multi_connect",
]
