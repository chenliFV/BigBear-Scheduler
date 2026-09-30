"""动态数据表生成模块。

通过 JSON 配置文件定义表结构，自动在调度器使用的数据库中创建/更新对应的数据表。
底层使用 SQLAlchemy Core（Table/Column/MetaData）实现，与调度器自身的
SQLModel 存储后端共用同一个 engine，从而保证所有表落在同一个数据库。

设计要点:
    - 使用独立的 MetaData，不污染 SQLModel.metadata（避免与调度器内置表耦合）
    - 建表前先确保数据库存在：目标数据库不存在时自动 CREATE DATABASE
      （SQLite 数据库即文件，首次连接时自动创建，无需处理）
    - 表不存在时直接 CREATE TABLE（含所有列、约束、索引）
    - 表已存在时执行模式迁移:
        * 配置中有但数据库中没有的列 → ALTER TABLE ADD COLUMN
        * 配置中有但数据库中没有的索引 → CREATE INDEX
        * 已存在的列不修改类型（避免数据丢失，需用户手动迁移）
        * 配置中已删除的列/索引不会被删除（避免误删数据）
    - 类型、索引、默认值、注释、是否可空、主键自增均通过 JSON 字段配置
    - 每次执行产生的 DDL 变更（建库、建表、新增/删除的列与索引）会追加写入
      目标数据库中的 table_schema_change_log 表，便于事后查询与回滚
    - 当调度器未使用 SQLModel 后端（如 JSON 文件存储）时，给出清晰错误提示

JSON 配置按目标数据库拆分多份（如 business.json 声明 DWD/DWS 表、ods.json 声明
ODS 归档表），每份配置顶层的 database 字段指向 Config/database.json 中的逻辑库名；
若该逻辑库未配置，则跳过这份配置，不影响其他库的建表。项目内的批次数据归集链路
表声明位于 BatchDataCollection/tables_config/，执行时需通过 --config 指定该目录。

用法:
    from SchedulerTask.TableGenerator import generate_tables
    generate_tables(scheduler)  # 使用默认配置目录(逐份执行)
    generate_tables(scheduler, config_path="path/to/your.json")  # 单个配置文件
    generate_tables(scheduler, config_path="path/to/config_dir")  # 配置目录
"""
import json
import logging
import os
import re
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

# 独立的日志记录器：与 worker 进程的 logger 解耦
# CLI 运行时通过 _setup_cli_logging() 初始化文件+控制台 handler；
# 作为库被 worker 进程导入时，由 worker 的 root logger 接管输出。
logger = logging.getLogger("table_generator")
logger.addHandler(logging.NullHandler())


# 类型名 -> SQLAlchemy 类型类的映射（键名小写匹配，兼容用户大小写习惯）
# 在导入时构建，避免每次调用都重新构造
def _build_type_map():
    """构建类型映射表。延迟导入 sqlalchemy，避免在模块加载时强依赖。"""
    from sqlalchemy import (
        JSON,
        BigInteger,
        Boolean,
        DateTime,
        Float,
        Integer,
        LargeBinary,
        Numeric,
        String,
        Text,
    )

    # 优先使用现代 Double 类型（SQLAlchemy 1.4+），不可用则降级为 Float
    try:
        from sqlalchemy import Double as _Double
    except ImportError:  # pragma: no cover - 旧版 SQLAlchemy 兼容
        _Double = Float

    return {
        "int": Integer,
        "integer": Integer,
        "bigint": BigInteger,
        "str": String,
        "string": String,
        "varchar": String,
        "text": Text,
        "float": Float,
        "double": _Double,
        "bool": Boolean,
        "boolean": Boolean,
        "json": JSON,
        "datetime": DateTime,
        "numeric": Numeric,
        "bytes": LargeBinary,
    }


def _resolve_column_type(col_cfg: Dict[str, Any]):
    """根据列配置解析 SQLAlchemy 类型实例。

    Args:
        col_cfg: 列配置字典，需包含 type 字段

    Returns:
        SQLAlchemy 类型实例

    Raises:
        ValueError: 当 type 字段为未知类型时
    """
    from sqlalchemy import Numeric, String, Text

    type_map = _build_type_map()
    type_name = (col_cfg.get("type") or "str").lower()
    if type_name not in type_map:
        raise ValueError(
            f"未知字段类型 '{type_name}'。支持的类型: {sorted(type_map.keys())}"
        )
    type_cls = type_map[type_name]

    # String 类型需要 length 参数；未指定 length 时退化为 Text（无长度限制）
    if type_cls is String:
        length = col_cfg.get("length")
        if length is None:
            return Text()
        return String(length=int(length))

    # Numeric 支持 precision/scale
    if type_cls is Numeric:
        precision = col_cfg.get("precision")
        scale = col_cfg.get("scale")
        if precision is not None and scale is not None:
            return Numeric(precision=int(precision), scale=int(scale))
        return Numeric()

    return type_cls()


def _build_column(col_cfg: Dict[str, Any]):
    """根据单列配置创建 SQLAlchemy Column 对象。

    支持的配置字段:
        name (必填): 列名
        type (必填): 类型名，见 _build_type_map
        length: String 类型的长度（仅 type=str 时生效）
        primary_key: 是否为主键（默认 False）
        autoincrement: 是否自增（仅整数主键生效，默认 None 由数据库决定）
        nullable: 是否允许 NULL（默认 True，主键默认 False）
        unique: 是否唯一（默认 False）
        default: 默认值，字符串 "now" 表示使用数据库当前时间
        comment: 列注释
        index: 是否对该列单独建索引（默认 False）
    """
    from sqlalchemy import Column

    if "name" not in col_cfg:
        raise ValueError(f"列配置缺少 'name' 字段: {col_cfg}")

    name = col_cfg["name"]
    col_type = _resolve_column_type(col_cfg)

    kwargs: Dict[str, Any] = {}
    for key in ("primary_key", "autoincrement", "nullable", "unique", "comment", "index"):
        if key in col_cfg:
            kwargs[key] = col_cfg[key]

    # 默认值处理；字符串 "now"（不区分大小写）映射为 SQL 级 CURRENT_TIMESTAMP
    if "default" in col_cfg:
        default_val = col_cfg["default"]
        if isinstance(default_val, str) and default_val.lower() == "now":
            from sqlalchemy import func
            kwargs["default"] = func.now()
        else:
            kwargs["default"] = default_val

    return Column(name, col_type, **kwargs)


def _build_table(table_cfg: Dict[str, Any], metadata):
    """根据表配置创建 SQLAlchemy Table 对象（含索引）。

    Args:
        table_cfg: 表配置字典
        metadata: SQLAlchemy MetaData 对象，用于承载表定义

    Returns:
        Table 对象
    """
    from sqlalchemy import Index, Table

    if "name" not in table_cfg:
        raise ValueError(f"表配置缺少 'name' 字段: {table_cfg}")

    table_name = table_cfg["name"]
    columns_cfg = table_cfg.get("columns", [])
    if not columns_cfg:
        raise ValueError(f"表 '{table_name}' 没有定义任何列")

    columns = [_build_column(c) for c in columns_cfg]

    # 表级参数：表注释
    table_kwargs: Dict[str, Any] = {}
    if "comment" in table_cfg:
        table_kwargs["comment"] = table_cfg["comment"]

    table = Table(table_name, metadata, *columns, **table_kwargs)

    # 复合索引（单列索引已通过 Column(index=True) 处理）
    for idx_cfg in table_cfg.get("indexes", []):
        idx_name = idx_cfg.get("name")
        if not idx_name:
            raise ValueError(f"表 '{table_name}' 的索引配置缺少 'name'")
        idx_columns = idx_cfg.get("columns", [])
        if not idx_columns:
            raise ValueError(f"索引 '{idx_name}' 未指定任何列")
        # 校验列是否存在，给出清晰错误
        for col_name in idx_columns:
            if col_name not in table.c:
                raise ValueError(
                    f"索引 '{idx_name}' 引用了不存在的列: '{col_name}'"
                )
        idx_unique = idx_cfg.get("unique", False)
        Index(
            idx_name,
            *[table.c[c] for c in idx_columns],
            unique=idx_unique,
        )

    return table


def _get_engine_from_scheduler(scheduler):
    """从 Scheduler 实例获取数据库 engine。

    Scheduler 的存储后端保存在 self._storage 中。SQLModelStorageBackend
    暴露 self.engine 供外部使用；JSON 等其他后端没有 engine，返回 None。
    """
    storage = getattr(scheduler, "_storage", None)
    if storage is None:
        return None
    return getattr(storage, "engine", None)


def _resolve_engine(
    scheduler=None,
    engine=None,
    database_url: Optional[str] = None,
    config: Optional[Dict[str, Any]] = None,
    echo: bool = False,
):
    """按优先级解析出要使用的数据库 engine。

    优先级（从高到低）:
        1. 显式传入的 engine 参数（已经是 Engine 对象）
        2. 显式传入的 database_url 参数（连接字符串）
        3. JSON 配置文件顶层的 database_url 字段
        4. Config/database.json 中统一生成的 business 数据库 URL
        5. scheduler._storage.engine（调度器自身的存储后端）

    若 1/2/3/4 任何一种方式提供了独立的数据库，则不再依赖 scheduler，
    可以将业务表建在与调度器不同的数据库中。

    Args:
        scheduler: Scheduler 实例（可选）。仅当其他来源都未提供数据库时使用。
        engine: SQLAlchemy Engine 对象（可选，优先级最高）。
        database_url: 数据库连接字符串（可选）。
        config: 已解析的 JSON 配置字典（可选，从中读取顶层 database_url 或
            database 字段指定逻辑数据库名）。
        echo: 是否开启 SQLAlchemy echo（SQL 日志），仅对新建的 engine 生效。

    Returns:
        SQLAlchemy Engine 对象

    Raises:
        RuntimeError: 所有来源都未提供数据库
    """
    # 1. 显式传入的 engine 优先级最高
    if engine is not None:
        return engine

    # 2. 显式传入的 database_url 次之
    url = database_url
    if url is None and config is not None:
        # 3. JSON 配置文件中的 database_url
        url = config.get("database_url")

    if url:
        from sqlalchemy import create_engine

        # SQLite 需要禁用线程检查；其他数据库使用默认参数
        if url.startswith("sqlite"):
            return create_engine(url, echo=echo, connect_args={"check_same_thread": False})
        return create_engine(url, echo=echo)

    # 4. 从统一配置生成业务数据库 URL（默认 "business"）
    try:
        from sqlalchemy import create_engine

        from Config import get_database_url

        db_name = "business"
        if config is not None:
            db_name = config.get("database", "business")
        url = get_database_url(db_name)
        if url.startswith("sqlite"):
            return create_engine(
                url, echo=echo, connect_args={"check_same_thread": False}
            )
        return create_engine(url, echo=echo)
    except Exception:
        pass

    # 5. 最后回退到 scheduler 的存储后端
    if scheduler is not None:
        eng = _get_engine_from_scheduler(scheduler)
        if eng is not None:
            return eng

    raise RuntimeError(
        "未提供数据库。请通过以下任一方式指定:\n"
        "  1) 传入 engine 参数（SQLAlchemy Engine 对象）\n"
        "  2) 传入 database_url 参数（如 'sqlite:///./biz.db' 或 'mysql+pymysql://user:pwd@host/db')\n"
        "  3) 在 JSON 配置文件顶层设置 'database_url' 字段\n"
        "  4) 在 Config/database.json 中配置 business 数据库\n"
        "  5) 传入 scheduler 参数（使用调度器自身的存储后端）"
    )


# 默认配置目录名（与本模块同目录）。目录内按目标数据库拆分为多份 JSON 配置。
_DEFAULT_CONFIG_DIR_NAME = "tables_config"


def _list_config_files(config_dir: str) -> List[str]:
    """列出配置目录内的全部 JSON 配置文件，按文件名排序保证执行顺序稳定。"""
    return sorted(
        os.path.join(config_dir, name)
        for name in os.listdir(config_dir)
        if name.endswith(".json")
    )


def _is_database_configured(
    config: Dict[str, Any],
    engine=None,
    database_url: Optional[str] = None,
) -> bool:
    """判断本份配置的目标数据库是否可用。

    显式传入 engine/database_url、或配置顶层自带 database_url 时一律视为可用；
    否则要求配置顶层的 database 逻辑名已在 Config/database.json 中定义。
    未配置时返回 False，由调用方跳过该份配置，避免误建到调度器自身的库上。
    """
    if engine is not None or database_url or config.get("database_url"):
        return True
    db_name = config.get("database", "business")
    try:
        from Config import load_config

        databases = load_config("database").get("databases", {})
    except Exception:  # noqa: BLE001 - 配置缺失时按未配置处理
        return False
    return db_name in databases


def _resolve_config_files(
    config_path: Optional[str], quiet: bool = False
) -> List[str]:
    """解析配置入口，返回待处理的 JSON 配置文件列表。

    - config_path 为 None 时使用与本模块同目录的默认配置目录 tables_config/；
    - config_path 指向目录时处理目录内全部 JSON 文件（按文件名排序）；
    - config_path 指向单个文件时只处理该文件；
    - 路径不存在时返回空列表并记录日志，不抛异常。
    """
    if config_path is None:
        config_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), _DEFAULT_CONFIG_DIR_NAME
        )

    if os.path.isdir(config_path):
        config_files = _list_config_files(config_path)
        if not config_files and not quiet:
            logger.info(f"配置目录 {config_path} 内没有 JSON 配置文件，跳过动态建表。")
        return config_files

    if not os.path.exists(config_path):
        if not quiet:
            logger.info(
                f"未找到表配置文件 {config_path}，跳过动态建表。"
                "如需启用，请创建该文件或传入 config_path 参数。"
            )
        return []

    return [config_path]


# 需要在建表前"先建库"的方言（SQLite 的数据库就是文件，连接时自动创建）
_CREATE_DB_DIALECTS = ("mysql", "mariadb", "postgresql")

# 合法的字符集名（由连接串 query 提供，需过滤以避免拼进 DDL 造成注入）
_CHARSET_PATTERN = re.compile(r"^[A-Za-z0-9_]+$")


def _quote_db_identifier(db_name: str, dialect_name: str) -> str:
    """按方言对数据库名加引号并转义，避免 SQL 注入及特殊字符报错。"""
    if dialect_name in ("mysql", "mariadb"):
        return "`" + db_name.replace("`", "``") + "`"
    return '"' + db_name.replace('"', '""') + '"'


def _server_url(url, database: Optional[str]):
    """基于目标数据库的 URL 生成"只连服务器"的 URL（可指定维护库或留空）。

    注意: URL.set() 会忽略 None，无法用于清空 database 字段，故用 URL.create。
    """
    from sqlalchemy.engine import URL

    return URL.create(
        url.drivername,
        url.username,
        url.password,
        url.host,
        url.port,
        database=database,
        query=dict(url.query),
    )


def _build_create_database_sql(engine, db_name: str) -> str:
    """生成 CREATE DATABASE 语句。

    MySQL/MariaDB 会带上连接串中指定的字符集（默认 utf8mb4），
    保证新建库可以存放中文与 emoji；PostgreSQL 使用数据库默认编码。

    Args:
        engine: 目标数据库的 engine（用于取方言与连接串参数）
        db_name: 待创建的数据库名

    Returns:
        CREATE DATABASE 语句字符串
    """
    dialect_name = engine.dialect.name
    quoted = _quote_db_identifier(db_name, dialect_name)

    if dialect_name in ("mysql", "mariadb"):
        charset = engine.url.query.get("charset") or "utf8mb4"
        if not _CHARSET_PATTERN.match(charset):
            charset = "utf8mb4"
        return f"CREATE DATABASE {quoted} CHARACTER SET {charset}"

    return f"CREATE DATABASE {quoted}"


# 并发幂等保护：多个实例（如多 worker 进程 / 多副本）可能在同一时刻执行
# 相同的 DDL。后到的实例会收到数据库抛出的"对象已存在"（创建类）或
# "对象不存在"（删除类）错误——这类错误说明另一实例已完成同一变更，
# 属于良性幂等竞态，应安全忽略而不是记为失败。
#
# 判定优先级：驱动错误码 > 错误文案。
#   - MySQL/MariaDB: pymysql 的错误码为整数（exc.orig.args[0]）
#   - PostgreSQL:    psycopg2 暴露 5 位 SQLSTATE 字符串（exc.orig.pgcode）
#   - 其他方言:      退化为错误文案匹配（SQLite 无可区分的错误码）
_BENIGN_CREATE_ERRNOS = frozenset({1007, 1022, 1050, 1060, 1061, 1826})
_BENIGN_DROP_ERRNOS = frozenset({1091})

# PostgreSQL 的 SQLSTATE；比文案可靠，且不受服务端 lc_messages 本地化影响
_BENIGN_CREATE_SQLSTATES = frozenset({
    "42P04",  # duplicate_database
    "42P06",  # duplicate_schema
    "42P07",  # duplicate_table（索引在 PG 中亦为 relation）
    "42701",  # duplicate_column
    "42710",  # duplicate_object
})
_BENIGN_DROP_SQLSTATES = frozenset({
    "42703",  # undefined_column
    "42704",  # undefined_object（索引不存在）
    "42P01",  # undefined_table
})

# 每个元素是一组"必须同时出现"的子串；单元素组即普通子串匹配。
# 删除类用"对象关键字 + does not exist"限定，避免误吞 type/schema 等真实故障。
_BENIGN_CREATE_PATTERNS = (
    ("already exists",),        # 通用：表/索引/库/列已存在
    ("duplicate column",),      # MySQL 1060
    ("duplicate key",),         # MySQL 1061 / 1022
    ("duplicate object",),      # PostgreSQL 42710
)
_BENIGN_DROP_PATTERNS = (
    ("check that column/key exists",),  # MySQL 1091（errno 之外的兜底）
    ("no such index",),                 # SQLite：索引不存在
    ("no such column",),                # SQLite：列不存在
    ("index", "does not exist"),        # PostgreSQL：索引不存在
    ("column", "does not exist"),       # PostgreSQL：列不存在
)


def _is_benign_ddl_race(exc: BaseException, dropping: bool = False) -> bool:
    """判断 DDL 异常是否属于"并发下另一实例已完成同一变更"的幂等竞态。

    Args:
        exc: DDL 执行时捕获到的异常
        dropping: True 表示当前是删除类操作（DROP），False 为创建类操作

    Returns:
        True 表示可安全忽略（按"已由其他实例完成"处理）
    """
    errnos = _BENIGN_DROP_ERRNOS if dropping else _BENIGN_CREATE_ERRNOS
    sqlstates = _BENIGN_DROP_SQLSTATES if dropping else _BENIGN_CREATE_SQLSTATES
    patterns = _BENIGN_DROP_PATTERNS if dropping else _BENIGN_CREATE_PATTERNS

    orig = getattr(exc, "orig", None) or exc

    # 1) MySQL/MariaDB：驱动错误码为整数（最可靠，且不受文案本地化影响）
    args = getattr(orig, "args", ())
    if args and isinstance(args[0], int) and args[0] in errnos:
        return True

    # 2) PostgreSQL：5 位 SQLSTATE 字符串（psycopg2 的 pgcode / diag.sqlstate）
    #    避免服务端 lc_messages 本地化导致英文文案匹配失效而漏判
    sqlstate = getattr(orig, "pgcode", None) or getattr(
        getattr(orig, "diag", None), "sqlstate", None
    )
    if isinstance(sqlstate, str) and sqlstate in sqlstates:
        return True

    # 3) 退化为错误文案匹配（SQLite，或文案为英文的 PG/MySQL）
    #    只匹配驱动原始文案（str(orig)），不匹配 str(exc)——后者会带上 SQL
    #    语句本身（如 "ALTER TABLE t DROP COLUMN c"），其中的关键字会污染判定。
    message = str(orig).lower()
    return any(all(part in message for part in group) for group in patterns)


def _ensure_database_exists(engine, quiet: bool = False) -> Optional[str]:
    """确保 engine 指向的数据库存在，不存在则先创建该数据库。

    - SQLite: 数据库即文件，首次连接时自动创建，无需处理
    - MySQL/MariaDB: 以不带库名的方式连接服务器，执行
      CREATE DATABASE `<name>` CHARACTER SET <charset>
    - PostgreSQL: 连接维护库 postgres，执行 CREATE DATABASE "<name>"
      （CREATE DATABASE 不能在事务块中执行，故使用 AUTOCOMMIT 隔离级别）

    Args:
        engine: 目标数据库的 SQLAlchemy Engine
        quiet: 是否抑制 info 级别日志（错误日志仍会输出）

    Returns:
        本次新建的数据库名；数据库已存在或无需建库时返回 None

    Raises:
        RuntimeError: 已确认数据库不存在，但自动创建失败
    """
    from sqlalchemy import create_engine, text

    dialect_name = engine.dialect.name
    db_name = engine.url.database

    # SQLite（文件自动创建）、URL 未指定库名、其他方言 → 不做处理
    if dialect_name not in _CREATE_DB_DIALECTS or not db_name:
        if not quiet:
            logger.debug(
                f"跳过建库检查（方言={dialect_name}, 目标库={db_name!r}）: "
                f"SQLite 的库文件由驱动自动创建，或连接串未指定库名/方言不支持"
            )
        return None

    # 构造"只连服务器、不指定目标库"的 engine
    if dialect_name == "postgresql":
        server_url = _server_url(engine.url, "postgres")
        check_sql = "SELECT 1 FROM pg_database WHERE datname = :name"
    else:
        server_url = _server_url(engine.url, None)
        check_sql = (
            "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA "
            "WHERE SCHEMA_NAME = :name"
        )

    if not quiet:
        logger.debug(
            f"检查数据库 '{db_name}' 是否存在（方言={dialect_name}, 服务器="
            f"{_mask_url(server_url.render_as_string(hide_password=False))}）"
        )

    # 存在性检查与建库复用同一个 AUTOCOMMIT 连接，减少一次连接开销
    # （CREATE DATABASE 不能在事务块中执行，故 server 连接整体使用 AUTOCOMMIT）
    server_engine = create_engine(server_url).execution_options(
        isolation_level="AUTOCOMMIT"
    )
    try:
        # --- 1. 建立服务器连接 ---
        # 建连失败（服务器不可达/认证失败）与查询失败同样视为"无法确认"，
        # 仅告警放行，交由后续建表阶段以连接错误的形式暴露
        try:
            conn = server_engine.connect()
        except Exception as e:
            if not quiet:
                logger.warning(f"无法确认数据库 '{db_name}' 是否存在: {e}")
            return None

        with conn:
            # --- 2. 检查目标数据库是否已存在 ---
            try:
                exists = (
                    conn.execute(text(check_sql), {"name": db_name}).first()
                    is not None
                )
            except Exception as e:
                # 无法确认（如权限不足）→ 不阻断流程，交由建表阶段报错
                if not quiet:
                    logger.warning(f"无法确认数据库 '{db_name}' 是否存在: {e}")
                return None

            if exists:
                if not quiet:
                    logger.debug(f"数据库 '{db_name}' 已存在，无需创建")
                return None

            # --- 3. 不存在 → 创建数据库 ---
            create_sql = _build_create_database_sql(engine, db_name)
            if not quiet:
                logger.info(f"数据库 '{db_name}' 不存在，正在创建: {create_sql}")
            try:
                conn.execute(text(create_sql))
            except Exception as e:
                # 并发建库竞态：另一实例已抢先创建 → 视为成功
                if _is_benign_ddl_race(e):
                    if not quiet:
                        logger.info(
                            f"数据库 '{db_name}' 已由其他实例创建，跳过"
                        )
                    return db_name
                # 建库失败属于必须暴露的严重错误，不受 quiet 抑制
                logger.error(f"数据库 '{db_name}' 不存在，且自动创建失败: {e}")
                raise RuntimeError(
                    f"数据库 '{db_name}' 不存在，且自动创建失败: {e}\n"
                    f"请确认账号有建库权限，或手动执行: {create_sql}"
                ) from e

            if not quiet:
                logger.info(f"数据库不存在，已自动创建: {db_name} ({create_sql})")
            return db_name
    finally:
        server_engine.dispose()


def _compile_default_expr(col, dialect):
    """把 Column 的默认值（server_default 或 default）编译为可放入 SQL 的字面字符串。

    支持两类默认值:
        - SQL 函数表达式（如 func.now()）：用 SQLAlchemy 编译器编译
        - Python 字面量（str/int/float/bool）：转成 SQL 字面量

    Args:
        col: SQLAlchemy Column 对象
        dialect: 数据库方言

    Returns:
        编译后的 SQL 字符串，若无默认值返回 None
    """
    default_arg = None
    if col.server_default is not None:
        # server_default 可能是 DefaultClause 包装
        default_arg = getattr(col.server_default, "arg", col.server_default)
    elif col.default is not None:
        default_arg = getattr(col.default, "arg", col.default)

    if default_arg is None:
        return None

    # SQL 表达式（如 func.now()）通过编译器输出该方言下的 SQL
    if hasattr(default_arg, "compile"):
        return default_arg.compile(
            dialect=dialect, compile_kwargs={"literal_binds": True}
        ).string

    # Python 字面量
    if isinstance(default_arg, bool):
        # SQLite 没有 TRUE/FALSE 字面量，用 1/0
        return "1" if dialect.name == "sqlite" else ("TRUE" if default_arg else "FALSE")
    if isinstance(default_arg, (int, float)):
        return str(default_arg)
    if isinstance(default_arg, str):
        # 转义单引号避免注入
        escaped = default_arg.replace("'", "''")
        return f"'{escaped}'"
    if default_arg is None:
        return "NULL"
    return str(default_arg)


def _widen_column_sql(table_name, col, col_info, dialect):
    """为"Integer 扩宽为 BigInteger"生成方言相关的 ALTER 语句.

    仅处理无损扩宽: 配置声明为 bigint 而库中现列为普通 int. 收窄或
    int/str 互转等有数据丢失风险的变更不在此列, 仍需人工迁移.

    Args:
        table_name: 表名.
        col: 配置构建的 SQLAlchemy Column 对象(目标类型 BigInteger).
        col_info: inspector 反射得到的现有列信息字典(含 type/nullable).
        dialect: 目标数据库方言.

    Returns:
        ALTER 语句文本; 方言不支持时返回 None(调用方跳过并告警).
    """
    from sqlalchemy import BigInteger, Integer

    # 目标必须是 BigInteger, 现列必须是"非 BigInteger 的 Integer", 其余组合不处理.
    if not isinstance(col.type, BigInteger):
        return None
    if not isinstance(col_info["type"], Integer) or isinstance(
        col_info["type"], BigInteger
    ):
        return None

    if dialect.name in ("mysql", "mariadb"):
        # MySQL 的 MODIFY 会整体重建列定义, 必须带上原空值约束与注释,
        # 否则 NOT NULL/COMMENT 会在修改类型时被静默丢弃.
        parts = [
            f"ALTER TABLE {table_name}",
            f"MODIFY COLUMN {col.name} {col.type.compile(dialect)}",
        ]
        parts.append("NOT NULL" if not col_info["nullable"] else "NULL")
        if col.comment:
            escaped = col.comment.replace("'", "''")
            parts.append(f"COMMENT '{escaped}'")
        return " ".join(parts)
    if dialect.name == "postgresql":
        # PG 的 TYPE 变更不改动空值约束, 无需补 NOT NULL.
        return (
            f"ALTER TABLE {table_name} "
            f"ALTER COLUMN {col.name} TYPE {col.type.compile(dialect)}"
        )
    # SQLite 等动态类型方言无 int 宽度语义, 扩宽无意义.
    return None


def _migrate_table(
    table,
    engine,
    quiet: bool = False,
    drop_missing: bool = False,
) -> Tuple[List[str], List[str], List[str], List[str], List[str]]:
    """对已存在的表执行模式迁移：添加缺失的列和索引，可选删除多余项。

    迁移策略:
        - 配置中存在但数据库中缺失的列 → ALTER TABLE ADD COLUMN
        - 配置声明 bigint 而库中为 int 的列 → 无损扩宽 ALTER TABLE
          (MODIFY COLUMN / ALTER COLUMN TYPE); 其余类型变更不做, 需人工迁移
        - 配置中存在但数据库中缺失的索引 → CREATE INDEX
        - 配置中已删除的列/索引:
            * drop_missing=False（默认）→ 保留不动，避免误删
            * drop_missing=True → ALTER TABLE DROP COLUMN / DROP INDEX

    ALTER TABLE ADD COLUMN 注意事项:
        - SQLite/MySQL/PG 都支持 ADD COLUMN
        - NOT NULL 新列必须有 DEFAULT，否则已有行无法填充 → 自动改为 nullable
        - PRIMARY KEY/UNIQUE 约束无法在 ALTER TABLE 中安全添加 → 跳过并警告

    ALTER TABLE DROP COLUMN 注意事项:
        - SQLite 3.35+ 才支持 DROP COLUMN，旧版会失败（捕获并跳过）
        - MySQL 8.0+ 支持；PostgreSQL 支持
        - 主键列、被索引/外键依赖的列可能 DROP 失败 → 捕获并报错
        - 删除列前会先尝试删除依赖该列的索引，降低失败概率

    DROP INDEX 注意事项:
        - SQLite/PostgreSQL: `DROP INDEX <name>`
        - MySQL: `DROP INDEX <name> ON <table>`
        - 主键索引、唯一约束创建的索引通常不在 inspector.get_indexes() 中
          不会被误删

    Args:
        table: SQLAlchemy Table 对象（来自配置构建）
        engine: 数据库 engine
        quiet: 是否抑制 info 级别日志
        drop_missing: 是否删除配置中已不存在的列和索引（默认 False）

    Returns:
        (added_columns, added_indexes, dropped_columns, dropped_indexes,
         widened_columns) 元组
    """
    from sqlalchemy import inspect, text

    inspector = inspect(engine)
    table_name = table.name
    dialect = engine.dialect

    added_columns: List[str] = []
    added_indexes: List[str] = []
    dropped_columns: List[str] = []
    dropped_indexes: List[str] = []
    widened_columns: List[str] = []

    # 配置中定义的列名集合（小写，用于比较）
    configured_col_names = {c.name.lower() for c in table.columns}
    # 配置中定义的索引名集合
    configured_idx_names = {idx.name for idx in table.indexes}

    # --- 1. 反射现有列，比较后 ADD COLUMN ---
    try:
        existing_cols_info = inspector.get_columns(table_name)
        existing_cols = {c["name"].lower() for c in existing_cols_info}
    except Exception as e:
        logger.error(f"反射表 '{table_name}' 列信息失败: {e}")
        existing_cols_info = []
        existing_cols = set()

    new_cols = [c for c in table.columns if c.name.lower() not in existing_cols]

    if new_cols:
        # 复用单个连接批量执行，减少反复建连/开事务的开销；
        # 每条 DDL 独立提交，保留"单条失败不影响其余列"的容错能力
        with engine.connect() as conn:
            for col in new_cols:
                try:
                    type_str = col.type.compile(dialect)
                    sql_parts = [
                        f"ALTER TABLE {table_name}",
                        f"ADD COLUMN {col.name} {type_str}",
                    ]

                    # 默认值：用编译后的字面 SQL
                    default_sql = _compile_default_expr(col, dialect)
                    if default_sql is not None:
                        sql_parts.append(f"DEFAULT {default_sql}")

                    # NOT NULL 仅在已有 DEFAULT 时添加，否则已有行无法填充
                    if not col.nullable:
                        if default_sql is not None:
                            sql_parts.append("NOT NULL")
                        elif not quiet:
                            logger.warning(
                                f"表 '{table_name}' 新增 NOT NULL 列 '{col.name}' "
                                f"未指定 default，将允许 NULL 以避免迁移失败"
                            )

                    # 主键/唯一约束在 ALTER TABLE 中难以安全添加，跳过
                    if col.primary_key and not quiet:
                        logger.warning(
                            f"表 '{table_name}' 新增列 '{col.name}' 声明为 primary_key，"
                            f"ALTER TABLE 不支持添加主键，已跳过该约束"
                        )
                    if col.unique and not quiet:
                        logger.warning(
                            f"表 '{table_name}' 新增列 '{col.name}' 声明为 unique，"
                            f"ALTER TABLE 添加唯一约束可能失败，将尝试"
                        )

                    sql = " ".join(sql_parts)
                    conn.execute(text(sql))
                    conn.commit()
                    added_columns.append(col.name)
                    if not quiet:
                        logger.info(
                            f"表 '{table_name}' 新增列: {col.name} ({type_str})"
                        )
                except Exception as e:
                    conn.rollback()
                    if _is_benign_ddl_race(e):
                        # 并发竞态：另一实例已添加该列 → 视为成功
                        added_columns.append(col.name)
                        if not quiet:
                            logger.info(
                                f"表 '{table_name}' 新增列 '{col.name}' "
                                f"已由其他实例完成，跳过"
                            )
                    else:
                        logger.error(
                            f"表 '{table_name}' 新增列 '{col.name}' 失败: {e}"
                        )

    # --- 1.5 已存在列的无损类型扩宽(int -> bigint) ---
    # 放在 ADD COLUMN 之后: 新加的列按配置类型直接创建, 无需扩宽.
    # MySQL 的 MODIFY / PG 的 TYPE 变更均为在线 DDL, 对已有数据只做
    # 无损扩宽, 不触碰默认值/主键/唯一约束.
    existing_col_info_map = {
        c["name"].lower(): c for c in existing_cols_info
    }
    widen_candidates = [
        (col, existing_col_info_map[col.name.lower()])
        for col in table.columns
        if col.name.lower() in existing_col_info_map
    ]
    if widen_candidates:
        with engine.connect() as conn:
            for col, col_info in widen_candidates:
                sql = _widen_column_sql(table_name, col, col_info, dialect)
                if sql is None:
                    continue
                try:
                    conn.execute(text(sql))
                    conn.commit()
                    widened_columns.append(col.name)
                    if not quiet:
                        logger.info(
                            f"表 '{table_name}' 列 '{col.name}' 类型扩宽: "
                            f"int -> bigint"
                        )
                except Exception as e:
                    conn.rollback()
                    if _is_benign_ddl_race(e):
                        widened_columns.append(col.name)
                        if not quiet:
                            logger.info(
                                f"表 '{table_name}' 列 '{col.name}' 的扩宽 "
                                f"已由其他实例完成，跳过"
                            )
                    else:
                        logger.error(
                            f"表 '{table_name}' 列 '{col.name}' 类型扩宽失败: {e}"
                        )

    # --- 2. 反射现有索引，比较后 CREATE INDEX ---
    try:
        existing_idxs_info = inspector.get_indexes(table_name)
        existing_idxs = {i["name"] for i in existing_idxs_info}
    except Exception as e:
        logger.error(f"反射表 '{table_name}' 索引信息失败: {e}")
        existing_idxs_info = []
        existing_idxs = set()

    missing_indexes = [idx for idx in table.indexes if idx.name not in existing_idxs]
    if missing_indexes:
        # 复用单个连接批量建索引，减少反复建连/开事务的开销
        with engine.connect() as conn:
            for idx in missing_indexes:
                try:
                    col_names = [c.name for c in idx.columns]
                    cols_str = ", ".join(col_names)
                    unique_str = "UNIQUE " if idx.unique else ""
                    sql = (
                        f"CREATE {unique_str}INDEX {idx.name} "
                        f"ON {table_name} ({cols_str})"
                    )
                    conn.execute(text(sql))
                    conn.commit()
                    added_indexes.append(idx.name)
                    if not quiet:
                        logger.info(
                            f"表 '{table_name}' 新增索引: {idx.name} ({cols_str})"
                        )
                except Exception as e:
                    conn.rollback()
                    if _is_benign_ddl_race(e):
                        # 并发竞态：另一实例已创建该索引 → 视为成功
                        added_indexes.append(idx.name)
                        if not quiet:
                            logger.info(
                                f"表 '{table_name}' 新增索引 '{idx.name}' "
                                f"已由其他实例完成，跳过"
                            )
                    else:
                        logger.error(
                            f"表 '{table_name}' 新增索引 '{idx.name}' 失败: {e}"
                        )

    # --- 3. 可选：删除配置中已不存在的索引 ---
    # 放在 DROP COLUMN 之前：先删除依赖被删列的索引，避免 DROP COLUMN 失败
    if drop_missing:
        stale_idx_names = [
            i["name"] for i in existing_idxs_info
            if i["name"] not in configured_idx_names
        ]
        if stale_idx_names:
            # 复用单个连接批量删索引，减少反复建连/开事务的开销
            with engine.connect() as conn:
                for idx_name in stale_idx_names:
                    try:
                        # MySQL 语法需要 ON <table>；SQLite/PG 用 `DROP INDEX <name>`
                        if dialect.name == "mysql":
                            sql = f"DROP INDEX {idx_name} ON {table_name}"
                        else:
                            sql = f"DROP INDEX {idx_name}"
                        conn.execute(text(sql))
                        conn.commit()
                        dropped_indexes.append(idx_name)
                        if not quiet:
                            logger.info(
                                f"表 '{table_name}' 删除索引: {idx_name}"
                            )
                    except Exception as e:
                        conn.rollback()
                        if _is_benign_ddl_race(e, dropping=True):
                            # 并发竞态：另一实例已删除该索引 → 视为成功
                            dropped_indexes.append(idx_name)
                            if not quiet:
                                logger.info(
                                    f"表 '{table_name}' 删除索引 '{idx_name}' "
                                    f"已由其他实例完成，跳过"
                                )
                        else:
                            logger.error(
                                f"表 '{table_name}' 删除索引 '{idx_name}' 失败: {e}"
                            )

    # --- 4. 可选：删除配置中已不存在的列 ---
    if drop_missing:
        # 跳过主键列，避免破坏表结构
        try:
            pk_info = inspector.get_pk_constraint(table_name)
            pk_cols = {c.lower() for c in pk_info.get("constrained_columns", [])}
        except Exception:
            pk_cols = set()

        # 先筛出可删除的列（排除配置中的列与主键列），再复用单连接批量删除
        droppable_columns: List[str] = []
        for col_info in existing_cols_info:
            col_name = col_info["name"]
            if col_name.lower() in configured_col_names:
                continue
            if col_name.lower() in pk_cols:
                if not quiet:
                    logger.warning(
                        f"表 '{table_name}' 跳过删除主键列 '{col_name}'"
                    )
                continue
            droppable_columns.append(col_name)

        if droppable_columns:
            # 复用单个连接批量删列，减少反复建连/开事务的开销
            with engine.connect() as conn:
                for col_name in droppable_columns:
                    try:
                        sql = f"ALTER TABLE {table_name} DROP COLUMN {col_name}"
                        conn.execute(text(sql))
                        conn.commit()
                        dropped_columns.append(col_name)
                        if not quiet:
                            logger.info(
                                f"表 '{table_name}' 删除列: {col_name}"
                            )
                    except Exception as e:
                        # 旧版 SQLite、依赖关系等都可能导致失败
                        conn.rollback()
                        if _is_benign_ddl_race(e, dropping=True):
                            # 并发竞态：另一实例已删除该列 → 视为成功
                            dropped_columns.append(col_name)
                            if not quiet:
                                logger.info(
                                    f"表 '{table_name}' 删除列 '{col_name}' "
                                    f"已由其他实例完成，跳过"
                                )
                        else:
                            logger.error(
                                f"表 '{table_name}' 删除列 '{col_name}' 失败: {e}。"
                                f"该列可能被索引/外键依赖，或数据库不支持 DROP COLUMN"
                            )

    return (
        added_columns, added_indexes, dropped_columns, dropped_indexes,
        widened_columns,
    )


# ==================== 变更记录表 ====================
# 每次执行 generate_tables 产生的 DDL 操作（建库/建表/新增或删除列与索引）
# 都会写入目标数据库中的 table_schema_change_log 表，便于事后查询与回滚。

_CHANGE_LOG_TABLE_NAME = "table_schema_change_log"

# 操作类型
_OP_CREATE_DATABASE = "create_database"
_OP_CREATE_TABLE = "create_table"
_OP_ADD_COLUMN = "add_column"
_OP_DROP_COLUMN = "drop_column"
_OP_ADD_INDEX = "add_index"
_OP_DROP_INDEX = "drop_index"
_OP_MODIFY_COLUMN = "modify_column"
_OP_TABLE_ERROR = "error"


def _build_change_log_table(metadata):
    """构建变更记录表的 Table 定义。

    记录表是一张库内固定表，字段固定，不参与配置驱动的迁移。
    """
    from sqlalchemy import Boolean, Column, DateTime, Integer, String, Table, Text

    return Table(
        _CHANGE_LOG_TABLE_NAME,
        metadata,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column(
            "batch_id", String(36), nullable=False, index=True,
            comment="本次执行批次标识（UUID4），同一次执行的所有变更共享",
        ),
        Column("executed_at", DateTime, nullable=False, comment="变更执行时间"),
        Column("database_name", String(255), comment="目标数据库名"),
        Column("table_name", String(255), comment="目标表名（建库事件为空）"),
        Column(
            "operation", String(32), nullable=False,
            comment="create_database/create_table/add_column/drop_column/"
                    "add_index/drop_index/modify_column/error",
        ),
        Column("object_type", String(16), comment="对象类型: database/table/column/index"),
        Column("object_name", String(255), comment="对象名（库名/表名/列名/索引名）"),
        Column("ddl_sql", Text, comment="对应的 DDL 语句（由配置重建，供回滚参考）"),
        Column("success", Boolean, nullable=False, default=True, comment="是否执行成功"),
        Column("note", Text, comment="备注（失败原因、随表创建等）"),
        Column("operator", String(64), comment="执行者（操作系统用户名）"),
    )


def _current_operator() -> Optional[str]:
    """返回当前执行者（操作系统用户名），取不到时返回 None。"""
    for key in ("USER", "USERNAME", "LOGNAME"):
        value = os.environ.get(key)
        if value:
            return value
    return None


def _render_change_ddl(dialect, table_name, operation, object_name, table=None) -> Optional[str]:
    """按操作类型重建 DDL 语句文本（best-effort，仅用于记录与回滚参考）。

    无法可靠重建时返回 None（例如配置中已找不到对应列/索引）。
    """
    if operation == _OP_ADD_COLUMN:
        col = table.columns.get(object_name) if table is not None else None
        if col is None:
            return None
        sql = (
            f"ALTER TABLE {table_name} ADD COLUMN {object_name} "
            f"{col.type.compile(dialect)}"
        )
        default_sql = _compile_default_expr(col, dialect)
        if default_sql is not None:
            sql += f" DEFAULT {default_sql}"
        return sql
    if operation == _OP_DROP_COLUMN:
        return f"ALTER TABLE {table_name} DROP COLUMN {object_name}"
    if operation == _OP_ADD_INDEX:
        idx = None
        if table is not None:
            idx = next((i for i in table.indexes if i.name == object_name), None)
        if idx is None:
            return None
        cols_str = ", ".join(c.name for c in idx.columns)
        unique_str = "UNIQUE " if idx.unique else ""
        return f"CREATE {unique_str}INDEX {object_name} ON {table_name} ({cols_str})"
    if operation == _OP_DROP_INDEX:
        if dialect.name == "mysql":
            return f"DROP INDEX {object_name} ON {table_name}"
        return f"DROP INDEX {object_name}"
    if operation == _OP_MODIFY_COLUMN:
        # 扩宽类修改可从配置可靠重建; 其他修改暂不渲染, 返回 None.
        if table is not None and object_name in table.columns:
            col = table.columns[object_name]
            return (
                f"ALTER TABLE {table_name} MODIFY COLUMN {object_name} "
                f"{col.type.compile(dialect)}"
            )
        return None
    return None


def _record_migration(recorder, table, dialect, added_cols, added_idxs,
                      dropped_cols, dropped_idxs, widened_cols) -> None:
    """把一次模式迁移的增删改结果逐条写入变更记录器。

    记录顺序与 _migrate_table 的实际执行顺序保持一致：
    新增列 → 类型扩宽 → 新增索引 → 删除索引 → 删除列。
    """
    for operation, object_type, names in (
        (_OP_ADD_COLUMN, "column", added_cols),
        (_OP_MODIFY_COLUMN, "column", widened_cols),
        (_OP_ADD_INDEX, "index", added_idxs),
        (_OP_DROP_INDEX, "index", dropped_idxs),
        (_OP_DROP_COLUMN, "column", dropped_cols),
    ):
        for name in names:
            recorder.record(
                operation,
                table_name=table.name,
                object_type=object_type,
                object_name=name,
                ddl_sql=_render_change_ddl(
                    dialect, table.name, operation, name, table
                ),
            )


def _record_table_created(recorder, table, engine) -> None:
    """记录一次完整建表：一条建表记录，外加每个列/索引各一条新增记录。"""
    from sqlalchemy.schema import CreateTable

    recorder.record(
        _OP_CREATE_TABLE,
        table_name=table.name,
        object_type="table",
        object_name=table.name,
        ddl_sql=str(CreateTable(table).compile(dialect=engine.dialect)),
    )
    for col in table.columns:
        recorder.record(
            _OP_ADD_COLUMN,
            table_name=table.name,
            object_type="column",
            object_name=col.name,
            note="随新建表创建",
        )
    for idx in table.indexes:
        # 索引无法从建表 DDL 中还原（CreateTable 不输出独立索引），
        # 因此这里补记 CREATE INDEX 语句，供后续 drop 后回滚使用。
        recorder.record(
            _OP_ADD_INDEX,
            table_name=table.name,
            object_type="index",
            object_name=idx.name,
            ddl_sql=_render_change_ddl(
                engine.dialect, table.name, _OP_ADD_INDEX, idx.name, table
            ),
            note="随新建表创建",
        )


class _SchemaChangeRecorder:
    """收集本次执行的 DDL 变更，最后批量写入变更记录表。

    记录写入属于旁路能力：任何写入失败（无建表权限、库不存在等）只记录
    ERROR 日志，绝不中断建表/迁移主流程。
    """

    def __init__(self, engine, database_name: Optional[str] = None):
        self.engine = engine
        self.database_name = database_name
        self.batch_id = str(uuid.uuid4())
        self.operator = _current_operator()
        self._rows: List[Dict[str, Any]] = []

    def record(
        self,
        operation: str,
        *,
        table_name: Optional[str] = None,
        object_type: Optional[str] = None,
        object_name: Optional[str] = None,
        ddl_sql: Optional[str] = None,
        success: bool = True,
        note: Optional[str] = None,
    ) -> None:
        """缓存一条变更记录，待 flush() 时统一落库。"""
        self._rows.append(
            {
                "batch_id": self.batch_id,
                "executed_at": datetime.now(),
                "database_name": self.database_name,
                "table_name": table_name,
                "operation": operation,
                "object_type": object_type,
                "object_name": object_name,
                "ddl_sql": ddl_sql,
                "success": success,
                "note": note,
                "operator": self.operator,
            }
        )

    def flush(self) -> None:
        """把缓存的变更记录写入记录表（无记录时不做任何事）。"""
        if not self._rows:
            return
        rows, self._rows = self._rows, []

        from sqlalchemy import MetaData

        metadata = MetaData()
        table = _build_change_log_table(metadata)
        try:
            table.create(bind=self.engine, checkfirst=True)
        except Exception as e:
            # 并发竞态：另一实例已创建记录表 → 继续写入即可
            if not _is_benign_ddl_race(e):
                logger.error(
                    f"创建变更记录表 '{_CHANGE_LOG_TABLE_NAME}' 失败，"
                    f"本次 {len(rows)} 条 DDL 变更未记录: {e}"
                )
                return
        try:
            with self.engine.begin() as conn:
                conn.execute(table.insert(), rows)
        except Exception as e:
            logger.error(
                f"写入变更记录表 '{_CHANGE_LOG_TABLE_NAME}' 失败，"
                f"本次 {len(rows)} 条 DDL 变更未记录: {e}"
            )


def generate_tables(
    scheduler=None,
    config_path: Optional[str] = None,
    quiet: bool = False,
    drop_missing: Optional[bool] = None,
    engine=None,
    database_url: Optional[str] = None,
    echo: bool = False,
    create_database: Optional[bool] = None,
) -> List[str]:
    """根据 JSON 配置文件创建或更新数据表。

    支持多种数据库来源，按优先级（从高到低）选用:
        1. engine 参数（SQLAlchemy Engine 对象）
        2. database_url 参数（连接字符串）
        3. JSON 配置文件顶层 "database_url" 字段
        4. Config/database.json 中统一生成的 business 数据库 URL
        5. scheduler._storage.engine（调度器自身的存储后端）

    当通过 1/2/3/4 提供独立数据库时，scheduler 参数可省略，
    业务表可以建在与调度器不同的数据库中。

    流程:
        1. 读取并解析 JSON 配置文件
        2. 按优先级解析数据库 engine
        3. 数据库不存在时先创建数据库（create_database 控制）
        4. 用独立 MetaData 根据配置构建 Table 对象
        5. 对每张表:
           - 表不存在 → CREATE TABLE（含所有列、约束、索引）
           - 表已存在 → 模式迁移:
             * 添加缺失的列和索引
             * 根据 drop_missing 决定是否删除配置中已不存在的列和索引

    drop_missing 优先级:
        1. 显式传入的函数参数（最高）
        2. JSON 配置文件顶层 "drop_missing" 字段
        3. 默认 False（不删除，避免误删数据）

    create_database 优先级:
        1. 显式传入的函数参数（最高）
        2. JSON 配置文件顶层 "create_database" 字段
        3. 默认 True（数据库不存在时自动创建）

    Args:
        scheduler: Scheduler 实例（可选）。仅当未提供 engine/database_url 时使用。
        config_path: 配置入口。为 None 时使用同目录的 tables_config/ 配置目录；
            传入目录时逐份执行目录内全部 JSON 配置；传入单个文件时只执行该文件。
        quiet: 是否抑制 info 级别日志（错误日志仍会输出）
        drop_missing: 是否删除配置中已不存在的列和索引。
            None 表示从 JSON 配置读取，未配置则默认 False。
        engine: SQLAlchemy Engine 对象（优先级最高的数据库来源）。
        database_url: 数据库连接字符串（如 'sqlite:///./biz.db'）。
        echo: 是否开启 SQLAlchemy echo（SQL 日志），仅对内部新建的 engine 生效。
        create_database: 数据库不存在时是否自动创建（MySQL/PostgreSQL 有效，
            SQLite 的文件由驱动自动创建）。None 表示从 JSON 配置读取，默认 True。

    Returns:
        处理过的表名列表（多份配置时为各份配置表名的并集）

    Raises:
        RuntimeError: 未提供任何数据库来源，或数据库不存在且自动创建失败
        FileNotFoundError: 配置文件不存在
        ValueError: 配置文件格式错误（缺字段、未知类型等）
    """
    processed_tables: List[str] = []
    for one_config_path in _resolve_config_files(config_path, quiet=quiet):
        processed_tables.extend(
            _apply_table_config(
                one_config_path,
                scheduler=scheduler,
                quiet=quiet,
                drop_missing=drop_missing,
                engine=engine,
                database_url=database_url,
                echo=echo,
                create_database=create_database,
            )
        )
    return processed_tables


def _apply_table_config(
    config_path: str,
    scheduler=None,
    quiet: bool = False,
    drop_missing: Optional[bool] = None,
    engine=None,
    database_url: Optional[str] = None,
    echo: bool = False,
    create_database: Optional[bool] = None,
) -> List[str]:
    """对单份 JSON 配置执行建表/迁移。

    若配置顶层 database 指向的逻辑库未在 Config/database.json 中配置，
    且未显式提供 engine/database_url，则跳过本份配置并返回空列表。

    Returns:
        本份配置处理过的表名列表
    """
    from sqlalchemy import MetaData, inspect

    # 读取并校验 JSON
    with open(config_path, "r", encoding="utf-8") as f:
        try:
            config = json.load(f)
        except json.JSONDecodeError as e:
            raise ValueError(f"配置文件 JSON 解析失败: {e}") from e

    tables_cfg = config.get("tables", [])
    if not tables_cfg:
        if not quiet:
            logger.info("表配置为空，无需创建任何表")
        return []

    # 目标数据库未配置时跳过本份配置，避免误建到调度器自身的库上
    if not _is_database_configured(config, engine=engine, database_url=database_url):
        if not quiet:
            logger.info(
                f"配置 {config_path} 指定的数据库 "
                f"'{config.get('database', 'business')}' 未在 "
                "Config/database.json 中配置，跳过该份配置"
            )
        return []

    if not quiet:
        logger.debug(f"已加载表配置: {config_path}（共 {len(tables_cfg)} 张表）")

    # 解析 drop_missing：函数参数优先，否则读 JSON 顶层字段，默认 False
    if drop_missing is None:
        drop_missing = bool(config.get("drop_missing", False))
    if drop_missing and not quiet:
        logger.warning(
            "drop_missing=True：配置中已删除的列和索引将被同步删除，"
            "请确认配置文件已是最新版本"
        )

    # 解析 create_database：函数参数优先，否则读 JSON 顶层字段，默认 True
    if create_database is None:
        create_database = bool(config.get("create_database", True))
    if not quiet:
        logger.debug(
            f"建库/迁移开关: create_database={create_database}, "
            f"drop_missing={drop_missing}"
        )

    # 按优先级解析 engine
    engine = _resolve_engine(
        scheduler=scheduler,
        engine=engine,
        database_url=database_url,
        config=config,
        echo=echo,
    )

    # 变更记录器：收集本次执行的 DDL 变更，结束时统一写入变更记录表
    recorder = _SchemaChangeRecorder(engine, getattr(engine.url, "database", None))

    # 数据库不存在时先创建数据库，再创建数据表
    if create_database:
        created_db = _ensure_database_exists(engine, quiet=quiet)
        if created_db:
            recorder.record(
                _OP_CREATE_DATABASE,
                object_type="database",
                object_name=created_db,
                ddl_sql=f"CREATE DATABASE {created_db}",
            )
            if not quiet:
                logger.info(f"新数据库 '{created_db}' 已就绪，开始初始化数据表")
    elif not quiet:
        logger.debug("create_database=False，跳过数据库存在性检查")

    if not quiet:
        # 不打印完整 URL 避免泄露密码
        db_url = getattr(engine.url, "render_as_string", lambda: str(engine.url))()
        # 脱敏：隐藏密码部分
        db_url = _mask_url(db_url)
        logger.info(f"使用数据库: {db_url}")

    # 反射数据库中已有的表名集合，用于区分"新建"与"迁移"
    inspector = inspect(engine)
    existing_table_names = set(inspector.get_table_names())

    # 使用独立 MetaData，避免与 SQLModel.metadata 冲突
    metadata = MetaData()
    processed_tables: List[str] = []

    # 逐表处理：不存在则创建，存在则迁移
    for table_cfg in tables_cfg:
        table_name = table_cfg.get("name", "<未知>")
        try:
            table = _build_table(table_cfg, metadata)
            if table.name in existing_table_names:
                # 表已存在 → 模式迁移（添加缺失项，可选删除多余项）
                if not quiet:
                    logger.debug(f"数据表 '{table.name}' 已存在，开始模式迁移")
                added_cols, added_idxs, dropped_cols, dropped_idxs, widened_cols = _migrate_table(
                    table, engine, quiet=quiet, drop_missing=drop_missing
                )
                _record_migration(
                    recorder, table, engine.dialect,
                    added_cols, added_idxs, dropped_cols, dropped_idxs,
                    widened_cols,
                )
                processed_tables.append(table.name)
                if not quiet:
                    has_changes = any(
                        [added_cols, added_idxs, dropped_cols, dropped_idxs,
                         widened_cols]
                    )
                    if has_changes:
                        parts = [
                            f"新增列 {len(added_cols)}",
                            f"扩宽列 {len(widened_cols)}",
                            f"新增索引 {len(added_idxs)}",
                            f"删除列 {len(dropped_cols)}",
                            f"删除索引 {len(dropped_idxs)}",
                        ]
                        logger.info(
                            f"数据表已迁移: {table.name} ({', '.join(parts)})"
                        )
                    else:
                        logger.info(f"数据表已就绪: {table.name} (无变更)")
            else:
                # 表不存在 → 完整创建
                if not quiet:
                    logger.debug(f"数据表 '{table.name}' 不存在，开始创建")
                try:
                    table.create(bind=engine, checkfirst=True)
                except Exception as e:
                    # 并发竞态：另一实例已抢先建表 → 视为成功
                    if not _is_benign_ddl_race(e):
                        raise
                    if not quiet:
                        logger.info(
                            f"数据表 '{table.name}' 已由其他实例创建，跳过"
                        )
                processed_tables.append(table.name)
                _record_table_created(recorder, table, engine)
                if not quiet:
                    logger.info(f"数据表已创建: {table.name}")
        except Exception as e:
            # 单表失败不影响其他表
            logger.error(f"处理表 '{table_name}' 失败: {e}")
            recorder.record(
                _OP_TABLE_ERROR,
                table_name=table_name,
                object_type="table",
                object_name=table_name,
                success=False,
                note=str(e),
            )

    if not quiet and processed_tables:
        logger.info(
            f"动态建表/迁移完成，共处理 {len(processed_tables)} 张表: {processed_tables}"
        )

    recorder.flush()
    return processed_tables


def _mask_url(url: str) -> str:
    """对数据库 URL 中的密码进行脱敏，避免日志泄露。

    示例:
        'mysql+pymysql://user:pwd@host/db' → 'mysql+pymysql://user:***@host/db'
        'sqlite:///./biz.db' → 原样返回（无密码段）
    """
    if "@" not in url or "://" not in url:
        return url  # SQLite 等没有用户密码段
    scheme_end = url.find("://")
    scheme = url[:scheme_end]
    rest = url[scheme_end + 3:]  # 'user:password@host:3306/db'
    at_idx = rest.find("@")
    if at_idx == -1:
        return url
    user_part = rest[:at_idx]  # 'user:password'
    host_part = rest[at_idx + 1:]  # 'host:3306/db'
    if ":" in user_part:
        user = user_part.split(":", 1)[0]
        return f"{scheme}://{user}:***@{host_part}"
    return url  # 没有密码段


if __name__ == "__main__":
    # 命令行独立运行：手动初始化业务表，不启动调度循环
    # 用法:
    #   python SchedulerTable/TableGenerator.py --config BatchDataCollection/tables_config  # 建批次数据归集链路的表(未配置的库自动跳过)
    #   python SchedulerTable/TableGenerator.py --config <文件或目录>    # 指定单个配置文件或配置目录
    #   python SchedulerTable/TableGenerator.py --db sqlite:///./biz.db # 命令行指定数据库
    #   python SchedulerTable/TableGenerator.py --drop                  # 启用删除多余项
    import argparse
    import sys
    from logging.handlers import TimedRotatingFileHandler

    # 把项目根目录加入 sys.path，便于直接 python SchedulerTable/TableGenerator.py
    _project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if _project_root not in sys.path:
        sys.path.insert(0, _project_root)

    parser = argparse.ArgumentParser(description="根据 JSON 配置创建/迁移数据表")
    parser.add_argument(
        "--config",
        help="JSON 配置入口：配置文件路径或配置目录（默认用同目录 tables_config/ 目录）",
    )
    parser.add_argument("--db", help="数据库连接字符串（覆盖配置文件中的 database_url）")
    parser.add_argument("--drop", action="store_true", help="删除配置中已不存在的列和索引")
    parser.add_argument(
        "--create-db",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="数据库不存在时自动创建（默认启用，--no-create-db 可关闭）",
    )
    parser.add_argument("--echo", action="store_true", help="打印 SQL 日志")
    args = parser.parse_args()

    # 初始化独立日志：文件按天轮转 + 控制台输出
    # 日志文件: Log/table_generator/table_generator.log（保留 30 天）
    _log_dir = os.path.join(_project_root, "Log", "table_generator")
    os.makedirs(_log_dir, exist_ok=True)
    _log_file = os.path.join(_log_dir, "table_generator.log")
    _formatter = logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    _file_handler = TimedRotatingFileHandler(
        _log_file, when="midnight", interval=1, backupCount=30, encoding="utf-8"
    )
    _file_handler.suffix = "%Y-%m-%d"
    _file_handler.setFormatter(_formatter)
    _stream_handler = logging.StreamHandler(sys.stdout)
    _stream_handler.setFormatter(_formatter)
    logger.setLevel(logging.INFO)
    # 移除模块加载时添加的 NullHandler，改用实际的 handler
    logger.handlers.clear()
    logger.addHandler(_file_handler)
    logger.addHandler(_stream_handler)
    logger.propagate = False  # 不向 root logger 传播，避免重复输出

    # 直接通过 database_url 参数指定数据库
    # 如果 --db 未指定且配置文件中也没有 database_url，会抛出 RuntimeError 提示
    created = generate_tables(
        config_path=args.config,
        database_url=args.db,
        drop_missing=args.drop or None,
        echo=args.echo,
        create_database=args.create_db,
        quiet=False,
    )
    print(f"已处理的表: {created}")
