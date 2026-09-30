"""配置加载器。

从 Config/ 目录读取配置文件，提供统一的访问入口。
- worker.json: Scheduler worker 进程配置
- fastapi.json:   FastAPI 进程配置

使用方式:
    from Config import load_config
    config = load_config("worker")   # 加载 worker.json
    config = load_config("fastapi")     # 加载 fastapi.json
"""
import json
import logging
import os
import sys
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path
from typing import Any

# 项目根目录（Config/ 的上一级）
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_CONFIG_DIR = _PROJECT_ROOT / "Config"

# 已加载的配置缓存（按名称）
_configs: dict = {}


def _expand(value: Any) -> Any:
    """递归展开字符串中的环境变量和 ~ 路径。"""
    if isinstance(value, str):
        return os.path.expanduser(os.path.expandvars(value))
    if isinstance(value, dict):
        return {k: _expand(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_expand(v) for v in value]
    return value


def load_config(name: str) -> dict:
    """加载指定名称的配置文件（Config/{name}.json）。

    Args:
        name: 配置名称，如 "scheduler" 或 "fastapi"

    Returns:
        配置字典（已展开环境变量和 ~ 路径），结果会缓存。
    """
    if name in _configs:
        return _configs[name]

    config_path = _CONFIG_DIR / f"{name}.json"
    if not config_path.exists():
        raise FileNotFoundError(f"配置文件不存在: {config_path}")

    with open(config_path, "r", encoding="utf-8") as f:
        cfg = _expand(json.load(f))

    _configs[name] = cfg
    return cfg


def get_database_url(db_name: str = "scheduler") -> str:
    """根据数据库逻辑名生成统一的连接 URL。

    所有数据库连接 URL 集中由此函数生成，避免在多处硬编码连接串。
    连接参数和数据库名称定义在 Config/database.json 中。

    Args:
        db_name: 数据库逻辑名，对应 Config/database.json 中 databases 的键
            - "scheduler": 调度器自身的存储数据库
            - "business": 业务数据表所在的数据库

    Returns:
        SQLAlchemy 连接 URL 字符串，如
        "mysql+pymysql://user:pass@host:3306/dbname?charset=utf8mb4"

    Raises:
        ValueError: 当 db_name 未在 database.json 中定义时
        FileNotFoundError: 当 Config/database.json 不存在时
    """
    db_config = load_config("database")

    databases = db_config.get("databases", {})
    if db_name not in databases:
        raise ValueError(
            f"未找到数据库配置 '{db_name}'，"
            f"可用的数据库: {list(databases.keys())}"
        )

    db_info = databases[db_name]
    connection_name = db_info.get("connection", "default")

    connections = db_config.get("connections", {})
    conn = connections.get(connection_name)
    if conn is None:
        raise ValueError(f"未找到连接配置 '{connection_name}'")

    # 构建 URL: driver://user:password@host:port/dbname[?params]
    url = f"{conn['driver']}://{conn['user']}:{conn['password']}@{conn['host']}"

    port = conn.get("port")
    if port:
        url += f":{port}"

    url += f"/{db_info['name']}"

    # 附加查询参数（如 charset）
    params = []
    charset = conn.get("charset")
    if charset:
        params.append(f"charset={charset}")
    if params:
        url += "?" + "&".join(params)

    return url


def _resolve_path(path: str) -> str:
    """将相对路径解析为基于项目根目录的绝对路径。"""
    p = Path(path)
    if not p.is_absolute():
        p = _PROJECT_ROOT / p
    return str(p)


def setup_logging(config: dict, component: str) -> tuple:
    """根据配置初始化日志。

    Args:
        config: 配置字典（由 load_config 加载）
        component: "worker" 或 "api"，对应 config["logging"][component]

    Returns:
        (file_handler, stream_handler) 便于额外 logger（如 uvicorn）复用。
    """
    log_cfg = config["logging"]
    comp_cfg = log_cfg[component]

    # 确保日志目录存在
    log_dir = _resolve_path(comp_cfg["dir"])
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, comp_cfg["filename"])

    # 统一格式
    formatter = logging.Formatter(
        fmt=log_cfg["format"], datefmt=log_cfg["datefmt"]
    )

    # 文件 handler（按天轮转）
    rot_cfg = log_cfg["rotation"]
    file_handler = TimedRotatingFileHandler(
        log_file,
        when=rot_cfg["when"],
        interval=rot_cfg["interval"],
        backupCount=rot_cfg["backup_count"],
        encoding="utf-8",
    )
    file_handler.suffix = "%Y-%m-%d"
    file_handler.setFormatter(formatter)

    # 控制台 handler
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)

    handlers = [file_handler]
    if log_cfg.get("console", True):
        handlers.append(stream_handler)

    level = getattr(logging, str(log_cfg["level"]).upper(), logging.INFO)
    logging.basicConfig(level=level, handlers=handlers)
    logging.getLogger("worker").setLevel(level)

    return file_handler, stream_handler
