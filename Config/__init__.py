"""配置包：从 Config/ 目录加载项目配置。

- scheduler.json: Scheduler worker 进程配置
- fastapi.json:   FastAPI 进程配置

使用:
    from Config import load_config, setup_logging
    config = load_config("scheduler")
    setup_logging(config, "worker")
"""
from .loader import get_database_url, load_config, setup_logging

__all__ = ["load_config", "setup_logging", "get_database_url"]
