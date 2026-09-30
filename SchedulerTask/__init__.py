"""定时任务注册子包。

从 tasks 子模块重导出 register_tasks，使外部可通过
``from scheduler_tasks import register_tasks`` 直接引用。
同时导出数据库公共方法，便于任务脚本统一访问多数据库。
连接串统一由 Config/database.json 生成。
"""
from .RegisterTask import register_tasks
from .Utils.db_utils import Database, connect, get_db_url, multi_connect

__all__ = [
    "register_tasks",
    "Database",
    "connect",
    "get_db_url",
    "multi_connect",
]
