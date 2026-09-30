"""存储后端子包初始化模块。

本模块负责导出 Scheduler 的存储后端相关 API，并提供了
SQLModel 后端的延迟导入机制。

存储后端用于持久化调度器状态（jobs、history、statistics、dead letters），
支持以下两种实现：
    - JSONStorageBackend: 基于 JSON 文件存储（默认，适合简单场景和开发环境）
    - SQLModelStorageBackend: 基于数据库存储（SQLite/MySQL/PostgreSQL，适合生产环境）

导出内容:
    - StorageBackend: 存储后端抽象基类
    - JSONStorageBackend: JSON 文件存储后端
    - get_sqlmodel_backend(): 获取 SQLModel 存储后端类的函数（延迟导入）
"""

from .base import StorageBackend
from .json_backend import JSONStorageBackend

__all__ = ["StorageBackend", "JSONStorageBackend"]


# 延迟导入 SQLModel 后端，避免强制依赖 sqlmodel 包
# 只有当用户实际使用数据库存储时才需要安装 sqlmodel
def get_sqlmodel_backend():
    """获取 SQLModelStorageBackend 类（需要安装 sqlmodel 包）。

    采用延迟导入策略，避免在不需要数据库存储时强制安装 sqlmodel 依赖。

    Returns:
        SQLModelStorageBackend 类

    Raises:
        ImportError: 当未安装 sqlmodel 包时抛出，提示安装命令
    """
    try:
        from .sqlmodel_backend import SQLModelStorageBackend

        return SQLModelStorageBackend
    except ImportError as e:
        raise ImportError(
            "SQLModel storage backend requires sqlmodel. "
            "Install with: pip install scheduler[database]"
        ) from e
