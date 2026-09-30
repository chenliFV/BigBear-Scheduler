"""Scheduler 包的初始化模块。

本模块负责导出 Scheduler 库的核心 API，使用户可以通过
`from Scheduler import Scheduler, Job, JobHistory, JobStatus` 的方式
直接访问调度器的主要组件。

导出内容:
    - Scheduler: 调度器主类，负责任务的注册、调度和执行
    - Job: 任务数据类，封装单个任务的所有元信息
    - JobHistory: 历史记录数据类，记录任务执行事件
    - JobStatus: 任务状态枚举（SCHEDULED/RUNNING/COMPLETED/FAILED/MISSED）
"""

from .main import Scheduler
from .models import Job, JobHistory, JobStatus

# 版本号：遵循语义化版本规范 (SemVer)
# MAJOR.MINOR.PATCH: 主版本.次版本.修订号
__version__ = "0.2.1"

# __all__ 显式声明公开 API，限制 `from Scheduler import *` 的导出范围
__all__ = ["Scheduler", "Job", "JobHistory", "JobStatus"]
