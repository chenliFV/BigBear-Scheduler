"""SQLModel 数据库表定义模块。

本模块定义了 SQLModel 存储后端使用的数据库表模型。
每个模型类对应一张数据库表，使用 SQLModel（SQLAlchemy + Pydantic 的结合）定义。

数据库表:
    - scheduler_jobs: 存储所有调度任务
    - scheduler_history: 存储任务执行历史
    - scheduler_dead_letters: 存储死信队列（失败任务记录）
    - scheduler_metadata: 存储调度器元数据（job_counter、statistics 等）

设计说明:
    - 时间戳字段使用 DOUBLE 而非 FLOAT，避免 Unix 时间戳精度丢失
    - schedule_days 和 script_args 以 JSON 字符串形式存储（数据库不原生支持列表）
    - 所有模型提供 to_dict/from_dict 方法，便于与 Scheduler 内部的字典交互
"""

from typing import Optional

try:
    from sqlalchemy import DOUBLE, Column
    from sqlmodel import Field, SQLModel
except ImportError as e:
    raise ImportError(
        "SQLModel storage requires sqlmodel. "
        "Install with: pip install scheduler[database]"
    ) from e


class JobModel(SQLModel, table=True):
    """调度任务数据库模型（对应 scheduler_jobs 表）。

    存储所有注册的调度任务信息。每条记录对应一个 Job 实例。

    注意:
        - func 字段不存储（函数对象无法序列化），加载时通过 job_registry 重建
        - schedule_days 存储为 JSON 字符串 "[0, 1, 2]"（星期列表）
        - script_args 存储为 JSON 字符串 '["arg1", "arg2"]'（命令行参数列表）
    """

    __tablename__ = "scheduler_jobs"

    # 自增主键
    id: Optional[int] = Field(default=None, primary_key=True)
    # 业务唯一标识符（格式 "job_N"），用于索引和查找
    job_id: str = Field(unique=True, index=True)
    func_name: str  # 任务函数名
    func_module: str  # 函数所在模块（用于持久化后重建函数引用）
    # 使用 DOUBLE 而非默认 FLOAT，避免 Unix 时间戳精度丢失
    # next_run 建索引，加速调度循环中的查询
    next_run: float = Field(sa_column=Column(DOUBLE, index=True))
    interval: Optional[float] = Field(default=None, sa_column=Column(DOUBLE))
    repeat: bool = True  # 是否重复执行
    status: str = "scheduled"  # 当前状态（字符串形式）
    created_at: float = Field(sa_column=Column(DOUBLE))
    last_run: Optional[float] = Field(default=None, sa_column=Column(DOUBLE))
    run_count: int = 0  # 总执行次数
    max_retries: int = 3  # 最大重试次数
    retry_count: int = 0  # 当前已重试次数
    catch_up: bool = True  # 是否补偿错过的执行
    schedule_type: str = "interval"  # 调度类型
    schedule_time: Optional[str] = None  # 定时执行时间 (HH:MM)
    schedule_days: Optional[str] = None  # 存储为 JSON 字符串 "[0, 1, 2]"
    timeout: Optional[float] = Field(default=None, sa_column=Column(DOUBLE))
    paused: bool = False  # 是否暂停
    timezone: Optional[str] = "Asia/Shanghai"  # 时区, 默认使用上海时区
    cron_expression: Optional[str] = None  # cron 表达式
    # 脚本任务专用字段
    script_path: Optional[str] = None  # 外部脚本路径
    script_args: Optional[str] = None  # 存储为 JSON 字符串 '["arg1", "arg2"]'
    script_code: Optional[str] = None  # 动态代码内容（与 script_path 互斥）
    # 函数任务的调用参数(存储为 JSON 字符串), 脚本任务不使用
    args: Optional[str] = None  # 存储为 JSON 字符串 '["a", 1]'
    kwargs: Optional[str] = None  # 存储为 JSON 字符串 '{"k": "v"}'

    def to_dict(self) -> dict:
        """转换为字典，用于 Job 对象重建。

        将数据库行转换为 Scheduler 内部使用的字典格式。
        特别处理:
            - schedule_days: JSON 字符串 -> 列表
            - script_args: JSON 字符串 -> 列表

        Returns:
            包含所有字段的字典
        """
        import json
        return {
            "job_id": self.job_id,
            "func_name": self.func_name,
            "func_module": self.func_module,
            "next_run": self.next_run,
            "interval": self.interval,
            "repeat": self.repeat,
            "status": self.status,
            "created_at": self.created_at,
            "last_run": self.last_run,
            "run_count": self.run_count,
            "max_retries": self.max_retries,
            "retry_count": self.retry_count,
            "catch_up": self.catch_up,
            "schedule_type": self.schedule_type,
            "schedule_time": self.schedule_time,
            # JSON 字符串反序列化为列表
            "schedule_days": json.loads(self.schedule_days) if self.schedule_days else None,
            "timeout": self.timeout,
            "paused": self.paused,
            "timezone": self.timezone,
            "cron_expression": self.cron_expression,
            "script_path": self.script_path,
            # JSON 字符串反序列化为列表，空值返回空列表
            "script_args": json.loads(self.script_args) if self.script_args else [],
            "script_code": self.script_code,
            "args": json.loads(self.args) if self.args else [],
            "kwargs": json.loads(self.kwargs) if self.kwargs else {},
        }

    @classmethod
    def from_dict(cls, data: dict) -> "JobModel":
        """从字典创建 JobModel 实例。

        处理需要序列化的字段:
            - schedule_days: 列表 -> JSON 字符串
            - script_args: 列表 -> JSON 字符串

        Args:
            data: 任务字典（通常来自 Job.to_dict()）

        Returns:
            JobModel 实例
        """
        import json

        # schedule_days 若为列表，转换为 JSON 字符串存储
        schedule_days = data.get("schedule_days")
        if schedule_days is not None and not isinstance(schedule_days, str):
            schedule_days = json.dumps(schedule_days)

        # script_args 若为列表，转换为 JSON 字符串存储
        script_args = data.get("script_args")
        if script_args is not None and not isinstance(script_args, str):
            script_args = json.dumps(script_args)

        # 函数任务调用参数: 列表/字典转换为 JSON 字符串存储
        args_value = data.get("args")
        if args_value is not None and not isinstance(args_value, str):
            args_value = json.dumps(args_value)
        kwargs_value = data.get("kwargs")
        if kwargs_value is not None and not isinstance(kwargs_value, str):
            kwargs_value = json.dumps(kwargs_value)

        return cls(
            job_id=data["job_id"],
            func_name=data["func_name"],
            func_module=data["func_module"],
            next_run=data["next_run"],
            interval=data.get("interval"),
            repeat=data.get("repeat", True),
            status=data.get("status", "scheduled"),
            created_at=data.get("created_at", 0),
            last_run=data.get("last_run"),
            run_count=data.get("run_count", 0),
            max_retries=data.get("max_retries", 3),
            retry_count=data.get("retry_count", 0),
            catch_up=data.get("catch_up", True),
            schedule_type=data.get("schedule_type", "interval"),
            schedule_time=data.get("schedule_time"),
            schedule_days=schedule_days,
            timeout=data.get("timeout"),
            paused=data.get("paused", False),
            timezone=data.get("timezone"),
            cron_expression=data.get("cron_expression"),
            script_path=data.get("script_path"),
            script_args=script_args,
            args=args_value,
            kwargs=kwargs_value,
            script_code=data.get("script_code"),
        )


class JobHistoryModel(SQLModel, table=True):
    """任务执行历史数据库模型（对应 scheduler_history 表）。

    记录任务的每一次状态变更事件。每条记录对应一次事件
    （调度、开始执行、完成、失败等）。

    索引:
        - job_id: 按任务查询历史
        - func_name: 按函数名查询历史
        - timestamp: 按时间排序查询
    """

    __tablename__ = "scheduler_history"

    id: Optional[int] = Field(default=None, primary_key=True)
    job_id: str = Field(index=True)  # 关联的任务 ID
    func_name: str = Field(index=True)  # 任务函数名
    status: str  # 事件状态
    timestamp: float = Field(sa_column=Column(DOUBLE, index=True))  # 事件时间
    error: Optional[str] = None  # 错误信息（仅失败时）
    run_count: int = 0  # 当时的执行次数
    retry_count: int = 0  # 当时的重试次数
    execution_time: Optional[float] = Field(default=None, sa_column=Column(DOUBLE))  # 执行耗时

    def to_dict(self) -> dict:
        """转换为字典。

        额外添加 timestamp_readable 字段，提供人类可读时间。

        Returns:
            包含所有字段及 timestamp_readable 的字典
        """
        from datetime import datetime
        return {
            "job_id": self.job_id,
            "func_name": self.func_name,
            "status": self.status,
            "timestamp": self.timestamp,
            "error": self.error,
            "run_count": self.run_count,
            "retry_count": self.retry_count,
            "execution_time": self.execution_time,
            "timestamp_readable": datetime.fromtimestamp(self.timestamp).strftime(
                "%Y-%m-%d %H:%M:%S"
            ),
        }

    @classmethod
    def from_dict(cls, data: dict) -> "JobHistoryModel":
        """从字典创建 JobHistoryModel 实例。

        Args:
            data: 历史记录字典（通常来自 JobHistory.to_dict()）
                注意: 会忽略 timestamp_readable 字段（非数据库字段）

        Returns:
            JobHistoryModel 实例
        """
        return cls(
            job_id=data["job_id"],
            func_name=data["func_name"],
            status=data["status"],
            timestamp=data["timestamp"],
            error=data.get("error"),
            run_count=data.get("run_count", 0),
            retry_count=data.get("retry_count", 0),
            execution_time=data.get("execution_time"),
        )


class DeadLetterModel(SQLModel, table=True):
    """死信队列数据库模型（对应 scheduler_dead_letters 表）。

    存储达到最大重试次数后仍然失败的任务记录，
    用于后续分析和排查问题。

    结构与 JobHistoryModel 相同，但语义不同：
        - JobHistoryModel: 记录所有事件
        - DeadLetterModel: 只记录最终失败的任务
    """

    __tablename__ = "scheduler_dead_letters"

    id: Optional[int] = Field(default=None, primary_key=True)
    job_id: str = Field(index=True)
    func_name: str = Field(index=True)
    status: str  # 通常为 "failed"
    timestamp: float = Field(sa_column=Column(DOUBLE, index=True))
    error: Optional[str] = None  # 失败原因
    run_count: int = 0
    retry_count: int = 0
    execution_time: Optional[float] = Field(default=None, sa_column=Column(DOUBLE))

    def to_dict(self) -> dict:
        """转换为字典，含 timestamp_readable。"""
        from datetime import datetime
        return {
            "job_id": self.job_id,
            "func_name": self.func_name,
            "status": self.status,
            "timestamp": self.timestamp,
            "error": self.error,
            "run_count": self.run_count,
            "retry_count": self.retry_count,
            "execution_time": self.execution_time,
            "timestamp_readable": datetime.fromtimestamp(self.timestamp).strftime(
                "%Y-%m-%d %H:%M:%S"
            ),
        }

    @classmethod
    def from_dict(cls, data: dict) -> "DeadLetterModel":
        """从字典创建 DeadLetterModel 实例。"""
        return cls(
            job_id=data["job_id"],
            func_name=data["func_name"],
            status=data["status"],
            timestamp=data["timestamp"],
            error=data.get("error"),
            run_count=data.get("run_count", 0),
            retry_count=data.get("retry_count", 0),
            execution_time=data.get("execution_time"),
        )


class SchedulerMetadataModel(SQLModel, table=True):
    """调度器元数据数据库模型（对应 scheduler_metadata 表）。

    存储调度器的元信息，采用键值对结构：
        - "job_counter": 当前 job ID 计数器值
        - "statistics": 统计信息 JSON
        - "last_save": 上次保存时间和运行状态

    使用键值对设计而非固定列，便于扩展新的元数据项。
    """

    __tablename__ = "scheduler_metadata"

    id: Optional[int] = Field(default=None, primary_key=True)
    # 键名唯一，建立索引加速查找
    key: str = Field(unique=True, index=True)
    value: str  # JSON 编码的值

    @classmethod
    def get_value(cls, key: str, default: str = "{}") -> str:
        """获取值的占位方法。

        注意: 此方法仅为类型提示占位，实际实现在 SQLModelStorageBackend._get_metadata 中。
        不能直接调用此类方法读取数据库。

        Args:
            key: 元数据键名
            default: 默认值

        Returns:
            默认值
        """
        return default
