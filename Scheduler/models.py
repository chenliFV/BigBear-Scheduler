"""Scheduler 的数据模型定义模块。

本模块定义了调度器核心使用的数据结构，包括：
    - JobStatus: 任务状态的枚举类型
    - Job: 任务数据类，封装单个任务的所有信息（使用 dataclass 实现，支持堆排序）
    - JobHistory: 任务执行历史记录数据类

这些模型被 Scheduler 主类、存储后端、调度器类等广泛使用，
是整个调度系统的数据基础。
"""

import time
from dataclasses import asdict, dataclass, field
from datetime import datetime
from enum import Enum
from typing import Callable, Dict, List, Optional


class JobStatus(Enum):
    """任务状态枚举。

    定义了任务在整个生命周期中可能处于的状态。
    状态流转：
        SCHEDULED -> RUNNING -> COMPLETED (成功)
        SCHEDULED -> RUNNING -> FAILED (失败，可能重试)
        SCHEDULED -> MISSED (错过执行，scheduler 停机期间)

    属性:
        SCHEDULED: 已调度，等待执行（默认状态）
        RUNNING: 正在执行中
        COMPLETED: 执行成功完成
        FAILED: 执行失败
        MISSED: 错过执行时间（scheduler 停机期间的任务）
    """

    SCHEDULED = "scheduled"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    MISSED = "missed"


@dataclass(order=True)
class Job:
    """任务数据类，表示一个调度任务。

    使用 @dataclass 装饰器自动生成 __init__、__repr__ 等方法。
    order=True 启用排序支持，使 Job 可以放入 heapq 优先级队列。

    排序规则:
        仅 next_run 字段参与比较（compare=True），其他字段 compare=False。
        这样堆顶始终是下次执行时间最早的 job，实现优先级调度。

    字段说明:
        next_run: 下次执行的 Unix 时间戳（参与排序，决定堆的优先级）
        func: 要执行的可调用对象（None 表示脚本任务）
        interval: 执行间隔（秒），用于 interval 类型任务
        job_id: 任务唯一标识符（格式 "job_N"）
        func_name: 任务函数名（用于显示和日志）
        func_module: 函数所在模块（用于持久化后重建函数引用）
        args: 位置参数元组
        kwargs: 关键字参数字典
        repeat: 是否重复执行（False 表示一次性任务）
        status: 当前任务状态
        created_at: 创建时间戳
        last_run: 上次执行时间戳
        run_count: 总执行次数
        max_retries: 最大重试次数（失败后）
        retry_count: 当前已重试次数
        catch_up: 是否补偿 scheduler 停机期间错过的执行
        schedule_type: 调度类型 (interval/daily/weekly/hourly/cron)
        schedule_time: 定时执行时间 (HH:MM 格式，用于 daily/weekly/hourly)
        schedule_days: 每周执行的星期列表 (0=周一 ~ 6=周日，用于 weekly)
        timeout: 执行超时时间（秒），None 表示不限时
        paused: 是否暂停（暂停的任务不会被调度执行）
        timezone: 时区名称 (如 "Asia/Shanghai")
        cron_expression: cron 表达式（用于 cron 类型任务）
        script_path: 外部脚本文件路径（脚本任务专用，func 为 None 时使用）
        script_args: 传给外部脚本的命令行参数列表
    """

    # ===== 参与排序的字段（compare=True，默认）=====
    # next_run 是堆排序的关键，决定任务的执行优先级
    next_run: float = field(compare=True)

    # ===== 不参与排序的字段（compare=False）=====
    # func 设为 repr=False，避免日志输出函数对象的冗长表示
    func: Optional[Callable] = field(default=None, compare=False, repr=False)
    interval: Optional[float] = field(default=None, compare=False)
    job_id: str = field(default="", compare=False)
    func_name: str = field(default="", compare=False)
    func_module: str = field(default="", compare=False)
    # args/kwargs 设为 repr=False，避免日志输出参数详情
    args: tuple = field(default_factory=tuple, compare=False, repr=False)
    kwargs: dict = field(default_factory=dict, compare=False, repr=False)
    repeat: bool = field(default=False, compare=False)
    status: JobStatus = field(default=JobStatus.SCHEDULED, compare=False)
    # created_at 使用 time.time 作为默认工厂，记录任务创建时刻
    created_at: float = field(default_factory=time.time, compare=False)
    last_run: Optional[float] = field(default=None, compare=False)
    run_count: int = field(default=0, compare=False)
    max_retries: int = field(default=3, compare=False)
    retry_count: int = field(default=0, compare=False)
    catch_up: bool = field(default=True, compare=False)
    schedule_type: str = field(default="interval", compare=False)
    schedule_time: Optional[str] = field(default=None, compare=False)
    schedule_days: Optional[List[int]] = field(default=None, compare=False)
    timeout: Optional[float] = field(default=None, compare=False)
    paused: bool = field(default=False, compare=False)
    timezone: Optional[str] = field(default=None, compare=False)
    cron_expression: Optional[str] = field(default=None, compare=False)
    # 脚本任务：执行外部 py 文件（func 为 None 时，用 subprocess 执行 script_path）
    # 这两个字段支持通过 REST API 动态添加执行外部脚本的任务
    script_path: Optional[str] = field(default=None, compare=False)
    script_args: list = field(default_factory=list, compare=False, repr=False)
    # 动态代码任务：直接执行用户输入的 Python 代码字符串
    # 与 script_path 互斥（两者只能存在一个），通过临时文件方式执行
    script_code: Optional[str] = field(default=None, compare=False, repr=False)

    def to_dict(self) -> Dict:
        """将 Job 序列化为字典，用于持久化存储。

        注意:
            - func 字段不序列化（函数对象无法序列化），加载时通过 job_registry 重建
            - status 转换为字符串值（枚举的 .value）
            - schedule_days 保持为列表（JSON 后端直接存储，SQLModel 后端会转 JSON 字符串）
            - script_args 保持为列表（同上）

        Returns:
            包含所有可持久化字段的字典
        """
        return {
            "job_id": self.job_id,
            "func_name": self.func_name,
            "func_module": self.func_module,
            "next_run": self.next_run,
            "interval": self.interval,
            "repeat": self.repeat,
            "status": self.status.value,  # 枚举转字符串
            "created_at": self.created_at,
            "last_run": self.last_run,
            "run_count": self.run_count,
            "max_retries": self.max_retries,
            "retry_count": self.retry_count,
            "catch_up": self.catch_up,
            "schedule_type": self.schedule_type,
            "schedule_time": self.schedule_time,
            "schedule_days": self.schedule_days,
            "timeout": self.timeout,
            "paused": self.paused,
            "timezone": self.timezone,
            "cron_expression": self.cron_expression,
            "script_path": self.script_path,
            "script_args": self.script_args,
            "script_code": self.script_code,
            # 函数任务的调用参数: 不序列化会导致重启后带参任务被无参调用
            "args": list(self.args),
            "kwargs": self.kwargs,
        }

    def get_schedule_description(self) -> str:
        """生成可读的调度描述字符串，用于日志和 Dashboard 显示。

        根据 schedule_type 返回不同的描述格式：
            - cron: "Cron: <expression> (<timezone>) ： <描述>"
            - daily: "Daily at HH:MM (<timezone>)"
            - weekly: "Every Mon, Wed at HH:MM (<timezone>)"
            - hourly: "Hourly at :MM (<timezone>)"
            - interval: "Every N seconds/minutes/hours/days"
            - 其他: "One-time job"

        Returns:
            调度描述字符串
        """
        # 时区后缀（有时区时附加显示）
        tz_suffix = f" ({self.timezone})" if self.timezone else ""

        if self.schedule_type == "cron" and self.cron_expression:
            desc = _describe_cron(self.cron_expression)
            return f"Cron: {self.cron_expression}{tz_suffix} ： {desc}"
        elif self.schedule_type == "daily" and self.schedule_time:
            return f"Daily at {self.schedule_time}{tz_suffix}"
        elif (
            self.schedule_type == "weekly" and self.schedule_time and self.schedule_days
        ):
            # 将数字星期转换为缩写名称 (0=Mon ~ 6=Sun)
            days = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
            day_names = [days[d] for d in self.schedule_days]
            return f"Every {', '.join(day_names)} at {self.schedule_time}{tz_suffix}"
        elif self.schedule_type == "hourly" and self.schedule_time:
            return f"Hourly at {self.schedule_time}{tz_suffix}"
        elif self.schedule_type == "interval" and self.interval:
            # 根据间隔大小选择合适的单位
            if self.interval < 60:
                return f"Every {int(self.interval)} seconds"
            elif self.interval < 3600:
                return f"Every {int(self.interval/60)} minutes"
            elif self.interval < 86400:
                return f"Every {int(self.interval/3600)} hours"
            else:
                return f"Every {int(self.interval/86400)} days"
        return "One-time job"


@dataclass
class JobHistory:
    """任务执行历史记录数据类。

    记录任务的每一次状态变更事件（调度、开始执行、完成、失败等），
    用于历史查询、统计分析和死信队列（dead letter queue）。

    字段说明:
        job_id: 关联的任务 ID
        func_name: 任务函数名（便于按名称查询历史）
        status: 事件状态（字符串形式，如 "completed"、"failed"）
        timestamp: 事件发生的时间戳
        error: 错误信息（仅失败时记录）
        run_count: 当时的总执行次数
        retry_count: 当时的重试次数
        execution_time: 本次执行耗时（秒）
    """

    job_id: str
    func_name: str
    status: str
    timestamp: float
    error: Optional[str] = None
    run_count: int = 0
    retry_count: int = 0
    execution_time: Optional[float] = None

    def to_dict(self):
        """将历史记录序列化为字典。

        额外添加 timestamp_readable 字段，提供人类可读的时间字符串，
        便于 Dashboard 直接显示，无需前端再做时间戳转换。

        Returns:
            包含所有字段及 timestamp_readable 的字典
        """
        return {
            **asdict(self),  # 展开所有字段
            # 附加可读时间字符串（YYYY-MM-DD HH:MM:SS 格式）
            "timestamp_readable": datetime.fromtimestamp(self.timestamp).strftime(
                "%Y-%m-%d %H:%M:%S"
            ),
        }


def _describe_cron(expr: str) -> str:
    """将 5 段 cron 表达式转换为可读的中文描述。

    支持标准 5 字段: 分 时 日 月 周。
    覆盖常见模式: 每 N 分钟/小时、每天/每周/每月定时、范围、列表、步进。

    Args:
        expr: 5 段 cron 表达式，如 "*/5 * * * *"

    Returns:
        中文描述，如 "每 5 分钟"
    """
    WEEKDAY_NAMES = {
        "0": "周日", "1": "周一", "2": "周二", "3": "周三",
        "4": "周四", "5": "周五", "6": "周六", "7": "周日",
    }

    parts = expr.split()
    if len(parts) != 5:
        return expr

    minute, hour, day, month, weekday = parts

    def _fmt_time(h: str, m: str) -> str:
        """格式化时间，HH:MM。"""
        try:
            hi = int(h)
            mi = int(m)
            return f"{hi:02d}:{mi:02d}"
        except ValueError:
            return f"{h}:{m}"

    def _fmt_weekday(w: str) -> str:
        return WEEKDAY_NAMES.get(str(w), w)

    def _expand(field: str):
        """解析字段: 返回 ('every'|'step'|'value'|'list'|'range', payload)。"""
        if field == "*":
            return ("every", None)
        if field.startswith("*/"):
            return ("step", field[2:])
        if "-" in field and "," not in field:
            lo, hi = field.split("-", 1)
            return ("range", (lo, hi))
        if "," in field:
            vals = field.split(",")
            return ("list", vals)
        return ("value", field)

    # --- 月字段 ---
    if month != "*":
        m_mode, m_val = _expand(month)
        if m_mode == "step":
            return f"每 {m_val} 个月"
        if m_mode == "value":
            return f"仅在 {m_val} 月"

    # --- 周字段 ---
    w_mode, w_val = _expand(weekday)
    # --- 日字段 ---
    d_mode, d_val = _expand(day)

    # 1) 周字段非 * -> "每周X HH:MM"
    if w_mode != "every":
        if w_mode == "list":
            wd = "、".join(_fmt_weekday(v) for v in w_val)
        elif w_mode == "range":
            wd = f"{_fmt_weekday(w_val[0])}至{_fmt_weekday(w_val[1])}"
        elif w_mode == "step":
            wd = f"每 {w_val} 个星期"
        else:
            wd = _fmt_weekday(w_val)

        # 时间部分
        if hour == "*" and minute == "*":
            return f"每 {wd} 每分钟"
        if hour != "*" and minute != "*":
            time_str = _fmt_time(hour, minute)
            return f"每 {wd} {time_str}"
        return f"每 {wd}"

    # 2) 日字段非 * -> "每月X日 HH:MM"
    if d_mode != "every":
        if d_mode == "step":
            return f"每 {d_val} 天 {_fmt_time(hour, minute)}" if hour != "*" else f"每 {d_val} 天"
        if d_mode == "list":
            return f"每月 {','.join(d_val)} 日 {_fmt_time(hour, minute)}"
        if d_mode == "range":
            return f"每月 {d_val[0]} 至 {d_val[1]} 日 {_fmt_time(hour, minute)}"
        # value
        return f"每月 {d_val} 日 {_fmt_time(hour, minute)}"

    # 3) 周和日都是 *
    # 小时和分钟
    h_mode, h_val = _expand(hour)
    m_mode, m_val = _expand(minute)

    # 每天/每小时/每分钟
    if m_mode == "step" and h_mode == "every":
        return f"每 {m_val} 分钟"
    if h_mode == "step" and m_mode == "every":
        return f"每 {h_val} 小时"
    if h_mode == "step" and m_mode == "value":
        return f"每 {h_val} 小时的第 {m_val} 分钟"
    if m_mode == "every" and h_mode == "every":
        return "每分钟"
    if m_mode == "value" and h_mode == "every":
        return f"每小时的第 {m_val} 分钟"
    if m_mode == "value" and h_mode == "value":
        return f"每天 {_fmt_time(hour, minute)}"
    if m_mode == "list" and h_mode == "value":
        return f"每天 {_fmt_time(hour, minute)}"

    return expr
