"""Scheduler 调度器主模块 - 核心调度引擎实现。

本模块实现了 Scheduler 类，是整个调度系统的核心。它负责：
    1. 任务注册与管理：通过链式 API 注册任务（interval/daily/weekly/hourly/cron/once）
    2. 任务调度：基于最小堆（heapq）的优先级队列，按 next_run 时间排序
    3. 任务执行：通过线程池并发执行任务，支持同步/异步函数和外部脚本
    4. 状态持久化：通过可插拔的存储后端（JSON/SQLModel）持久化状态
    5. 错误处理：失败自动重试（指数退避），超限进入死信队列
    6. 多进程支持：API 进程只读 + Worker 进程执行，通过存储后端同步状态

核心数据结构:
    - self.jobs: 最小堆（heapq），存储 Job 对象，按 next_run 排序
    - self.job_registry: 函数注册表，键为 "module.func_name"，值为函数对象
    - self.history: 历史记录列表（JobHistory 对象）
    - self.dead_letters: 死信队列（失败任务的记录）

线程模型:
    - 主线程: Scheduler-Main，调度循环，每 0.1 秒检查一次
    - 工作线程池: Scheduler-Worker（默认 10 个），执行任务
    - 保存线程: Scheduler-Saver（1 个），异步保存状态
    - 锁: self.lock（RLock），保护所有共享数据的线程安全

使用示例:
    # 基本用法
    scheduler = Scheduler()
    @scheduler.every(60).seconds
    def task():
        print("每分钟执行")
    scheduler.start()

    # 数据库存储
    scheduler = Scheduler(
        storage="sqlmodel",
        database_url="mysql://user:pass@localhost/db"
    )

    # 上下文管理器（自动 start/stop）
    with Scheduler() as scheduler:
        @scheduler.daily.at("09:00")
        def morning():
            ...
        time.sleep(3600)
"""

import asyncio
import heapq
import itertools
import logging
import threading
import time
import traceback
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Callable, Dict, Iterator, List, Optional, Union
from zoneinfo import ZoneInfo

from .models import Job, JobHistory, JobStatus
from .schedulers import (
    CronScheduler,
    DailyScheduler,
    HourlyScheduler,
    IntervalScheduler,
    OnceScheduler,
    WeeklyScheduler,
)

if TYPE_CHECKING:
    from .storage import StorageBackend

try:
    from croniter import croniter

    CRONITER_AVAILABLE = True
except ImportError:
    CRONITER_AVAILABLE = False
    croniter = None  # type: ignore

logger = logging.getLogger("worker")
logger.addHandler(logging.NullHandler())


def _format_uptime_cn(seconds: int) -> str:
    """将秒数格式化为中文运行时长，例如 '2天3小时45分12秒'。

    替代 str(timedelta(seconds=...))，后者输出英文 '2 days, 3:45:12'。
    """
    if seconds < 0:
        seconds = 0
    days, rem = divmod(seconds, 86400)
    hours, rem = divmod(rem, 3600)
    minutes, secs = divmod(rem, 60)
    parts = []
    if days > 0:
        parts.append(f"{days}天")
    if hours > 0:
        parts.append(f"{hours}小时")
    if minutes > 0:
        parts.append(f"{minutes}分")
    if secs > 0 or not parts:
        parts.append(f"{secs}秒")
    return "".join(parts)


def _validate_schedule_config(
    schedule_type: Optional[str],
    interval: Optional[float],
    schedule_time: Optional[str],
    cron_expression: Optional[str],
    schedule_days: Optional[List[int]],
) -> None:
    """校验调度类型与必填字段的组合合法性（注册与更新路径共用）。

    缺失必填字段的组合不会在注册期报错, 而是在 _calculate_next_run
    中落入 +60s 后验兜底, 表现为"用户以为创建了每日任务, 实际每分钟
    执行一次"的静默错行为; 必须在写入前拒绝.

    Args:
        schedule_type: 调度类型 (interval/daily/weekly/hourly/cron/once)
        interval: 间隔秒数
        schedule_time: 定时时间 (daily 为 "HH:MM", hourly 为 ":MM")
        cron_expression: cron 表达式
        schedule_days: 每周执行日列表 (weekly 专用)

    Raises:
        ValueError: 组合不合法或字段越界时
    """
    if schedule_type == "interval":
        if not isinstance(interval, (int, float)) or isinstance(interval, bool) or interval <= 0:
            raise ValueError(
                f"schedule_type 'interval' requires a positive interval in seconds, "
                f"got {interval!r}"
            )
    elif schedule_type == "daily":
        _validate_clock_time(schedule_time, "daily")
    elif schedule_type == "hourly":
        minute_raw = (schedule_time or "").strip(":")
        try:
            minute = int(minute_raw)
        except (TypeError, ValueError):
            minute = -1
        if not 0 <= minute <= 59:
            raise ValueError(
                f"schedule_type 'hourly' requires schedule_time in :MM format "
                f"with minute 0-59, got {schedule_time!r}"
            )
    elif schedule_type == "weekly":
        if not schedule_days:
            raise ValueError(
                "schedule_type 'weekly' requires schedule_days "
                "(not settable via API; use the decorator API)"
            )
    elif schedule_type == "cron":
        if not cron_expression:
            raise ValueError(
                "schedule_type 'cron' requires a cron_expression"
            )
        if CRONITER_AVAILABLE:
            try:
                croniter(cron_expression)
            except (KeyError, ValueError) as e:
                raise ValueError(
                    f"Invalid cron expression '{cron_expression}': {e}"
                ) from e
    elif schedule_type == "once":
        # interval 缺省表示立即执行一次, 合法; 提供时须为正数
        if interval is not None and (
            not isinstance(interval, (int, float))
            or isinstance(interval, bool)
            or interval < 0
        ):
            raise ValueError(
                f"schedule_type 'once' requires a non-negative interval as "
                f"delay seconds, got {interval!r}"
            )
    else:
        raise ValueError(
            f"Unknown schedule_type '{schedule_type}' "
            f"(expected interval/daily/weekly/hourly/cron/once)"
        )


def _validate_option_types(
    max_retries=None,
    timeout=None,
    script_args=None,
) -> None:
    """校验可选配置项的类型（注册与更新路径共用）。

    API 层的 JSON 字段不做类型约束, 字符串数字等错误类型若直接写入
    Job, 会在执行期才触发 TypeError(如 retry_count < "abc" 的比较、
    future.result(timeout="abc")), 表现为任务反复失败进入死信;
    必须在写入前拒绝.

    Args:
        max_retries: 最大重试次数, 须为非负 int
        timeout: 执行超时秒数, 须为正数或 None
        script_args: 命令行参数, 须为 list

    Raises:
        ValueError: 类型不符或取值非法时
    """
    if max_retries is not None and (
        isinstance(max_retries, bool)
        or not isinstance(max_retries, int)
        or max_retries < 0
    ):
        raise ValueError(
            f"max_retries must be a non-negative integer, got {max_retries!r}"
        )
    if timeout is not None and (
        isinstance(timeout, bool)
        or not isinstance(timeout, (int, float))
        or timeout <= 0
    ):
        raise ValueError(
            f"timeout must be a positive number of seconds, got {timeout!r}"
        )
    if script_args is not None and not isinstance(script_args, list):
        raise ValueError(
            f"script_args must be a list, got {type(script_args).__name__}"
        )


def _validate_clock_time(schedule_time: Optional[str], type_label: str) -> None:
    """校验 HH:MM 形式的定时时间及其取值范围。

    Args:
        schedule_time: 定时时间字符串, 如 "09:30"
        type_label: 调度类型标签(用于错误信息)

    Raises:
        ValueError: 格式不合法或时/分越界时
    """
    parts = (schedule_time or "").split(":")
    if len(parts) != 2:
        raise ValueError(
            f"schedule_type '{type_label}' requires schedule_time in HH:MM "
            f"format, got {schedule_time!r}"
        )
    try:
        hour, minute = int(parts[0]), int(parts[1])
    except ValueError:
        raise ValueError(
            f"schedule_type '{type_label}' requires numeric schedule_time, "
            f"got {schedule_time!r}"
        ) from None
    if not (0 <= hour <= 23 and 0 <= minute <= 59):
        raise ValueError(
            f"schedule_time {schedule_time!r} out of range: "
            f"hour 0-23, minute 0-59"
        )


class Scheduler:
    """Scheduler - 简单、强大、持久化的任务调度器，支持异步任务。

    这是整个调度系统的核心类，提供任务注册、调度、执行、持久化等完整功能。

    特性:
        - 链式 API: 通过 @scheduler.every(60).seconds 等链式语法注册任务
        - 多种调度模式: 间隔执行、每日定时、每周定时、每小时、cron、一次性
        - 持久化: 支持 JSON 文件和 SQLModel 数据库（SQLite/MySQL/PostgreSQL）
        - 异步支持: 任务函数可以是同步或异步（async def）
        - 外部脚本: 支持通过 subprocess 执行外部 Python 脚本
        - 失败重试: 指数退避重试，超限进入死信队列
        - 多进程: API 进程只读 + Worker 进程执行，通过存储后端同步
        - Web Dashboard: 集成 FastAPI 提供实时监控界面

    线程模型:
        - Scheduler-Main: 调度主循环线程，每 0.1 秒检查到期任务
        - Scheduler-Worker: 任务执行线程池（默认 10 个并发）
        - Scheduler-Saver: 状态保存线程（单线程，串行化保存）

    Args:
        state_file: JSON 状态文件路径（仅 JSON 后端使用，默认 "scheduler_state.json"）
        storage: 存储后端
            - None 或 "json": JSON 文件存储（默认）
            - "sqlmodel": SQLModel 数据库存储（需提供 database_url）
            - StorageBackend 实例: 自定义存储后端
        database_url: 数据库连接 URL（仅 storage="sqlmodel" 时使用）
            示例: "sqlite:///scheduler.db"
                   "postgresql://user:pass@host/db"
                   "mysql://user:pass@host/db"
        auto_start: 是否立即启动调度器（默认 False）
        quiet: 静默模式，抑制 info 级别日志（默认 False）
        max_history: 历史记录最大保留数量（默认 10000）
        max_workers: 任务执行线程池大小（默认 10）
        history_retention_days: 历史记录保留天数（默认 7）
        max_dead_letters: 死信队列最大长度（默认 500）

    Examples:
        # JSON 存储（默认，向后兼容）
        scheduler = Scheduler(state_file="scheduler.json")

        # SQLite 数据库
        scheduler = Scheduler(
            storage="sqlmodel",
            database_url="sqlite:///scheduler.db"
        )

        # PostgreSQL 数据库
        scheduler = Scheduler(
            storage="sqlmodel",
            database_url="postgresql://user:pass@localhost/mydb"
        )

        # 自定义存储后端
        scheduler = Scheduler(storage=MyCustomStorageBackend())
    """

    def __init__(
        self,
        state_file: str = "scheduler_state.json",
        storage: Optional[Union[str, "StorageBackend"]] = None,
        database_url: Optional[str] = None,
        auto_start: bool = False,
        quiet: bool = False,
        max_history: int = 10000,
        max_workers: int = 10,
        history_retention_days: int = 7,
        max_dead_letters: int = 500,
    ):
        """初始化调度器。

        Args:
            state_file: JSON 状态文件路径（仅 JSON 后端使用，默认 "scheduler_state.json"）
            storage: 存储后端类型或实例
                - None 或 "json": 使用 JSON 文件存储（默认）
                - "sqlmodel": 使用 SQLModel 数据库存储（需提供 database_url）
                - StorageBackend 实例: 使用自定义存储后端
            database_url: 数据库连接 URL（仅 storage="sqlmodel" 时使用）
                示例: "sqlite:///scheduler.db"
                       "postgresql://user:pass@host/db"
                       "mysql://user:pass@host/db"
            auto_start: 是否立即启动调度器（默认 False，需手动调用 start()）
            quiet: 静默模式，抑制 info 级别日志（默认 False）
            max_history: 历史记录最大保留数量（默认 10000）
            max_workers: 任务执行线程池大小（默认 10）
            history_retention_days: 历史记录保留天数（默认 7 天）
            max_dead_letters: 死信队列最大长度（默认 500）

        初始化流程:
            1. 初始化存储后端
            2. 创建线程池（任务执行池 + 状态保存池）
            3. 从存储后端加载历史状态（jobs、history、dead_letters）
            4. 若 auto_start=True，启动调度循环
        """
        self._storage = self._init_storage(storage, state_file, database_url, quiet)
        self.state_file = Path(state_file)

        # 任务队列：使用最小堆（heapq），按 next_run 时间排序，堆顶是最早执行的任务
        self.jobs: List[Job] = []
        # 函数注册表：键 "module.func_name" -> 函数对象
        # 持久化后重建 Job 时，通过此表查找函数对象
        self.job_registry: Dict[str, Callable] = {}
        # 调度器运行状态标志
        self.running = False
        # 外部 scheduler 进程的运行状态（多进程只读模式下，API 进程通过此标志
        # 感知 worker 进程是否在运行，用于 Dashboard 显示）
        self._external_running = False
        # 调度主循环线程
        self.thread: Optional[threading.Thread] = None
        # 可重入锁，保护所有共享数据（jobs、history、stats 等）的线程安全
        self.lock = threading.RLock()
        # Job ID 生成器（itertools.count，线程安全）
        self._job_counter: Iterator[int] = itertools.count()
        self._job_counter_value = 0
        # 历史记录列表
        self.history: List[JobHistory] = []
        self.max_history = max_history
        self.max_workers = max_workers
        self.history_retention_days = history_retention_days
        self.max_dead_letters = max_dead_letters
        self.quiet = quiet
        # 正在执行中的 job_id 集合（防止重复执行，状态显示用）
        self._running_jobs: set = set()
        # 正在执行中的 job_id -> Job 对象映射: 执行中的 job 已被 pop 出堆,
        # 控制状态同步(_sync_control_state)需要借助此映射把 API 发起的
        # 暂停标志应用到执行中的对象上, 否则暂停会随执行完成的全量保存丢失
        self._running_job_objs: dict = {}
        # 正在执行期间被 API 取消的 job_id 集合
        # _execute_job 完成后会检查此集合，若被取消则不再重新入队
        self._cancelled_during_run: set = set()
        # refresh_state 的节流时间戳(多 SSE 客户端共享, 见 refresh_state)
        self._last_refresh: float = 0.0
        # 存储侧历史清理的节流时间戳(每小时一次, 见 _enforce_history_retention)
        self._last_history_cleanup: float = 0.0
        # 任务执行线程池（并发执行多个任务）
        self._executor = ThreadPoolExecutor(
            max_workers=max_workers, thread_name_prefix="Scheduler-Worker"
        )
        # 状态保存线程池（单线程，串行化保存避免并发写入冲突）
        self._save_executor = ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="Scheduler-Saver"
        )
        # 线程池是否已随 stop 关闭: start 时据此重建, 支持实例 stop 后复用
        self._executors_stopped = False
        # 本进程显式暂停/恢复意图表: job_id -> (时间戳, paused 目标值),
        # 供 _save_state 的落库前暂停合并豁免本地意图(10s 过期),
        # 防止 resume 后首个保存被存储旧值 True 反向改回暂停
        self._local_pause_events: dict = {}

        # 死信队列：存储达到最大重试次数后仍然失败的任务记录
        self.dead_letters: List[JobHistory] = []
        self._dead_letters_file = Path(
            str(self.state_file).replace(".json", "_dead_letters.json")
        )

        # 统计信息
        self.stats = {
            "total_runs": 0,       # 总执行次数
            "total_failures": 0,   # 总失败次数
            "total_retries": 0,    # 总重试次数
            "start_time": None,    # 启动时间戳
        }

        # 从存储后端加载历史状态
        self._load_state()
        self._load_dead_letters()

        # 可选：立即启动调度器
        if auto_start:
            self.start()

    def _init_storage(
        self,
        storage: Optional[Union[str, "StorageBackend"]],
        state_file: str,
        database_url: Optional[str],
        quiet: bool,
    ) -> "StorageBackend":
        """初始化存储后端。

        根据 storage 参数的类型创建对应的存储后端实例：
            - StorageBackend 实例: 直接使用
            - None 或 "json": 创建 JSONStorageBackend
            - "sqlmodel": 创建 SQLModelStorageBackend（延迟导入）

        Args:
            storage: 存储后端类型或实例
            state_file: JSON 状态文件路径（仅 JSON 后端使用）
            database_url: 数据库连接 URL（仅 SQLModel 后端使用）
            quiet: 是否抑制 info 日志

        Returns:
            StorageBackend 实例

        Raises:
            ValueError: 当 storage 参数为未知的字符串时
        """
        from .storage import JSONStorageBackend, StorageBackend

        if isinstance(storage, StorageBackend):
            return storage

        if storage is None or storage == "json":
            return JSONStorageBackend(state_file=state_file, quiet=quiet)

        if storage == "sqlmodel":
            from .storage import get_sqlmodel_backend

            SQLModelStorageBackend = get_sqlmodel_backend()
            url = database_url or "sqlite:///scheduler.db"
            return SQLModelStorageBackend(database_url=url, quiet=quiet)

        raise ValueError(
            f"Unknown storage backend: {storage}. "
            "Use 'json', 'sqlmodel', or provide a StorageBackend instance."
        )

    # ==================== User-Friendly Scheduling API ====================
    # 以下方法提供用户友好的链式调度 API，每个方法返回一个调度器对象，
    # 用户通过链式调用配置参数后，用装饰器或 do() 方法注册任务。

    def every(self, interval: Union[int, float]) -> IntervalScheduler:
        """创建间隔执行调度器 - 每隔指定时间执行一次。

        Args:
            interval: 执行间隔（初始单位为秒，可通过 .minutes/.hours/.days 转换）

        Returns:
            IntervalScheduler 实例

        使用示例:
            @scheduler.every(60).seconds
            def task(): ...

            @scheduler.every(5).minutes
            def task(): ...
        """
        return IntervalScheduler(self, interval)

    @property
    def daily(self) -> DailyScheduler:
        """创建每日定时执行调度器。

        需配合 .at("HH:MM") 指定执行时间。

        Returns:
            DailyScheduler 实例

        使用示例:
            @scheduler.daily.at("09:00")
            def morning(): ...
        """
        return DailyScheduler(self)

    @property
    def weekly(self) -> WeeklyScheduler:
        """创建每周指定日执行调度器。

        需先指定星期（如 .monday/.weekdays），再指定时间 .at("HH:MM")。

        Returns:
            WeeklyScheduler 实例

        使用示例:
            @scheduler.weekly.monday.at("09:00")
            def weekly_report(): ...
        """
        return WeeklyScheduler(self)

    @property
    def hourly(self) -> HourlyScheduler:
        """创建每小时指定分钟执行调度器。

        需配合 .at(":MM") 指定分钟。

        Returns:
            HourlyScheduler 实例

        使用示例:
            @scheduler.hourly.at(":30")  # 每小时第30分钟
            def task(): ...
        """
        return HourlyScheduler(self)

    def cron(self, expression: str) -> CronScheduler:
        """创建基于 cron 表达式的调度器。

        cron 表达式格式: "分 时 日 月 周"
            - 分: 0-59
            - 时: 0-23
            - 日: 1-31
            - 月: 1-12
            - 周: 0-6 (0=周日)

        特殊字符:
            * : 任意值
            */N : 每 N 个单位
            1,3,5 : 列表
            1-5 : 范围

        Args:
            expression: cron 表达式，如 "0 9 * * 1-5"（工作日9点）

        Returns:
            CronScheduler 实例

        Raises:
            ImportError: 未安装 croniter 库

        使用示例:
            @scheduler.cron("0 9 * * 1-5")  # 工作日早上9点
            def workday_morning(): ...

            @scheduler.cron("*/30 * * * *")  # 每30分钟
            def frequent(): ...
        """
        if not CRONITER_AVAILABLE:
            raise ImportError(
                "Cron scheduling requires croniter. "
                "Install with: pip install scheduler[cron]"
            )
        return CronScheduler(self, expression)

    def once(self, delay: Union[int, float]) -> OnceScheduler:
        """创建一次性延迟执行调度器 - 执行一次后自动移除。

        Args:
            delay: 延迟时间（秒，可通过 .minutes/.hours 转换）

        Returns:
            OnceScheduler 实例（已设为装饰器模式）

        使用示例:
            @scheduler.once(60).seconds
            def delayed(): ...

            @scheduler.once(5).minutes
            def task(): ...
        """
        scheduler = OnceScheduler(self, delay)
        scheduler._decorator_mode = True
        return scheduler

    def at(self, target_time: Union[datetime, str]) -> OnceScheduler:
        """创建在指定时间执行的一次性任务。

        与 once() 不同，at() 接收绝对时间而非相对延迟。

        Args:
            target_time: 目标执行时间
                - datetime 对象
                - 字符串格式 "YYYY-MM-DD HH:MM:SS"

        Returns:
            OnceScheduler 实例（已设为装饰器模式）

        Raises:
            ValueError: 当目标时间已过去时

        使用示例:
            @scheduler.at("2024-12-25 00:00:00")
            def christmas(): ...

            @scheduler.at(datetime(2024, 12, 31, 23, 59, 59))
            def new_year(): ...
        """
        if isinstance(target_time, str):
            target_time = datetime.strptime(target_time, "%Y-%m-%d %H:%M:%S")

        delay = (target_time - datetime.now()).total_seconds()
        if delay < 0:
            raise ValueError("Target time is in the past")

        scheduler = OnceScheduler(self, delay)
        scheduler._decorator_mode = True
        return scheduler

    # ==================== 内部方法 ====================
    # 以下为内部方法，不直接面向用户调用。
    # 它们负责任务注册、ID 生成、队列管理等底层操作。

    def _register_function(self, func: Callable):
        """将函数注册到函数注册表，并提取源代码用于持久化。

        函数注册表是任务持久化的关键：
            - 保存时: 只保存函数名和模块名（func_name, func_module）
            - 加载时: 通过 "module.func_name" 在注册表中查找函数对象
            - 若找不到: Job 的 func 为 None（多进程只读模式下可正常展示但不执行）

        除函数对象外，还提取函数源代码并以 "__source__" 后缀键缓存，
        供 _add_job 填充 Job.script_code 字段，使装饰器注册的函数任务
        能以内联代码形式序列化到数据库。

        Args:
            func: 要注册的可调用对象
        """
        key = f"{func.__module__}.{func.__name__}"
        self.job_registry[key] = func
        # 提取函数源代码，用于序列化为内联代码
        source = self._extract_function_source(func)
        if source is not None:
            self.job_registry[f"{key}.__source__"] = source

    @staticmethod
    def _extract_function_source(func: Callable) -> Optional[str]:
        """提取函数源代码，组装为可独立执行的脚本。

        将装饰器注册的函数转换为可独立运行的内联代码，用于持久化到数据库。
        转换步骤:
            1. inspect.getsource 获取原始源码（含装饰器）
            2. textwrap.dedent 去除统一缩进
            3. 跳过装饰器行（@...），从 def 行开始保留
            4. 从函数所在模块提取 import 语句，作为前置依赖
            5. 末尾追加函数调用（无参数形式 func_name()）

        Args:
            func: 要提取源码的函数对象

        Returns:
            可独立执行的 Python 代码字符串；无法获取源码时返回 None
        """
        import inspect
        import textwrap

        try:
            source = inspect.getsource(func)
        except (OSError, TypeError):
            return None

        source = textwrap.dedent(source)
        lines = source.split('\n')

        # 跳过装饰器行，从 def 开始保留
        func_start = 0
        for i, line in enumerate(lines):
            if line.startswith('def '):
                func_start = i
                break

        func_source = '\n'.join(lines[func_start:])

        # 提取函数所在模块的 import 语句，确保内联代码可独立运行
        imports = []
        try:
            module = inspect.getmodule(func)
            if module is not None:
                module_source = inspect.getsource(module)
                for line in module_source.split('\n'):
                    stripped = line.strip()
                    if stripped.startswith('import ') or stripped.startswith('from '):
                        imports.append(line)
        except (OSError, TypeError):
            pass

        # 组装：imports + 函数定义 + 调用
        parts = []
        if imports:
            parts.append('\n'.join(imports))
            parts.append('')
        parts.append(func_source.rstrip())
        parts.append(f'\n\n{func.__name__}()\n')
        return '\n'.join(parts)

    def _next_job_id(self) -> str:
        """生成下一个 Job ID（线程安全）。

        使用 itertools.count 生成递增序列，格式为 "job_N"。
        N 从 0 开始递增，确保全局唯一。

        Returns:
            Job ID 字符串，如 "job_0"、"job_1" 等

        注意:
            _job_counter_value 保存当前值，用于持久化。
            加载状态时会从持久化的值恢复计数器。
        """
        self._job_counter_value = next(self._job_counter)
        return f"job_{self._job_counter_value}"

    def _add_job(self, job: Job):
        """将 Job 加入调度队列（最小堆）。

        加入队列的逻辑:
            1. 若 func 已注册但 script_code 为空，从函数注册表提取源代码填充，
               使装饰器注册的函数任务能以内联代码形式序列化到数据库
            2. 检查是否已存在相同 job_id 的任务
               若存在且旧任务的 func 为 None（从数据库恢复的占位任务），
               用新任务替换，使其可被执行
            3. 按 func_name + func_module 去重：
               从数据库恢复的 Job（func=None, script_code 有值）与新注册的
               函数 Job 同名时，用新 Job 替换（继承旧 job_id），避免重复执行
            4. 若不存在重复，push 到堆中，记录历史，异步保存状态

        Args:
            job: 要添加的 Job 对象
        """
        with self.lock:
            # 注册入口统一校验: interval 类任务间隔必须为正, 否则到期后
            # next_run 不前移, 主循环将以 0.1s 节奏热循环重触发.
            # (once 类型以 interval 充当延迟秒数, 0/负值=立即执行, 不受限)
            if job.schedule_type == "interval" and (job.interval or 0) <= 0:
                raise ValueError(
                    f"interval must be positive for interval job "
                    f"{job.func_name}, got {job.interval}"
                )

            # 若 func 已注册但 script_code 为空，从注册表提取源代码填充
            # 仅限无参函数: 提取出的内联脚本以 func_name() 形式追加调用,
            # 带参函数填充后重启会以无参调用错执行, 宁可不填充(靠重新注册恢复).
            if job.func is not None and not job.script_code and not job.args and not job.kwargs:
                source_key = f"{job.func_module}.{job.func_name}.__source__"
                source = self.job_registry.get(source_key)
                if source:
                    job.script_code = source

            for i, j in enumerate(self.jobs):
                if j.job_id == job.job_id:
                    # 已存在相同 job_id 的 job：
                    # 若旧 job 的 func 为 None（从数据库恢复但本进程无函数定义），
                    # 用新注册的 job 替换，使其可被执行
                    if j.func is None and job.func is not None:
                        self.jobs[i] = job
                        heapq.heapify(self.jobs)
                        if not self.quiet:
                            logger.info(
                                f"Replaced placeholder job {job.job_id} "
                                f"with function {job.func_name}"
                            )
                        self._save_state_async()
                        return
                    logger.warning(f"Job {job.job_id} already exists, skipping")
                    return

                # 按 func_name + func_module 去重：
                # 从数据库恢复的 Job（func=None）与新注册的函数 Job 同名时，
                # 用新 Job 替换（继承旧 job_id），避免重启后重复执行
                if (job.func is not None
                    and j.func is None
                    and j.func_name == job.func_name
                    and j.func_module == job.func_module):
                    job.job_id = j.job_id
                    self.jobs[i] = job
                    heapq.heapify(self.jobs)
                    if not self.quiet:
                        logger.info(
                            f"Replaced restored job {job.job_id} "
                            f"with in-memory function {job.func_name}"
                        )
                    self._save_state_async()
                    return

            # 同名函数任务正在执行(已被 pop 出堆, 上面的堆内去重触及不到):
            # 跳过重复注册, 避免执行完成后双对象并存导致同任务双调度;
            # 需更新配置时待其完成后重新注册.
            for running_job in self._running_job_objs.values():
                if (
                    job.func is not None
                    and running_job.func_name == job.func_name
                    and running_job.func_module == job.func_module
                ):
                    logger.warning(
                        f"Job {job.func_name} is running, skip duplicate registration"
                    )
                    return

            heapq.heappush(self.jobs, job)
            self._log_history(job.job_id, job.func_name, JobStatus.SCHEDULED)

            schedule_desc = job.get_schedule_description()
            if not self.quiet:
                logger.info(f"Scheduled: {job.func_name} - {schedule_desc}")

        self._save_state_async()

    def _log_history(
        self,
        job_id: str,
        func_name: str,
        status: JobStatus,
        error: Optional[str] = None,
        run_count: int = 0,
        retry_count: int = 0,
        execution_time: Optional[float] = None,
    ):
        """记录任务事件到历史记录。

        每次任务状态变更（调度、开始、完成、失败）都会调用此方法。
        失败的事件同时会加入死信队列。

        Args:
            job_id: 任务 ID
            func_name: 任务函数名
            status: 事件状态（SCHEDULED/RUNNING/COMPLETED/FAILED）
            error: 错误信息（仅失败时）
            run_count: 当时的执行次数
            retry_count: 当时的重试次数
            execution_time: 执行耗时（秒，仅完成/失败时）
        """
        history_entry = JobHistory(
            job_id=job_id,
            func_name=func_name,
            status=status.value,
            timestamp=time.time(),
            error=error,
            run_count=run_count,
            retry_count=retry_count,
            execution_time=execution_time,
        )

        with self.lock:
            self.history.append(history_entry)
            self._cleanup_history()

            if status == JobStatus.FAILED and error:
                self.dead_letters.append(history_entry)
                if len(self.dead_letters) > self.max_dead_letters:
                    self.dead_letters = self.dead_letters[-self.max_dead_letters :]

        # 增量落库(存储后端支持时): 历史与死信实时可查, 且大幅降低全量
        # 保存的数据库压力; 不支持增量的后端(如 JSON)由周期性全量保存覆盖.
        incremental_history = getattr(self._storage, "save_history_entry", None)
        if incremental_history is not None:
            incremental_history(history_entry.to_dict())

        if status == JobStatus.FAILED and error:
            incremental_dead_letter = getattr(self._storage, "add_dead_letter", None)
            if incremental_dead_letter is not None:
                incremental_dead_letter(
                    history_entry.to_dict(), self.max_dead_letters
                )
            else:
                self._save_dead_letters_async()

    def _cleanup_history(self):
        """清理过期的历史记录。

        按两个维度清理:
            1. 时间维度: 删除超过 history_retention_days 天的记录
            2. 数量维度: 超过 max_history 条时，只保留最近的 max_history 条

        此方法在每次记录历史后自动调用，避免历史记录无限增长。
        """
        if self.history_retention_days > 0:
            cutoff_time = time.time() - (self.history_retention_days * 86400)
            self.history = [h for h in self.history if h.timestamp >= cutoff_time]

        if len(self.history) > self.max_history:
            self.history = self.history[-self.max_history :]

    def start(self):
        """启动调度器。

        启动流程:
            1. 检查是否已在运行（避免重复启动）
            2. 若实例曾 stop 过, 重建线程池(stop 会 shutdown 执行池与
               保存池, 不重建的话 submit 抛 RuntimeError 被主循环吞掉,
               任务静默永不执行)——支持同一实例 stop 后再次 start
            3. 设置 running 标志和启动时间
            4. 处理停机期间错过的任务（_handle_missed_jobs）
            5. 创建并启动后台调度线程（Scheduler-Main）
            6. 异步保存状态

        调度线程以 daemon 模式运行，主进程退出时自动终止。
        若已启动，会打印警告并直接返回。
        """
        if self.running:
            logger.warning("Scheduler already running")
            return

        if self._executors_stopped:
            # 旧主循环线程理论上已随 stop 退出; join 兜底防双循环
            if self.thread and self.thread.is_alive():
                self.thread.join(timeout=5)
            self._executor = ThreadPoolExecutor(
                max_workers=self.max_workers, thread_name_prefix="Scheduler-Worker"
            )
            self._save_executor = ThreadPoolExecutor(
                max_workers=1, thread_name_prefix="Scheduler-Saver"
            )
            self._executors_stopped = False
            if not self.quiet:
                logger.info("Rebuilt executor pools for scheduler restart")

        self.running = True
        self.stats["start_time"] = time.time()

        self._handle_missed_jobs()

        self.thread = threading.Thread(
            target=self._run,
            daemon=True,
            name="Scheduler-Main",
        )
        self.thread.start()

        if not self.quiet:
            logger.info("Scheduler started")
        self._save_state_async()

    def stop(self, wait: bool = True, timeout: int = 30):
        """优雅停止调度器。

        停止流程:
            1. 设置 running = False，通知调度循环退出
            2. 等待调度主线程结束（最多 timeout 秒）
            3. 关闭线程池:
                - wait=True: 等待所有正在执行的任务完成
                - wait=False: 立即取消未开始的任务
            4. 同步保存最终状态
            5. 关闭存储后端（释放数据库连接等）

        Args:
            wait: 是否等待正在执行的任务完成（默认 True）
            timeout: 等待调度线程退出的超时时间（秒，默认 30）
        """
        if not self.running:
            return

        logger.info("Stopping scheduler...")
        self.running = False

        if wait and self.thread and self.thread.is_alive():
            self.thread.join(timeout=timeout)

        if wait:
            # 执行池有界关闭: 无界 shutdown(wait=True) 会因单个挂死任务
            # 导致优雅停机永远无法完成
            self._shutdown_executor_bounded(timeout)
            self._save_executor.shutdown(wait=True)
        else:
            self._executor.shutdown(wait=False, cancel_futures=True)
            self._save_executor.shutdown(wait=False)

        # 标记线程池已关闭: 再次 start 时重建, 支持实例复用(R1)
        self._executors_stopped = True

        self._save_state()
        self._storage.close()

        if not self.quiet:
            logger.info("Scheduler stopped")

    def _shutdown_executor_bounded(self, timeout: float) -> None:
        """有界关闭任务执行池.

        先不阻塞地发起关闭, 轮询在途任务(_running_jobs)至多 timeout 秒;
        超时后取消排队中未开始的任务并告警. 在途任务无法强杀, 其线程将
        继续运行至自然结束(进程退出时由解释器 join, 必要时需 kill -9).

        Args:
            timeout: 等待在途任务完成的最长秒数.
        """
        self._executor.shutdown(wait=False, cancel_futures=False)
        deadline = time.time() + max(1.0, float(timeout))
        while time.time() < deadline:
            with self.lock:
                if not self._running_jobs:
                    return
            time.sleep(0.2)
        self._executor.shutdown(wait=False, cancel_futures=True)
        with self.lock:
            stuck = sorted(self._running_jobs)
        logger.warning(
            f"Executor shutdown timed out after {timeout}s, "
            f"{len(stuck)} job(s) still running: {stuck}"
        )

    def _handle_missed_jobs(self):
        """处理 scheduler 停机期间错过的任务。

        当 scheduler 重启时，检查所有任务的 next_run：
            - 若 next_run 已过去且任务设置了 catch_up=True:
                * 定时任务（daily/weekly/hourly）: 重新计算下次执行时间
                * 间隔任务（interval）: 计算错过的次数，设为立即执行
                * 一次性任务（!repeat）: 设为立即执行（避免永久错过）
            - 若 catch_up=False: 跳过，不处理错过的执行

        此方法在 start() 中调用，确保停机期间的任务不会丢失。
        """
        now = time.time()

        with self.lock:
            for job in self.jobs:
                if not job.catch_up:
                    continue

                if job.next_run < now and job.repeat:
                    if job.schedule_type in ["daily", "weekly", "hourly"]:
                        self._calculate_next_run(job)
                    elif job.interval:
                        missed_count = int((now - job.next_run) / job.interval)
                        if missed_count > 0:
                            if not self.quiet:
                                logger.warning(
                                    f"Job {job.func_name} missed {missed_count} runs, running now"
                                )
                            job.next_run = now

                elif job.next_run < now and not job.repeat:
                    if not self.quiet:
                        logger.warning(
                            f"One-time job {job.func_name} was missed, running now"
                        )
                    job.next_run = now

    def _calculate_next_run(self, job: Job):
        """计算定时任务的下次执行时间。

        根据 schedule_type 计算下次执行时间:
            - cron: 使用 croniter 解析 cron 表达式
            - daily: 设为明天的 HH:MM（若今天时间未过则为今天）
            - weekly: 遍历未来 8 天，找到第一个匹配的星期和时间
            - hourly: 设为下一小时的 :MM（若当前小时时间未过则为当前小时）

        支持时区：若 job.timezone 不为 None，按指定时区计算。

        Args:
            job: 要计算的任务（直接修改 job.next_run）
        """
        if job.timezone:
            tz = ZoneInfo(job.timezone)
            now = datetime.now(tz)
        else:
            now = datetime.now()
            tz = None

        if job.schedule_type == "cron" and job.cron_expression and CRONITER_AVAILABLE:
            base_time = now if tz else datetime.now()
            cron = croniter(job.cron_expression, base_time)
            next_run = cron.get_next(datetime)
            job.next_run = next_run.timestamp()

        elif job.schedule_type == "daily" and job.schedule_time:
            hour, minute = map(int, job.schedule_time.split(":"))
            next_run = now.replace(hour=hour, minute=minute, second=0, microsecond=0)

            if next_run <= now:
                next_run += timedelta(days=1)

            job.next_run = next_run.timestamp()

        elif job.schedule_type == "weekly" and job.schedule_time and job.schedule_days:
            hour, minute = map(int, job.schedule_time.split(":"))

            for i in range(8):
                check_date = now + timedelta(days=i)
                if check_date.weekday() in job.schedule_days:
                    next_run = check_date.replace(
                        hour=hour, minute=minute, second=0, microsecond=0
                    )
                    if next_run > now:
                        job.next_run = next_run.timestamp()
                        return

        elif job.schedule_type == "hourly" and job.schedule_time:
            minute = int(job.schedule_time.strip(":"))
            next_run = now.replace(minute=minute, second=0, microsecond=0)

            if next_run <= now:
                next_run += timedelta(hours=1)

            job.next_run = next_run.timestamp()

        # 后验兜底: 任一分支未能把 next_run 推进到未来(如 croniter 不可用、
        # 未知的 schedule_type、配置字段缺失)时固定顺延 60s, 避免到期任务
        # 被主循环以 0.1s 节奏热循环重触发.
        if job.next_run <= time.time():
            logger.warning(
                f"next_run not advanced for {job.func_name} "
                f"(schedule_type={job.schedule_type}), fallback to +60s"
            )
            job.next_run = time.time() + 60.0

    def _schedule_next_run(self, job: Job):
        """为重复执行的任务设置下次执行时间。

        任务执行完成后调用此方法计算下次执行时间:
            - 定时任务（daily/weekly/hourly/cron）: 调用 _calculate_next_run
            - 间隔任务（interval）: next_run = now + interval

        Args:
            job: 要调度的任务（直接修改 job.next_run）
        """
        if job.schedule_type in ["daily", "weekly", "hourly", "cron"]:
            self._calculate_next_run(job)
        elif job.interval and job.interval > 0:
            job.next_run = time.time() + job.interval
        elif job.repeat:
            # 未知类型或非法 interval(如历史脏数据恢复): 兜底顺延, 防热循环;
            # _calculate_next_run 内部同样有后验兜底, 双保险覆盖两处入口.
            logger.warning(
                f"cannot schedule next run for {job.func_name} "
                f"(schedule_type={job.schedule_type}, interval={job.interval}), "
                f"fallback to +60s"
            )
            job.next_run = time.time() + 60.0

    def _run(self):
        """调度主循环 - 在后台线程中运行。

        循环逻辑（每 0.1 秒一次）:
            1. 每秒调用 _sync_control_state() 从数据库同步控制状态
               （多进程模式下，同步 API 进程的 pause/cancel 操作）
            2. 检查堆顶任务，若 next_run <= now 则弹出
            3. 跳过暂停的任务（设为 1 秒后重试）
            4. 将到期任务提交到线程池执行

        异常处理:
            主循环中的异常会被捕获并记录，1 秒后继续循环，
            确保单次异常不会导致调度器停止。

        多进程同步说明:
            每秒从数据库读取最新状态，使 API 进程的操作能及时生效。
            这是多进程模式的核心机制，确保 pause/cancel/add 操作
            能跨进程传递。
        """
        if not self.quiet:
            logger.info("Scheduler main loop started")

        # 多进程模式下，定期从数据库同步 API 进程写入的控制状态（paused/cancel）
        last_sync = 0.0

        while self.running:
            try:
                now = time.time()
                jobs_to_run = []

                # 每秒从数据库同步控制状态，使 API 进程的 pause/cancel 能影响 scheduler
                if now - last_sync >= 1.0:
                    self._sync_control_state()
                    # 周期性把历史保留策略应用到存储后端(节流, 见方法说明)
                    self._enforce_history_retention(now)
                    last_sync = now

                with self.lock:
                    while self.jobs and self.jobs[0].next_run <= now:
                        job = heapq.heappop(self.jobs)

                        if job.job_id in self._running_jobs:
                            # 该任务已有实例在执行(堆内重复条目或手动触发中),
                            # 丢弃冗余条目; 执行中的实例完成后会重新计算
                            # next_run 入队, 不会漏掉后续调度.
                            continue

                        # pop 后立即登记为执行中: 关闭"pop -> executor 线程 add"
                        # 之间的间隙窗口, 防止该间隙内 _sync_control_state 把
                        # 尚未登记的 job 误判为数据库新增而重建副本.
                        self._running_jobs.add(job.job_id)
                        self._running_job_objs[job.job_id] = job

                        if job.paused:
                            # 暂停任务不执行, 撤销执行中登记后重新入堆等待恢复
                            self._running_jobs.discard(job.job_id)
                            self._running_job_objs.pop(job.job_id, None)
                            job.next_run = time.time() + 1.0
                            heapq.heappush(self.jobs, job)
                            continue

                        jobs_to_run.append(job)
                        # Don't pre-schedule here - job will be re-scheduled
                        # in _execute_job after execution completes

                for job in jobs_to_run:
                    future = self._executor.submit(self._execute_job, job)
                    # 排队任务在停机取消(cancel_futures)时不会执行, 其在 pop 时
                    # 预登记的执行中状态无人清理; 由取消回调兜底移除,
                    # 防止 _running_jobs 永久泄漏已取消的任务 id.
                    future.add_done_callback(
                        lambda f, jid=job.job_id: self._cleanup_cancelled_submit(f, jid)
                    )

                time.sleep(0.1)

            except Exception as e:
                logger.error(f"Error in main loop: {e}\n{traceback.format_exc()}")
                time.sleep(1)

        if not self.quiet:
            logger.info("Scheduler main loop stopped")

    def _enforce_history_retention(self, now: float) -> None:
        """周期性把内存侧的历史保留策略应用到存储后端.

        内存历史每次记录后即被 _cleanup_history 裁剪, 但存储后端(数据库)
        中的历史表若无人调用 cleanup_history 会随执行事件无限增长
        (高频任务约每年千万行); 此处每小时按 max_history 与
        history_retention_days 双维度裁剪一次, 启动后首个周期立即执行.

        Args:
            now: 当前时间戳(由调用方传入便于测试).
        """
        if now - self._last_history_cleanup < 3600.0:
            return
        self._last_history_cleanup = now
        cleanup = getattr(self._storage, "cleanup_history", None)
        if cleanup is None:
            return
        try:
            cleanup(self.max_history, self.history_retention_days)
        except Exception as e:
            logger.error(f"Failed to cleanup history in storage: {e}")

    def _sync_control_state(self):
        """从数据库同步控制状态（paused/cancel）。

        多进程模式下，API 进程调用 pause_job/cancel_job 只修改了 API 进程内存
        并写入数据库。scheduler 通过此方法读取最新的控制标志，但保留自己的运行时
        状态（next_run、run_count 等执行时数据），避免覆盖。

        对于正在执行中的 job（在 _running_jobs 中）被取消的情况，会记录到
        _cancelled_during_run 集合，_execute_job 完成后会检查并跳过重新入队。

        并发说明: 加载与应用同在一个锁临界区内. 若在锁外加载, 本进程
        pause_job/resume_job 修改标志并保存后, 同步仍可能用加载到的旧记录
        把标志覆写回旧值(如暂停被瞬时回退为 False, 到期任务可趁机漏跑一次).
        """
        try:
            with self.lock:
                state = self._storage.load_state()
                if state is None:
                    return
                db_jobs = {j["job_id"]: j for j in state.get("jobs", [])}

                # 处理正在执行但已被取消的 job（数据库中不存在）
                for running_id in list(self._running_jobs):
                    if running_id not in db_jobs:
                        self._cancelled_during_run.add(running_id)
                        if not self.quiet:
                            logger.info(
                                f"Job {running_id} cancelled during execution, "
                                f"will not re-schedule"
                            )

                # 移除已被取消的 job（数据库中不存在）
                before = len(self.jobs)
                self.jobs = [j for j in self.jobs if j.job_id in db_jobs]
                after = len(self.jobs)
                if after < before:
                    heapq.heapify(self.jobs)
                    if not self.quiet:
                        logger.info(
                            f"Synced control state: removed {before - after} cancelled job(s)"
                        )

                # 同步 paused 状态及配置字段（仅对已暂停任务同步配置更新）
                # 运行时字段（next_run、run_count、last_run、status、retry_count）保留 scheduler 侧值
                for job in self.jobs:
                    db_job = db_jobs.get(job.job_id)
                    if db_job is not None:
                        db_paused = db_job.get("paused", False)
                        if job.paused != db_paused:
                            job.paused = db_paused
                            # 恢复时重新计算下次执行时间，避免因暂停期间
                            # next_run 被不断重置为 now+1 导致恢复后立即执行
                            if not db_paused:
                                self._schedule_next_run(job)
                                heapq.heapify(self.jobs)
                            if not self.quiet:
                                action = "Paused" if db_paused else "Resumed"
                                logger.info(f"{action} job {job.func_name} ({job.job_id}) via API")
                        # 暂停任务可能已被 API 进程修改配置，需要同步配置字段
                        if db_paused:
                            # 注意：func_name 不允许修改，不同步
                            if db_job.get("script_path") != job.script_path:
                                job.script_path = db_job.get("script_path")
                                job.script_code = None
                            if db_job.get("script_code") != job.script_code:
                                job.script_code = db_job.get("script_code")
                                job.script_path = None
                            if db_job.get("script_args") != job.script_args:
                                job.script_args = db_job.get("script_args", [])
                            if db_job.get("schedule_type") and job.schedule_type != db_job["schedule_type"]:
                                job.schedule_type = db_job["schedule_type"]
                                job.repeat = db_job["schedule_type"] != "once"
                            if db_job.get("interval") != job.interval:
                                job.interval = db_job.get("interval")
                            if db_job.get("schedule_time") != job.schedule_time:
                                job.schedule_time = db_job.get("schedule_time")
                            if db_job.get("cron_expression") != job.cron_expression:
                                job.cron_expression = db_job.get("cron_expression")
                            if db_job.get("max_retries") is not None and job.max_retries != db_job["max_retries"]:
                                job.max_retries = db_job["max_retries"]
                            if db_job.get("timeout") != job.timeout:
                                job.timeout = db_job.get("timeout")
                            if db_job.get("timezone") != job.timezone:
                                job.timezone = db_job.get("timezone")
                            if db_job.get("catch_up") is not None and job.catch_up != db_job["catch_up"]:
                                job.catch_up = db_job["catch_up"]

                # 同步执行中任务的暂停标志: 执行中的 job 已被 pop 出堆, 上面
                # 的堆内同步触及不到它. 若不同步, API 在执行期间发起的暂停会
                # 随执行完成后的全量状态保存(以本进程内存为准)被覆写回 False,
                # 暂停静默失效. 标志落地后, 执行完成回堆的任务会被主循环的
                # 暂停分支持有, 不会产生额外执行.
                for running_id, running_job in list(self._running_job_objs.items()):
                    db_running = db_jobs.get(running_id)
                    if db_running is not None:
                        db_running_paused = db_running.get("paused", False)
                        if running_job.paused != db_running_paused:
                            running_job.paused = db_running_paused
                            if not self.quiet:
                                action = (
                                    "Paused" if db_running_paused else "Resumed"
                                )
                                logger.info(
                                    f"{action} running job "
                                    f"{running_job.func_name} ({running_id}) via API"
                                )

                # 检测数据库中新增的 job（如通过 API 添加的脚本任务/动态代码任务）
                # existing_ids 必须并入 _running_jobs: 正在执行的 job 已被主循环
                # pop 出堆(执行完成后才 re-schedule), 若只看堆内 id 会把执行中的
                # job 误判为"数据库新增"而重建副本入堆, 导致同 job_id 双执行.
                existing_ids = {j.job_id for j in self.jobs} | set(self._running_jobs)
                for db_jd in db_jobs.values():
                    if db_jd["job_id"] not in existing_ids and (
                        db_jd.get("script_path") or db_jd.get("script_code")
                    ):
                        # 已终结的 once 任务(completed/failed/执行中快照 running)
                        # 不得重建: 终结算子的 delete_job 可能被"载荷快照收集于
                        # 执行期间的排队保存"以 INSERT 方式复活(密集并发下可
                        # 反复发生), 重建副本会再次执行形成死循环. 待执行的
                        # once(API 新建)status 为 scheduled, 不受影响.
                        if not db_jd.get("repeat", True) and db_jd.get(
                            "status", "scheduled"
                        ) != "scheduled":
                            self._storage.delete_job(db_jd["job_id"])
                            if not self.quiet:
                                logger.info(
                                    f"Removed terminated once-job record: "
                                    f"{db_jd['func_name']} ({db_jd['job_id']})"
                                )
                            continue
                        new_job = Job(
                            job_id=db_jd["job_id"],
                            func=None,
                            func_name=db_jd["func_name"],
                            func_module=db_jd.get("func_module", "__script__"),
                            next_run=db_jd["next_run"],
                            interval=db_jd.get("interval"),
                            repeat=db_jd.get("repeat", True),
                            status=JobStatus(db_jd.get("status", "scheduled")),
                            created_at=db_jd.get("created_at", time.time()),
                            last_run=db_jd.get("last_run"),
                            run_count=db_jd.get("run_count", 0),
                            max_retries=db_jd.get("max_retries", 3),
                            retry_count=db_jd.get("retry_count", 0),
                            catch_up=db_jd.get("catch_up", True),
                            schedule_type=db_jd.get("schedule_type", "interval"),
                            schedule_time=db_jd.get("schedule_time"),
                            schedule_days=db_jd.get("schedule_days"),
                            timeout=db_jd.get("timeout"),
                            paused=db_jd.get("paused", False),
                            timezone=db_jd.get("timezone"),
                            cron_expression=db_jd.get("cron_expression"),
                            script_path=db_jd.get("script_path"),
                            script_args=db_jd.get("script_args", []),
                            script_code=db_jd.get("script_code"),
                            # 与 _load_state 保持对称: 带参函数任务经此重建时
                            # 参数不丢失(脚本任务无参数, 不受影响)
                            args=tuple(db_jd.get("args") or []),
                            kwargs=db_jd.get("kwargs") or {},
                        )
                        heapq.heappush(self.jobs, new_job)
                        if not self.quiet:
                            logger.info(
                                f"Synced new script job: {db_jd['func_name']} "
                                f"({db_jd['job_id']})"
                            )
        except Exception as e:
            logger.error(f"Failed to sync control state: {e}")

    def _run_subprocess(self, cmd: list, timeout, env: dict):
        """执行子进程并捕获输出, 超时时杀整个进程组.

        subprocess.run 的超时只 kill 直接子进程; 脚本若派生了孙进程会残留.
        这里以独立进程组启动(start_new_session), 超时后 killpg 整组,
        再回收输出并按原语义抛 TimeoutExpired.

        Args:
            cmd: 命令行列表.
            timeout: 超时秒数, None 表示不限.
            env: 子进程环境变量.

        Returns:
            (returncode, stdout, stderr) 三元组.

        Raises:
            subprocess.TimeoutExpired: 执行超时(整个进程组已被杀).
        """
        import os
        import signal
        import subprocess

        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=env,
            start_new_session=True,
        )
        try:
            stdout, stderr = proc.communicate(timeout=timeout)
        except subprocess.TimeoutExpired:
            try:
                os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
            except (ProcessLookupError, PermissionError, OSError):
                proc.kill()  # 进程组已不存在或无权限时退化为杀直接子进程
            stdout, stderr = proc.communicate()
            raise
        return proc.returncode, stdout, stderr

    def _run_script(self, job: Job):
        """执行外部 Python 脚本文件或动态代码。

        支持两种执行模式（互斥）:
            - script_path 模式: 直接执行指定路径的 .py 文件
            - script_code 模式: 将动态代码写入临时文件后执行，执行完毕自动清理

        使用当前 Python 解释器（sys.executable，即虚拟环境的解释器）
        通过 subprocess 执行。子进程环境变量额外注入 SCHEDULER_JOB_ID /
        SCHEDULER_JOB_RETRY_COUNT / SCHEDULER_JOB_MAX_RETRIES, 供脚本感知
        调度上下文(如实现"仅在重试耗尽进入死信时告警"的策略)。

        执行方式:
            python <script_path> <script_args...>
            python <temp_file> <script_args...>  (动态代码模式)

        错误处理:
            - 脚本文件不存在: 抛出 FileNotFoundError
            - 脚本退出码非 0: 抛出 RuntimeError（包含 stderr 输出）
            - 执行超时: subprocess.run 内部抛出 TimeoutExpired

        Args:
            job: 包含 script_path 或 script_code 的 Job 对象

        Raises:
            FileNotFoundError: 脚本文件不存在
            RuntimeError: 脚本执行失败（退出码非 0）
            subprocess.TimeoutExpired: 执行超时
        """
        import os
        import sys
        import tempfile

        # 调度上下文环境变量: retry_count 为本次执行前已累计的失败次数,
        # 等于 max_retries 时表示本次失败将进入死信队列.
        script_env = {
            **os.environ,
            "SCHEDULER_JOB_ID": job.job_id,
            "SCHEDULER_JOB_RETRY_COUNT": str(job.retry_count),
            "SCHEDULER_JOB_MAX_RETRIES": str(job.max_retries),
        }

        # 动态代码模式：写入临时文件执行
        if job.script_code:
            # 使用 NamedTemporaryFile 创建临时 .py 文件
            # delete=False 让我们在 subprocess 执行期间保留文件，事后手动删除
            with tempfile.NamedTemporaryFile(
                mode="w", suffix=".py", delete=False, encoding="utf-8"
            ) as tmp:
                tmp.write(job.script_code)
                tmp_path = tmp.name

            try:
                cmd = [sys.executable, tmp_path] + [str(a) for a in job.script_args]
                if not self.quiet:
                    logger.info(f"Executing inline code (job: {job.func_name})")

                returncode, stdout, stderr = self._run_subprocess(
                    cmd, job.timeout, script_env
                )

                if returncode != 0:
                    raise RuntimeError(
                        f"Inline code exited with code {returncode}: "
                        f"{stderr.strip()}"
                    )

                if stdout and not self.quiet:
                    logger.info(f"Inline code output: {stdout.strip()}")
            finally:
                # 无论成功失败，都清理临时文件
                try:
                    os.unlink(tmp_path)
                except OSError:
                    pass
            return

        # 脚本文件模式
        if not os.path.exists(job.script_path):
            raise FileNotFoundError(f"Script not found: {job.script_path}")

        cmd = [sys.executable, job.script_path] + [str(a) for a in job.script_args]

        if not self.quiet:
            logger.info(f"Executing script: {' '.join(cmd)}")

        returncode, stdout, stderr = self._run_subprocess(cmd, job.timeout, script_env)

        if returncode != 0:
            raise RuntimeError(
                f"Script exited with code {returncode}: "
                f"{stderr.strip()}"
            )

        if stdout and not self.quiet:
            logger.info(f"Script output: {stdout.strip()}")

    def _execute_job(self, job: Job):
        """执行单个任务（含重试逻辑）。

        这是任务执行的核心方法，在线程池中调用。执行流程:

        1. 前置检查:
            - func、script_code、script_path 都为 None 时，记录错误并返回
            - 将 job_id 加入 _running_jobs 集合

        2. 执行任务（按优先级选择执行方式）:
            - func 不为 None: 优先 in-process 执行
              * 异步函数（iscoroutinefunction）: asyncio.run() 执行
              * 同步函数: 直接调用，支持超时控制
            - func 为 None 但有 script_path/script_code: 调用 _run_script()
              （重启后未通过 register_tasks 重新注册时的回退路径）

        3. 成功处理:
            - 记录 COMPLETED 历史
            - 若为重复任务，计算下次执行时间并重新入队
            - 若为一次性任务，标记为 COMPLETED（不入队）

        4. 失败处理（指数退避重试）:
            - 若 retry_count < max_retries: 重试
              * 延迟 = 2^retry_count 秒（指数退避）
              * 设为 SCHEDULED，重新入队
            - 若达到最大重试次数: 记录 FAILED 历史
              * 重复任务仍会调度下次正常执行（重置 retry_count）

        5. 取消检查（finally 块）:
            - 若任务在执行期间被 API 取消，从队列中移除
            - 异步保存状态

        Args:
            job: 要执行的 Job 对象
        """
        if job.func is None and not job.script_path and not job.script_code:
            # 占位任务(重启后函数未重新注册且无脚本可执行): 出堆后不再有
            # 执行者, 显式删除存储记录, 防止每次重启重复加载成死记录
            # (save_state 不再做全表删除, 残留记录不会自愈).
            logger.error(f"Job {job.func_name} has no function, skipping")
            self._storage.delete_job(job.job_id)
            # 此 return 不经过下方 finally, 主循环 pop 时已预登记执行中,
            # 必须在此清理, 否则该 job_id 永久滞留 _running_jobs
            # (幽灵"执行中"状态, 且有界停机会被卡到超时).
            with self.lock:
                self._running_jobs.discard(job.job_id)
                self._running_job_objs.pop(job.job_id, None)
            return

        with self.lock:
            self._running_jobs.add(job.job_id)
            self._running_job_objs[job.job_id] = job

        job.status = JobStatus.RUNNING
        job.last_run = time.time()
        job.run_count += 1

        self._log_history(
            job.job_id,
            job.func_name,
            JobStatus.RUNNING,
            run_count=job.run_count,
            retry_count=job.retry_count,
        )

        start_time = time.time()

        try:
            if job.func is not None:
                # 优先使用内存中的函数对象（in-process 执行，无子进程开销）
                if asyncio.iscoroutinefunction(job.func):
                    if job.timeout:
                        asyncio.run(
                            asyncio.wait_for(
                                job.func(*job.args, **job.kwargs), timeout=job.timeout
                            )
                        )
                    else:
                        asyncio.run(job.func(*job.args, **job.kwargs))
                else:
                    if job.timeout:
                        from concurrent.futures import (
                            TimeoutError as FuturesTimeoutError,
                        )

                        executor = ThreadPoolExecutor(max_workers=1)
                        try:
                            future = executor.submit(job.func, *job.args, **job.kwargs)
                            future.result(timeout=job.timeout)
                        except FuturesTimeoutError:
                            # CPython 无法强杀线程: 超时后不等待跑飞的任务
                            # (若用 with 块, 其 __exit__ 的 shutdown(wait=True)
                            # 会阻塞到函数自然结束, 超时形同虚设且卡死 worker
                            # 线程); 此处立即释放超时语义并进入重试链路,
                            # 泄漏的线程运行至函数自然结束.
                            logger.warning(
                                f"{job.func_name} timed out after {job.timeout}s, "
                                f"worker slot released (thread runs to completion)"
                            )
                            raise TimeoutError(
                                f"Job timed out after {job.timeout}s"
                            ) from None
                        finally:
                            executor.shutdown(wait=False, cancel_futures=True)
                    else:
                        job.func(*job.args, **job.kwargs)
            elif job.script_path or job.script_code:
                # 回退到脚本/内联代码执行（subprocess）
                self._run_script(job)

            execution_time = time.time() - start_time

            with self.lock:
                self.stats["total_runs"] += 1

            self._log_history(
                job.job_id,
                job.func_name,
                JobStatus.COMPLETED,
                run_count=job.run_count,
                retry_count=job.retry_count,
                execution_time=execution_time,
            )

            if not self.quiet:
                logger.info(f"{job.func_name} completed ({execution_time:.2f}s)")

            # Schedule next run AFTER successful execution
            if job.repeat:
                job.status = JobStatus.SCHEDULED
                job.retry_count = 0
                self._schedule_next_run(job)
                with self.lock:
                    heapq.heappush(self.jobs, job)
            else:
                job.status = JobStatus.COMPLETED
                # once 任务生命周期终结: 显式删除存储记录. save_state 不做
                # 删除(P1), 残留记录会被 _sync_control_state 当作新增任务
                # 重建副本, next_run 为已过的旧值, 导致一次性任务被无限
                # 重复执行(实测 4 秒内重执行 4 次).
                self._storage.delete_job(job.job_id)

        except Exception as e:
            execution_time = time.time() - start_time
            error_msg = f"{type(e).__name__}: {str(e)}"

            with self.lock:
                self.stats["total_failures"] += 1
                # total_runs 口径与 query_stats 一致: completed + failed
                # (含 missed), 失败同样计入总执行次数.
                self.stats["total_runs"] += 1

            if job.retry_count < job.max_retries:
                job.retry_count += 1
                job.status = JobStatus.SCHEDULED
                retry_delay = 2**job.retry_count
                job.next_run = time.time() + retry_delay

                with self.lock:
                    heapq.heappush(self.jobs, job)
                    self.stats["total_retries"] += 1

                if not self.quiet:
                    logger.warning(
                        f"{job.func_name} failed, retrying in {retry_delay}s "
                        f"({job.retry_count}/{job.max_retries})"
                    )

                self._log_history(
                    job.job_id,
                    job.func_name,
                    JobStatus.FAILED,
                    error=f"Retry {job.retry_count}/{job.max_retries}: {error_msg}",
                    run_count=job.run_count,
                    retry_count=job.retry_count,
                    execution_time=execution_time,
                )
            else:
                job.status = JobStatus.FAILED
                if not self.quiet:
                    logger.error(
                        f"{job.func_name} failed after {job.max_retries} retries: {error_msg}"
                    )

                self._log_history(
                    job.job_id,
                    job.func_name,
                    JobStatus.FAILED,
                    error=f"Max retries exceeded: {error_msg}",
                    run_count=job.run_count,
                    retry_count=job.retry_count,
                    execution_time=execution_time,
                )

                # For repeating jobs, schedule next regular run even after max retries
                if job.repeat:
                    job.retry_count = 0
                    self._schedule_next_run(job)
                    with self.lock:
                        heapq.heappush(self.jobs, job)
                else:
                    # once 任务重试耗尽进入死信后生命周期终结, 同成功路径:
                    # 显式删除存储记录, 防止 sync 重建导致重复执行
                    self._storage.delete_job(job.job_id)

        finally:
            with self.lock:
                self._running_jobs.discard(job.job_id)
                self._running_job_objs.pop(job.job_id, None)
                # 若 job 在执行期间被 API 取消，从队列中移除（之前可能已被 push 回去）
                if job.job_id in self._cancelled_during_run:
                    self._cancelled_during_run.discard(job.job_id)
                    self.jobs = [j for j in self.jobs if j.job_id != job.job_id]
                    heapq.heapify(self.jobs)
                    if not self.quiet:
                        logger.info(
                            f"Removed cancelled job {job.func_name} ({job.job_id}) after execution"
                        )
            # 保存前采纳存储中的暂停标志: 若 API 在执行的最后瞬间(尚无同步
            # 周期介入)发起暂停, 此处兜底采纳, 防止全量保存把它覆写丢失
            self._adopt_paused_flags()
            self._save_state_async()

    def _adopt_paused_flags(self):
        """从存储采纳暂停标志(仅暂停方向), 在执行完成保存前调用.

        场景: API 在任务执行的尾声发起暂停, 距执行完成不足一个同步周期,
        _sync_control_state 尚未把标志应用到内存对象; 若不采纳, 执行完成
        后的全量保存(以本进程内存为准)会把存储中的暂停覆写回 False.

        并发与方向约束:
            - 加载在锁内进行, 与本进程 pause/resume 的"改标志+保存"串行化,
              不会读到本进程写前的旧值;
            - 只把 False 采纳为 True(暂停方向). 反向(内存 True 被存储 False
              覆盖)可能是本进程刚恢复的旧值, 交给周期同步处理更安全.
        """
        try:
            with self.lock:
                state = self._storage.load_state()
                if not state:
                    return
                db_jobs = {j["job_id"]: j for j in state.get("jobs", [])}
                changed = False
                for heap_job in self.jobs:
                    db_entry = db_jobs.get(heap_job.job_id)
                    if db_entry and db_entry.get("paused", False) and not heap_job.paused:
                        heap_job.paused = True
                        changed = True
                for running_id, running_job in list(self._running_job_objs.items()):
                    db_entry = db_jobs.get(running_id)
                    if db_entry and db_entry.get("paused", False) and not running_job.paused:
                        running_job.paused = True
                        changed = True
            if changed and not self.quiet:
                logger.info("Adopted paused flags from storage before save")
        except Exception as e:
            logger.error(f"Failed to adopt paused flags: {e}")

    def _save_state_async(self):
        """异步保存状态到存储后端。

        将保存任务提交到 _save_executor（单线程池）异步执行，
        避免阻塞调度主循环。单线程池确保保存操作串行化，
        避免并发写入冲突。

        若保存线程池已满或关闭，记录错误日志但不影响调度。
        """
        try:
            self._save_executor.submit(self._save_state)
        except Exception as e:
            logger.error(f"Failed to queue state save: {e}")

    def flush_state(self, timeout: float = 10.0) -> None:
        """等待所有积压的异步保存任务完成。

        在 register_tasks 后、scheduler.start() 前调用，确保所有已注册
        任务持久化到数据库，避免 _sync_control_state 误判未落库的任务
        为"已取消"而从内存队列移除。

        实现原理：向 _save_executor（单线程 FIFO 池）提交一个空任务并
        阻塞等待其完成。由于线程池是串行的，当空任务开始执行时，之前
        排队的所有 _save_state 任务已执行完毕。

        Args:
            timeout: 最大等待秒数（默认 10 秒）
        """
        try:
            future = self._save_executor.submit(lambda: None)
            future.result(timeout=timeout)
        except Exception as e:
            logger.warning(f"flush_state timed out or failed: {e}")

    def _save_state(self):
        """同步保存状态到存储后端。

        序列化以下数据并调用存储后端的 save_state:
            - jobs: 所有任务的字典表示（最近 1000 条历史）
            - history: 历史记录列表
            - statistics: 统计信息
            - job_counter: 当前 job ID 计数器
            - scheduler_running: 调度器运行状态

        使用 self.lock 保护共享数据的读取。

        落库前暂停保留: 载荷收集(锁内快照)与实际落库之间存在时间窗,
        其他进程(API)在该窗口写入的暂停标志会被旧快照覆盖且此后无人
        再写回(实测高频场景约 4% 命中). "读取存储现有暂停标志 -> 合并
        -> 写入"必须原子执行, 由存储后端在文件锁/行锁内完成
        (json: flock; sqlmodel: SELECT FOR UPDATE).
        """
        try:
            with self.lock:
                heap_ids = {job.job_id for job in self.jobs}
                jobs = [job.to_dict() for job in self.jobs]
                # 执行中的任务已被 pop 出堆, 但必须随状态一起持久化: 否则
                # 其他任务完成触发的保存会把执行中的任务从存储抹掉, 下一轮
                # _sync_control_state 会把它误判为"数据库中不存在=已取消",
                # 执行完成后被永久移除(长耗时任务必然中招); API 视图在
                # 任务执行期间也会看不到它. 执行中对象的 next_run 为执行前
                # 旧值, 执行完成重排后的下一次保存会自然修正.
                jobs.extend(
                    job.to_dict()
                    for job_id, job in self._running_job_objs.items()
                    if job_id not in heap_ids
                )
                history = [h.to_dict() for h in self.history[-1000:]]

                # resume 豁免一次性消费: resume_job 内的同步保存(首个
                # 消费者)把本地恢复意图的 False 落库后即从意图表移除,
                # 后续保存不再豁免 -- 否则 10s 豁免窗口内其他进程(API)
                # 的二次暂停会被本进程旧载荷继续吞掉. 豁免失效后由
                # 保留逻辑+周期同步接管: 内存经 sync 读回 True, 或旧
                # 载荷被保留合并纠正为 True.
                now = time.time()
                self._local_pause_events = {
                    jid: evt
                    for jid, evt in self._local_pause_events.items()
                    if now - evt[0] <= 10.0
                }
                resume_exempt = {
                    jid
                    for jid, (_, paused) in self._local_pause_events.items()
                    if paused is False
                }
                for jid in resume_exempt:
                    del self._local_pause_events[jid]

            self._storage.save_state(
                jobs=jobs,
                history=history,
                statistics=self.stats,
                job_counter=self._job_counter_value,
                scheduler_running=self.running,
                pause_preserve_exempt=resume_exempt,
            )

        except Exception as e:
            logger.error(f"Failed to save state: {e}")

    def _load_state(self):
        """从存储后端加载历史状态。

        加载流程:
            1. 调用 storage.load_state() 获取状态字典
            2. 恢复 job_counter（并确保不与已加载的 job_id 冲突）
            3. 恢复 scheduler_running 标志（多进程只读模式用）
            4. 恢复历史记录（过滤掉 timestamp_readable 字段）
            5. 恢复统计数据
            6. 恢复任务队列:
                * 通过 job_registry 查找函数对象
                * 找不到的函数设为 None（多进程只读模式可展示但不执行）
                * 脚本任务（func_module="__script__"）直接恢复

        job_counter 冲突处理:
            多进程模式下，API 进程可能已分配 job_id 但 job_counter 未同步，
            因此需要遍历所有 job_id，确保 job_counter 从最大值+1 开始。
        """
        try:
            state = self._storage.load_state()

            if state is None:
                return

            self._job_counter_value = state.get("_job_counter", 0)

            # 读取外部 scheduler 进程的运行状态（多进程只读模式）
            self._external_running = state.get("scheduler_running", False)

            self.history = [
                JobHistory(**{k: v for k, v in h.items() if k != "timestamp_readable"})
                for h in state.get("history", [])
            ]

            with self.lock:
                self._cleanup_history()

            self.stats.update(state.get("statistics", {}))

            job_data = state.get("jobs", [])
            restored_count = 0

            for jd in job_data:
                # 逐条容错: 单条损坏记录(缺字段/非法枚举/类型异常)只跳过
                # 该条并告警, 不能让整个 _load_state 异常导致全部任务丢失
                # (外层 except 会吞掉异常, 调度器以空任务集启动, 下一次
                # 保存还会把存储里的其余任务抹掉).
                try:
                    # 已终结的 once 任务不恢复(否则重启即再执行一次):
                    # 其记录可能因并发保存快照复活残留在存储中,
                    # 与 _sync_control_state 的清理判定保持同一口径.
                    if not jd.get("repeat", True) and jd.get(
                        "status", "scheduled"
                    ) != "scheduled":
                        continue
                    func_key = f"{jd['func_module']}.{jd['func_name']}"

                    # 函数在注册表中：恢复完整 job；不在注册表中：恢复为 func=None 的只读 job
                    # （多进程只读模式下，API 进程没有 scheduler 的函数，但仍需展示 job 信息）
                    restored_status = JobStatus(jd["status"])
                    # 重启后不存在任何在途执行, 持久化的 RUNNING 状态是
                    # 保存时执行中的快照, 若原样恢复会成为无人推进的
                    # 幽灵"running"显示, 统一重置回 SCHEDULED.
                    if restored_status == JobStatus.RUNNING:
                        restored_status = JobStatus.SCHEDULED
                    job = Job(
                        job_id=jd["job_id"],
                        func=self.job_registry.get(func_key),
                        func_name=jd["func_name"],
                        func_module=jd["func_module"],
                        next_run=jd["next_run"],
                        interval=jd["interval"],
                        repeat=jd["repeat"],
                        status=restored_status,
                        created_at=jd["created_at"],
                        last_run=jd.get("last_run"),
                        run_count=jd.get("run_count", 0),
                        max_retries=jd.get("max_retries", 3),
                        retry_count=jd.get("retry_count", 0),
                        catch_up=jd.get("catch_up", True),
                        schedule_type=jd.get("schedule_type", "interval"),
                        schedule_time=jd.get("schedule_time"),
                        schedule_days=jd.get("schedule_days"),
                        timeout=jd.get("timeout"),
                        paused=jd.get("paused", False),
                        timezone=jd.get("timezone"),
                        cron_expression=jd.get("cron_expression"),
                        script_path=jd.get("script_path"),
                        script_args=jd.get("script_args", []),
                        script_code=jd.get("script_code"),
                        args=tuple(jd.get("args") or []),
                        kwargs=jd.get("kwargs") or {},
                    )
                except (KeyError, ValueError, TypeError) as e:
                    logger.error(
                        f"Skipping corrupted job record during load: {e} "
                        f"(record={jd if not isinstance(jd, dict) else jd.get('job_id', '<no-id>')})"
                    )
                    continue
                heapq.heappush(self.jobs, job)
                restored_count += 1

            if restored_count > 0:
                if not self.quiet:
                    logger.info(f"Loaded state: {restored_count} jobs restored")

            # 确保 job_counter 不会与已加载的 job_id 冲突
            # （多进程模式下，API 进程可能已分配 job_id 但 job_counter 未同步）
            max_existing = self._job_counter_value
            for j in self.jobs:
                if j.job_id and j.job_id.startswith("job_"):
                    try:
                        num = int(j.job_id[4:])
                        if num >= max_existing:
                            max_existing = num + 1
                    except ValueError:
                        pass
            self._job_counter_value = max_existing
            self._job_counter = itertools.count(self._job_counter_value)

        except Exception as e:
            logger.error(f"Failed to load state: {e}")

    def _load_dead_letters(self):
        """从存储后端加载死信队列。

        死信队列存储达到最大重试次数后仍然失败的任务记录。
        加载时过滤掉 timestamp_readable 字段（非 JobHistory 字段），
        并限制队列长度不超过 max_dead_letters。
        """
        try:
            dead_letter_dicts = self._storage.load_dead_letters()

            self.dead_letters = [
                JobHistory(**{k: v for k, v in h.items() if k != "timestamp_readable"})
                for h in dead_letter_dicts
            ]

            if len(self.dead_letters) > self.max_dead_letters:
                self.dead_letters = self.dead_letters[-self.max_dead_letters :]

            if not self.quiet and self.dead_letters:
                logger.info(f"Loaded {len(self.dead_letters)} dead letter entries")

        except Exception as e:
            logger.error(f"Failed to load dead letters: {e}")

    def refresh_state(self):
        """从存储后端重新加载状态（用于多进程只读模式）。

        在多进程部署中，API 进程不执行任务，只负责展示状态。
        此方法清空内存中的 jobs/history/dead_letters，
        然后从存储后端重新加载，使本进程能反映 worker 进程写入的最新状态。

        调用场景:
            - FastAPI 的 SSE 事件流每秒调用一次
            - 每次 API 请求前确保数据最新

        并发与节流:
            - 整个"清空+重载"在同一锁临界区内完成(self.lock 为 RLock,
              _load_state 内部加锁可重入), 避免并发读方在清空与重载
              之间看到空列表造成 dashboard 闪烁/404;
            - 多个 SSE 客户端共享节流: 0.8s 内的重复调用直接跳过,
              避免每个连接每秒各自全量加载存储.

        注意:
            此方法不会启动调度循环，仅更新用于展示的数据。
        """
        now = time.time()
        if now - self._last_refresh < 0.8:
            return
        self._last_refresh = now
        with self.lock:
            self.jobs = []
            self.history = []
            self.dead_letters = []
            self._job_counter_value = 0
            self._job_counter = itertools.count(0)
            self._load_state()
            self._load_dead_letters()

    def is_running(self) -> bool:
        """获取调度器运行状态。

        多进程模式下，返回本进程运行状态 OR 外部 scheduler 进程运行状态。
        用于 Dashboard 显示整体系统是否在运行。

        Returns:
            True: 本进程或外部 scheduler 进程在运行
            False: 都未运行
        """
        return self.running or self._external_running

    def _save_dead_letters_async(self):
        """异步保存死信队列到存储后端。

        将保存任务提交到 _save_executor（单线程池）异步执行，
        避免阻塞调用方。
        """
        try:
            self._save_executor.submit(self._save_dead_letters)
        except Exception as e:
            logger.error(f"Failed to queue dead letters save: {e}")

    def _save_dead_letters(self):
        """同步保存死信队列到存储后端。

        序列化 dead_letters 列表为字典，调用 storage.save_dead_letters。
        使用 self.lock 保护读取。
        """
        try:
            with self.lock:
                dead_letters = [dl.to_dict() for dl in self.dead_letters]

            self._storage.save_dead_letters(dead_letters, self.max_dead_letters)

        except Exception as e:
            logger.error(f"Failed to save dead letters: {e}")

    def get_dead_letters(self, limit: int = 100) -> List[Dict]:
        """获取死信队列记录（失败的任务）。

        死信队列存储达到最大重试次数后仍然失败的任务记录，
        用于后续分析和排查。

        Args:
            limit: 返回记录数量上限（默认 100，返回最近的记录）

        Returns:
            死信记录字典列表（按时间倒序，最新的在前）
        """
        with self.lock:
            # 上界钳制: 与 get_history 一致, 防外部传入超大 limit 触发全量序列化
            bounded_limit = max(1, min(int(limit), 1000))
            return [dl.to_dict() for dl in self.dead_letters[-bounded_limit:][::-1]]

    def clear_dead_letters(self) -> int:
        """清空所有死信队列记录。

        Returns:
            清除的记录数量
        """
        with self.lock:
            count = len(self.dead_letters)
            self.dead_letters = []
        self._save_dead_letters_async()
        return count

    # ==================== Monitoring & Management ====================
    # 以下方法提供监控和管理功能，供 Dashboard 和 REST API 调用。
    # 所有方法都是线程安全的（使用 self.lock）。

    def get_jobs(self) -> List[Dict]:
        """获取所有任务列表（按 next_run 排序）。

        返回每个任务的摘要信息，包含:
            - job_id: 任务 ID
            - func_name: 函数名
            - status: 当前状态（额外显示 "running" 和 "paused" 状态）
            - schedule: 调度描述（如 "Every 60 seconds"）
            - next_run: 下次执行时间（格式化字符串）
            - next_run_in: 距下次执行的秒数
            - run_count: 总执行次数
            - retry_count: 当前重试次数
            - paused: 是否暂停
            - last_run: 上次执行时间

        Returns:
            任务字典列表，按 next_run 升序排列
        """
        with self.lock:
            # 执行中的任务已被 pop 出堆, 必须并入 _running_job_objs 才可见:
            # 否则单进程/嵌入式部署下, 长任务执行期间会从 Dashboard 消失.
            entries = list(self.jobs)
            heap_ids = {j.job_id for j in entries}
            entries.extend(
                job
                for jid, job in self._running_job_objs.items()
                if jid not in heap_ids
            )
            return [
                {
                    "job_id": job.job_id,
                    "func_name": job.func_name,
                    "status": (
                        JobStatus.RUNNING.value
                        if job.job_id in self._running_jobs
                        else ("paused" if job.paused else job.status.value)
                    ),
                    "schedule": job.get_schedule_description(),
                    "next_run": datetime.fromtimestamp(job.next_run).strftime(
                        "%Y-%m-%d %H:%M:%S"
                    ),
                    "next_run_in": max(0, job.next_run - time.time()),
                    "run_count": job.run_count,
                    "retry_count": job.retry_count,
                    "paused": job.paused,
                    "last_run": (
                        datetime.fromtimestamp(job.last_run).strftime(
                            "%Y-%m-%d %H:%M:%S"
                        )
                        if job.last_run
                        else None
                    ),
                }
                for job in sorted(entries, key=lambda j: j.next_run)
            ]

    def get_history(
        self, func_name: Optional[str] = None, limit: int = 50
    ) -> List[Dict]:
        """获取任务执行历史记录。

        Args:
            func_name: 可选，按函数名过滤（只返回匹配的记录）
            limit: 返回记录数量上限（默认 50，返回最近的记录）

        Returns:
            历史记录字典列表，每条包含:
                - job_id, func_name, status, timestamp, error
                - run_count, retry_count, execution_time
                - timestamp_readable: 人类可读时间字符串
        """
        with self.lock:
            history = (
                self.history
                if not func_name
                else [h for h in self.history if h.func_name == func_name]
            )
            # 上界钳制: API 层 limit 由外部传入, 无上界时会触发全量序列化
            bounded_limit = max(1, min(int(limit), 1000))
            return [h.to_dict() for h in history[-bounded_limit:]]

    def get_history_paged(
        self,
        func_name: Optional[str] = None,
        status: Optional[str] = None,
        keyword: Optional[str] = None,
        page: int = 1,
        page_size: int = 20,
    ) -> Dict:
        """分页获取任务执行历史记录，支持按状态、任务名、关键词筛选。

        优先直接查询数据库（避免内存缓存只有最近 1000 条的限制），
        若存储后端不支持数据库分页查询则 fallback 到内存分页。

        Args:
            func_name: 可选，按函数名精确过滤
            status:    可选，按状态过滤（completed/failed/missed）
            keyword:   可选，按函数名或错误信息模糊匹配（大小写不敏感）
            page:      页码（从 1 开始）
            page_size: 每页条数

        Returns:
            {"items": [...], "total": int, "page": int, "page_size": int}
        """
        # 参数钳制: page/page_size 由 API 外部传入, 防超大 page_size 触发
        # 全量查询与序列化(DB 路径的 LIMIT 同样受益)
        page = max(1, int(page))
        page_size = max(1, min(int(page_size), 200))

        # 优先直接查询数据库（数据库中有完整历史，内存仅缓存最近 1000 条）
        db_query = getattr(self._storage, "query_history_paged", None)
        if db_query is not None:
            return db_query(func_name, status, keyword, page, page_size)

        # Fallback: 内存分页（JSON 后端或无数据库时）
        with self.lock:
            items = list(self.history)

        items = [h for h in items if h.status in ("completed", "failed", "missed")]

        if func_name and func_name != "all":
            items = [h for h in items if h.func_name == func_name]
        if status and status != "all":
            items = [h for h in items if h.status == status]
        if keyword:
            kw = keyword.lower()
            items = [
                h for h in items
                if kw in h.func_name.lower()
                or (h.error and kw in h.error.lower())
            ]

        items.reverse()

        total = len(items)
        start = (page - 1) * page_size
        end = start + page_size
        page_items = items[start:end]

        return {
            "items": [h.to_dict() for h in page_items],
            "total": total,
            "page": page,
            "page_size": page_size,
        }

    def get_dead_letters_paged(
        self,
        keyword: Optional[str] = None,
        page: int = 1,
        page_size: int = 20,
    ) -> Dict:
        """分页获取死信队列记录，支持关键词筛选。

        优先直接查询数据库，若存储后端不支持则 fallback 到内存分页。

        Args:
            keyword:   可选，按函数名或错误信息模糊匹配（大小写不敏感）
            page:      页码（从 1 开始）
            page_size: 每页条数

        Returns:
            {"items": [...], "total": int, "page": int, "page_size": int}
        """
        # 参数钳制: 与 get_history_paged 一致
        page = max(1, int(page))
        page_size = max(1, min(int(page_size), 200))

        # 优先直接查询数据库
        db_query = getattr(self._storage, "query_dead_letters_paged", None)
        if db_query is not None:
            return db_query(keyword, page, page_size)

        # Fallback: 内存分页
        with self.lock:
            items = list(self.dead_letters)

        if keyword:
            kw = keyword.lower()
            items = [
                dl for dl in items
                if kw in dl.func_name.lower()
                or (dl.error and kw in dl.error.lower())
            ]

        items.reverse()

        total = len(items)
        start = (page - 1) * page_size
        end = start + page_size
        page_items = items[start:end]

        return {
            "items": [dl.to_dict() for dl in page_items],
            "total": total,
            "page": page,
            "page_size": page_size,
        }

    def get_statistics(self) -> Dict:
        """获取调度器统计数据。

        返回全局统计和每个任务的统计:
            - total_runs: 总执行次数
            - total_failures: 总失败次数
            - total_retries: 总重试次数
            - start_time: 启动时间戳
            - uptime_seconds: 运行时长（秒）
            - uptime_readable: 运行时长（人类可读）
            - active_jobs: 当前活跃任务数
            - per_job: 每个任务的统计（completed/failed/total_runs）

        数据来源优先级:
            1. 数据库历史记录表（query_stats）— 完整且准确，多进程安全
            2. 内存统计（self.stats + self.history）— fallback，仅近 max_history 条

        Returns:
            统计信息字典
        """
        # 优先从数据库历史记录表统计（完整且准确）
        db_stats = getattr(self._storage, "query_stats", None)
        if db_stats is not None:
            db_result = db_stats()
            if db_result is not None:
                with self.lock:
                    stats = self.stats.copy()
                    # 用数据库统计覆盖内存计数器
                    stats["total_runs"] = db_result["total_runs"]
                    stats["total_failures"] = db_result["total_failures"]
                    stats["per_job"] = db_result["per_job"]

                    if stats["start_time"]:
                        stats["uptime_seconds"] = time.time() - stats["start_time"]
                        stats["uptime_readable"] = _format_uptime_cn(
                            int(stats["uptime_seconds"])
                        )
                    stats["active_jobs"] = len(self.jobs)
                    return stats

        # Fallback: 内存统计（JSON 后端或数据库查询失败时）
        with self.lock:
            stats = self.stats.copy()

            if stats["start_time"]:
                stats["uptime_seconds"] = time.time() - stats["start_time"]
                stats["uptime_readable"] = _format_uptime_cn(
                    int(stats["uptime_seconds"])
                )

            job_stats = defaultdict(
                lambda: {"completed": 0, "failed": 0, "total_runs": 0}
            )

            for event in self.history:
                if event.status in ["completed", "failed"]:
                    job_stats[event.func_name]["total_runs"] += 1
                    job_stats[event.func_name][event.status] += 1

            stats["per_job"] = dict(job_stats)
            stats["active_jobs"] = len(self.jobs)

            return stats

    def cancel_job(self, job_id: str) -> bool:
        """取消并移除指定 ID 的任务。

        将任务从队列中删除，并记录一条 COMPLETED 历史。
        若任务正在执行，无法立即停止执行，但会阻止后续重试和重新调度
        （标记 _cancelled_during_run，执行完成后不再入队）。

        落库方式: 移除内存条目后通过 storage.delete_job 显式删除存储记录。
        save_state 只做 upsert 不删除（防止 worker 的周期性保存在跨进程
        写-写竞态中吞掉 API 进程新增的任务），取消语义必须在此显式落库。

        Args:
            job_id: 要取消的任务 ID

        Returns:
            True: 取消成功（含"正在执行、已登记待移除"的情况）
            False: 任务不存在
        """
        with self.lock:
            for i, job in enumerate(self.jobs):
                if job.job_id == job_id:
                    self.jobs.pop(i)
                    heapq.heapify(self.jobs)
                    self._log_history(job_id, job.func_name, JobStatus.COMPLETED)
                    self._storage.delete_job(job_id)
                    if not self.quiet:
                        logger.info(f"Cancelled job: {job.func_name} ({job_id})")
                    self._save_state_async()
                    return True

            # 任务不在堆中但在执行中: 在途执行无法中断(也不应中断),
            # 标记完成后不再重新入队, 并立即从存储移除(API 视图即时生效)
            if job_id in self._running_job_objs:
                self._cancelled_during_run.add(job_id)
                self._storage.delete_job(job_id)
                running_job = self._running_job_objs[job_id]
                if not self.quiet:
                    logger.info(
                        f"Cancelling running job: {running_job.func_name} "
                        f"({job_id}), removal after current execution"
                    )
                return True
        return False

    def cancel_job_by_name(self, func_name: str) -> int:
        """取消所有指定函数名的任务。

        适用于批量取消同名的任务。

        Args:
            func_name: 任务函数名

        Returns:
            取消的任务数量
        """
        with self.lock:
            cancelled = 0
            jobs_to_keep = []
            for job in self.jobs:
                if job.func_name == func_name:
                    self._log_history(job.job_id, job.func_name, JobStatus.COMPLETED)
                    # 与 cancel_job 一致: 显式删除存储记录, 不依赖全量保存覆盖
                    self._storage.delete_job(job.job_id)
                    cancelled += 1
                else:
                    jobs_to_keep.append(job)

            if cancelled > 0:
                self.jobs = jobs_to_keep
                heapq.heapify(self.jobs)
                if not self.quiet:
                    logger.info(f"Cancelled {cancelled} job(s) with name: {func_name}")
                self._save_state_async()

            return cancelled

    def pause_job(self, job_id: str) -> bool:
        """暂停指定任务。

        任务保留在队列中但不会被调度执行，直到调用 resume_job 恢复。
        适用于临时停止某个任务而不删除它。

        Args:
            job_id: 要暂停的任务 ID

        Returns:
            True: 暂停成功
            False: 任务不存在
        """
        with self.lock:
            for job in self.jobs:
                if job.job_id == job_id:
                    job.paused = True
                    # 记录本地意图: 落库前合并豁免, 防止旧存储值反向覆盖
                    self._local_pause_events[job_id] = (time.time(), True)
                    if not self.quiet:
                        logger.info(f"Paused job: {job.func_name} ({job_id})")
                    self._save_state()
                    return True

            # 执行中的任务不在堆内: 直接在其对象上置暂停标志(在途执行
            # 不受影响), 执行完成回堆后由主循环暂停分支持有, 语义与
            # _sync_control_state 的执行中同步段一致.
            running_job = self._running_job_objs.get(job_id)
            if running_job is not None:
                running_job.paused = True
                self._local_pause_events[job_id] = (time.time(), True)
                if not self.quiet:
                    logger.info(
                        f"Paused running job: {running_job.func_name} ({job_id})"
                    )
                self._save_state()
                return True
        return False

    def resume_job(self, job_id: str) -> bool:
        """恢复已暂停的任务。

        恢复时会重新计算下次执行时间。因为暂停期间主循环不断将
        next_run 重置为 now+1，若不重算则恢复后任务会立即触发执行。
        重算后需重新堆化以维持堆序（next_run 是堆排序键）。
        Args:
            job_id: 要恢复的任务 ID

        Returns:
            True: 恢复成功
            False: 任务不存在
        """
        with self.lock:
            for job in self.jobs:
                if job.job_id == job_id:
                    job.paused = False
                    # 记录本地恢复意图: 落库前合并豁免此 id, 保证 False 落库
                    self._local_pause_events[job_id] = (time.time(), False)
                    # 重新计算下次执行时间，避免恢复后立即执行
                    self._schedule_next_run(job)
                    # next_run 改变后重新堆化以维持堆序
                    heapq.heapify(self.jobs)
                    if not self.quiet:
                        logger.info(f"Resumed job: {job.func_name} ({job_id})")
                    self._save_state()
                    return True

            # 执行中的任务不在堆内: 只清暂停标志; 其 next_run 由执行完成
            # 时的 _schedule_next_run 重排, 此处无需(也无法安全)重算.
            running_job = self._running_job_objs.get(job_id)
            if running_job is not None and running_job.paused:
                running_job.paused = False
                self._local_pause_events[job_id] = (time.time(), False)
                if not self.quiet:
                    logger.info(
                        f"Resumed running job: {running_job.func_name} ({job_id})"
                    )
                self._save_state()
                return True
        return False

    def _apply_script_job_updates(self, job: Job, **kwargs) -> None:
        """将脚本任务配置更新应用到 job 上（不检查暂停状态，不保存状态）。

        供 update_job（API 更新）和 add_script_job（同名 upsert）复用。
        调用方负责加锁和保存状态。

        支持更新的字段: script_path/script_code(互斥)、schedule_type、interval、
        schedule_time、cron_expression、script_args、max_retries、timeout、
        timezone、catch_up。更新后会重新计算下次执行时间。
        注意：func_name 不允许修改（任务唯一标识的一部分）。

        Args:
            job: 要更新的 Job 对象
            **kwargs: 要更新的字段键值对

        Raises:
            ValueError: script_path 和 script_code 同时提供，或 script_code 为空
            FileNotFoundError: script_path 指定的文件不存在
        """
        import os

        # 任务源互斥处理：script_path / script_code
        script_path = kwargs.get("script_path")
        script_code = kwargs.get("script_code")
        if script_path and script_code:
            raise ValueError("script_path and script_code are mutually exclusive")
        if script_path is not None:
            if not os.path.exists(script_path):
                raise FileNotFoundError(f"Script not found: {script_path}")
        if script_code is not None and not script_code.strip():
            raise ValueError("script_code cannot be empty")

        # 组合预校验: 以"当前值 + 本次更新"预览最终组合, 通过后才应用字段.
        # 若在应用后校验, 失败异常会把 job 留在半应用状态(如类型已改为
        # daily 而时间仍为空), 破坏后续的正常更新.
        _validate_schedule_config(
            kwargs.get("schedule_type") if kwargs.get("schedule_type") is not None else job.schedule_type,
            kwargs.get("interval") if kwargs.get("interval") is not None else job.interval,
            kwargs.get("schedule_time") if kwargs.get("schedule_time") is not None else job.schedule_time,
            kwargs.get("cron_expression") if kwargs.get("cron_expression") is not None else job.cron_expression,
            job.schedule_days,
        )
        # 可选项类型预校验(同一原则: 失败不产生半应用状态)
        _validate_option_types(
            max_retries=kwargs.get("max_retries"),
            timeout=kwargs.get("timeout"),
            script_args=kwargs.get("script_args"),
        )

        if script_path is not None:
            job.script_path = script_path
            job.script_code = None
        if script_code is not None:
            job.script_code = script_code
            job.script_path = None

        # 调度相关字段
        if kwargs.get("schedule_type") is not None:
            job.schedule_type = kwargs["schedule_type"]
        if kwargs.get("interval") is not None:
            job.interval = kwargs["interval"]
        if kwargs.get("schedule_time") is not None:
            job.schedule_time = kwargs["schedule_time"]
        if kwargs.get("cron_expression") is not None:
            job.cron_expression = kwargs["cron_expression"]

        # 其它可编辑字段
        if kwargs.get("script_args") is not None:
            job.script_args = kwargs["script_args"] or []
        # 注意：func_name 不允许修改（任务唯一标识的一部分）
        if kwargs.get("max_retries") is not None:
            job.max_retries = kwargs["max_retries"]
        if kwargs.get("timeout") is not None:
            job.timeout = kwargs["timeout"]
        if kwargs.get("timezone") is not None:
            job.timezone = kwargs["timezone"]
        if kwargs.get("catch_up") is not None:
            job.catch_up = kwargs["catch_up"]

        # 更新重复执行标志：once 类型为一次性任务，其它类型为重复任务
        if kwargs.get("schedule_type") is not None:
            job.repeat = kwargs["schedule_type"] != "once"

        # 重新计算下次执行时间
        if job.schedule_type in ("daily", "weekly", "hourly", "cron"):
            self._calculate_next_run(job)
        elif job.interval:
            job.next_run = time.time() + job.interval

    def update_job(self, job_id: str, **kwargs) -> bool:
        """更新已暂停任务的配置。

        仅允许更新处于暂停状态的任务，避免运行中任务被修改导致调度混乱。
        支持更新的字段: script_path/script_code(互斥)、schedule_type、interval、
        schedule_time、cron_expression、script_args、max_retries、timeout、
        timezone、catch_up。更新后会重新计算下次执行时间。
        注意：func_name 不允许修改（任务唯一标识的一部分）。

        Args:
            job_id: 要更新的任务 ID
            **kwargs: 要更新的字段键值对

        Returns:
            True: 更新成功
            False: 任务不存在或未处于暂停状态
        """
        with self.lock:
            for job in self.jobs:
                if job.job_id == job_id:
                    # 安全限制：仅允许更新已暂停的任务
                    if not job.paused:
                        return False

                    self._apply_script_job_updates(job, **kwargs)

                    if not self.quiet:
                        logger.info(f"Updated job: {job.func_name} ({job_id})")
                    self._save_state()
                    return True
        return False

    def run_job_now(self, job_id: str) -> bool:
        """立即触发任务执行（仅限已暂停的任务）。

        安全约束: 只有处于暂停状态的任务允许手动触发. 暂停任务不会被调度
        主循环定时执行, 因此手动触发不会与定时执行形成同任务并发双执行
        (尤其多进程部署下, API 进程与 worker 进程相互看不到对方的执行状态,
        以"必须暂停"为前提从调度层面根除并发窗口). 执行完成后任务保持
        暂停状态, 恢复调度需显式调用 resume_job.

        Args:
            job_id: 要立即执行的任务 ID

        Returns:
            True: 触发成功
            False: 任务不存在、未处于暂停状态或正在执行中
        """
        with self.lock:
            for job in self.jobs:
                if job.job_id == job_id:
                    if job.job_id in self._running_jobs:
                        logger.warning(f"Job {job_id} is already running")
                        return False
                    if not job.paused:
                        logger.warning(
                            f"Job {job_id} is not paused, manual run rejected"
                        )
                        return False
                    if not self.quiet:
                        logger.info(f"Manually triggered: {job.func_name} ({job_id})")
                    # 提交前先登记为执行中: 关闭"移出堆 -> executor 线程登记"
                    # 之间的间隙窗口, 防止该间隙内 _sync_control_state 把任务
                    # 误判为数据库新增而重建副本, 造成同 job_id 双对象双执行.
                    self._running_jobs.add(job.job_id)
                    self._running_job_objs[job.job_id] = job
                    # 先将该任务移出调度堆再提交执行: _execute_job 完成后(成功
                    # 重排/失败重试)会重新入队, 若不移出旧条目, 堆内将存在指向
                    # 同一对象的两个条目, 下次到期时主循环在同一迭代内把两个
                    # 条目都弹出并提交, 造成同一任务并发双执行.
                    self.jobs = [j for j in self.jobs if j.job_id != job_id]
                    heapq.heapify(self.jobs)
                    future = self._executor.submit(self._execute_job, job)
                    # 与主循环提交一致: 停机 cancel_futures 取消排队任务时,
                    # 由回调清理提交前的预登记, 避免 _running_jobs 泄漏
                    # 卡住有界停机(_shutdown_executor_bounded 空等超时).
                    future.add_done_callback(
                        lambda f, jid=job_id: self._cleanup_cancelled_submit(f, jid)
                    )
                    return True
        return False

    def _cleanup_cancelled_submit(self, future, job_id: str) -> None:
        """提交被取消(停机 cancel_futures)时清理预登记的执行中状态.

        正常执行路径的清理由 _execute_job 的 finally 负责; 被取消的排队
        任务不会执行, 需在此移除主循环/run_job_now 在提交前登记的
        _running_jobs / _running_job_objs, 避免停机后残留幽灵"执行中"状态.
        """
        if future.cancelled():
            with self.lock:
                self._running_jobs.discard(job_id)
                self._running_job_objs.pop(job_id, None)
            logger.info(f"Cancelled queued job {job_id} during shutdown")

    def get_job(self, job_id: str) -> Optional[Dict]:
        """获取指定 ID 的任务详情。

        与 get_jobs() 不同，此方法返回单个任务的完整信息，
        包含 timeout 字段。

        Args:
            job_id: 任务 ID

        Returns:
            任务字典（同 get_jobs 的元素，但额外包含 timeout），
            若任务不存在返回 None
        """
        with self.lock:
            job = None
            for j in self.jobs:
                if j.job_id == job_id:
                    job = j
                    break
            # 执行中的任务不在堆内(已被 pop), 从执行中映射兜底查找
            if job is None:
                job = self._running_job_objs.get(job_id)
            if job is None:
                return None
            return {
                "job_id": job.job_id,
                "func_name": job.func_name,
                "status": (
                    JobStatus.RUNNING.value
                    if job.job_id in self._running_jobs
                    else ("paused" if job.paused else job.status.value)
                ),
                "schedule": job.get_schedule_description(),
                "next_run": datetime.fromtimestamp(job.next_run).strftime(
                    "%Y-%m-%d %H:%M:%S"
                ),
                "next_run_in": max(0, job.next_run - time.time()),
                "run_count": job.run_count,
                "retry_count": job.retry_count,
                "paused": job.paused,
                "timeout": job.timeout,
                "last_run": (
                    datetime.fromtimestamp(job.last_run).strftime(
                        "%Y-%m-%d %H:%M:%S"
                    )
                    if job.last_run
                    else None
                ),
                # 编辑任务所需的完整配置字段
                "schedule_type": job.schedule_type,
                "interval": job.interval,
                "schedule_time": job.schedule_time,
                "cron_expression": job.cron_expression,
                "script_path": job.script_path,
                "script_code": job.script_code,
                "script_args": job.script_args,
                "max_retries": job.max_retries,
                "timezone": job.timezone,
                "catch_up": job.catch_up,
            }

    def add_script_job(
        self,
        script_path: Optional[str] = None,
        schedule_type: str = "interval",
        interval: Optional[float] = None,
        schedule_time: Optional[str] = None,
        cron_expression: Optional[str] = None,
        script_args: Optional[list] = None,
        func_name: Optional[str] = None,
        max_retries: int = 3,
        timeout: Optional[float] = None,
        timezone: Optional[str] = None,
        catch_up: bool = True,
        script_code: Optional[str] = None,
        allow_update_running: bool = True,
    ) -> str:
        """添加执行外部 Python 脚本或动态代码的任务。

        支持两种互斥模式:
            - script_path: 执行指定路径的 .py 文件
            - script_code: 执行用户输入的 Python 代码字符串（写入临时文件执行）

        两者必须提供其一，若同时提供则报错。通过 subprocess 执行，
        使用当前 Python 解释器（虚拟环境）。

        Args:
            script_path: 脚本文件路径（与 script_code 互斥）
            schedule_type: 调度类型 ("interval"/"daily"/"cron"/"once")
            interval: 间隔秒数（interval/once 类型必填）
            schedule_time: 执行时间（daily 类型格式 "HH:MM"）
            cron_expression: cron 表达式（cron 类型必填）
            script_args: 传给脚本的命令行参数列表
            func_name: 任务名称（script 模式默认用脚本文件名，code 模式默认 "inline_code"）
            max_retries: 最大重试次数
            timeout: 超时秒数
            timezone: 时区
            catch_up: 是否补偿错过的执行
            script_code: Python 代码字符串（与 script_path 互斥）
            allow_update_running: 同名 upsert 是否允许更新未暂停的任务. 默认 True
                供 worker 启动注册链使用（注册方即执行方，直接生效无分歧）；
                API 进程等非执行方应传 False：对未暂停任务的配置更新不会被
                worker 的控制状态同步应用，会被静默忽略甚至被 worker 的
                周期性保存反向覆盖，必须显式拒绝

        Returns:
            job_id

        Raises:
            ValueError: script_path 和 script_code 同时为空或同时提供；
                或 allow_update_running 为 False 时同名任务未处于暂停状态
            FileNotFoundError: script_path 指定的文件不存在
        """
        import os

        # 互斥校验：必须且只能提供 script_path 或 script_code 之一
        if script_path and script_code:
            raise ValueError("script_path and script_code are mutually exclusive")
        if not script_path and not script_code:
            raise ValueError("Either script_path or script_code must be provided")

        # 组合校验: 缺必填字段的类型若放行, 会落入 _calculate_next_run 的
        # +60s 后验兜底, 静默变成"每分钟执行一次"的周期任务
        _validate_schedule_config(
            schedule_type, interval, schedule_time, cron_expression, None
        )
        # 可选项类型校验: 错误类型会在执行期才触发 TypeError 反复失败
        _validate_option_types(
            max_retries=max_retries, timeout=timeout, script_args=script_args
        )

        # 脚本文件模式：校验文件存在性
        if script_path and not os.path.exists(script_path):
            raise FileNotFoundError(f"Script not found: {script_path}")

        # 动态代码模式：校验代码非空
        if script_code is not None and not script_code.strip():
            raise ValueError("script_code cannot be empty")

        # 生成默认任务名
        if func_name is None:
            func_name = f"job_{datetime.now().strftime('%Y%m%d%H%M%S')}"

        # upsert 模式：若存在同名任务，则更新其配置后返回 job_id；
        # 否则创建新任务。更新不受暂停状态限制（注册场景）。
        with self.lock:
            for j in self.jobs:
                if j.func_name == func_name:
                    # 非执行方(如 API 进程)禁止更新未暂停任务: 此类更新不会
                    # 被 worker 应用, 静默成功会造成两侧配置分歧.
                    if allow_update_running is False and not j.paused:
                        raise ValueError(
                            f"任务 {func_name} 未暂停, 请先暂停后再更新"
                            f"(运行中任务的配置更新不会被执行进程应用)"
                        )
                    self._apply_script_job_updates(
                        j,
                        script_path=script_path,
                        script_code=script_code,
                        schedule_type=schedule_type,
                        interval=interval,
                        schedule_time=schedule_time,
                        cron_expression=cron_expression,
                        script_args=script_args,
                        max_retries=max_retries,
                        timeout=timeout,
                        timezone=timezone,
                        catch_up=catch_up,
                    )
                    logger.info(
                        f"Updated existing script job on register: "
                        f"{func_name} ({j.job_id})"
                    )
                    self._save_state_async()
                    return j.job_id
                # 如果script_path或者script_code跟存在的数据一样，则直接跳过。
                # 必须排除 None：内联代码任务的 script_path 为 None，脚本文件任务
                # 的 script_code 为 None，若不做判空会导致新任务被误判为重复而静默跳过。
                # 脚本文件模式还需比较 script_args：同一个入口脚本可按不同命令行参数
                # 注册多个调度任务（如 BatchDataCollection/run_batch_report.py
                # 按 --action 拆分为 6 个定时任务），参数不同即视为不同任务，不能静默跳过。
                elif (
                    script_path is not None
                    and j.script_path == script_path
                    and (j.script_args or []) == (script_args or [])
                ) or (script_code is not None and j.script_code == script_code):
                    return j.job_id

        job_id = self._next_job_id()
        repeat = schedule_type != "once"

        job = Job(
            next_run=time.time(),
            func=None,
            func_name=func_name,
            func_module="__script__",
            job_id=job_id,
            interval=interval,
            repeat=repeat,
            schedule_type=schedule_type,
            schedule_time=schedule_time,
            cron_expression=cron_expression,
            script_path=script_path,
            script_args=script_args or [],
            script_code=script_code,
            max_retries=max_retries,
            timeout=timeout,
            timezone=timezone,
            catch_up=catch_up,
        )

        # 计算首次执行时间
        if schedule_type in ("daily", "weekly", "hourly", "cron"):
            self._calculate_next_run(job)
        elif interval:
            job.next_run = time.time() + interval

        self._add_job(job)
        return job_id

    def register_script_file(
        self,
        script_path: str,
        schedule_type: str = "interval",
        interval: Optional[float] = None,
        schedule_time: Optional[str] = None,
        cron_expression: Optional[str] = None,
        script_args: Optional[list] = None,
        func_name: Optional[str] = None,
        max_retries: int = 3,
        timeout: Optional[float] = None,
        timezone: Optional[str] = None,
        catch_up: bool = True,
    ) -> str:
        """注册脚本文件任务的公共方法。

        用于在代码中（如 RegisterTask.py 的 register_tasks()）注册执行
        外部 Python 脚本文件的定时任务。与 add_script_job 功能一致，
        但提供更清晰的语义化入口，专用于脚本文件场景。

        Args:
            script_path: 脚本文件绝对路径
            schedule_type: 调度类型 ("interval"/"daily"/"cron"/"once")
            interval: 间隔秒数（interval/once 类型必填）
            schedule_time: 执行时间（daily 类型格式 "HH:MM"）
            cron_expression: cron 表达式（cron 类型必填）
            script_args: 传给脚本的命令行参数列表
            func_name: 任务名称（默认用脚本文件名）
            max_retries: 最大重试次数（默认 3）
            timeout: 执行超时秒数
            timezone: 时区
            catch_up: 是否补偿错过的执行（默认 True）

        Returns:
            job_id

        Raises:
            FileNotFoundError: script_path 指定的文件不存在
            ValueError: 参数不合法或任务名重复

        使用示例:
            # 在 RegisterTask.py 中注册每日清理任务
            scheduler.register_script_file(
                script_path="/path/to/cleanup.py",
                schedule_type="daily",
                schedule_time="02:00",
                func_name="cleanup_task",
            )
        """
        return self.add_script_job(
            script_path=script_path,
            schedule_type=schedule_type,
            interval=interval,
            schedule_time=schedule_time,
            cron_expression=cron_expression,
            script_args=script_args,
            func_name=func_name,
            max_retries=max_retries,
            timeout=timeout,
            timezone=timezone,
            catch_up=catch_up,
        )

    def print_status(self):
        """打印调度器状态到标准输出。

        输出内容包括:
            - 运行状态（RUNNING/STOPPED）
            - 运行时长
            - 任务总数、执行总数、失败总数
            - 前 5 个活跃任务列表（含状态符号、函数名、调度描述、距下次执行时间）

        状态符号:
            " " - scheduled（已调度）
            ">" - running（运行中）
            "+" - completed（已完成）
            "x" - failed（已失败）
        """
        status = "RUNNING" if self.running else "STOPPED"
        stats = self.get_statistics()
        jobs = self.get_jobs()

        print(f"\nScheduler [{status}]")
        if stats.get("uptime_readable"):
            print(f"Uptime: {stats['uptime_readable']}")
        print(
            f"Jobs: {len(jobs)} | Runs: {stats['total_runs']} | Failures: {stats['total_failures']}"
        )

        if jobs:
            print("\nActive jobs:")
            for job in jobs[:5]:
                next_in = job["next_run_in"]
                if next_in > 86400:
                    next_in_str = f"{int(next_in/86400)}d"
                elif next_in > 3600:
                    next_in_str = f"{int(next_in/3600)}h"
                elif next_in > 60:
                    next_in_str = f"{int(next_in/60)}m"
                elif next_in > 0:
                    next_in_str = f"{int(next_in)}s"
                else:
                    next_in_str = "now"

                status_char = {
                    "scheduled": " ",
                    "running": ">",
                    "completed": "+",
                    "failed": "x",
                }.get(job["status"], " ")

                print(
                    f"  [{status_char}] {job['func_name']:<20} {job['schedule']:<20} next: {next_in_str}"
                )

            if len(jobs) > 5:
                print(f"      ... and {len(jobs) - 5} more")
        print()

    def __enter__(self):
        """上下文管理器入口 - 启动调度器。

        支持 with 语法，自动启动调度器:
            with Scheduler() as scheduler:
                @scheduler.every(60).seconds
                def task(): ...
                time.sleep(3600)
        """
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器出口 - 停止调度器。

        退出 with 块时自动调用 stop(wait=True)，
        等待所有正在执行的任务完成后停止。
        """
        self.stop(wait=True)
