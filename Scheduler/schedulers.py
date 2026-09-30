"""调度器类模块 - 定义不同调度模式的调度器。

本模块实现了多种调度模式，每种模式对应一个调度器类。
所有调度器类都采用链式 API（Builder 模式）设计，使用户可以
流畅地配置任务的各种参数。

调度器类层次:
    - IntervalScheduler: 间隔执行（每隔 N 秒/分/时/天）
    - DailyScheduler -> DailyAtScheduler: 每日定时执行
    - WeeklyScheduler -> WeeklyAtScheduler: 每周指定日执行
    - HourlyScheduler -> HourlyAtScheduler: 每小时指定分钟执行
    - OnceScheduler: 一次性任务（延迟执行）
    - CronScheduler: 基于 cron 表达式执行

使用方式:
    1. 装饰器语法（最常用）:
        @scheduler.every(60).seconds
        def task():
            ...

    2. 编程式调用:
        scheduler.every(60).seconds.do(task, arg1, arg2)

设计说明:
    - 每个调度器类持有一个 Scheduler 实例的引用
    - 配置完成后调用 do() 或 __call__() 创建 Job 对象
    - 通过 scheduler._add_job() 将 Job 加入调度队列
    - 链式方法（seconds/minutes/hours 等）返回 self，支持流式调用
"""

import re
import time
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Callable, List, Optional
from zoneinfo import ZoneInfo

from .models import Job

try:
    from croniter import croniter
    CRONITER_AVAILABLE = True
except ImportError:
    CRONITER_AVAILABLE = False
    croniter = None  # type: ignore

if TYPE_CHECKING:
    from .main import Scheduler


class IntervalScheduler:
    """间隔执行调度器 - 按固定时间间隔重复执行任务。

    这是最常用的调度器，支持秒、分、时、天为单位的间隔执行。
    通过链式属性（seconds/minutes/hours/days）设置间隔单位。

    属性:
        scheduler: 关联的 Scheduler 主类实例
        interval: 执行间隔（秒，内部统一转换为秒存储）
        _max_retries: 失败后最大重试次数（默认 3）
        _catch_up: 是否补偿 scheduler 停机期间错过的执行（默认 True）
        _timeout: 单次执行超时时间（秒），None 表示不限时

    使用示例:
        # 装饰器语法
        @scheduler.every(60).seconds
        def task():
            ...

        # 链式配置
        @scheduler.every(5).minutes.retries(5).timeout(30)
        def task():
            ...

        # 编程式调用（带参数）
        scheduler.every(1).hours.do(task, arg1="value")

        # 禁用补偿（停机期间错过的执行不再补偿）
        @scheduler.every(60).seconds.no_catch_up()
        def task():
            ...
    """

    def __init__(self, scheduler: "Scheduler", interval: float):
        """初始化间隔调度器。

        Args:
            scheduler: 关联的 Scheduler 实例
            interval: 执行间隔（初始单位为秒，可通过 minutes/hours/days 转换）
        """
        self.scheduler = scheduler
        self.interval = interval
        self._max_retries = 3
        self._catch_up = True
        self._timeout: Optional[float] = None

    @property
    def seconds(self):
        """以秒为单位（interval 值不变，返回 self 以支持链式调用）。

        Returns:
            self（支持链式调用）
        """
        return self

    @property
    def minutes(self):
        """将间隔单位从秒转换为分钟（interval *= 60）。

        Returns:
            self（支持链式调用）
        """
        self.interval *= 60
        return self

    @property
    def hours(self):
        """将间隔单位从秒转换为小时（interval *= 3600）。

        Returns:
            self（支持链式调用）
        """
        self.interval *= 3600
        return self

    @property
    def days(self):
        """将间隔单位从秒转换为天（interval *= 86400）。

        Returns:
            self（支持链式调用）
        """
        self.interval *= 86400
        return self

    def retries(self, max_retries: int):
        """设置失败后的最大重试次数。

        任务执行失败后会自动重试，达到最大重试次数后
        将任务记录到死信队列（dead letter queue）。

        Args:
            max_retries: 最大重试次数

        Returns:
            self（支持链式调用）
        """
        self._max_retries = max_retries
        return self

    def no_catch_up(self):
        """禁用补偿执行。

        默认情况下，scheduler 停机期间错过的执行会在重启后补偿执行。
        调用此方法后，错过的执行将被跳过，只从当前时间开始执行。

        Returns:
            self（支持链式调用）
        """
        self._catch_up = False
        return self

    def timeout(self, seconds: float):
        """设置单次执行的超时时间。

        任务执行超过此时间将被强制终止（通过线程中断）。

        Args:
            seconds: 超时时间（秒）

        Returns:
            self（支持链式调用）
        """
        self._timeout = seconds
        return self

    def do(self, func: Callable, *args, **kwargs):
        """以编程方式注册任务（非装饰器用法）。

        与装饰器用法不同，此方法允许传递参数给任务函数。

        Args:
            func: 要执行的可调用对象
            *args: 传给 func 的位置参数
            **kwargs: 传给 func 的关键字参数

        Returns:
            func（原始函数，便于链式使用）

        说明:
            创建的 Job 对象会被自动加入调度队列，
            无需手动调用 _add_job。
        """
        self.scheduler._register_function(func)

        job = Job(
            next_run=time.time() + self.interval,
            func=func,
            func_name=func.__name__,
            func_module=func.__module__,
            interval=self.interval,
            job_id=self.scheduler._next_job_id(),
            args=args,
            kwargs=kwargs,
            repeat=True,
            max_retries=self._max_retries,
            catch_up=self._catch_up,
            schedule_type="interval",
            timeout=self._timeout,
        )
        self.scheduler._add_job(job)
        return func

    def __call__(self, func: Callable):
        """装饰器用法 - 将函数注册为间隔执行任务。

        Args:
            func: 被装饰的函数

        Returns:
            func（原始函数，保持函数的可调用性）

        使用示例:
            @scheduler.every(60).seconds
            def my_task():
                print("每60秒执行一次")
        """
        return self.do(func)


class DailyScheduler:
    """每日定时执行调度器 - 在每天的指定时间执行任务。

    通过 at() 方法指定执行时间，支持时区设置。

    属性:
        scheduler: 关联的 Scheduler 主类实例
        _max_retries: 失败后最大重试次数（默认 3）
        _timeout: 单次执行超时时间（秒）
        _timezone: 时区名称（如 "Asia/Shanghai"）

    使用示例:
        # 基本用法
        @scheduler.daily.at("09:00")
        def morning_task():
            ...

        # 带时区
        @scheduler.daily.at("09:00", tz="America/New_York")
        def nyc_task():
            ...

        # 链式配置
        @scheduler.daily.tz("Asia/Shanghai").retries(5).at("18:00")
        def evening_task():
            ...
    """

    def __init__(self, scheduler: "Scheduler"):
        """初始化每日调度器。

        Args:
            scheduler: 关联的 Scheduler 实例
        """
        self.scheduler = scheduler
        self._max_retries = 3
        self._timeout: Optional[float] = None
        self._timezone: Optional[str] = None

    def at(self, time_str: str, tz: Optional[str] = None):
        """指定每日执行的时间。

        Args:
            time_str: 执行时间，格式 "HH:MM"（24小时制，如 "09:30"）
            tz: 时区名称（如 "Asia/Shanghai"、"America/New_York"），
                为 None 时使用系统本地时区

        Returns:
            DailyAtScheduler 实例（用于最终注册任务）

        Raises:
            ValueError: 当 time_str 格式不符合 HH:MM 时（在 DailyAtScheduler 中检查）
        """
        timezone = tz or self._timezone
        return DailyAtScheduler(
            self.scheduler, time_str, self._max_retries, self._timeout, timezone
        )

    def retries(self, max_retries: int):
        """设置失败后的最大重试次数。

        Args:
            max_retries: 最大重试次数

        Returns:
            self（支持链式调用）
        """
        self._max_retries = max_retries
        return self

    def timeout(self, seconds: float):
        """设置单次执行的超时时间。

        Args:
            seconds: 超时时间（秒）

        Returns:
            self（支持链式调用）
        """
        self._timeout = seconds
        return self

    def tz(self, timezone: str):
        """设置时区（影响任务执行时间的计算）。

        Args:
            timezone: 时区名称（IANA 时区标识符，如 "Asia/Shanghai"）

        Returns:
            self（支持链式调用）
        """
        self._timezone = timezone
        return self


class DailyAtScheduler:
    """每日定时执行调度器（已指定时间）。

    由 DailyScheduler.at() 创建，包含具体的执行时间。
    作为装饰器使用时创建并注册 Job。

    属性:
        scheduler: 关联的 Scheduler 实例
        time_str: 执行时间字符串（HH:MM 格式）
        _max_retries: 最大重试次数
        _timeout: 执行超时时间
        _timezone: 时区名称
    """

    def __init__(
        self,
        scheduler: "Scheduler",
        time_str: str,
        max_retries: int,
        timeout: Optional[float] = None,
        timezone: Optional[str] = None,
    ):
        """初始化已指定时间的每日调度器。

        Args:
            scheduler: 关联的 Scheduler 实例
            time_str: 执行时间（HH:MM 格式）
            max_retries: 最大重试次数
            timeout: 执行超时时间（秒），None 表示不限时
            timezone: 时区名称，None 表示使用本地时区

        Raises:
            ValueError: 当 time_str 不符合 HH:MM 格式时
        """
        self.scheduler = scheduler
        self.time_str = time_str
        self._max_retries = max_retries
        self._timeout = timeout
        self._timezone = timezone

        # 验证时间格式必须为 HH:MM（24小时制）; 正则只查形状,
        # "99:99" 这类越界值需在此拒绝, 否则要到 replace() 才抛出
        # 一条与用户输入无关联的晦涩错误
        if not re.match(r"^\d{2}:\d{2}$", time_str):
            raise ValueError("Time must be in HH:MM format (24-hour)")
        hour, minute = map(int, time_str.split(":"))
        if not (0 <= hour <= 23 and 0 <= minute <= 59):
            raise ValueError(
                f"Time {time_str!r} out of range: hour 0-23, minute 0-59"
            )

    def timeout(self, seconds: float):
        """设置单次执行的超时时间（秒）。

        Args:
            seconds: 超时时间（秒）

        Returns:
            self（支持链式调用）
        """
        self._timeout = seconds
        return self

    def tz(self, timezone: str):
        """设置时区。

        Args:
            timezone: 时区名称

        Returns:
            self（支持链式调用）
        """
        self._timezone = timezone
        return self

    def __call__(self, func: Callable):
        """装饰器用法 - 将函数注册为每日定时执行任务。

        计算下次执行时间的逻辑:
            1. 根据时区获取当前时间
            2. 将时间设为今天的 HH:MM:00
            3. 若该时间已过，则设为明天同一时间

        Args:
            func: 被装饰的函数

        Returns:
            func（原始函数）

        使用示例:
            @scheduler.daily.at("09:00")
            def morning_report():
                ...
        """
        self.scheduler._register_function(func)

        # 根据时区获取当前时间
        if self._timezone:
            tz = ZoneInfo(self._timezone)
            now = datetime.now(tz)
        else:
            now = datetime.now()

        hour, minute = map(int, self.time_str.split(":"))
        next_run = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
        # 若今天的时间已过，推迟到明天
        if next_run <= now:
            next_run += timedelta(days=1)

        job = Job(
            next_run=next_run.timestamp(),
            func=func,
            func_name=func.__name__,
            func_module=func.__module__,
            job_id=self.scheduler._next_job_id(),
            repeat=True,
            max_retries=self._max_retries,
            schedule_type="daily",
            schedule_time=self.time_str,
            timeout=self._timeout,
            timezone=self._timezone,
        )
        self.scheduler._add_job(job)
        return func


class WeeklyScheduler:
    """每周指定日执行调度器 - 在每周的指定星期几执行任务。

    通过属性（monday/tuesday/.../weekdays/weekends）或 on() 方法指定星期，
    再通过 at() 方法指定执行时间。

    星期编码:
        0=Monday, 1=Tuesday, 2=Wednesday, 3=Thursday,
        4=Friday, 5=Saturday, 6=Sunday

    属性:
        scheduler: 关联的 Scheduler 实例
        _days: 执行日期列表（星期编码）
        _max_retries: 最大重试次数
        _timeout: 执行超时时间
        _timezone: 时区名称

    使用示例:
        # 每周一执行
        @scheduler.weekly.monday.at("09:00")
        def weekly_report():
            ...

        # 工作日执行
        @scheduler.weekly.weekdays.at("18:00")
        def daily_summary():
            ...

        # 自定义日期
        @scheduler.weekly.on([0, 2, 4]).at("10:00")  # 周一、三、五
        def custom_task():
            ...
    """

    def __init__(self, scheduler: "Scheduler"):
        """初始化每周调度器。

        Args:
            scheduler: 关联的 Scheduler 实例
        """
        self.scheduler = scheduler
        self._days: List[int] = []
        self._max_retries = 3
        self._timeout: Optional[float] = None
        self._timezone: Optional[str] = None

    @property
    def monday(self):
        """设置在每周一执行。

        Returns:
            self（支持链式调用）
        """
        self._days = [0]
        return self

    @property
    def tuesday(self):
        """设置在每周二执行。

        Returns:
            self（支持链式调用）
        """
        self._days = [1]
        return self

    @property
    def wednesday(self):
        """设置在每周三执行。

        Returns:
            self（支持链式调用）
        """
        self._days = [2]
        return self

    @property
    def thursday(self):
        """设置在每周四执行。

        Returns:
            self（支持链式调用）
        """
        self._days = [3]
        return self

    @property
    def friday(self):
        """设置在每周五执行。

        Returns:
            self（支持链式调用）
        """
        self._days = [4]
        return self

    @property
    def saturday(self):
        """设置在每周六执行。

        Returns:
            self（支持链式调用）
        """
        self._days = [5]
        return self

    @property
    def sunday(self):
        """设置在每周日执行。

        Returns:
            self（支持链式调用）
        """
        self._days = [6]
        return self

    @property
    def weekdays(self):
        """设置在工作日（周一至周五）执行。

        Returns:
            self（支持链式调用）
        """
        self._days = [0, 1, 2, 3, 4]
        return self

    @property
    def weekends(self):
        """设置在周末（周六、周日）执行。

        Returns:
            self（支持链式调用）
        """
        self._days = [5, 6]
        return self

    def on(self, days: List[int]):
        """自定义执行日期（星期列表）。

        Args:
            days: 星期编码列表（0=周一 ~ 6=周日），如 [0, 2, 4] 表示周一、三、五

        Returns:
            self（支持链式调用）

        Raises:
            ValueError: 星期编码越界(不在 0~6)时; 越界编码永远不会匹配,
            任务会静默永不触发, 必须在注册期拒绝.
        """
        invalid = [d for d in days if not isinstance(d, int) or not 0 <= d <= 6]
        if invalid:
            raise ValueError(
                f"week days must be integers in [0, 6] (0=Mon..6=Sun), got {invalid}"
            )
        self._days = days
        return self

    def at(self, time_str: str, tz: Optional[str] = None):
        """指定每周执行的时间。

        Args:
            time_str: 执行时间（HH:MM 格式）
            tz: 时区名称，为 None 时使用本地时区

        Returns:
            WeeklyAtScheduler 实例（用于最终注册任务）

        Raises:
            ValueError: 当未先指定日期（_days 为空）时
        """
        if not self._days:
            raise ValueError("Must specify days before time")
        timezone = tz or self._timezone
        return WeeklyAtScheduler(
            self.scheduler,
            self._days,
            time_str,
            self._max_retries,
            self._timeout,
            timezone,
        )

    def retries(self, max_retries: int):
        """设置失败后的最大重试次数。

        Args:
            max_retries: 最大重试次数

        Returns:
            self（支持链式调用）
        """
        self._max_retries = max_retries
        return self

    def timeout(self, seconds: float):
        """设置单次执行的超时时间。

        Args:
            seconds: 超时时间（秒）

        Returns:
            self（支持链式调用）
        """
        self._timeout = seconds
        return self

    def tz(self, timezone: str):
        """设置时区。

        Args:
            timezone: 时区名称

        Returns:
            self（支持链式调用）
        """
        self._timezone = timezone
        return self


class WeeklyAtScheduler:
    """每周指定日定时执行调度器（已指定日期和时间）。

    由 WeeklyScheduler.at() 创建，包含具体的执行日期和时间。
    作为装饰器使用时创建并注册 Job。

    属性:
        scheduler: 关联的 Scheduler 实例
        days: 执行日期列表（星期编码）
        time_str: 执行时间（HH:MM 格式）
        _max_retries: 最大重试次数
        _timeout: 执行超时时间
        _timezone: 时区名称
    """

    def __init__(
        self,
        scheduler: "Scheduler",
        days: List[int],
        time_str: str,
        max_retries: int,
        timeout: Optional[float] = None,
        timezone: Optional[str] = None,
    ):
        """初始化已指定日期和时间的每周调度器。

        Args:
            scheduler: 关联的 Scheduler 实例
            days: 执行日期列表（星期编码 0~6）
            time_str: 执行时间（HH:MM 格式）
            max_retries: 最大重试次数
            timeout: 执行超时时间（秒）
            timezone: 时区名称

        Raises:
            ValueError: 当 time_str 不符合 HH:MM 格式时
        """
        self.scheduler = scheduler
        self.days = days
        self.time_str = time_str
        self._max_retries = max_retries
        self._timeout = timeout
        self._timezone = timezone

        if not re.match(r"^\d{2}:\d{2}$", time_str):
            raise ValueError("Time must be in HH:MM format")
        hour, minute = map(int, time_str.split(":"))
        if not (0 <= hour <= 23 and 0 <= minute <= 59):
            raise ValueError(
                f"Time {time_str!r} out of range: hour 0-23, minute 0-59"
            )

    def timeout(self, seconds: float):
        """设置单次执行的超时时间。

        Args:
            seconds: 超时时间（秒）

        Returns:
            self（支持链式调用）
        """
        self._timeout = seconds
        return self

    def tz(self, timezone: str):
        """设置时区。

        Args:
            timezone: 时区名称

        Returns:
            self（支持链式调用）
        """
        self._timezone = timezone
        return self

    def __call__(self, func: Callable):
        """装饰器用法 - 将函数注册为每周指定日定时执行任务。

        计算下次执行时间的逻辑:
            1. 从今天开始向后遍历 8 天
            2. 找到第一个在 days 列表中且时间未过的日期
            3. 若 8 天内没找到（理论上不会发生），设为 7 天后

        Args:
            func: 被装饰的函数

        Returns:
            func（原始函数）
        """
        self.scheduler._register_function(func)

        if self._timezone:
            tz = ZoneInfo(self._timezone)
            now = datetime.now(tz)
        else:
            now = datetime.now()

        hour, minute = map(int, self.time_str.split(":"))

        # 遍历未来 8 天，找到第一个匹配的执行时间
        next_run = None
        for i in range(8):
            check_date = now + timedelta(days=i)
            if check_date.weekday() in self.days:
                candidate = check_date.replace(
                    hour=hour, minute=minute, second=0, microsecond=0
                )
                if candidate > now:
                    next_run = candidate
                    break

        # 兜底：理论上不会执行到这里
        if not next_run:
            next_run = now + timedelta(days=7)

        job = Job(
            next_run=next_run.timestamp(),
            func=func,
            func_name=func.__name__,
            func_module=func.__module__,
            job_id=self.scheduler._next_job_id(),
            repeat=True,
            max_retries=self._max_retries,
            schedule_type="weekly",
            schedule_time=self.time_str,
            schedule_days=self.days,
            timeout=self._timeout,
            timezone=self._timezone,
        )
        self.scheduler._add_job(job)
        return func


class HourlyScheduler:
    """每小时指定分钟执行调度器 - 在每小时的指定分钟执行任务。

    适用于需要每小时执行但要求在特定分钟触发的场景。
    通过 at() 方法指定分钟。

    属性:
        scheduler: 关联的 Scheduler 实例
        _max_retries: 最大重试次数
        _timeout: 执行超时时间
        _timezone: 时区名称

    使用示例:
        # 每小时的第 30 分钟执行
        @scheduler.hourly.at(":30")
        def half_hour_task():
            ...

        # 每小时的第 0 分钟执行（整点）
        @scheduler.hourly.at(":00")
        def top_of_hour():
            ...
    """

    def __init__(self, scheduler: "Scheduler"):
        """初始化每小时调度器。

        Args:
            scheduler: 关联的 Scheduler 实例
        """
        self.scheduler = scheduler
        self._max_retries = 3
        self._timeout: Optional[float] = None
        self._timezone: Optional[str] = None

    def at(self, minute_str: str, tz: Optional[str] = None):
        """指定每小时执行的分钟。

        Args:
            minute_str: 分钟字符串，格式 ":MM"（如 ":30" 表示第 30 分钟）
            tz: 时区名称，为 None 时使用本地时区

        Returns:
            HourlyAtScheduler 实例（用于最终注册任务）

        Raises:
            ValueError: 当 minute_str 不符合 :MM 格式时（在 HourlyAtScheduler 中检查）
        """
        timezone = tz or self._timezone
        return HourlyAtScheduler(
            self.scheduler, minute_str, self._max_retries, self._timeout, timezone
        )

    def retries(self, max_retries: int):
        """设置失败后的最大重试次数。

        Args:
            max_retries: 最大重试次数

        Returns:
            self（支持链式调用）
        """
        self._max_retries = max_retries
        return self

    def timeout(self, seconds: float):
        """设置单次执行的超时时间。

        Args:
            seconds: 超时时间（秒）

        Returns:
            self（支持链式调用）
        """
        self._timeout = seconds
        return self

    def tz(self, timezone: str):
        """设置时区。

        Args:
            timezone: 时区名称

        Returns:
            self（支持链式调用）
        """
        self._timezone = timezone
        return self


class HourlyAtScheduler:
    """每小时指定分钟执行调度器（已指定分钟）。

    由 HourlyScheduler.at() 创建，包含具体的执行分钟。
    作为装饰器使用时创建并注册 Job。

    属性:
        scheduler: 关联的 Scheduler 实例
        minute_str: 分钟字符串（:MM 格式）
        _max_retries: 最大重试次数
        _timeout: 执行超时时间
        _timezone: 时区名称
    """

    def __init__(
        self,
        scheduler: "Scheduler",
        minute_str: str,
        max_retries: int,
        timeout: Optional[float] = None,
        timezone: Optional[str] = None,
    ):
        """初始化已指定分钟的每小时调度器。

        Args:
            scheduler: 关联的 Scheduler 实例
            minute_str: 分钟字符串（:MM 格式）
            max_retries: 最大重试次数
            timeout: 执行超时时间（秒）
            timezone: 时区名称

        Raises:
            ValueError: 当 minute_str 不符合 :MM 格式时
        """
        self.scheduler = scheduler
        self.minute_str = minute_str
        self._max_retries = max_retries
        self._timeout = timeout
        self._timezone = timezone

        if not re.match(r"^:\d{2}$", minute_str):
            raise ValueError("Minute must be in :MM format")
        minute = int(minute_str.strip(":"))
        if not 0 <= minute <= 59:
            raise ValueError(
                f"Minute {minute_str!r} out of range: minute 0-59"
            )

    def timeout(self, seconds: float):
        """设置单次执行的超时时间。

        Args:
            seconds: 超时时间（秒）

        Returns:
            self（支持链式调用）
        """
        self._timeout = seconds
        return self

    def tz(self, timezone: str):
        """设置时区。

        Args:
            timezone: 时区名称

        Returns:
            self（支持链式调用）
        """
        self._timezone = timezone
        return self

    def __call__(self, func: Callable):
        """装饰器用法 - 将函数注册为每小时指定分钟执行任务。

        计算下次执行时间的逻辑:
            1. 将当前时间的分钟设为指定值，秒和微秒清零
            2. 若该时间已过，加 1 小时

        Args:
            func: 被装饰的函数

        Returns:
            func（原始函数）
        """
        self.scheduler._register_function(func)

        if self._timezone:
            tz = ZoneInfo(self._timezone)
            now = datetime.now(tz)
        else:
            now = datetime.now()

        minute = int(self.minute_str.strip(":"))
        next_run = now.replace(minute=minute, second=0, microsecond=0)
        if next_run <= now:
            next_run += timedelta(hours=1)

        job = Job(
            next_run=next_run.timestamp(),
            func=func,
            func_name=func.__name__,
            func_module=func.__module__,
            job_id=self.scheduler._next_job_id(),
            interval=3600,  # 固定 1 小时间隔
            repeat=True,
            max_retries=self._max_retries,
            schedule_type="hourly",
            schedule_time=self.minute_str,
            timeout=self._timeout,
            timezone=self._timezone,
        )
        self.scheduler._add_job(job)
        return func


class OnceScheduler:
    """一次性任务调度器 - 延迟执行一次后自动移除。

    与重复执行调度器不同，OnceScheduler 创建的任务只执行一次，
    执行完成后自动从队列中移除。

    属性:
        scheduler: 关联的 Scheduler 实例
        delay: 延迟时间（秒，可通过 minutes/hours 转换）
        _decorator_mode: 是否为装饰器模式（影响 do() 的返回值）
        _max_retries: 最大重试次数
        _timeout: 执行超时时间

    使用示例:
        # 装饰器语法
        @scheduler.once(60).seconds
        def delayed_task():
            ...

        # 编程式调用
        job = scheduler.once(5).minutes.do(task, arg1="value")

        # 指定具体时间
        @scheduler.at("2024-12-25 00:00:00")
        def christmas_task():
            ...
    """

    def __init__(self, scheduler: "Scheduler", delay: float):
        """初始化一次性任务调度器。

        Args:
            scheduler: 关联的 Scheduler 实例
            delay: 延迟时间（秒，初始单位）
        """
        self.scheduler = scheduler
        self.delay = delay
        self._decorator_mode = False
        self._max_retries = 3
        self._timeout: Optional[float] = None

    @property
    def seconds(self):
        """以秒为单位（delay 值不变）。

        Returns:
            self（支持链式调用）
        """
        return self

    @property
    def minutes(self):
        """将延迟单位从秒转换为分钟（delay *= 60）。

        Returns:
            self（支持链式调用）
        """
        self.delay *= 60
        return self

    @property
    def hours(self):
        """将延迟单位从秒转换为小时（delay *= 3600）。

        Returns:
            self（支持链式调用）
        """
        self.delay *= 3600
        return self

    def retries(self, max_retries: int):
        """设置失败后的最大重试次数。

        Args:
            max_retries: 最大重试次数

        Returns:
            self（支持链式调用）
        """
        self._max_retries = max_retries
        return self

    def timeout(self, seconds: float):
        """设置单次执行的超时时间。

        Args:
            seconds: 超时时间（秒）

        Returns:
            self（支持链式调用）
        """
        self._timeout = seconds
        return self

    def do(self, func: Callable, *args, **kwargs):
        """以编程方式注册一次性任务。

        Args:
            func: 要执行的可调用对象
            *args: 传给 func 的位置参数
            **kwargs: 传给 func 的关键字参数

        Returns:
            装饰器模式下返回 func，编程模式下返回创建的 Job 对象
        """
        self.scheduler._register_function(func)

        job = Job(
            next_run=time.time() + self.delay,
            func=func,
            func_name=func.__name__,
            func_module=func.__module__,
            job_id=self.scheduler._next_job_id(),
            args=args,
            kwargs=kwargs,
            repeat=False,  # 一次性任务
            # 显式标注 once: 默认值是 "interval", 会被注册入口的
            # "interval 类型必须提供正数 interval" 校验误拒,
            # 导致 once 装饰器路径完全不可用
            schedule_type="once",
            max_retries=self._max_retries,
            timeout=self._timeout,
        )
        self.scheduler._add_job(job)

        # 装饰器模式返回函数，编程模式返回 Job 对象
        if self._decorator_mode:
            return func
        return job

    def __call__(self, func: Callable):
        """装饰器用法 - 将函数注册为一次性延迟执行任务。

        Args:
            func: 被装饰的函数

        Returns:
            func（原始函数）
        """
        return self.do(func)


class CronScheduler:
    """Cron 表达式调度器 - 使用标准 cron 表达式配置任务执行时间。

    支持标准的 5 字段 cron 表达式，提供最灵活的调度配置。
    需要安装 croniter 包: pip install scheduler[cron]

    Cron 表达式格式:
        ┌───────── 分钟 (0-59)
        │ ┌─────── 小时 (0-23)
        │ │ ┌───── 日 (1-31)
        │ │ │ ┌─── 月 (1-12)
        │ │ │ │ ┌─ 星期 (0-6, 0=周日 或 MON-SUN)
        │ │ │ │ │
        * * * * *

    特殊字符:
        * : 任意值
        */N : 每 N 个单位
        , : 列表分隔符 (如 "1,3,5")
        - : 范围 (如 "1-5")
        MON-FRI : 星期缩写

    属性:
        scheduler: 关联的 Scheduler 实例
        expression: cron 表达式字符串
        _max_retries: 最大重试次数
        _timeout: 执行超时时间
        _timezone: 时区名称

    使用示例:
        # 每周一到周五 9 点执行
        @scheduler.cron("0 9 * * MON-FRI")
        def weekday_morning():
            ...

        # 每 5 分钟执行
        @scheduler.cron("*/5 * * * *")
        def frequent_task():
            ...

        # 带时区
        @scheduler.cron("0 9 * * MON-FRI", tz="America/New_York")
        def nyc_task():
            ...

        # 编程式调用（带参数）
        scheduler.cron("0 0 * * *").do(task, debug=True)
    """

    def __init__(self, scheduler: "Scheduler", expression: str):
        """初始化 cron 调度器。

        Args:
            scheduler: 关联的 Scheduler 实例
            expression: cron 表达式字符串

        Raises:
            ImportError: 当未安装 croniter 包时
            ValueError: 当 cron 表达式格式无效时
        """
        self.scheduler = scheduler
        self.expression = expression
        self._max_retries = 3
        self._timeout: Optional[float] = None
        self._timezone: Optional[str] = None

        # 检查 croniter 是否可用
        if not CRONITER_AVAILABLE:
            raise ImportError(
                "Cron scheduling requires croniter. "
                "Install with: pip install scheduler[cron]"
            )

        # 验证 cron 表达式格式
        try:
            croniter(expression)
        except (KeyError, ValueError) as e:
            raise ValueError(f"Invalid cron expression '{expression}': {e}")

    def retries(self, max_retries: int):
        """设置失败后的最大重试次数。

        Args:
            max_retries: 最大重试次数

        Returns:
            self（支持链式调用）
        """
        self._max_retries = max_retries
        return self

    def timeout(self, seconds: float):
        """设置单次执行的超时时间。

        Args:
            seconds: 超时时间（秒）

        Returns:
            self（支持链式调用）
        """
        self._timeout = seconds
        return self

    def tz(self, timezone: str):
        """设置时区（影响 cron 表达式的计算基准时间）。

        Args:
            timezone: 时区名称（IANA 标识符，如 "Asia/Shanghai"）

        Returns:
            self（支持链式调用）
        """
        self._timezone = timezone
        return self

    def __call__(self, func: Callable):
        """装饰器用法 - 将函数注册为 cron 表达式调度任务。

        使用 croniter 计算下次执行时间。

        Args:
            func: 被装饰的函数

        Returns:
            func（原始函数）
        """
        self.scheduler._register_function(func)

        if self._timezone:
            tz = ZoneInfo(self._timezone)
            now = datetime.now(tz)
        else:
            now = datetime.now()

        cron = croniter(self.expression, now)
        next_run = cron.get_next(datetime)

        job = Job(
            next_run=next_run.timestamp(),
            func=func,
            func_name=func.__name__,
            func_module=func.__module__,
            job_id=self.scheduler._next_job_id(),
            repeat=True,
            max_retries=self._max_retries,
            schedule_type="cron",
            cron_expression=self.expression,
            timeout=self._timeout,
            timezone=self._timezone,
        )
        self.scheduler._add_job(job)
        return func

    def do(self, func: Callable, *args, **kwargs):
        """以编程方式注册 cron 任务（支持传参）。

        与装饰器用法不同，此方法允许传递参数给任务函数。

        Args:
            func: 要执行的可调用对象
            *args: 传给 func 的位置参数
            **kwargs: 传给 func 的关键字参数

        Returns:
            func（原始函数）
        """
        self.scheduler._register_function(func)

        if self._timezone:
            tz = ZoneInfo(self._timezone)
            now = datetime.now(tz)
        else:
            now = datetime.now()

        cron = croniter(self.expression, now)
        next_run = cron.get_next(datetime)

        job = Job(
            next_run=next_run.timestamp(),
            func=func,
            func_name=func.__name__,
            func_module=func.__module__,
            job_id=self.scheduler._next_job_id(),
            args=args,
            kwargs=kwargs,
            repeat=True,
            max_retries=self._max_retries,
            schedule_type="cron",
            cron_expression=self.expression,
            timeout=self._timeout,
            timezone=self._timezone,
        )
        self.scheduler._add_job(job)
        return func
