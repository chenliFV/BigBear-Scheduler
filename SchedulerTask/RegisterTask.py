"""定时任务注册中心。

将任务定义与 scheduler 进程解耦：任务在此注册，scheduler 调用 register_tasks()。
新增任务只需在本文件添加，无需修改 scheduler_worker.py。

支持两种注册方式:
    1. 函数任务（装饰器）: @scheduler.every(N).seconds
       - 源码会被提取为内联代码序列化到数据库，重启后自动恢复
    2. 脚本文件任务: scheduler.register_script_file(...)
       - 执行指定路径的 .py 文件，适合复杂业务逻辑

本文件当前只包含示例任务：5 个装饰器任务分别演示 interval / daily / hourly /
weekly / cron 五种调度节奏；2 个脚本文件任务演示 register_script_file 的用法，
脚本位于同目录 tasks/ 下，可按需替换为真实业务逻辑。
"""
import os

from Scheduler import Scheduler

# 脚本文件任务目录（本文件位于 SchedulerTask/ 下）
_TASKS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "tasks")


def register_tasks(scheduler: Scheduler) -> None:
    """向调度器注册所有定时任务。

    Args:
        scheduler: Scheduler 实例
    """
    _register_interval_example(scheduler)
    _register_daily_example(scheduler)
    _register_hourly_example(scheduler)
    _register_weekly_example(scheduler)
    _register_cron_example(scheduler)
    _register_script_file_examples(scheduler)


def _register_interval_example(scheduler: Scheduler) -> None:
    """间隔任务示例：每 5 秒执行一次"""

    @scheduler.every(5).seconds
    def heartbeat_task():
        """每 5 秒打印一次心跳"""
        print("Background work")


def _register_daily_example(scheduler: Scheduler) -> None:
    """每日任务示例：每天 01:00 执行"""

    @scheduler.daily.at("01:00")
    def daily_report_task():
        """生成日度报表"""
        print("Daily report generated")


def _register_hourly_example(scheduler: Scheduler) -> None:
    """每小时任务示例：每小时第 30 分钟执行"""

    @scheduler.hourly.at(":30")
    def hourly_summary_task():
        """小时级数据汇总"""
        print("Hourly summary done")


def _register_weekly_example(scheduler: Scheduler) -> None:
    """每周任务示例：每周一 09:00 执行"""

    @scheduler.weekly.monday.at("09:00")
    def weekly_review_task():
        """周报汇总"""
        print("Weekly review done")


def _register_cron_example(scheduler: Scheduler) -> None:
    """cron 表达式任务示例：工作日（周一至周五）18:00 执行"""

    @scheduler.cron("0 18 * * 1-5")
    def workday_close_task():
        """工作日收盘任务"""
        print("Workday close done")


def _register_script_file_examples(scheduler: Scheduler) -> None:
    """脚本文件任务示例：以子进程方式执行 tasks/ 下的 .py 文件。

    - hello_script_task 每 10 分钟执行一次，演示最小结构与调度上下文变量；
    - dir_size_report_task 每天 08:30 执行，演示 script_args 传参与退出码约定。
    """
    scheduler.register_script_file(
        script_path=os.path.join(_TASKS_DIR, "hello_script_task.py"),
        schedule_type="interval",
        interval=600,
        func_name="hello_script_task",
        max_retries=2,
        timeout=60,
        catch_up=False,
    )
    scheduler.register_script_file(
        script_path=os.path.join(_TASKS_DIR, "dir_size_report_task.py"),
        schedule_type="daily",
        schedule_time="08:30",
        script_args=["--path", "Log", "--top", "5"],
        func_name="dir_size_report_task",
        max_retries=2,
        timeout=300,
        catch_up=False,
    )
