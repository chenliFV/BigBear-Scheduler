"""Scheduler worker 进程。

负责：调度循环 + 任务执行。
不启动 FastAPI，可独立运行、独立重启。

启动: python SchedulerRun.py
停止: Ctrl+C 或 kill <pid>（会优雅关闭，等待任务完成）

日志: 由 Config/scheduler.json 的 logging.worker 配置（默认 Log/worker/worker.log，按天轮转，保留30天）
"""
import signal
import sys
import time

from Config import get_database_url, load_config, setup_logging
from Scheduler import Scheduler
from SchedulerTask import register_tasks

# 加载配置并初始化日志
config = load_config("scheduler")
setup_logging(config, "worker")

# 从配置创建调度器
_db_cfg = config["database"]
_sched_cfg = config["scheduler"]
scheduler = Scheduler(
    storage=_db_cfg["storage"],
    database_url=get_database_url("scheduler"),
    quiet=_sched_cfg["quiet"],
    auto_start=False,
    max_history=_sched_cfg["max_history"],
    max_workers=_sched_cfg["max_workers"],
    history_retention_days=_sched_cfg["history_retention_days"],
    max_dead_letters=_sched_cfg["max_dead_letters"],
)

# 注册任务：将定时任务加入调度器
register_tasks(scheduler)

# 等待所有异步保存任务完成，确保任务落库后再启动调度循环。
# 若不等待，调度循环的 _sync_control_state 可能在异步保存完成前从数据库
# 读取，误判刚注册的任务为"已取消"而从内存队列移除。
scheduler.flush_state()


def shutdown(signum, frame):
    """优雅关闭：等待正在执行的任务完成"""
    print(f"\n收到信号 {signum}，正在优雅关闭...")
    scheduler.stop(wait=True, timeout=config["worker"]["shutdown_timeout"])
    print("Worker 已停止")
    sys.exit(0)


# 注册信号处理（supervisor/systemd 停止时发送 SIGTERM）
signal.signal(signal.SIGTERM, shutdown)
signal.signal(signal.SIGINT, shutdown)

if __name__ == "__main__":
    scheduler.start()
    print(f"Scheduler worker 已启动 (PID: {__import__('os').getpid()})")
    print(f"已注册任务: {len(scheduler.jobs)} 个")
    print("按 Ctrl+C 停止")

    # 主循环保持进程运行
    try:
        while scheduler.running:
            time.sleep(1)
    except KeyboardInterrupt:
        shutdown(None, None)
