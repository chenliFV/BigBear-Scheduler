"""FastAPI API 进程。

只负责：Dashboard 展示 + REST API 接口。
不启动调度循环，不执行任务。
可独立重启，不影响 scheduler 进程。

启动: python FastApiRun.py
日志: 由 Config/fastapi.json 的 logging.api 配置（默认 Log/api/api.log，按天轮转，保留30天）
"""
from fastapi import FastAPI

from Config import get_database_url, load_config, setup_logging
from Scheduler import Scheduler
from Scheduler.fastapi_integration import create_scheduler_routes

# 加载配置并初始化日志
config = load_config("fastapi")
_file_handler, _stream_handler = setup_logging(config, "api")

# 替换 uvicorn 自带 handler 为我们统一格式的 handler
for _name in ("uvicorn", "uvicorn.access", "uvicorn.error", "scheduler"):
    _lg = __import__("logging").getLogger(_name)
    _lg.handlers = [_file_handler, _stream_handler]
    _lg.propagate = False

# 从配置创建调度器（API 进程不启动调度）
_db_cfg = config["database"]
_sched_cfg = config["scheduler"]
scheduler = Scheduler(
    storage=_db_cfg["storage"],
    database_url=get_database_url("scheduler"),
    quiet=True,  # API 进程始终静默
    auto_start=False,
    max_history=_sched_cfg["max_history"],
    max_workers=_sched_cfg["max_workers"],
    history_retention_days=_sched_cfg["history_retention_days"],
    max_dead_letters=_sched_cfg["max_dead_letters"],
)

app = FastAPI(title="Scheduler Dashboard")

# Dashboard 和 API 路由
app.include_router(create_scheduler_routes(scheduler))


if __name__ == "__main__":
    import uvicorn

    _api_cfg = config["api"]
    uvicorn.run(
        "FastApiRun:app",
        host=_api_cfg["host"],
        port=_api_cfg["port"],
        reload=_api_cfg["reload"],
    )
