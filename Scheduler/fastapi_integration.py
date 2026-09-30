"""FastAPI 集成模块 - 提供 Web Dashboard 和 REST API。

本模块将 Scheduler 与 FastAPI 框架集成，提供以下功能：
    1. Web Dashboard: 实时监控界面（基于 SSE 推送数据）
    2. REST API: 任务管理接口（查询、暂停、恢复、取消、立即执行）
    3. 脚本任务 API: 动态添加执行外部 Python 脚本的任务

架构说明:
    - 多进程模式: API 进程只读，通过 refresh_state 从存储后端同步状态
    - SSE 事件流: 每秒推送一次最新状态到前端 Dashboard
    - 模板加载: 从包资源加载 HTML 模板，失败时返回降级模板

使用示例:
    from fastapi import FastAPI
    from Scheduler import Scheduler
    from Scheduler.fastapi_integration import create_scheduler_routes

    app = FastAPI()
    scheduler = Scheduler(storage="sqlmodel", database_url="...")
    app.include_router(create_scheduler_routes(scheduler))
    scheduler.start()

API 端点:
    GET  /scheduler/                    - Web Dashboard 页面
    GET  /scheduler/favicon.svg         - 网站图标
    GET  /scheduler/events              - SSE 实时事件流
    GET  /scheduler/api/status          - 获取调度器状态
    GET  /scheduler/api/jobs            - 获取所有任务
    GET  /scheduler/api/jobs/{job_id}   - 获取指定任务
    POST /scheduler/api/jobs/{job_id}/pause   - 暂停任务
    POST /scheduler/api/jobs/{job_id}/resume  - 恢复任务
    POST /scheduler/api/jobs/{job_id}/cancel  - 取消任务
    POST /scheduler/api/jobs/{job_id}/run     - 立即执行任务
    POST /scheduler/api/scripts              - 添加脚本任务
    PUT  /scheduler/api/jobs/{job_id}        - 更新已暂停任务
    GET  /scheduler/api/history              - 获取历史记录
    GET  /scheduler/api/dead-letters          - 获取死信队列
    DELETE /scheduler/api/dead-letters        - 清空死信队列
"""

import asyncio
import importlib.resources
import json
import logging
from typing import TYPE_CHECKING, AsyncGenerator, Optional

try:
    from fastapi import APIRouter, Request
    from fastapi.responses import FileResponse, HTMLResponse, StreamingResponse
except ImportError as e:
    raise ImportError(
        "FastAPI integration requires FastAPI. "
        "Install with: pip install scheduler[fastapi]"
    ) from e

if TYPE_CHECKING:
    from .main import Scheduler

# 使用统一的 scheduler logger
logger = logging.getLogger("worker")


def _load_dashboard_template() -> str:
    """从包资源加载 Dashboard HTML 模板。

    使用 importlib.resources 从 Scheduler 包的 templates 目录读取
    dashboard.html 文件。此方式兼容打包后的资源访问。

    Returns:
        HTML 模板字符串。加载失败时返回一个最小的降级模板，
        显示错误提示信息，确保用户能感知到问题。
    """
    try:
        # Python 3.9+ 兼容的资源读取方式
        # 注意: 包名必须为 "Scheduler"（大写 S），与实际目录名一致
        files = importlib.resources.files("Scheduler")
        template_path = files.joinpath("dashboard", "dashboard.html")
        return template_path.read_text(encoding="utf-8")
    except Exception as e:
        logger.error(f"Failed to load dashboard template: {e}")
        # 降级模板：返回一个最小可用的 HTML，告知用户加载失败
        return """<!DOCTYPE html>
<html><head><title>Scheduler</title></head>
<body style="background:#111;color:#fff;font-family:sans-serif;padding:40px;">
<h1>Scheduler Dashboard</h1>
<p>Error loading template. Check Log for details.</p>
</body></html>"""


def create_scheduler_routes(scheduler: "Scheduler", **router_kwargs) -> APIRouter:
    """创建 Scheduler 的 FastAPI 路由。

    这是一个工厂函数，返回配置好的 APIRouter 实例。
    所有路由处理函数都通过闭包捕获 scheduler 实例。

    Args:
        scheduler: Scheduler 实例（多进程模式下为只读实例）
        **router_kwargs: 传递给 APIRouter 的额外参数
            默认设置:
                - prefix: "/scheduler"（所有路由的前缀）
                - tags: ["scheduler"]（OpenAPI 文档分组）

    Returns:
        配置好的 APIRouter 实例，可通过 app.include_router() 挂载

    使用示例:
        from fastapi import FastAPI
        from Scheduler import Scheduler
        from Scheduler.fastapi_integration import create_scheduler_routes

        app = FastAPI()
        scheduler = Scheduler()
        app.include_router(create_scheduler_routes(scheduler))
        scheduler.start()
    """
    # 设置默认路由前缀和标签
    if "prefix" not in router_kwargs:
        router_kwargs['prefix'] = "/scheduler"

    if "tags" not in router_kwargs:
        router_kwargs['tags'] = ["scheduler"]

    router = APIRouter(**router_kwargs)

    async def event_generator() -> AsyncGenerator[str, None]:
        """生成 SSE（Server-Sent Events）事件流。

        每秒从 scheduler 获取最新状态并推送到前端 Dashboard。
        在多进程模式下，每次推送前调用 refresh_state() 从存储后端
        重新加载 scheduler 进程写入的最新状态。

        推送的数据格式:
            data: {"running": bool, "stats": {...}, "jobs": [...],
                   "history": [...], "dead_letters": [...], "dead_letter_count": int}

        Yields:
            SSE 格式的事件字符串（"data: <json>\\n\\n"）

        异常处理:
            - asyncio.CancelledError: 客户端断开连接，正常退出
            - 其他异常: 记录错误日志，等待 1 秒后重试（避免紧密错误循环）
        """
        while True:
            try:
                # 多进程只读模式：每次推送前从存储后端重新加载最新状态
                scheduler.refresh_state()

                # 获取当前状态数据
                stats = scheduler.get_statistics()
                jobs = scheduler.get_jobs()
                history = scheduler.get_history(limit=50)
                dead_letters = scheduler.get_dead_letters(limit=100)

                # 组装推送数据
                data = {
                    "running": scheduler.is_running(),
                    "stats": stats,
                    "jobs": jobs,
                    "history": history,
                    "dead_letters": dead_letters,
                    "dead_letter_count": len(scheduler.dead_letters),
                }

                # 以 SSE 格式推送（注意结尾必须是 \n\n）
                yield f"data: {json.dumps(data)}\n\n"

                # 每秒推送一次
                await asyncio.sleep(1)
            except asyncio.CancelledError:
                # 客户端主动断开连接，属于正常行为，不记录错误日志
                logger.debug("SSE connection closed by client")
                break
            except Exception as e:
                # 记录详细错误信息（含堆栈），便于排查
                logger.error(
                    f"Error in SSE event generator: {type(e).__name__}: {e}",
                    exc_info=True,
                )
                # 等待后重试，防止紧密错误循环消耗 CPU
                await asyncio.sleep(1)

    @router.get("/events")
    async def events():
        """SSE 端点 - 提供实时状态更新的事件流。

        前端通过 EventSource 连接此端点，接收实时推送的调度器状态。

        Returns:
            StreamingResponse，media_type 为 "text/event-stream"

        响应头说明:
            - Cache-Control: no-cache - 禁用缓存
            - Connection: keep-alive - 保持长连接
            - X-Accel-Buffering: no - 禁用 Nginx 缓冲（重要，否则 SSE 无法实时推送）
        """
        return StreamingResponse(
            event_generator(),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",
            },
        )

    @router.get("/", response_class=HTMLResponse)
    async def dashboard():
        """Web Dashboard 页面 - 提供可视化监控界面。

        加载 HTML 模板并替换其中的 {{prefix}} 占位符为实际路由前缀，
        使前端 JS 能正确构造 API 请求路径。

        Returns:
            渲染后的 HTML 页面
        """
        template = _load_dashboard_template()
        # 替换模板中的前缀占位符
        html = template.replace("{{prefix}}", router_kwargs['prefix'])
        return html

    @router.get("/favicon.svg", include_in_schema=False)
    async def favicon():
        """提供 Dashboard 网站图标（本地 SVG 文件）。

        从包资源加载 favicon.svg，避免依赖外部 CDN。
        include_in_schema=False 表示不在 OpenAPI 文档中显示此端点。

        Returns:
            FileResponse，media_type 为 "image/svg+xml"
        """
        # 注意: 包名必须为 "Scheduler"（大写 S）
        files = importlib.resources.files("Scheduler")
        favicon_path = files.joinpath("dashboard", "static", "favicon.svg")
        with importlib.resources.as_file(favicon_path) as p:
            return FileResponse(str(p), media_type="image/svg+xml")

    @router.get("/dashboard/{file_path:path}", include_in_schema=False)
    async def static_assets(file_path: str):
        """提供 Dashboard 静态资源（CSS / JS）。

        从包资源 Scheduler/dashboard 目录读取文件，按扩展名设置 Content-Type。
        限制只允许 css/ 和 js/ 子目录下的文件，防止路径遍历。
        include_in_schema=False 表示不在 OpenAPI 文档中显示此端点。

        Args:
            file_path: 相对于 dashboard/ 的文件路径，如 "css/dashboard.css"

        Returns:
            FileResponse，media_type 根据扩展名自动判断
        """
        # 安全校验：只允许 css/ 或 js/ 开头的相对路径，禁止目录回溯
        if ".." in file_path or file_path.startswith("/"):
            return HTMLResponse("Forbidden", status_code=403)
        if not (file_path.startswith("css/") or file_path.startswith("js/")):
            return HTMLResponse("Forbidden", status_code=403)

        # 扩展名 -> MIME 映射
        ext_media = {
            ".css": "text/css",
            ".js": "application/javascript",
            ".svg": "image/svg+xml",
            ".png": "image/png",
            ".ico": "image/x-icon",
        }
        ext = "." + file_path.rsplit(".", 1)[-1].lower() if "." in file_path else ""
        media_type = ext_media.get(ext, "application/octet-stream")

        # 注意: 包名必须为 "Scheduler"（大写 S）
        files = importlib.resources.files("Scheduler")
        asset_path = files.joinpath("dashboard", *file_path.split("/"))
        with importlib.resources.as_file(asset_path) as p:
            return FileResponse(str(p), media_type=media_type)

    @router.get("/api/status")
    async def get_status():
        """获取调度器运行状态和统计信息。

        Returns:
            {"running": bool, "statistics": {...}}
            statistics 包含 total_runs、total_failures、total_retries、start_time 等
        """
        return {"running": scheduler.is_running(), "statistics": scheduler.get_statistics()}

    @router.get("/api/jobs")
    async def get_jobs():
        """获取所有已注册的任务列表。

        Returns:
            {"jobs": [...]} 任务字典列表，每个任务包含
            job_id、func_name、schedule_type、next_run、status 等字段
        """
        return {"jobs": scheduler.get_jobs()}

    @router.get("/api/jobs/{job_id}")
    async def get_job(job_id: str):
        """获取指定 ID 的任务详情。

        Args:
            job_id: 任务 ID（格式 "job_N"）

        Returns:
            成功: {"job": {...}}
            失败: {"error": "Job not found", "job_id": job_id}
        """
        job = scheduler.get_job(job_id)
        if job is None:
            return {"error": "Job not found", "job_id": job_id}
        return {"job": job}

    @router.post("/api/jobs/{job_id}/pause")
    async def pause_job(job_id: str):
        """暂停指定任务。

        暂停的任务不会被调度执行，但保留在队列中，可随时恢复。

        Args:
            job_id: 要暂停的任务 ID

        Returns:
            成功: {"success": true, "message": "Job {job_id} paused"}
            失败: {"success": false, "error": "Job {job_id} not found"}
        """
        success = scheduler.pause_job(job_id)
        if success:
            return {"success": True, "message": f"Job {job_id} paused"}
        return {"success": False, "error": f"Job {job_id} not found"}

    @router.post("/api/jobs/{job_id}/resume")
    async def resume_job(job_id: str):
        """恢复已暂停的任务。

        Args:
            job_id: 要恢复的任务 ID

        Returns:
            成功: {"success": true, "message": "Job {job_id} resumed"}
            失败: {"success": false, "error": "Job {job_id} not found"}
        """
        success = scheduler.resume_job(job_id)
        if success:
            return {"success": True, "message": f"Job {job_id} resumed"}
        return {"success": False, "error": f"Job {job_id} not found"}

    @router.post("/api/jobs/{job_id}/cancel")
    async def cancel_job(job_id: str):
        """取消并移除指定任务。

        取消后任务从队列中永久删除，无法恢复。
        若任务正在执行中，会在执行完成后不再重新调度。

        Args:
            job_id: 要取消的任务 ID

        Returns:
            成功: {"success": true, "message": "Job {job_id} cancelled"}
            失败: {"success": false, "error": "Job {job_id} not found"}
        """
        success = scheduler.cancel_job(job_id)
        if success:
            return {"success": True, "message": f"Job {job_id} cancelled"}
        return {"success": False, "error": f"Job {job_id} not found"}

    @router.post("/api/jobs/{job_id}/run")
    async def run_job_now(job_id: str):
        """触发任务的立即执行（仅限已暂停的任务）。

        只有处于暂停状态的任务允许手动触发：暂停任务不会被调度循环定时
        执行，因此手动触发不会与 worker 的定时执行形成同任务并发双执行。
        执行完成后任务保持暂停状态，恢复调度需调用 resume 接口。

        Args:
            job_id: 要立即执行的任务 ID

        Returns:
            成功: {"success": true, "message": "Job {job_id} triggered"}
            失败: {"success": false, "error": "任务不存在/未暂停/正在执行"}
        """
        job = scheduler.get_job(job_id)
        if job is None:
            return {"success": False, "error": f"Job {job_id} not found"}
        if not job.get("paused"):
            return {
                "success": False,
                "error": f"Job {job_id} is not paused; pause it before manual run",
            }
        success = scheduler.run_job_now(job_id)
        if success:
            return {"success": True, "message": f"Job {job_id} triggered"}
        return {
            "success": False,
            "error": f"Job {job_id} not found or already running",
        }

    @router.post("/api/scripts")
    async def add_script_job(request: "Request"):
        """添加执行外部 Python 脚本或动态代码的任务。

        通过此端点可以动态添加任务，支持两种互斥模式:
            - script_path: 执行指定路径的 .py 文件
            - script_code: 执行用户输入的 Python 代码字符串

        两者必须提供其一，同时提供会返回错误。
        脚本/代码将在项目虚拟环境中通过 subprocess 执行。

        同名更新限制: 若已存在同名任务(func_name 相同)且未处于暂停状态,
        返回错误"请先暂停后再更新"; 已暂停的同名任务会被更新(与
        PUT /api/jobs/{job_id} 语义一致), 避免对运行中任务的配置更新
        被 worker 静默忽略造成两侧分歧.

        请求体 (JSON):
            script_path (str): 脚本文件绝对路径（与 script_code 互斥）
            script_code (str): Python 代码字符串（与 script_path 互斥）
            schedule_type (str): 调度类型，可选值:
                - "interval": 间隔执行（需提供 interval）
                - "daily": 每日执行（需提供 schedule_time）
                - "cron": cron 表达式（需提供 cron_expression）
                - "once": 一次性任务（需提供 interval 作为延迟秒数）
            interval (float): 间隔秒数（interval/once 类型必填）
            schedule_time (str): 执行时间 HH:MM（daily 类型必填）
            cron_expression (str): cron 表达式（cron 类型必填）
            script_args (list): 传给脚本的命令行参数，如 ["--flag", "value"]
            func_name (str): 任务名称（可选，script 模式默认用文件名，code 模式默认 "inline_code"）
            max_retries (int): 最大重试次数（默认 3）
            timeout (float): 执行超时秒数（可选）
            timezone (str): 时区（可选，如 "Asia/Shanghai"）
            catch_up (bool): 是否补偿错过的执行（默认 True）

        请求示例:
            # 脚本文件模式
            {
                "script_path": "/path/to/script.py",
                "schedule_type": "interval",
                "interval": 60,
                "script_args": ["--debug"],
                "func_name": "my_script"
            }

            # 动态代码模式
            {
                "script_code": "print('hello world')",
                "schedule_type": "interval",
                "interval": 60
            }

        Returns:
            成功: {"success": true, "job_id": "job_N"}
            失败: {"success": false, "error": "错误描述"}
        """
        try:
            data = await request.json()
        except Exception:
            return {"success": False, "error": "Invalid JSON body"}

        # 互斥校验：script_path 和 script_code 必须提供其一
        script_path = data.get("script_path")
        script_code = data.get("script_code")
        if not script_path and not script_code:
            return {
                "success": False,
                "error": "Either script_path or script_code must be provided",
            }
        if script_path and script_code:
            return {
                "success": False,
                "error": "script_path and script_code are mutually exclusive",
            }

        try:
            job_id = scheduler.add_script_job(
                script_path=data.get("script_path"),
                schedule_type=data.get("schedule_type", "interval"),
                interval=data.get("interval"),
                schedule_time=data.get("schedule_time"),
                cron_expression=data.get("cron_expression"),
                script_args=data.get("script_args", []),
                func_name=data.get("func_name"),
                max_retries=data.get("max_retries", 3),
                timeout=data.get("timeout"),
                timezone=data.get("timezone"),
                catch_up=data.get("catch_up", True),
                script_code=data.get("script_code"),
                # API 进程不是执行方: 对未暂停任务的同名更新会被 worker 静默
                # 忽略甚至反向覆盖, 必须显式拒绝, 与 PUT /api/jobs 语义对齐.
                allow_update_running=False,
            )
            return {"success": True, "job_id": job_id}
        except (FileNotFoundError, ValueError) as e:
            # 脚本文件不存在或参数错误
            return {"success": False, "error": str(e)}
        except Exception as e:
            # 其他未预期的错误
            return {"success": False, "error": f"{type(e).__name__}: {e}"}

    @router.put("/api/jobs/{job_id}")
    async def update_job(job_id: str, request: "Request"):
        """更新已暂停任务的配置。

        仅允许更新处于暂停状态的任务，否则返回错误。
        可更新字段与 POST /api/scripts 一致（除不能同时提供 script_path 和 script_code）。
        更新成功后会重新计算下次执行时间。

        Returns:
            成功: {"success": true, "message": "Job {job_id} updated"}
            失败: {"success": false, "error": "..."}
        """
        try:
            data = await request.json()
        except Exception:
            return {"success": False, "error": "Invalid JSON body"}

        # 任务源互斥校验
        script_path = data.get("script_path")
        script_code = data.get("script_code")
        if script_path and script_code:
            return {
                "success": False,
                "error": "script_path and script_code are mutually exclusive",
            }

        try:
            success = scheduler.update_job(
                job_id,
                script_path=data.get("script_path"),
                schedule_type=data.get("schedule_type"),
                interval=data.get("interval"),
                schedule_time=data.get("schedule_time"),
                cron_expression=data.get("cron_expression"),
                script_args=data.get("script_args"),
                func_name=data.get("func_name"),
                max_retries=data.get("max_retries"),
                timeout=data.get("timeout"),
                timezone=data.get("timezone"),
                catch_up=data.get("catch_up"),
                script_code=data.get("script_code"),
            )
            if success:
                return {"success": True, "message": f"Job {job_id} updated"}
            # 任务不存在或未处于暂停状态
            job = scheduler.get_job(job_id)
            if job is None:
                return {"success": False, "error": f"Job {job_id} not found"}
            return {"success": False, "error": "Job must be paused before editing"}
        except (FileNotFoundError, ValueError) as e:
            return {"success": False, "error": str(e)}
        except Exception as e:
            return {"success": False, "error": f"{type(e).__name__}: {e}"}

    @router.get("/api/history")
    async def get_history(
        func_name: Optional[str] = None,
        status: Optional[str] = None,
        keyword: Optional[str] = None,
        page: int = 1,
        page_size: int = 20,
        limit: Optional[int] = None,
    ):
        """获取任务执行历史记录（支持分页与筛选）。

        Args:
            func_name: 可选，按函数名精确过滤
            status:    可选，按状态过滤（completed/failed/missed）
            keyword:   可选，按函数名或错误信息模糊匹配
            page:      页码（从 1 开始，默认 1）
            page_size: 每页条数（默认 20）
            limit:     兼容旧调用，未提供 page_size 时使用（不分页直接截断）

        Returns:
            分页模式: {"items": [...], "total": int, "page": int, "page_size": int}
        """
        if limit is not None and page_size == 20:
            # 旧调用兼容
            return {"history": scheduler.get_history(func_name, limit)}
        return scheduler.get_history_paged(func_name, status, keyword, page, page_size)

    @router.get("/api/dead-letters")
    async def get_dead_letters(
        keyword: Optional[str] = None,
        page: int = 1,
        page_size: int = 20,
        limit: Optional[int] = None,
    ):
        """获取死信队列（失败任务记录，支持分页与筛选）。

        Args:
            keyword:   可选，按函数名或错误信息模糊匹配
            page:      页码（从 1 开始，默认 1）
            page_size: 每页条数（默认 20）
            limit:     兼容旧调用，未提供 page_size 时使用（不分页直接截断）

        Returns:
            分页模式: {"items": [...], "total": int, "page": int, "page_size": int}
        """
        if limit is not None and page_size == 20:
            # 旧调用兼容
            return {
                "dead_letters": scheduler.get_dead_letters(limit),
                "total": len(scheduler.dead_letters),
            }
        return scheduler.get_dead_letters_paged(keyword, page, page_size)

    @router.delete("/api/dead-letters")
    async def clear_dead_letters():
        """清空所有死信记录。

        清空后无法恢复，请谨慎操作。

        Returns:
            {"success": true, "cleared": int} cleared 为清除的记录数量
        """
        count = scheduler.clear_dead_letters()
        return {"success": True, "cleared": count}

    return router
