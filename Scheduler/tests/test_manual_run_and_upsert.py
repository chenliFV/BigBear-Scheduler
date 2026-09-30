"""调度器手动触发暂停门槛与同名 upsert 限制的单元测试.

覆盖范围:
    - run_job_now 的暂停门槛(未暂停拒绝/已暂停执行一次且保持暂停/无堆内重复);
    - 暂停任务不被调度循环执行(高频间隔计数冻结)与恢复后重新调度;
    - 控制状态同步的锁内加载结构(防"锁外读-锁内应用"竞态);
    - 执行中暂停对 cron 任务的零额外执行(暂停标志不因完成保存而丢失);
    - add_script_job 的 allow_update_running 限制(默认放行注册链, 非执行方拒绝).
"""
import sys
import threading
import time
from pathlib import Path

import pytest

# sys.path 引导: 以脚本方式运行 pytest 时保证包内绝对导入可解析
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from Scheduler import Scheduler  # noqa: E402


def _new_scheduler(tmp_path) -> Scheduler:
    """构造隔离 JSON storage 的调度器实例(不自动启动)."""
    return Scheduler(
        state_file=str(tmp_path / "state.json"),
        storage="json",
        quiet=True,
        auto_start=False,
    )


# ==================== run_job_now: 暂停门槛 ====================


def test_run_job_now_rejects_not_paused(tmp_path):
    """未暂停的任务拒绝手动触发: 返回 False 且不产生执行."""
    scheduler = _new_scheduler(tmp_path)
    run_counts = {"n": 0}

    @scheduler.every(3600).seconds
    def far_future_job():
        run_counts["n"] += 1

    job_id = scheduler.jobs[0].job_id
    assert scheduler.run_job_now(job_id) is False
    time.sleep(0.5)
    assert run_counts["n"] == 0, "未暂停任务不应被执行"
    assert len(scheduler.jobs) == 1, "任务应保留在调度堆中"


def test_run_job_now_executes_paused_job_once(tmp_path):
    """已暂停任务可手动执行一次: 执行后保持暂停且不产生堆内重复条目."""
    scheduler = _new_scheduler(tmp_path)
    run_counts = {"n": 0}

    @scheduler.every(3600).seconds
    def far_future_job():
        run_counts["n"] += 1

    scheduler.flush_state()
    scheduler.start()
    job_id = scheduler.jobs[0].job_id
    assert scheduler.pause_job(job_id) is True
    try:
        assert scheduler.run_job_now(job_id) is True
        deadline = time.time() + 5
        while time.time() < deadline and run_counts["n"] == 0:
            time.sleep(0.1)
        assert run_counts["n"] == 1, "暂停任务的手动触发应执行一次"

        time.sleep(1.0)
        # 执行后回堆恰好一个条目, 且保持暂停状态
        assert len([j for j in scheduler.jobs if j.job_id == job_id]) == 1
        assert scheduler.get_job(job_id)["paused"] is True
        # 暂停任务不会被主循环重复执行
        time.sleep(1.5)
        assert run_counts["n"] == 1
    finally:
        scheduler.stop(wait=True, timeout=10)


def test_paused_job_never_executed_by_loop(tmp_path):
    """暂停后的高频间隔任务不再被调度循环执行(计数冻结).

    用 0.2s 高频间隔放大调度压力: 暂停成功(即任务处于堆内非执行态)后,
    覆盖多个调度周期与状态同步周期, 执行计数必须完全冻结.
    """
    scheduler = _new_scheduler(tmp_path)
    run_counts = {"n": 0}

    @scheduler.every(0.2).seconds
    def fast_job():
        run_counts["n"] += 1

    scheduler.flush_state()
    job_id = scheduler.jobs[0].job_id  # start 前捕获, 高频任务启动后可能已被弹出
    scheduler.start()
    try:
        # 先确认任务在正常调度执行
        deadline = time.time() + 5
        while time.time() < deadline and run_counts["n"] < 3:
            time.sleep(0.1)
        assert run_counts["n"] >= 3, "暂停前任务应正常调度执行"

        # 暂停: 任务恰在执行中时 pause_job 找不到(不在堆内), 重试至成功;
        # 暂停成功即代表任务处于堆内且无在途执行.
        deadline = time.time() + 5
        while time.time() < deadline and not scheduler.pause_job(job_id):
            time.sleep(0.05)
        assert scheduler.get_job(job_id)["paused"] is True

        time.sleep(0.5)  # 在途收尾缓冲
        frozen = run_counts["n"]
        time.sleep(2.0)  # 0.2s 间隔本可再执行约 10 次
        assert run_counts["n"] == frozen, "暂停后调度循环不应再执行该任务"
    finally:
        scheduler.stop(wait=True, timeout=10)


# ==================== 控制状态同步: 锁内加载与恢复调度 ====================


def test_sync_loads_state_under_lock(tmp_path, monkeypatch):
    """结构性回归: _sync_control_state 必须在持有调度锁时加载存储状态.

    若在锁外加载, 本进程 pause_job/resume_job 修改标志并保存后, 同步仍可能
    用加载到的旧记录把标志覆写回旧值(暂停被瞬时回退, 到期任务可趁机漏跑).
    """
    scheduler = _new_scheduler(tmp_path)

    @scheduler.every(3600).seconds
    def far_future_job():
        return None

    scheduler.flush_state()
    observed: dict = {}
    original_load = scheduler._storage.load_state

    def probing_load():
        # RLock 无 locked() 方法, 用 _is_owned 判断当前线程是否持有锁
        observed["locked"] = scheduler.lock._is_owned()
        return original_load()

    monkeypatch.setattr(scheduler._storage, "load_state", probing_load)
    scheduler._sync_control_state()
    assert observed.get("locked") is True, "load_state 必须在锁临界区内调用"


def test_resume_job_restarts_scheduling(tmp_path):
    """恢复功能链路: 暂停冻结 -> resume 后任务重新被调度执行."""
    scheduler = _new_scheduler(tmp_path)
    run_counts = {"n": 0}

    @scheduler.every(0.2).seconds
    def fast_job():
        run_counts["n"] += 1

    scheduler.flush_state()
    job_id = scheduler.jobs[0].job_id  # start 前捕获, 高频任务启动后可能已被弹出
    scheduler.start()
    try:
        deadline = time.time() + 5
        while time.time() < deadline and run_counts["n"] < 3:
            time.sleep(0.1)
        deadline = time.time() + 5
        while time.time() < deadline and not scheduler.pause_job(job_id):
            time.sleep(0.05)
        time.sleep(1.0)
        frozen = run_counts["n"]

        deadline = time.time() + 5
        while time.time() < deadline and not scheduler.resume_job(job_id):
            time.sleep(0.05)
        assert scheduler.get_job(job_id)["paused"] is False

        deadline = time.time() + 5
        while time.time() < deadline and run_counts["n"] <= frozen:
            time.sleep(0.1)
        assert run_counts["n"] > frozen, "恢复后任务应重新被调度执行"
    finally:
        scheduler.stop(wait=True, timeout=10)


def test_save_state_includes_running_job(tmp_path):
    """执行中的任务必须随状态保存持久化.

    执行中的任务已被 pop 出堆; 若保存只序列化堆内任务, 其他任务完成触发的
    保存会把执行中任务从存储抹掉, 下一轮同步误判为"已取消"并在执行完成后
    将其永久移除. 此处执行中强制保存一次, 断言存储仍包含该任务.
    """
    scheduler = _new_scheduler(tmp_path)
    started = threading.Event()
    release = threading.Event()

    @scheduler.cron("* * * * *")
    def cron_job():
        started.set()
        release.wait(timeout=15)

    scheduler.jobs[0].next_run = time.time()
    job_id = scheduler.jobs[0].job_id  # start 前捕获, 避免主循环弹出后堆空
    scheduler.flush_state()
    scheduler.start()
    try:
        assert started.wait(timeout=5), "任务应被触发执行"
        assert job_id in scheduler._running_jobs
        # 模拟其他任务完成触发的保存: 走保存线程池(与生产一致, 单线程串行)
        scheduler._save_state_async()
        scheduler.flush_state()
        state = scheduler._storage.load_state()
        assert job_id in {j["job_id"] for j in state["jobs"]}, "执行中任务不得被保存抹掉"
        assert job_id not in scheduler._cancelled_during_run, "不得被误判为已取消"
    finally:
        release.set()
        scheduler.stop(wait=True, timeout=15)


def test_pause_during_execution_cron_zero_extra_runs(tmp_path):
    """执行中暂停对 cron 任务零额外执行.

    用每分钟 cron 任务模拟生产周清理场景: 任务执行期间(已 pop 出堆)由
    API 进程视图(第二个 Scheduler 实例, 同状态文件)发起暂停, 断言:
        - 在途执行正常跑完(设计如此, 计 1 次);
        - 暂停标志穿透执行完成后的全量保存, 不被覆写丢失;
        - 之后零额外执行(cron 下次槽位遥远, 暂停分支持有任务).
    """
    scheduler = _new_scheduler(tmp_path)
    run_counts = {"n": 0}
    started = threading.Event()
    release = threading.Event()

    @scheduler.cron("* * * * *")
    def cron_job():
        run_counts["n"] += 1
        started.set()
        release.wait(timeout=15)

    # 将下次触发提前到现在, 避免等待下一分钟槽位
    scheduler.jobs[0].next_run = time.time()
    job_id = scheduler.jobs[0].job_id  # start 前捕获, 避免主循环弹出后堆空
    scheduler.flush_state()
    scheduler.start()
    try:
        assert started.wait(timeout=5), "cron 任务应被触发执行"
        assert job_id in scheduler._running_jobs, "任务应处于执行中"

        # API 进程视图: 同一状态文件、不启动调度, 与生产 API 行为一致
        api_view = Scheduler(
            state_file=str(tmp_path / "state.json"),
            storage="json",
            quiet=True,
            auto_start=False,
        )
        assert api_view.pause_job(job_id) is True, "API 视图暂停执行中任务"

        release.set()  # 放行在途执行
        deadline = time.time() + 5
        while time.time() < deadline and job_id in scheduler._running_jobs:
            time.sleep(0.1)
        time.sleep(2.5)  # 覆盖多个同步周期与暂停持有周期

        # 核心不变量(严格): 零额外执行. 跨进程暂停写与在途全量保存存在
        # 已披露的 <=1s 写-写窗口(json 后端无跨进程锁), 高负载下窗口命中
        # 时暂停标志会被旧快照短暂覆写, 但 sync 每秒自愈; 状态类断言按
        # 自愈语义做收敛式轮询, 执行计数始终保持硬断言.
        assert run_counts["n"] == 1, "在途执行恰好 1 次, 零额外执行"

        def _wait_paused_converged(timeout: float = 6.0) -> bool:
            """轮询等待暂停标志在内存与持久化两侧收敛落地."""
            end = time.time() + timeout
            while time.time() < end:
                in_mem = bool(scheduler.get_job(job_id).get("paused"))
                persisted_jobs = {
                    j["job_id"]: j
                    for j in api_view._storage.load_state()["jobs"]
                }
                on_disk = bool(persisted_jobs.get(job_id, {}).get("paused"))
                if in_mem and on_disk:
                    return True
                time.sleep(0.3)
            return False

        assert _wait_paused_converged(), "暂停标志应在自愈周期内收敛(内存+持久化)"

        time.sleep(1.5)
        assert run_counts["n"] == 1, "暂停持有期间不再执行"
    finally:
        release.set()
        scheduler.stop(wait=True, timeout=15)


# ==================== 执行中暂停/取消: 在途执行不被中断 ====================


def test_pause_during_execution_does_not_interrupt_inflight(tmp_path):
    """执行中点击暂停不中断在途执行: 当前这次跑完, 之后保持暂停不被调度.

    "不被中断"的判定依据: 任务函数体在暂停之后的代码确实执行完毕
    (body_finished 置位), 且全程恰好执行 1 次.
    """
    scheduler = _new_scheduler(tmp_path)
    progress: dict = {
        "started": threading.Event(),
        "release": threading.Event(),
        "body_finished": False,
        "runs": 0,
    }

    @scheduler.cron("* * * * *")
    def long_job():
        progress["runs"] += 1
        progress["started"].set()
        progress["release"].wait(timeout=15)
        progress["body_finished"] = True  # 若在途执行被中断, 此行不会运行

    scheduler.jobs[0].next_run = time.time()
    job_id = scheduler.jobs[0].job_id  # start 前捕获, 避免主循环弹出后堆空
    scheduler.flush_state()
    scheduler.start()
    try:
        assert progress["started"].wait(timeout=5), "任务应被触发执行"

        api_view = Scheduler(
            state_file=str(tmp_path / "state.json"),
            storage="json",
            quiet=True,
            auto_start=False,
        )
        assert api_view.pause_job(job_id) is True, "API 视图暂停执行中任务"

        # 等暂停标志经同步周期落到执行中对象(与生产分钟级任务语义一致)
        deadline = time.time() + 4
        while time.time() < deadline and not scheduler._running_job_objs[job_id].paused:
            time.sleep(0.1)
        assert scheduler._running_job_objs[job_id].paused is True, "暂停标志应落地"
        assert job_id in scheduler._running_jobs, "暂停不得中断在途执行"

        progress["release"].set()
        deadline = time.time() + 5
        while time.time() < deadline and not progress["body_finished"]:
            time.sleep(0.1)
        assert progress["body_finished"] is True, "在途执行必须完整跑完"
        assert progress["runs"] == 1

        time.sleep(1.5)  # 覆盖完成重排与暂停持有周期
        assert progress["runs"] == 1, "暂停后不得再次执行"
        assert scheduler.get_job(job_id)["paused"] is True, "完成后保持暂停"
    finally:
        progress["release"].set()
        scheduler.stop(wait=True, timeout=15)


def test_cancel_during_execution_does_not_interrupt_inflight(tmp_path):
    """执行中点击取消不中断在途执行: 当前这次跑完, 完成后任务被移除.

    等待 worker 同步确认取消(_cancelled_during_run 登记)后再放行, 与生产
    "分钟级任务执行期间被取消"的时序一致, 保证断言确定性.
    """
    scheduler = _new_scheduler(tmp_path)
    progress: dict = {
        "started": threading.Event(),
        "release": threading.Event(),
        "body_finished": False,
        "runs": 0,
    }

    @scheduler.cron("* * * * *")
    def long_job():
        progress["runs"] += 1
        progress["started"].set()
        progress["release"].wait(timeout=15)
        progress["body_finished"] = True  # 若在途执行被中断, 此行不会运行

    scheduler.jobs[0].next_run = time.time()
    job_id = scheduler.jobs[0].job_id  # start 前捕获, 避免主循环弹出后堆空
    scheduler.flush_state()
    scheduler.start()
    try:
        assert progress["started"].wait(timeout=5), "任务应被触发执行"

        api_view = Scheduler(
            state_file=str(tmp_path / "state.json"),
            storage="json",
            quiet=True,
            auto_start=False,
        )
        assert api_view.cancel_job(job_id) is True, "API 视图取消执行中任务"

        # 等 worker 同步读到取消(执行中任务从存储消失 -> 登记待移除)
        deadline = time.time() + 4
        while time.time() < deadline and job_id not in scheduler._cancelled_during_run:
            time.sleep(0.1)
        assert job_id in scheduler._cancelled_during_run, "取消应被同步感知"
        assert job_id in scheduler._running_jobs, "取消不得中断在途执行"

        progress["release"].set()
        deadline = time.time() + 5
        while time.time() < deadline and not progress["body_finished"]:
            time.sleep(0.1)
        assert progress["body_finished"] is True, "在途执行必须完整跑完"
        assert progress["runs"] == 1

        # 完成后任务被永久移除: 堆内与持久化状态均无, 登记清理
        deadline = time.time() + 3
        while time.time() < deadline and any(j.job_id == job_id for j in scheduler.jobs):
            time.sleep(0.1)
        assert not any(j.job_id == job_id for j in scheduler.jobs), "取消后不得重新入队"
        assert job_id not in scheduler._cancelled_during_run, "登记应被清理"
        scheduler.flush_state()  # 等待移除后的异步保存落盘, 消除读取时序依赖
        persisted = {j["job_id"] for j in api_view._storage.load_state()["jobs"]}
        assert job_id not in persisted, "存储中任务应被移除"

        time.sleep(1.0)
        assert progress["runs"] == 1, "取消后不得再次执行"
    finally:
        progress["release"].set()
        scheduler.stop(wait=True, timeout=15)


# ==================== add_script_job: 同名 upsert 限制 ====================


def _register_script_job(scheduler: Scheduler, script: Path, func_name: str) -> str:
    """按给定名称注册一个脚本任务, 返回 job_id."""
    return scheduler.register_script_file(
        script_path=str(script),
        schedule_type="interval",
        interval=3600,
        func_name=func_name,
    )


def test_add_script_job_upsert_default_allows_running(tmp_path):
    """默认 allow_update_running=True: worker 注册链可更新未暂停的同名任务."""
    scheduler = _new_scheduler(tmp_path)
    script = tmp_path / "task.py"
    script.write_text("print('ok')\n", encoding="utf-8")
    job_id = _register_script_job(scheduler, script, "demo_task")

    updated_id = scheduler.add_script_job(
        script_path=str(script), func_name="demo_task", interval=7200
    )
    assert updated_id == job_id
    job = scheduler.get_job(job_id)
    assert job["interval"] == 7200


def test_add_script_job_upsert_rejects_running_when_restricted(tmp_path):
    """allow_update_running=False: 未暂停的同名任务拒绝更新(防止配置分歧)."""
    scheduler = _new_scheduler(tmp_path)
    script = tmp_path / "task.py"
    script.write_text("print('ok')\n", encoding="utf-8")
    job_id = _register_script_job(scheduler, script, "demo_task")

    with pytest.raises(ValueError, match="未暂停"):
        scheduler.add_script_job(
            script_path=str(script),
            func_name="demo_task",
            interval=7200,
            allow_update_running=False,
        )
    # 原配置未被修改
    assert scheduler.get_job(job_id)["interval"] == 3600


def test_add_script_job_upsert_allows_paused_when_restricted(tmp_path):
    """allow_update_running=False: 已暂停的同名任务允许更新(与 PUT 语义对齐)."""
    scheduler = _new_scheduler(tmp_path)
    script = tmp_path / "task.py"
    script.write_text("print('ok')\n", encoding="utf-8")
    job_id = _register_script_job(scheduler, script, "demo_task")
    assert scheduler.pause_job(job_id) is True

    updated_id = scheduler.add_script_job(
        script_path=str(script),
        func_name="demo_task",
        interval=7200,
        allow_update_running=False,
    )
    assert updated_id == job_id
    assert scheduler.get_job(job_id)["interval"] == 7200
