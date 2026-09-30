"""调度器框架加固(D1-D7, O1)的回归测试.

覆盖:
    - D1: 同步函数超时即时释放 worker 槽位(不再被 with 块阻塞到函数自然结束);
    - D2: stop 对执行池有界等待(挂死任务不再卡死优雅停机);
    - D3: next_run 计算失败的 60s 兜底(croniter 不可用/未知类型, 防热循环);
    - D4: interval 非正数在注册入口被拒绝;
    - D5: 脚本超时杀整个进程组(孙进程不残留);
    - D7: stats["total_runs"] 口径包含失败(与 query_stats 对齐);
    - O1: sqlmodel save_state 批量同步与 history 去重.
"""
import os
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
from Scheduler.models import Job  # noqa: E402
from Scheduler.storage.sqlmodel_backend import SQLModelStorageBackend  # noqa: E402


def _new_scheduler(tmp_path) -> Scheduler:
    """构造隔离 JSON storage 的调度器实例(不自动启动)."""
    return Scheduler(
        state_file=str(tmp_path / "state.json"),
        storage="json",
        quiet=True,
        auto_start=False,
    )


def _wait_job_event(scheduler: Scheduler, job_id: str, predicate, timeout: float = 6.0):
    """轮询指定任务的历史事件直至谓词命中, 返回命中事件否则 None."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        for event in scheduler.history:
            if event.job_id == job_id and predicate(event):
                return event
        time.sleep(0.1)
    return None


# ==================== D1: 同步函数超时释放 worker ====================


def test_sync_func_timeout_releases_worker(tmp_path):
    """同步函数超时后即时进入失败链路, 不等待函数自然结束."""
    scheduler = _new_scheduler(tmp_path)
    hang_seconds = 3.0
    leak_finished = threading.Event()

    @scheduler.every(3600).retries(0).timeout(0.5).seconds
    def slow_job():
        time.sleep(hang_seconds)
        leak_finished.set()

    scheduler.jobs[0].next_run = time.time()
    job_id = scheduler.jobs[0].job_id  # start 前捕获, 避免主循环弹出后堆空
    scheduler.flush_state()
    scheduler.start()
    try:
        started = time.monotonic()
        event = _wait_job_event(
            scheduler,
            job_id,
            lambda e: e.status == "failed" and "timed out" in (e.error or ""),
        )
        assert event is not None, "超时应产生 failed 事件"
        elapsed = time.monotonic() - started
        # 修复前: with 块 shutdown(wait=True) 会阻塞到 sleep 结束(>= hang_seconds)
        assert elapsed < hang_seconds, f"超时应在 {0.5}s 附近生效, 实耗 {elapsed:.1f}s"

        # worker 槽位释放: 执行登记被清理, 死信落库
        deadline = time.time() + 2
        while time.time() < deadline and job_id in scheduler._running_jobs:
            time.sleep(0.1)
        assert job_id not in scheduler._running_jobs, "超时后应释放 worker 槽位"
        assert any(dl.job_id == job_id for dl in scheduler.dead_letters)
    finally:
        scheduler.stop(wait=True, timeout=5)
        # 等泄漏线程自然结束, 避免其后台活动干扰后续用例(如并发类测试)
        leak_finished.wait(timeout=hang_seconds + 5)


# ==================== D2: stop 有界等待 ====================


def test_stop_is_bounded_with_hanging_job(tmp_path):
    """挂死任务不阻断 stop: 有界等待后返回, 在途线程自然结束."""
    scheduler = _new_scheduler(tmp_path)
    release = threading.Event()

    @scheduler.every(3600).seconds
    def hang_job():
        release.wait(timeout=20)

    scheduler.jobs[0].next_run = time.time()
    job_id = scheduler.jobs[0].job_id  # start 前捕获, 避免主循环弹出后堆空
    scheduler.flush_state()
    scheduler.start()
    assert _wait_job_event(scheduler, job_id, lambda e: e.status == "running")

    started = time.monotonic()
    scheduler.stop(wait=True, timeout=2)
    elapsed = time.monotonic() - started
    # 修复前: executor.shutdown(wait=True) 无界阻塞到任务结束(>= 20s);
    # 上界放宽到 15s 以吸收全量测试并跑时的 CPU 争抢.
    assert elapsed < 15, f"stop 应在有界时间内返回, 实耗 {elapsed:.1f}s"
    assert job_id in scheduler._running_jobs, "stop 返回时在途任务仍未结束(未强杀)"

    release.set()  # 放行在途线程并等其收尾, 避免影响后续用例
    deadline = time.time() + 8
    while time.time() < deadline and job_id in scheduler._running_jobs:
        time.sleep(0.1)
    assert job_id not in scheduler._running_jobs


# ==================== D3: next_run 兜底 ====================


def _make_job(job_id: str, schedule_type: str, **kwargs) -> Job:
    """构造直接用于调度计算的 Job 实例."""
    base = {
        "job_id": job_id,
        "func": None,
        "func_name": job_id,
        "func_module": "__script__",
        "next_run": time.time() - 10,
        "repeat": True,
        "schedule_type": schedule_type,
    }
    base.update(kwargs)
    return Job(**base)


def test_schedule_next_run_fallback_prevents_hot_loop(tmp_path, monkeypatch):
    """croniter 不可用或未知类型时, next_run 兜底顺延 60s."""
    from Scheduler import main as scheduler_main

    scheduler = _new_scheduler(tmp_path)

    cron_job = _make_job("job_cron", "cron", cron_expression="* * * * *")
    monkeypatch.setattr(scheduler_main, "CRONITER_AVAILABLE", False)
    scheduler._schedule_next_run(cron_job)
    assert cron_job.next_run > time.time() + 30, "croniter 缺失时应兜底顺延"

    weird_job = _make_job("job_weird", "unknown_type", interval=None)
    scheduler._schedule_next_run(weird_job)
    assert weird_job.next_run > time.time() + 30, "未知类型应兜底顺延"

    monkeypatch.undo()
    normal = _make_job("job_interval", "interval", interval=120)
    scheduler._schedule_next_run(normal)
    assert abs(normal.next_run - (time.time() + 120)) < 5, "正常路径不受影响"


# ==================== D4: interval 非正数校验 ====================


def test_register_rejects_non_positive_interval(tmp_path):
    """interval 类任务间隔非正数在注册入口被拒(装饰器与脚本任务两条路径)."""
    scheduler = _new_scheduler(tmp_path)

    with pytest.raises(ValueError, match="interval must be positive"):

        @scheduler.every(0).seconds
        def zero_interval_job():
            return None

    script = tmp_path / "task.py"
    script.write_text("print('ok')\n", encoding="utf-8")
    # 脚本任务路径: 第四轮组合校验先于 _add_job 触发, 信息更具体
    with pytest.raises(ValueError, match="positive interval"):
        scheduler.add_script_job(
            script_path=str(script),
            schedule_type="interval",
            interval=0,
            func_name="bad_script",
        )

    # once 类型以 interval 充当延迟, 0 表示立即执行, 不受限制
    job_id = scheduler.add_script_job(
        script_path=str(script),
        schedule_type="once",
        interval=0,
        func_name="once_now",
    )
    assert job_id


# ==================== D5: 脚本超时杀进程组 ====================


def test_script_timeout_kills_process_group(tmp_path):
    """脚本超时被杀时, 其派生的孙进程(独立 sleep)一并被进程组清理."""
    scheduler = _new_scheduler(tmp_path)
    pid_file = tmp_path / "grandchild.pid"
    script = tmp_path / "spawner.py"
    script.write_text(
        "import subprocess, sys, time\n"
        "p = subprocess.Popen(['sleep', '30'])\n"
        "with open(sys.argv[1], 'w') as f:\n"
        "    f.write(str(p.pid))\n"
        "time.sleep(30)\n",
        encoding="utf-8",
    )
    scheduler.register_script_file(
        script_path=str(script),
        schedule_type="interval",
        interval=3600,
        script_args=[str(pid_file)],
        func_name="spawner",
        timeout=1,
    )
    scheduler.jobs[0].next_run = time.time()
    job_id = scheduler.jobs[0].job_id  # start 前捕获, 避免主循环弹出后堆空
    scheduler.flush_state()
    scheduler.start()
    grandchild_pid = None
    try:
        event = _wait_job_event(
            scheduler, job_id, lambda e: e.status == "failed" and "timed out" in (e.error or "")
        )
        assert event is not None, "脚本超时应产生 failed 事件"
        assert pid_file.exists(), "孙进程 pid 文件应已写出"

        grandchild_pid = int(pid_file.read_text().strip())
        deadline = time.time() + 4
        while time.time() < deadline:
            try:
                os.kill(grandchild_pid, 0)
                time.sleep(0.2)  # 孙进程仍在, 等待进程组清理传播
            except ProcessLookupError:
                break
        else:
            pytest.fail("孙进程应在进程组清理后消失")
    finally:
        if grandchild_pid is not None:
            try:
                os.kill(grandchild_pid, 9)
            except (ProcessLookupError, PermissionError):
                pass
        scheduler.stop(wait=True, timeout=5)


# ==================== D2 补充: 取消排队任务的登记清理 ====================


def test_stop_cleans_cancelled_queued_registrations(tmp_path):
    """停机取消排队任务时, 预登记的执行中状态被清理(无幽灵"执行中"残留).

    主循环在提交前把到期任务登记进 _running_jobs; 超出 worker 并发上限
    的排队任务被 stop 的 cancel_futures 取消后不会执行, 若无人清理,
    其登记将永久残留(压测实测: 25 个挂起任务停机后 25 条全部残留).
    """
    scheduler = Scheduler(
        state_file=str(tmp_path / "state.json"),
        storage="json",
        quiet=True,
        auto_start=False,
        max_workers=2,
    )
    release = threading.Event()

    def make_hanger():
        def hanger():
            release.wait(timeout=30)

        return hanger

    for _ in range(8):
        scheduler.every(3600).seconds.do(make_hanger())
    for job in scheduler.jobs:
        job.next_run = time.time()
    scheduler.flush_state()
    scheduler.start()

    deadline = time.time() + 5
    while time.time() < deadline and len(scheduler._running_jobs) < 8:
        time.sleep(0.1)
    assert len(scheduler._running_jobs) == 8, "到期任务提交前应全部预登记"

    scheduler.stop(wait=True, timeout=1)
    # 取消的 6 个排队任务登记被回调清理, 仅剩真正在执行的 2 个
    assert len(scheduler._running_jobs) == 2, "被取消的排队任务不得残留登记"

    release.set()
    deadline = time.time() + 8
    while time.time() < deadline and scheduler._running_jobs:
        time.sleep(0.1)
    assert not scheduler._running_jobs, "放行后在途任务应全部收尾"


# ==================== 第二轮审计: 历史保留与 refresh 节流 ====================


def test_history_retention_enforced_in_storage(tmp_path):
    """存储侧历史按 max_history 裁剪(此前 cleanup_history 无人调用, DB 无限增长)."""
    scheduler = Scheduler(
        state_file=str(tmp_path / "state.json"),
        storage="sqlmodel",
        database_url=f"sqlite:///{tmp_path / 'sched.db'}",
        quiet=True,
        auto_start=False,
        max_history=10,
        history_retention_days=0,  # 关闭时间维度, 只验证数量维度
    )
    base = time.time()
    for i in range(30):
        scheduler._storage.save_history_entry(
            _hist_dict("job_bulk", base + i)
        )
    assert len(scheduler._storage.load_state()["history"]) == 30, "清理前应全量可见"

    scheduler._last_history_cleanup = 0.0  # 强制触发(模拟启动后首个周期)
    scheduler._enforce_history_retention(time.time() + 1)
    remaining = len(scheduler._storage.load_state()["history"])
    assert remaining <= 10, f"存储历史应被裁剪至 max_history 以内, 实际 {remaining}"

    # 一小时内重复调用被节流
    calls = {"n": 0}
    original_cleanup = scheduler._storage.cleanup_history

    def counting_cleanup(*args, **kwargs):
        calls["n"] += 1
        return original_cleanup(*args, **kwargs)

    scheduler._storage.cleanup_history = counting_cleanup
    scheduler._enforce_history_retention(time.time() + 2)
    scheduler._enforce_history_retention(time.time() + 3)
    assert calls["n"] == 0, "节流窗口内的重复清理不应执行"


def test_refresh_state_throttled_and_loads_jobs(tmp_path, monkeypatch):
    """refresh_state: 0.8s 内重复调用被节流, 刷新后任务视图完整."""
    scheduler = _new_scheduler(tmp_path)

    @scheduler.every(3600).seconds
    def sample_job():
        return None

    scheduler.flush_state()
    calls = {"load": 0}
    original_load = scheduler._storage.load_state

    def probing_load():
        calls["load"] += 1
        return original_load()

    monkeypatch.setattr(scheduler._storage, "load_state", probing_load)

    scheduler.refresh_state()
    scheduler.refresh_state()  # 节流窗口内, 应跳过
    assert calls["load"] == 1, "0.8s 内的重复刷新应被节流"
    assert len(scheduler.jobs) == 1, "刷新后任务视图应完整"


# ==================== D7: stats 口径 ====================


def test_stats_total_runs_counts_failures(tmp_path):
    """失败执行同样计入 total_runs(与 query_stats 的 completed+failed 口径一致)."""
    scheduler = _new_scheduler(tmp_path)

    @scheduler.every(3600).retries(0).seconds
    def failing_job():
        raise RuntimeError("planned failure")

    scheduler.jobs[0].next_run = time.time()
    job_id = scheduler.jobs[0].job_id  # start 前捕获, 避免主循环弹出后堆空
    scheduler.flush_state()
    scheduler.start()
    try:
        event = _wait_job_event(
            scheduler,
            job_id,
            lambda e: e.status == "failed" and (e.error or "").startswith("Max retries"),
        )
        assert event is not None, "应到达死信终态"
        assert scheduler.stats["total_failures"] >= 1
        assert scheduler.stats["total_runs"] >= 1, "失败也应计入 total_runs"
    finally:
        scheduler.stop(wait=True, timeout=5)


# ==================== O1: sqlmodel 批量保存 ====================


def _job_dict(job_id: str, interval: float = 60.0, **extra) -> dict:
    payload = {
        "job_id": job_id,
        "func_name": job_id,
        "func_module": "__script__",
        "next_run": 2000.0,
        "interval": interval,
        "repeat": True,
        "schedule_type": "interval",
        "created_at": 1000.0,
        "paused": False,
    }
    payload.update(extra)
    return payload


def _hist_dict(job_id: str, timestamp: float) -> dict:
    return {
        "job_id": job_id,
        "func_name": job_id,
        "status": "completed",
        "timestamp": timestamp,
        "run_count": 1,
        "retry_count": 0,
    }


def test_sqlmodel_save_state_batch_roundtrip(tmp_path):
    """批量保存: 任务新增/更新正确, 保存不删除, 显式 delete_job 才删除."""
    backend = SQLModelStorageBackend(f"sqlite:///{tmp_path / 'sched.db'}")

    backend.save_state(
        jobs=[
            _job_dict("job_a", args=["hello", 1], kwargs={"times": 2}),
            _job_dict("job_b", interval=120.0),
        ],
        history=[_hist_dict("job_a", 1000.0)],
        statistics={"total_runs": 1},
        job_counter=2,
        scheduler_running=True,
    )
    # 第二次保存(worker 内存只有 job_a 的场景): 更新 job_a.interval,
    # 重放旧历史并新增一条; job_b 不在载荷中但不得被删除
    # (第四轮 P1: 全表删除会在跨进程写-写竞态中吞掉 API 新增的任务)
    backend.save_state(
        jobs=[_job_dict("job_a", interval=300.0, args=["hello", 1], kwargs={"times": 2})],
        history=[_hist_dict("job_a", 1000.0), _hist_dict("job_a", 1001.0)],
        statistics={"total_runs": 2},
        job_counter=2,
        scheduler_running=True,
    )

    state = backend.load_state()
    jobs = {j["job_id"]: j for j in state["jobs"]}
    assert set(jobs) == {"job_a", "job_b"}, "载荷之外的任务不被保存路径删除"
    assert jobs["job_a"]["interval"] == 300.0, "更新生效"
    assert jobs["job_a"]["args"] == ["hello", 1], "函数位置参数往返不丢失"
    assert jobs["job_a"]["kwargs"] == {"times": 2}, "函数关键字参数往返不丢失"

    # 取消语义: 显式 delete_job 才移除
    backend.delete_job("job_b")
    state = backend.load_state()
    jobs = {j["job_id"]: j for j in state["jobs"]}
    assert set(jobs) == {"job_a"}, "delete_job 显式删除生效"

    timestamps = [h["timestamp"] for h in state["history"]]
    assert timestamps.count(1000.0) == 1, "重复时间戳历史被去重"
    assert 1001.0 in timestamps, "新历史已入库"


# ==================== 第三轮审计: 参数往返与流式 API 校验 ====================


def test_function_args_roundtrip_and_inline_guard(tmp_path):
    """带参函数任务: args/kwargs 持久化往返完整, 且不填充无参内联脚本."""
    import json as json_lib

    scheduler = _new_scheduler(tmp_path)

    def greeter(name, times=1):
        return None

    scheduler.every(3600).seconds.do(greeter, "hello", times=2)
    argful = scheduler.jobs[0]
    assert argful.args == ("hello",) and argful.kwargs == {"times": 2}
    assert argful.script_code is None, "带参函数不得填充无参内联脚本(重启会错执行)"

    @scheduler.every(3600).seconds
    def no_arg_job():
        return None

    no_arg = next(j for j in scheduler.jobs if j.func_name == "no_arg_job")
    assert no_arg.script_code, "无参函数仍应填充内联脚本作为重启回退"

    scheduler.flush_state()
    saved = json_lib.loads(scheduler._storage.state_file.read_text(encoding="utf-8"))
    saved_argful = next(j for j in saved["jobs"] if j["func_name"] == "greeter")
    assert saved_argful["args"] == ["hello"], "位置参数应随状态持久化"
    assert saved_argful["kwargs"] == {"times": 2}, "关键字参数应随状态持久化"

    # 模拟重启: 新实例从存储恢复, 参数不丢失
    restarted = Scheduler(
        state_file=str(tmp_path / "state.json"),
        storage="json",
        quiet=True,
        auto_start=False,
    )
    restored = next(j for j in restarted.jobs if j.func_name == "greeter")
    assert restored.args == ("hello",) and restored.kwargs == {"times": 2}


def test_weekly_days_validation(tmp_path):
    """weekly 的星期编码越界在注册期被拒(否则任务静默永不触发)."""
    scheduler = _new_scheduler(tmp_path)
    with pytest.raises(ValueError, match="week days"):
        scheduler.weekly.on([7])
    with pytest.raises(ValueError, match="week days"):
        scheduler.weekly.on([-1])
    assert scheduler.weekly.on([0, 6]) is not None, "合法编码不受影响"


# ==================== 第四轮审计: 跨进程保存竞态与配置校验 ====================


def test_worker_save_keeps_other_writers_jobs(tmp_path):
    """P1: worker 的周期性保存不得吞掉其他进程(API)新增的任务.

    模拟双进程写-写竞态: API 进程写入 job_api_2 后, worker 进程
    (内存尚未同步到该任务)触发一次保存, DB 中 job_api_2 必须仍在.
    """
    backend = SQLModelStorageBackend(f"sqlite:///{tmp_path / 'mp.db'}")

    # API 进程: 新增两个任务
    backend.save_state(
        jobs=[_job_dict("job_api_1"), _job_dict("job_api_2")],
        history=[],
        statistics={"total_runs": 0},
        job_counter=2,
        scheduler_running=False,
    )
    # worker 进程: 内存只有 job_api_1(尚未 sync 到 job_api_2), 任务事件触发的保存
    backend.save_state(
        jobs=[_job_dict("job_api_1", interval=90.0)],
        history=[_hist_dict("job_api_1", 2000.0)],
        statistics={"total_runs": 1},
        job_counter=2,
        scheduler_running=True,
    )

    state = backend.load_state()
    ids = {j["job_id"] for j in state["jobs"]}
    assert ids == {"job_api_1", "job_api_2"}, "worker 保存不得删除 API 新增的任务"
    jobs = {j["job_id"]: j for j in state["jobs"]}
    assert jobs["job_api_1"]["interval"] == 90.0, "worker 侧更新仍生效"


def test_cancel_persists_via_delete_job(tmp_path):
    """P1: 取消任务通过显式 delete_job 落库, 存储记录即时消失."""
    import json as json_lib

    scheduler = _new_scheduler(tmp_path)
    script = tmp_path / "noop.py"
    script.write_text("pass\n", encoding="utf-8")
    scheduler.add_script_job(
        script_path=str(script),
        schedule_type="interval",
        interval=3600,
        func_name="cancel_me",
    )
    scheduler.flush_state()

    saved = json_lib.loads(
        scheduler._storage.state_file.read_text(encoding="utf-8")
    )
    assert any(j["func_name"] == "cancel_me" for j in saved["jobs"])

    job_id = scheduler.jobs[0].job_id
    assert scheduler.cancel_job(job_id) is True

    saved = json_lib.loads(
        scheduler._storage.state_file.read_text(encoding="utf-8")
    )
    assert not any(
        j["func_name"] == "cancel_me" for j in saved["jobs"]
    ), "取消后存储记录应消失"


def test_placeholder_job_cleans_registration(tmp_path):
    """P8: 占位任务执行路径清理预登记并删除存储记录.

    主循环 pop 即登记执行中; 无函数无脚本的占位任务在 _execute_job
    前置 return 不走 finally, 若不清理会永久滞留 _running_jobs
    (幽灵"执行中" + 有界停机被卡到超时).
    """
    scheduler = _new_scheduler(tmp_path)
    job = Job(
        next_run=time.time(),
        job_id="job_ph",
        func=None,
        func_name="placeholder",
        func_module="__script__",
        interval=3600,
        schedule_type="interval",
    )
    scheduler.jobs.append(job)
    scheduler.flush_state()

    # 模拟主循环"pop 即登记"后提交执行
    scheduler.jobs.remove(job)
    scheduler._running_jobs.add(job.job_id)
    scheduler._running_job_objs[job.job_id] = job
    scheduler._execute_job(job)

    assert job.job_id not in scheduler._running_jobs, "占位任务不得泄漏执行登记"
    assert job.job_id not in scheduler._running_job_objs
    state = scheduler._storage.load_state()
    assert state is None or all(
        j["job_id"] != job.job_id for j in state.get("jobs", [])
    ), "占位任务的存储记录应被显式清理"


def test_schedule_config_validation_on_add(tmp_path):
    """P2: 缺必填字段的组合在注册期被拒(否则静默落入 +60s 周期执行)."""
    scheduler = _new_scheduler(tmp_path)
    script = tmp_path / "noop.py"
    script.write_text("pass\n", encoding="utf-8")

    with pytest.raises(ValueError, match="positive interval"):
        scheduler.add_script_job(
            script_path=str(script), schedule_type="interval",
            interval=None, func_name="bad_interval",
        )
    with pytest.raises(ValueError, match="positive interval"):
        scheduler.add_script_job(
            script_path=str(script), schedule_type="interval",
            interval=-5, func_name="bad_negative",
        )
    with pytest.raises(ValueError, match="cron_expression"):
        scheduler.add_script_job(
            script_path=str(script), schedule_type="cron", func_name="bad_cron",
        )
    with pytest.raises(ValueError, match="HH:MM"):
        scheduler.add_script_job(
            script_path=str(script), schedule_type="daily", func_name="bad_daily",
        )
    with pytest.raises(ValueError, match="out of range"):
        scheduler.add_script_job(
            script_path=str(script), schedule_type="daily",
            schedule_time="25:00", func_name="bad_hour",
        )
    with pytest.raises(ValueError, match="0-59"):
        scheduler.add_script_job(
            script_path=str(script), schedule_type="hourly",
            schedule_time=":99", func_name="bad_minute",
        )
    # 合法组合不受影响
    job_id = scheduler.add_script_job(
        script_path=str(script), schedule_type="interval",
        interval=60, func_name="ok_job",
    )
    assert job_id, "合法注册应成功"


def test_schedule_config_validation_on_update(tmp_path):
    """P3: 更新路径同样校验组合(仅改类型不带必填字段/非法 interval 被拒)."""
    scheduler = _new_scheduler(tmp_path)
    script = tmp_path / "noop.py"
    script.write_text("pass\n", encoding="utf-8")
    scheduler.add_script_job(
        script_path=str(script), schedule_type="interval",
        interval=60, func_name="updatable",
    )
    job_id = scheduler.jobs[0].job_id
    scheduler.pause_job(job_id)

    with pytest.raises(ValueError, match="positive interval"):
        scheduler.update_job(job_id, interval=-5)
    with pytest.raises(ValueError, match="HH:MM"):
        # 只改类型为 daily 却不带 schedule_time: 最终组合校验拒绝
        scheduler.update_job(job_id, schedule_type="daily")

    # 合法更新成功
    assert scheduler.update_job(job_id, interval=120) is True
    updated = scheduler.get_job(job_id)
    assert updated["interval"] == 120


def test_at_time_range_validation(tmp_path):
    """P7: at() 时间越界在注册期被拒, 不再等到 replace() 抛晦涩错误."""
    scheduler = _new_scheduler(tmp_path)
    with pytest.raises(ValueError, match="out of range"):
        scheduler.daily.at("24:00")
    with pytest.raises(ValueError, match="out of range"):
        scheduler.daily.at("09:60")
    with pytest.raises(ValueError, match="out of range"):
        scheduler.hourly.at(":99")
    with pytest.raises(ValueError, match="out of range"):
        scheduler.weekly.monday.at("99:99")
    # 合法时间不受影响
    assert scheduler.daily.at("23:59") is not None
    assert scheduler.hourly.at(":30") is not None


def test_get_jobs_includes_running(tmp_path):
    """P5: 执行中任务在 get_jobs/get_job 可见且状态为 running."""
    scheduler = _new_scheduler(tmp_path)
    release = threading.Event()

    @scheduler.every(3600).seconds
    def long_job():
        release.wait(timeout=10)

    scheduler.jobs[0].next_run = time.time()
    scheduler.flush_state()
    scheduler.start()
    try:
        deadline = time.time() + 5
        while time.time() < deadline and not scheduler._running_jobs:
            time.sleep(0.05)
        assert scheduler._running_jobs, "任务应进入执行中"

        jobs = {j["func_name"]: j for j in scheduler.get_jobs()}
        assert "long_job" in jobs, "执行中任务在 get_jobs 可见"
        assert jobs["long_job"]["status"] == "running"

        detail = scheduler.get_job(jobs["long_job"]["job_id"])
        assert detail is not None, "执行中任务在 get_job 可见"
        assert detail["status"] == "running"
    finally:
        release.set()
        scheduler.stop(wait=True, timeout=10)


def test_cancel_running_job_registers_removal(tmp_path):
    """P6: 执行中任务可被取消(登记待移除), 在途执行完成后不再入队."""
    scheduler = _new_scheduler(tmp_path)
    release = threading.Event()
    body_started = threading.Event()

    @scheduler.every(3600).retries(0).seconds
    def block_job():
        body_started.set()
        release.wait(timeout=10)

    scheduler.jobs[0].next_run = time.time()
    job_id = scheduler.jobs[0].job_id
    scheduler.flush_state()
    scheduler.start()
    try:
        assert body_started.wait(timeout=5), "任务应开始执行"
        assert scheduler.cancel_job(job_id) is True, "执行中任务可取消"
        assert job_id in scheduler._cancelled_during_run

        release.set()
        deadline = time.time() + 5
        while time.time() < deadline and job_id in scheduler._running_jobs:
            time.sleep(0.05)
        assert job_id not in scheduler._running_jobs, "执行登记已清理"
        assert all(
            j.job_id != job_id for j in scheduler.jobs
        ), "取消的执行中任务完成后不再入队"
    finally:
        release.set()
        scheduler.stop(wait=True, timeout=10)


# ==================== 第五轮审计: 恢复容错/执行中控制/查询上界 ====================


def test_load_state_resets_running_status(tmp_path):
    """Q1: 重启后持久化的 RUNNING 状态重置为 SCHEDULED(无幽灵 running)."""
    import json as json_lib

    state_file = tmp_path / "state.json"
    state_file.write_text(
        json_lib.dumps({
            "jobs": [
                _job_dict("job_ghost", status="running"),
                _job_dict("job_normal", status="scheduled"),
            ],
            "history": [],
            "statistics": {},
            "_job_counter": 2,
        }),
        encoding="utf-8",
    )
    restarted = Scheduler(
        state_file=str(state_file), storage="json", quiet=True, auto_start=False,
    )
    statuses = {j.job_id: j.status.value for j in restarted.jobs}
    assert statuses["job_ghost"] == "scheduled", "RUNNING 快照应重置为 SCHEDULED"
    assert statuses["job_normal"] == "scheduled"


def test_load_state_skips_corrupted_records(tmp_path):
    """Q2: 单条损坏记录只跳过自身, 其余任务正常恢复."""
    import json as json_lib

    state_file = tmp_path / "state.json"
    state_file.write_text(
        json_lib.dumps({
            "jobs": [
                {"job_id": "job_bad"},  # 缺 func_module/func_name 等必填键
                _job_dict("job_good", status="scheduled"),
                {"job_id": "job_bad_status", "func_name": "x",
                 "func_module": "__script__", "next_run": 1.0, "interval": 60.0,
                 "repeat": True, "status": "not-a-status", "created_at": 1.0},
            ],
            "history": [],
            "statistics": {},
            "_job_counter": 3,
        }),
        encoding="utf-8",
    )
    restarted = Scheduler(
        state_file=str(state_file), storage="json", quiet=True, auto_start=False,
    )
    restored_ids = {j.job_id for j in restarted.jobs}
    assert restored_ids == {"job_good"}, "损坏记录被跳过, 正常记录完整恢复"


def test_pause_resume_running_job(tmp_path):
    """Q3: 执行中任务可 pause/resume(在途不被中断), 与 cancel 语义对称."""
    scheduler = _new_scheduler(tmp_path)
    release = threading.Event()
    body_started = threading.Event()

    @scheduler.every(3600).seconds
    def long_job():
        body_started.set()
        release.wait(timeout=10)

    scheduler.jobs[0].next_run = time.time()
    job_id = scheduler.jobs[0].job_id
    scheduler.flush_state()
    scheduler.start()
    try:
        assert body_started.wait(timeout=5), "任务应开始执行"
        assert scheduler.pause_job(job_id) is True, "执行中任务可暂停"
        assert scheduler._running_job_objs[job_id].paused is True
        assert body_started.is_set(), "在途执行不被暂停中断"

        assert scheduler.resume_job(job_id) is True, "执行中任务可恢复"
        assert scheduler._running_job_objs[job_id].paused is False

        release.set()
        deadline = time.time() + 5
        while time.time() < deadline and job_id in scheduler._running_jobs:
            time.sleep(0.05)
        # 恢复状态下执行完成应重新入队
        assert any(j.job_id == job_id for j in scheduler.jobs), "恢复后重新入队"
    finally:
        release.set()
        scheduler.stop(wait=True, timeout=10)


def test_query_limits_clamped(tmp_path):
    """Q4: limit/page_size 外部传入超大值时被钳制到上界."""
    from Scheduler.models import JobStatus

    scheduler = _new_scheduler(tmp_path)
    for i in range(5):
        scheduler._log_history("job_x", f"task_{i}", JobStatus.COMPLETED)

    result = scheduler.get_history(limit=10**9)
    assert len(result) <= 1000, "get_history limit 钳制到 1000"

    dead = scheduler.get_dead_letters(limit=10**9)
    assert len(dead) <= 1000, "get_dead_letters limit 钳制到 1000"

    paged = scheduler.get_history_paged(page=0, page_size=10**9)
    assert paged["page"] == 1 and paged["page_size"] == 200, "page/page_size 钳制"


def test_corrupt_state_file_preserved(tmp_path):
    """Q5: 损坏的状态文件被保留为 .corrupt-* 现场, 不被空启动覆盖丢失."""
    state_file = tmp_path / "state.json"
    state_file.write_text("{ this is not valid json", encoding="utf-8")

    scheduler = Scheduler(
        state_file=str(state_file), storage="json", quiet=True, auto_start=False,
    )
    assert scheduler.jobs == [], "损坏文件按无状态处理"

    backups = list(tmp_path.glob("state.json.corrupt-*"))
    assert len(backups) == 1, "损坏现场应被保留一个备份"


# ==================== 第六轮审计: 实例复用与内联脚本生成 ====================


def test_scheduler_restart_reuses_instance(tmp_path):
    """R1: 同一实例 stop 后再 start, 线程池被重建, 任务恢复执行.

    修复前: stop 关闭的执行池使 submit 抛 RuntimeError 且被主循环吞掉,
    第二轮任务静默永不执行.
    """
    scheduler = _new_scheduler(tmp_path)
    done1 = threading.Event()
    done2 = threading.Event()

    @scheduler.every(3600).retries(0).seconds
    def probe_job():
        if not done1.is_set():
            done1.set()
        else:
            done2.set()

    scheduler.jobs[0].next_run = time.time()
    scheduler.start()
    try:
        assert done1.wait(timeout=5), "首轮执行应成功"
    finally:
        scheduler.stop(wait=True, timeout=5)

    assert scheduler._executors_stopped, "stop 应标记线程池已关闭"

    scheduler.jobs[0].next_run = time.time()
    scheduler.start()
    try:
        assert done2.wait(timeout=5), "重启后任务应恢复执行"
    finally:
        scheduler.stop(wait=True, timeout=5)


def test_inline_script_top_level_imports_only(tmp_path, monkeypatch):
    """R2: 内联脚本只提取顶层 import, 缩进 import 不再生成语法错误代码."""
    import importlib.util

    mod_file = tmp_path / "inline_guard_mod.py"
    mod_file.write_text(
        "import json\n"
        "try:\n"
        "    import optional_dep\n"
        "except ImportError:\n"
        "    optional_dep = None\n"
        "\n"
        "\n"
        "def guarded_task():\n"
        "    return json.dumps({'ok': True})\n",
        encoding="utf-8",
    )
    spec = importlib.util.spec_from_file_location("inline_guard_mod", mod_file)
    module = importlib.util.module_from_spec(spec)
    sys.modules["inline_guard_mod"] = module
    spec.loader.exec_module(module)

    script = Scheduler._extract_function_source(module.guarded_task)
    assert script is not None, "应能提取源码"
    compile(script, "<inline>", "exec")  # 语法非法时抛 SyntaxError 使测试失败
    assert "    import optional_dep" not in script, "缩进 import 不得被提取"
    assert "import json" in script, "顶层 import 正常提取"


# ==================== 第七轮审计: once 任务终结与 API 类型校验 ====================


def test_once_job_executes_exactly_once(tmp_path):
    """S1: once 任务恰好执行一次, 存储记录随之清除, 不被 sync 重建.

    修复前(P1 回归): save_state 不删除后, once 完成时 DB 记录残留,
    _sync_control_state 重建副本且 next_run 为已过旧值, 任务被无限
    重复执行(实测 4 秒 4 次).
    """
    scheduler = Scheduler(
        state_file=str(tmp_path / "once.json"),
        storage="json",
        quiet=True,
        auto_start=False,
    )
    runs = {"count": 0}

    @scheduler.once(0).retries(0).seconds
    def once_probe():
        runs["count"] += 1

    job_id = scheduler.jobs[0].job_id
    scheduler.flush_state()
    scheduler.start()
    try:
        # 覆盖执行 + 至少两个 sync 周期: 若存在重建循环, 计数会持续增长
        deadline = time.time() + 4
        while time.time() < deadline:
            if runs["count"]:
                time.sleep(2.5)  # 跨过两个 1s sync 周期
                break
            time.sleep(0.05)
        assert runs["count"] == 1, f"once 任务应恰好执行一次, 实际 {runs['count']} 次"
        assert all(j.job_id != job_id for j in scheduler.jobs), "完成后不入堆"

        import json as json_lib
        saved = json_lib.loads(
            scheduler._storage.state_file.read_text(encoding="utf-8")
        )
        assert all(
            j["job_id"] != job_id for j in saved.get("jobs", [])
        ), "once 终结后存储记录应被清除"
    finally:
        scheduler.stop(wait=True, timeout=5)


def test_option_type_validation(tmp_path):
    """S2: max_retries/timeout/script_args 的错误类型在写入前被拒.

    修复前: 字符串数字等类型写入 Job 后在执行期触发 TypeError,
    任务反复失败进入死信.
    """
    scheduler = _new_scheduler(tmp_path)
    script = tmp_path / "noop.py"
    script.write_text("pass\n", encoding="utf-8")

    with pytest.raises(ValueError, match="max_retries"):
        scheduler.add_script_job(
            script_path=str(script), schedule_type="interval",
            interval=60, max_retries="3", func_name="bad_retries",
        )
    with pytest.raises(ValueError, match="timeout"):
        scheduler.add_script_job(
            script_path=str(script), schedule_type="interval",
            interval=60, timeout="abc", func_name="bad_timeout",
        )
    with pytest.raises(ValueError, match="script_args"):
        scheduler.add_script_job(
            script_path=str(script), schedule_type="interval",
            interval=60, script_args="--flag", func_name="bad_args",
        )

    # 更新路径同样拦截, 且失败不产生半应用状态
    scheduler.add_script_job(
        script_path=str(script), schedule_type="interval",
        interval=60, func_name="type_check",
    )
    job_id = scheduler.jobs[0].job_id
    scheduler.pause_job(job_id)
    with pytest.raises(ValueError, match="timeout"):
        scheduler.update_job(job_id, timeout="30")
    updated = scheduler.get_job(job_id)
    assert updated["timeout"] is None, "校验失败不污染原值"

    # 合法类型不受影响
    assert scheduler.update_job(job_id, timeout=30, max_retries=5) is True


# ==================== S1 专项压力: once 任务跨 sync 周期死循环防护 ====================


def test_once_jobs_stress_across_sync_cycles(tmp_path):
    """S1 压力: 多 once 任务并发终结, 跨 8+ 个 sync 周期不出现重执行死循环.

    场景组合(全部并发交错, 任务到期时间逐个错峰 0.3s, 使各自的终结
    delete_job 与主循环每秒的 _sync_control_state 边界充分交错):
        - 6 个瞬时 once 任务: 立即执行后终结;
        - 1 个慢任务(执行 2s): 执行期间跨 2 个 sync 周期, 验证执行中
          保存(并入执行中对象)不会被 sync 误重建;
        - 1 个失败任务(retries=2): 1 次执行 + 2 次退避重试(2s/4s, 跨
          多个 sync 周期)耗尽进入死信后终结;
        - 1 个暂停任务手动触发(run_job_now): 执行后终结.

    断言(死循环的数学特征是计数持续增长, 收敛特征是零增长):
        1. 观察末段 3s(跨 3 个 sync 周期)总计数零增长;
        2. 每个成功任务执行 1 次(竞态理论上界 2 次, 不应更多);
        3. 失败任务恰好执行 3 次(1+2 重试), 死信 1 条;
        4. 终态内存堆与数据库均无 once 任务残留.
    """
    scheduler = Scheduler(
        storage="sqlmodel",
        database_url=f"sqlite:///{tmp_path / 'once_stress.db'}",
        quiet=True,
        auto_start=False,
    )
    counts: dict = {}
    lock = threading.Lock()

    def make_probe(name, body=None):
        def _probe():
            with lock:
                counts[name] = counts.get(name, 0) + 1
            if body is not None:
                body()
        _probe.__name__ = name
        return _probe

    # 6 个瞬时任务
    fast_probes = [make_probe(f"fast_{i}") for i in range(6)]
    # 慢任务: 执行 2s, 跨 2 个 sync 周期
    slow_probe = make_probe("slow", body=lambda: time.sleep(2.0))
    # 失败任务: 1 次执行 + 2 次重试
    def fail_body():
        raise RuntimeError("intended failure")
    fail_probe = make_probe("fail", body=fail_body)
    # 手动触发的暂停任务
    manual_probe = make_probe("manual")

    for i, probe in enumerate(fast_probes + [slow_probe]):
        scheduler.once(3600).seconds.do(probe)
    scheduler.once(3600).retries(2).seconds.do(fail_probe)
    scheduler.once(3600).seconds.do(manual_probe)

    now = time.time()
    all_jobs = list(scheduler.jobs)
    for i, job in enumerate(all_jobs):
        job.next_run = now + 0.3 * i  # 错峰, 制造终结与 sync 边界交错

    manual_job = next(j for j in all_jobs if j.func_name == "manual")
    scheduler.pause_job(manual_job.job_id)
    scheduler.flush_state()
    scheduler.start()

    try:
        # 手动触发暂停的 once 任务
        assert scheduler.run_job_now(manual_job.job_id) is True, "暂停 once 可手动触发"

        # 第一阶段采样: 等失败重试链(1 + 2 + 4 = 7s 退避)与慢任务全部走完
        deadline = time.time() + 11
        while time.time() < deadline:
            with lock:
                done = (
                    counts.get("slow", 0) >= 1
                    and counts.get("fail", 0) >= 3
                    and counts.get("manual", 0) >= 1
                    and all(counts.get(f"fast_{i}", 0) >= 1 for i in range(6))
                )
            if done:
                break
            time.sleep(0.2)
        with lock:
            sample1 = dict(counts)

        # 第二阶段: 再观察 3s(跨 3 个 sync 周期), 计数必须零增长
        time.sleep(3.0)
        with lock:
            sample2 = dict(counts)

        assert sample2 == sample1, (
            f"once 任务出现重执行(死循环特征): {sample1} -> {sample2}"
        )

        # 每个成功任务执行 1 次(允许 sync 竞态理论上的 2 次上界)
        for name in [f"fast_{i}" for i in range(6)] + ["slow", "manual"]:
            assert 1 <= counts.get(name, 0) <= 2, (
                f"任务 {name} 执行 {counts.get(name, 0)} 次, 超出收敛上界"
            )
        # 失败任务恰好 3 次(1 执行 + 2 重试)
        assert counts.get("fail", 0) == 3, (
            f"失败任务应执行 3 次(1+2 重试), 实际 {counts.get('fail', 0)} 次"
        )
        # 失败链路最终进入死信
        assert any(
            dl.func_name == "fail" and "Max retries" in (dl.error or "")
            for dl in scheduler.dead_letters
        ), "重试耗尽应留下死信记录"

        # 终态: 内存堆无 once 残留
        assert all(j.repeat for j in scheduler.jobs), "once 任务完成后不得残留堆内"

        # 终态: 数据库无 once 残留
        from Scheduler.storage.sqlmodel_backend import SQLModelStorageBackend

        backend = SQLModelStorageBackend(f"sqlite:///{tmp_path / 'once_stress.db'}")
        state = backend.load_state()
        once_ids = {j.job_id for j in all_jobs}
        residual = [
            j["job_id"] for j in (state or {}).get("jobs", []) if j["job_id"] in once_ids
        ]
        assert not residual, f"数据库残留 once 任务记录: {residual}"
        backend.close()
    finally:
        scheduler.stop(wait=True, timeout=10)


def test_once_fail_path_terminates_in_storage(tmp_path):
    """S1 补充: once 任务重试耗尽进死信后, 存储记录同步清除.

    与压力测试分离的单路径验证: 失败链路(重试期间任务反复回堆, 跨
    多个 sync 周期)最终以死信终结, 不残留可被重建的记录.
    """
    scheduler = _new_scheduler(tmp_path)
    runs = {"count": 0}

    @scheduler.once(0).retries(1).seconds
    def failing_once():
        runs["count"] += 1
        raise RuntimeError("boom")

    job_id = scheduler.jobs[0].job_id
    scheduler.flush_state()
    scheduler.start()
    try:
        # 等待死信事件(Max retries exceeded)落地
        event = _wait_job_event(
            scheduler, job_id,
            lambda e: e.status == "failed" and "Max retries" in (e.error or ""),
            timeout=10,
        )
        assert event is not None, "重试耗尽应产生死信失败事件"
        assert runs["count"] == 2, "1 次执行 + 1 次重试后终结"

        # 存储记录应被终结算子清除
        deadline = time.time() + 3
        cleared = False
        while time.time() < deadline:
            state = scheduler._storage.load_state()
            job_ids = [j["job_id"] for j in (state or {}).get("jobs", [])]
            if job_id not in job_ids:
                cleared = True
                break
            time.sleep(0.3)
        assert cleared, "死信终结后存储记录应清除"
    finally:
        scheduler.stop(wait=True, timeout=5)


# ==================== 第八轮审计: 跨进程暂停写-写覆盖窗口 ====================


def test_save_state_preserves_cross_process_pause(tmp_path):
    """T1: 落库前的暂停保留: api 写入的暂停不被 worker 旧快照保存覆盖.

    复现路径(加压实测约 4% 命中): worker 载荷快照(paused=False)收集后、
    落库前, api 进程写入暂停(True), worker 的保存以旧快照覆盖且此后
    无人写回, 暂停静默永久丢失. 修复: 存储层在锁内"读现有 True ->
    合并 -> 写入"(json: flock, sqlmodel: FOR UPDATE/单写者锁).
    """
    import json as json_lib

    state_file = tmp_path / "state.json"
    scheduler = Scheduler(
        state_file=str(state_file), storage="json", quiet=True, auto_start=False,
    )

    @scheduler.every(3600).seconds
    def preserved_task():
        return None

    job_id = scheduler.jobs[0].job_id
    scheduler.flush_state()

    def _file_paused() -> bool:
        saved = json_lib.loads(state_file.read_text(encoding="utf-8"))
        target = [j for j in saved["jobs"] if j["job_id"] == job_id]
        return bool(target and target[0].get("paused"))

    # api 进程视图暂停(写 True)
    api_view = Scheduler(
        state_file=str(state_file), storage="json", quiet=True, auto_start=False,
    )
    assert api_view.pause_job(job_id) is True
    assert _file_paused() is True

    # worker 侧内存仍是 False(未到 sync 周期), 此时的保存模拟
    # "旧快照"落库: 暂停保留必须把 True 合并回载荷
    scheduler._save_state()
    assert _file_paused() is True, "worker 保存不得覆盖 api 写入的暂停"

    # 多次保存同样保留
    scheduler._save_state()
    assert _file_paused() is True


def test_resume_intent_not_swallowed_by_preserve(tmp_path):
    """T1 豁免: 本进程显式 resume 的意图优先于存储旧值, False 必须落库."""
    import json as json_lib

    state_file = tmp_path / "state.json"
    scheduler = Scheduler(
        state_file=str(state_file), storage="json", quiet=True, auto_start=False,
    )

    @scheduler.every(3600).seconds
    def resumed_task():
        return None

    job_id = scheduler.jobs[0].job_id
    scheduler.flush_state()

    def _file_paused() -> bool:
        saved = json_lib.loads(state_file.read_text(encoding="utf-8"))
        target = [j for j in saved["jobs"] if j["job_id"] == job_id]
        return bool(target and target[0].get("paused"))

    # 先暂停落库 True, 再本地 resume: 豁免集保证 False 落库
    scheduler.pause_job(job_id)
    assert _file_paused() is True
    assert scheduler.resume_job(job_id) is True
    assert _file_paused() is False, "本地 resume 意图不得被存储旧 True 吞掉"

    # resume 后的后续保存(豁免已被首个保存一次性消费): 文件已是 False,
    # 无 True 可保留, 状态稳定
    scheduler._save_state()
    assert _file_paused() is False

    # U2: 豁免一次性消费 -- resume 落库后, 其他进程的二次暂停
    # 不得再被本进程旧载荷的"豁免"吞掉(修复前 10s 窗口内会被吞)
    api_view = Scheduler(
        state_file=str(state_file), storage="json", quiet=True, auto_start=False,
    )
    assert api_view.pause_job(job_id) is True, "api 视图二次暂停"
    assert _file_paused() is True
    # worker 内存仍为 False(模拟未到 sync 周期), 保存必须保留 api 的 True
    scheduler._save_state()
    assert _file_paused() is True, "二次暂停不得被豁免窗口吞掉"
