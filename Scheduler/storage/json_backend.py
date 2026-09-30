"""基于 JSON 文件的存储后端实现。

本模块提供了 JSONStorageBackend，使用 JSON 文件持久化调度器状态。
这是默认的存储后端，适合简单部署和开发环境，无需额外依赖数据库。

存储文件:
    - 状态文件 (scheduler_state.json): 存储 jobs、history、statistics
    - 死信文件 (scheduler_state_dead_letters.json): 存储死信队列

特点:
    - 无需数据库依赖
    - 原子写入（通过临时文件 + rename 实现，避免写入中断导致数据损坏）
    - 历史记录限制为 1000 条（避免文件过大）
    - 不适合高并发场景（多进程同时写入可能冲突）
"""

import json
import logging
import os
import tempfile
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from .base import StorageBackend

# 文件锁(类 Unix): 用于"读暂停标志->合并->写"的跨进程原子保护;
# 非 Unix 平台缺失时降级为无锁写入(单进程部署不受影响)
try:
    import fcntl
    _FLOCK_AVAILABLE = True
except ImportError:
    fcntl = None  # type: ignore
    _FLOCK_AVAILABLE = False

# 使用统一的 scheduler logger
logger = logging.getLogger("worker")


class JSONStorageBackend(StorageBackend):
    """基于 JSON 文件的存储后端。

    将调度器状态序列化为 JSON 格式存储到文件中。
    适合单进程部署或开发测试环境。

    Args:
        state_file: 主状态文件路径（默认 "scheduler_state.json"）
        quiet: 若为 True，抑制 info 级别日志

    文件结构:
        状态文件:
            {
                "version": "1.0",
                "metadata": {
                    "last_save": <timestamp>,
                    "last_save_readable": "YYYY-MM-DD HH:MM:SS",
                    "scheduler_running": <bool>
                },
                "jobs": [...],
                "history": [...],  # 最多 1000 条
                "statistics": {...},
                "_job_counter": <int>
            }

        死信文件:
            {
                "dead_letters": [...],
                "max_dead_letters": <int>
            }
    """

    def __init__(self, state_file: str = "scheduler_state.json", quiet: bool = False):
        self.state_file = Path(state_file)
        # 死信文件名通过替换后缀生成（xxx.json -> xxx_dead_letters.json）
        self._dead_letters_file = Path(
            str(self.state_file).replace(".json", "_dead_letters.json")
        )
        self.quiet = quiet

    def save_state(
        self,
        jobs: List[Dict[str, Any]],
        history: List[Dict[str, Any]],
        statistics: Dict[str, Any],
        job_counter: int,
        scheduler_running: bool,
        pause_preserve_exempt: Optional[set] = None,
    ) -> None:
        """保存状态到 JSON 文件。

        采用原子写入策略：先写入临时文件，再 rename 替换原文件，
        避免写入过程中崩溃导致数据损坏。

        暂停保留(跨进程写-写竞态防护): "读取现有文件中 paused=True 的
        任务 -> 合并进载荷(仅 True 方向) -> 写入"整个过程包在
        flock 排他锁内, 与其他进程(如 API 进程的 pause_job 保存)互斥,
        防止载荷快照收集后落库前, 其他进程写入的暂停被旧快照覆盖且
        此后无人写回. 豁免集内的 job_id 不参与保留(本进程显式恢复的
        任务, 恢复意图优先).

        Args:
            jobs: 任务字典列表
            history: 历史记录字典列表（只保留最近 1000 条）
            statistics: 统计信息
            job_counter: job ID 计数器
            scheduler_running: 调度器运行状态
            pause_preserve_exempt: 豁免暂停保留的 job_id 集合
        """
        try:
            exempt = pause_preserve_exempt or set()
            lock_path = self.state_file.with_name(self.state_file.name + ".lock")
            lock_file = open(lock_path, "w")
            try:
                if _FLOCK_AVAILABLE:
                    fcntl.flock(lock_file, fcntl.LOCK_EX)
                try:
                    # 锁内读取现有暂停标志并合并(仅 True 方向)
                    merged_jobs = list(jobs)
                    if self.state_file.exists():
                        try:
                            with open(self.state_file, "r", encoding="utf-8") as f:
                                old_state = json.load(f)
                            preserved = {
                                j.get("job_id")
                                for j in old_state.get("jobs", [])
                                if j.get("paused")
                            }
                            merged_jobs = [
                                (
                                    {**j, "paused": True}
                                    if (
                                        not j.get("paused")
                                        and j.get("job_id") in preserved
                                        and j.get("job_id") not in exempt
                                    )
                                    else j
                                )
                                for j in merged_jobs
                            ]
                        except (OSError, ValueError):
                            pass  # 现有文件缺失/损坏时按无保留处理

                    state = {
                        "version": "1.0",
                        # metadata 记录保存元信息，便于调试和状态同步
                        "metadata": {
                            "last_save": time.time(),
                            "last_save_readable": datetime.now().strftime(
                                "%Y-%m-%d %H:%M:%S"
                            ),
                            "scheduler_running": scheduler_running,
                        },
                        "jobs": merged_jobs,
                        # 限制历史记录数量，避免文件无限增长
                        "history": history[-1000:],
                        "statistics": statistics,
                        "_job_counter": job_counter,
                    }

                    # 原子写入：先写唯一临时文件，再 rename
                    # temp_file.replace(state_file) 在同一文件系统下是原子操作;
                    # 临时文件名必须唯一(进程+线程), 否则并发写入方共用同一临时
                    # 路径会互相截断, 把撕裂内容 replace 进最终文件.
                    fd, temp_path = tempfile.mkstemp(
                        prefix=f"{self.state_file.name}.", suffix=".tmp",
                        dir=str(self.state_file.parent),
                    )
                    try:
                        with os.fdopen(fd, "w", encoding="utf-8") as f:
                            json.dump(state, f, indent=2)
                        os.replace(temp_path, self.state_file)
                    except Exception:
                        try:
                            os.unlink(temp_path)
                        except OSError:
                            pass
                        raise
                finally:
                    if _FLOCK_AVAILABLE:
                        fcntl.flock(lock_file, fcntl.LOCK_UN)
            finally:
                lock_file.close()

        except Exception as e:
            logger.error(f"Failed to save state: {e}")

    def delete_job(self, job_id: str) -> None:
        """按 job_id 显式删除单条任务记录。

        JSON 后端本身是全量覆盖式存储(下一次 save_state 会以内存为准重写
        全部任务), 此方法提供不依赖内存状态的独立删除路径, 保证取消操作
        与后续保存之间的窗口内任务不会复活.

        "读取 -> 过滤 -> 写回"全程持 flock 排他锁: 与 save_state 的
        锁内"读暂停标志 -> 合并 -> 写入"互斥, 否则取消删除与并发保存
        交错会互相覆盖(删除被写回复活, 或保存的暂停标志被删除路径的
        旧快照吞掉).

        Args:
            job_id: 要删除的任务 ID
        """
        state = self.load_state()
        if not state:
            return
        jobs = [j for j in state.get("jobs", []) if j.get("job_id") != job_id]
        if len(jobs) == len(state.get("jobs", [])):
            return  # 未命中, 无需写回
        state["jobs"] = jobs
        try:
            lock_path = self.state_file.with_name(self.state_file.name + ".lock")
            lock_file = open(lock_path, "w")
            try:
                if _FLOCK_AVAILABLE:
                    fcntl.flock(lock_file, fcntl.LOCK_EX)
                try:
                    # 锁内重读: 取锁期间文件可能已被其他进程修改,
                    # 基于"取锁后"的最新内容过滤, 防止旧快照覆盖
                    fresh = self.load_state()
                    if fresh:
                        fresh["jobs"] = [
                            j
                            for j in fresh.get("jobs", [])
                            if j.get("job_id") != job_id
                        ]
                        state = fresh
                    fd, temp_path = tempfile.mkstemp(
                        prefix=f"{self.state_file.name}.", suffix=".tmp",
                        dir=str(self.state_file.parent),
                    )
                    try:
                        with os.fdopen(fd, "w", encoding="utf-8") as f:
                            json.dump(state, f, indent=2)
                        os.replace(temp_path, self.state_file)
                    except Exception:
                        try:
                            os.unlink(temp_path)
                        except OSError:
                            pass
                        raise
                finally:
                    if _FLOCK_AVAILABLE:
                        fcntl.flock(lock_file, fcntl.LOCK_UN)
            finally:
                lock_file.close()
        except Exception as e:
            logger.error(f"Failed to delete job {job_id} from state file: {e}")

    def load_state(self) -> Optional[Dict[str, Any]]:
        """从 JSON 文件加载状态。

        文件损坏(部分写/磁盘故障)时把原文件改名保留为 .corrupt-<时间戳>
        现场, 再按"无历史状态"处理: 若不保留现场, 空启动后的下一次保存
        会直接覆盖损坏文件, 原始数据彻底丢失且无从排查.

        Returns:
            状态字典，或 None（文件不存在或加载失败时）
        """
        if not self.state_file.exists():
            if not self.quiet:
                logger.info("No previous state found, starting fresh")
            return None

        try:
            with open(self.state_file, "r") as f:
                state = json.load(f)
            return state
        except Exception as e:
            logger.error(f"Failed to load state: {e}")
            backup = self.state_file.with_name(
                f"{self.state_file.name}.corrupt-{int(time.time())}"
            )
            try:
                os.replace(self.state_file, backup)
                logger.error(
                    f"Corrupted state file preserved as: {backup}"
                )
            except OSError:
                logger.error("Failed to preserve corrupted state file")
            return None

    def save_dead_letters(self, dead_letters: List[Dict[str, Any]], max_dead_letters: int) -> None:
        """保存死信队列到 JSON 文件。

        与状态文件一致采用"唯一临时文件 + rename"的原子写入, 避免并发
        读写时读到撕裂内容.

        Args:
            dead_letters: 死信记录列表
            max_dead_letters: 最大保留数量
        """
        try:
            data = {
                "dead_letters": dead_letters,
                "max_dead_letters": max_dead_letters,
            }

            fd, temp_path = tempfile.mkstemp(
                prefix=f"{self._dead_letters_file.name}.",
                suffix=".tmp",
                dir=str(self._dead_letters_file.parent),
            )
            try:
                with os.fdopen(fd, "w", encoding="utf-8") as f:
                    json.dump(data, f, indent=2)
                os.replace(temp_path, self._dead_letters_file)
            except Exception:
                try:
                    os.unlink(temp_path)
                except OSError:
                    pass
                raise

        except Exception as e:
            logger.error(f"Failed to save dead letters: {e}")

    def load_dead_letters(self) -> List[Dict[str, Any]]:
        """从 JSON 文件加载死信队列。

        Returns:
            死信记录列表，空列表表示无死信或加载失败
        """
        if not self._dead_letters_file.exists():
            return []

        try:
            with open(self._dead_letters_file, "r") as f:
                data = json.load(f)
            return data.get("dead_letters", [])
        except Exception as e:
            logger.error(f"Failed to load dead letters: {e}")
            return []

    def clear_dead_letters(self) -> int:
        """清空所有死信记录。

        Returns:
            被清除的记录数量
        """
        count = len(self.load_dead_letters())
        self.save_dead_letters([], 0)
        return count

    @property
    def dead_letters_file(self) -> Path:
        """获取死信文件路径。

        Returns:
            死信文件的 Path 对象
        """
        return self._dead_letters_file
