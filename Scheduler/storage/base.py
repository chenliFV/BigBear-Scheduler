"""存储后端抽象基类模块。

本模块定义了所有存储后端必须实现的接口规范。
StorageBackend 是抽象基类（ABC），具体的存储实现（JSONStorageBackend、
SQLModelStorageBackend）必须继承并实现所有抽象方法。

这种设计遵循依赖倒置原则，使 Scheduler 主类可以与任何符合接口的
存储后端协作，而无需关心具体的存储实现细节。
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional


class StorageBackend(ABC):
    """存储后端抽象基类。

    定义了调度器状态持久化的标准接口。所有存储后端必须实现以下功能：
        1. 状态保存/加载（jobs、history、statistics、job_counter）
        2. 死信队列管理（save/load/clear）

    实现示例:
        - JSONStorageBackend: 使用 JSON 文件存储
        - SQLModelStorageBackend: 使用关系型数据库存储

    多进程模式说明:
        在多进程部署（API 进程 + scheduler worker 进程）下，
        两个进程共享同一个存储后端（如同一个数据库），
        通过存储后端实现状态同步：
            - API 进程: 只读模式，通过 refresh_state 读取最新状态
            - Worker 进程: 读写模式，执行任务并写入状态
    """

    @abstractmethod
    def save_state(
        self,
        jobs: List[Dict[str, Any]],
        history: List[Dict[str, Any]],
        statistics: Dict[str, Any],
        job_counter: int,
        scheduler_running: bool,
        pause_preserve_exempt: Optional[set] = None,
    ) -> None:
        """保存调度器完整状态.

        暂停保留契约(多进程写-写竞态防护): 实现方应在文件锁/行锁的
        原子保护下, 先读取存储中现有 paused=True 的任务, 把这些标志
        合并进本次载荷(仅 True 方向), 再落库. 否则载荷快照收集与落库
        之间的窗口内, 其他进程写入的暂停会被旧快照覆盖且无人写回.
        pause_preserve_exempt 中的 job_id 不参与保留(调用方本进程近期
        显式 resume 的任务, 恢复意图优先).

        Args:
            jobs: 所有任务的字典列表（Job.to_dict() 的结果）
            history: 历史记录字典列表（JobHistory.to_dict() 的结果）
            statistics: 统计信息字典（total_runs、total_failures 等）
            job_counter: 当前 job ID 计数器值
            scheduler_running: 调度器是否正在运行（多进程状态同步用）
            pause_preserve_exempt: 豁免暂停保留的 job_id 集合
        """

    @abstractmethod
    def load_state(self) -> Optional[Dict[str, Any]]:
        """加载调度器完整状态。

        Returns:
            包含 jobs/history/statistics/_job_counter/scheduler_running 的字典，
            或 None（无历史状态时）

        返回字典结构:
            {
                "jobs": [...],
                "history": [...],
                "statistics": {...},
                "_job_counter": int,
                "scheduler_running": bool
            }
        """

    @abstractmethod
    def save_dead_letters(
        self, dead_letters: List[Dict[str, Any]], max_dead_letters: int
    ) -> None:
        """保存死信队列（失败任务的记录）。

        Args:
            dead_letters: 死信记录字典列表
            max_dead_letters: 最大保留数量（超出时应丢弃旧记录）
        """

    @abstractmethod
    def load_dead_letters(self) -> List[Dict[str, Any]]:
        """加载死信队列。

        Returns:
            死信记录字典列表，空列表表示无死信
        """

    @abstractmethod
    def clear_dead_letters(self) -> int:
        """清空所有死信记录。

        Returns:
            被清除的记录数量
        """

    def delete_job(self, job_id: str) -> None:
        """按 job_id 显式删除单条任务记录。

        取消任务必须走此显式删除接口落库, save_state 只做 upsert 不做删除:
        多进程模式下若 save_state 负责"删除内存中不存在的任务", worker 进程
        的周期性保存会在其控制状态同步(1s 周期)之前, 把 API 进程刚写入的
        新任务误删(跨进程写-写竞态导致任务静默丢失).

        默认实现为空操作并记录警告, 自定义后端应重写以支持取消落库.

        Args:
            job_id: 要删除的任务 ID
        """
        import logging

        logging.getLogger("worker").warning(
            f"Storage backend {type(self).__name__} does not implement "
            f"delete_job; job {job_id} removal relies on full-state overwrite"
        )

    def close(self) -> None:
        """清理资源。

        子类可重写此方法以释放数据库连接、文件句柄等资源。
        默认实现为空操作。
        """
