"""SQLModel 数据库存储后端 - 基于关系型数据库的持久化实现。

本模块实现了 SQLModelStorageBackend，使用 SQLModel（构建于 SQLAlchemy 之上）
作为 ORM 框架，支持所有 SQLAlchemy 兼容的数据库。

支持的数据库:
    - SQLite: 轻量级文件数据库，适合单机部署
        URL 格式: "sqlite:///path/to/scheduler.db"
        内存模式: "sqlite:///:memory:"（用于测试）
    - PostgreSQL: 企业级关系型数据库，适合生产环境
        URL 格式: "postgresql://user:password@host:port/database"
    - MySQL: 流行的开源关系型数据库
        URL 格式: "mysql://user:password@host:port/database"
    - 其他 SQLAlchemy 兼容数据库（需安装对应驱动）

与 JSON 后端的对比:
    - 事务支持: 数据库提供 ACID 事务，避免数据损坏
    - 并发性能: 数据库支持多进程并发读写，适合多进程部署
    - 查询能力: 支持 SQL 查询，可按条件筛选历史记录
    - 数据量: 数据库可处理大量历史记录，JSON 文件会随数据增长变慢
    - 部署复杂度: 需要维护数据库服务，JSON 仅需文件系统

数据库表结构:
    - scheduler_jobs: 存储任务定义（JobModel）
    - scheduler_history: 存储执行历史（JobHistoryModel）
    - scheduler_dead_letters: 存储死信队列（DeadLetterModel）
    - scheduler_metadata: 存储元数据（SchedulerMetadataModel，如 job_counter）

多进程模式:
    在多进程部署中，worker 进程负责写入状态，API 进程通过 load_state 读取。
    数据库的 ACID 特性确保跨进程数据一致性。
"""

import json
import logging
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

try:
    from sqlalchemy import func, or_
    from sqlmodel import Session, SQLModel, create_engine, select
except ImportError as e:
    raise ImportError(
        "SQLModel storage requires sqlmodel. "
        "Install with: pip install scheduler[database]"
    ) from e

from .base import StorageBackend
from .models import DeadLetterModel, JobHistoryModel, JobModel, SchedulerMetadataModel

logger = logging.getLogger("worker")


class SQLModelStorageBackend(StorageBackend):
    """SQLModel 数据库存储后端。

    基于 SQLModel/SQLAlchemy 实现的关系型数据库存储后端，
    支持 SQLite、PostgreSQL、MySQL 等主流数据库。

    特性:
        - 事务完整性: 所有写入操作在事务中执行，保证数据一致性
        - 并发安全: 数据库级别的锁机制，支持多进程并发访问
        - 自动建表: 初始化时自动创建表（若不存在）
        - 模式迁移: 自动为旧版本表添加缺失的列（如新增的 script_path/script_args）
        - 密码脱敏: 日志中自动屏蔽数据库 URL 中的密码

    线程安全说明:
        - engine 对象本身是线程安全的，可被多线程共享
        - Session 对象不是线程安全的，每次操作创建独立的 Session
        - SQLite 特殊处理: 设置 check_same_thread=False 允许跨线程使用

    Args:
        database_url: SQLAlchemy 数据库 URL
            - SQLite: "sqlite:///scheduler.db" 或 "sqlite:///:memory:"
            - PostgreSQL: "postgresql://user:pass@host:port/db"
            - MySQL: "mysql://user:pass@host:port/db"
        echo: 是否在日志中打印所有 SQL 语句（默认 False，调试时可设为 True）
        quiet: 是否抑制 info 级别日志（默认 False）

    Example:
        # SQLite（文件型，适合单机部署）
        backend = SQLModelStorageBackend("sqlite:///scheduler.db")

        # SQLite（内存型，用于单元测试）
        backend = SQLModelStorageBackend("sqlite:///:memory:")

        # PostgreSQL（适合生产环境）
        backend = SQLModelStorageBackend("postgresql://user:pass@localhost/mydb")

        # MySQL
        backend = SQLModelStorageBackend("mysql://user:pass@localhost/mydb")

        # 在 Scheduler 中使用
        scheduler = Scheduler(
            storage="sqlmodel",
            database_url="mysql://user:pass@localhost/scheduler"
        )
    """

    def __init__(
        self,
        database_url: str = "sqlite:///scheduler.db",
        echo: bool = False,
        quiet: bool = False,
    ):
        """初始化数据库存储后端。

        初始化流程:
            1. 保存配置（database_url、quiet）
            2. 创建 SQLAlchemy engine（根据数据库类型设置连接参数）
            3. 创建所有表（若不存在）
            4. 执行模式迁移（为旧表添加新列）
            5. 记录初始化日志（密码已脱敏）

        Args:
            database_url: SQLAlchemy 数据库 URL
            echo: 是否打印 SQL 日志（调试用）
            quiet: 是否抑制 info 日志
        """
        self.database_url = database_url
        self.quiet = quiet

        # 根据数据库类型设置连接参数
        connect_args = {}
        if database_url.startswith("sqlite"):
            # SQLite 默认只允许创建线程使用，多线程调度需要关闭此限制
            connect_args["check_same_thread"] = False

        # 创建 engine（engine 本身线程安全，可全局共享）
        self.engine = create_engine(
            database_url,
            echo=echo,
            connect_args=connect_args,
        )

        # 创建所有表（create_all 只创建不存在的表，不会修改已有表结构）
        SQLModel.metadata.create_all(self.engine)
        # 为已存在的表添加缺失的列（create_all 不会修改已有表结构）
        self._migrate_schema()

        if not quiet:
            logger.info(f"SQLModel storage initialized: {self._safe_url()}")

    def _migrate_schema(self):
        """执行数据库模式迁移。

        SQLModel.metadata.create_all() 只能创建新表，无法为已存在的表添加列。
        当数据模型新增字段时（如 script_path、script_args），旧数据库的表
        不会有这些列，需要通过 ALTER TABLE 手动添加。

        迁移策略:
            - 使用 ALTER TABLE ADD COLUMN 添加缺失的列
            - 若列已存在但类型不符（如旧库中 script_code 被建成 VARCHAR(255)，
              无法容纳任务内联源码导致 Data too long 报错），使用
              MODIFY COLUMN 修正列类型
            - 采用"尝试-捕获"模式，不依赖数据库元数据查询，兼容性好
              （MODIFY 为 MySQL 语法，SQLite 不支持该语法时会静默跳过）

        当前迁移项:
            - scheduler_jobs.script_path: VARCHAR(1024)，脚本任务路径
            - scheduler_jobs.script_args: TEXT，脚本参数（JSON 格式）

        扩展说明:
            未来新增字段时，在 migrations 列表中添加迁移项即可。
            每项格式: (表名, 列名, SQL 类型)
        """
        from sqlalchemy import text

        migrations = [
            ("scheduler_jobs", "script_path", "VARCHAR(1024)"),
            ("scheduler_jobs", "script_args", "TEXT"),
            ("scheduler_jobs", "script_code", "TEXT"),
            ("scheduler_jobs", "schedule_days", "TEXT"),
            ("scheduler_jobs", "args", "TEXT"),
            ("scheduler_jobs", "kwargs", "TEXT"),
        ]
        with self.engine.connect() as conn:
            for table, column, col_type in migrations:
                try:
                    conn.execute(
                        text(f"ALTER TABLE {table} ADD COLUMN {column} {col_type}")
                    )
                    conn.commit()
                    if not self.quiet:
                        logger.info(
                            f"Schema migration: added column {table}.{column}"
                        )
                except Exception:
                    # 列已存在：若类型不符则修正（如旧库 VARCHAR(255) -> TEXT）
                    conn.rollback()
                    try:
                        conn.execute(
                            text(
                                f"ALTER TABLE {table} MODIFY COLUMN {column} {col_type}"
                            )
                        )
                        conn.commit()
                        if not self.quiet:
                            logger.info(
                                f"Schema migration: modified column {table}.{column} -> {col_type}"
                            )
                    except Exception:
                        # SQLite 等不支持 MODIFY 的库，或类型已符合，静默跳过
                        conn.rollback()

    def _safe_url(self) -> str:
        """获取密码脱敏的数据库 URL（用于日志记录）。

        将 URL 中的密码替换为 ***，避免密码泄露到日志文件。
        仅处理 "proto://user:pass@host/db" 格式的 URL。

        Returns:
            脱敏后的 URL，如 "postgresql://user:***@localhost/mydb"
        """
        url = self.database_url
        if "@" in url and "://" in url:
            # 按 @ 分隔，前半部分是 "proto://user:pass"，后半部分是 "host/db"
            pre, post = url.split("@", 1)
            if ":" in pre:
                # 从右向左找最后一个冒号，分隔 user 和 password
                proto_user = pre.rsplit(":", 1)[0]
                return f"{proto_user}:***@{post}"
        return url

    def save_state(
        self,
        jobs: List[Dict[str, Any]],
        history: List[Dict[str, Any]],
        statistics: Dict[str, Any],
        job_counter: int,
        scheduler_running: bool,
        pause_preserve_exempt: Optional[set] = None,
    ) -> None:
        """保存调度器完整状态到数据库。

        在一个事务中完成以下操作:
            1. 同步任务列表(仅 upsert, 不删除):
               - 新增: 数据库中不存在的新任务
               - 更新: 已存在任务的最新状态
               删除不走此路径: 取消任务由 delete_job 显式落库.
               早期版本会在此删除"数据库中存在但载荷中没有的任务", 但多进程
               部署下 worker 的周期性保存与 API 进程写入存在写-写竞态,
               会把 API 刚新增、尚未被 worker 同步(1s 周期)的任务误删.
            2. 增量同步历史记录:
               - 通过 timestamp 去重，避免重复插入
               - 仅插入新记录，不更新已有记录
            3. 保存元数据:
               - job_counter: 任务 ID 计数器
               - statistics: 统计信息（JSON）
               - last_save: 最后保存时间 + scheduler_running 标志

        多进程同步说明:
            scheduler_running 标志用于多进程模式，API 进程通过此标志
            判断 worker 进程是否在运行，用于 Dashboard 显示。

        Args:
            jobs: 所有任务的字典列表
            history: 历史记录字典列表（最近 1000 条）
            statistics: 统计信息字典
            job_counter: 当前任务 ID 计数器值
            scheduler_running: 调度器是否在运行
        """
        try:
            with Session(self.engine) as session:
                # 批量加载现有任务, 避免逐任务 SELECT 的 N+1 查询;
                # 删除判定复用同一份加载结果, 不再全表二次扫描.
                # MySQL/PG 上加行锁(FOR UPDATE): 与其他进程(如 API 的
                # pause_job UPDATE)串行化, 保证"读暂停标志->合并->写"
                # 的原子性; SQLite 不支持该子句, 但其单写者锁天然互斥,
                # 事务内读到的是最新已提交值, 同样安全.
                stmt = select(JobModel)
                if self.engine.dialect.name != "sqlite":
                    stmt = stmt.with_for_update()
                existing_jobs = {
                    db_job.job_id: db_job
                    for db_job in session.exec(stmt).all()
                }
                # 暂停保留: 存储中现有 paused=True 的任务, 若载荷快照为
                # False 且不在豁免集内, 合并回 True, 防止载荷收集与落库
                # 之间其他进程写入的暂停被旧快照覆盖且无人写回.
                exempt = pause_preserve_exempt or set()
                preserved_paused = {
                    jid
                    for jid, db_job in existing_jobs.items()
                    if db_job.paused
                }
                for job_dict in jobs:
                    if (
                        not job_dict.get("paused")
                        and job_dict["job_id"] in preserved_paused
                        and job_dict["job_id"] not in exempt
                    ):
                        job_dict = {**job_dict, "paused": True}
                for job_dict in jobs:
                    db_job = existing_jobs.get(job_dict["job_id"])

                    if db_job:
                        for key, value in job_dict.items():
                            if key == "schedule_days" and value is not None:
                                value = json.dumps(value)
                            if key == "script_args" and value is not None:
                                value = json.dumps(value)
                            if (
                                key in ("args", "kwargs")
                                and value is not None
                                and not isinstance(value, str)
                            ):
                                value = json.dumps(value)
                            if key == "status" and hasattr(value, "value"):
                                value = value.value
                            if hasattr(db_job, key):
                                setattr(db_job, key, value)
                        session.add(db_job)
                    else:
                        session.add(JobModel.from_dict(job_dict))

                # Delete jobs that no longer exist
                # 已移除: 见方法 docstring. 全表删除会在多进程写-写竞态中
                # 吞掉 API 进程新增的任务, 取消统一走 delete_job 显式删除.

                # Sync history - add new entries that aren't in the database
                # 去重查询限定在本次载荷的最老时间戳之后(走 timestamp 索引),
                # 避免每次保存全表扫描已有历史.
                if history:
                    min_payload_ts = min(
                        h.get("timestamp", 0) for h in history
                    )
                    existing_timestamps = set(
                        row
                        for row in session.exec(
                            select(JobHistoryModel.timestamp).where(
                                JobHistoryModel.timestamp >= min_payload_ts
                            )
                        ).all()
                    )

                    for hist_dict in history:
                        if hist_dict.get("timestamp") not in existing_timestamps:
                            # Remove timestamp_readable if present (not a model field)
                            clean_dict = {
                                k: v
                                for k, v in hist_dict.items()
                                if k != "timestamp_readable"
                            }
                            hist_model = JobHistoryModel.from_dict(clean_dict)
                            session.add(hist_model)

                # Save metadata
                self._save_metadata(
                    session,
                    "job_counter",
                    str(job_counter),
                )
                self._save_metadata(
                    session,
                    "statistics",
                    json.dumps(statistics),
                )
                self._save_metadata(
                    session,
                    "last_save",
                    json.dumps(
                        {
                            "timestamp": time.time(),
                            "readable": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            "scheduler_running": scheduler_running,
                        }
                    ),
                )

                session.commit()

        except Exception as e:
            logger.error(f"Failed to save state to database: {e}")

    def _save_metadata(self, session: Session, key: str, value: str) -> None:
        """保存元数据键值对（在事务中调用）。

        元数据存储在 scheduler_metadata 表中，采用 key-value 结构。
        若 key 已存在则更新，否则插入新记录。

        存储的元数据包括:
            - job_counter: 任务 ID 计数器（字符串形式）
            - statistics: 统计信息（JSON 字符串）
            - last_save: 最后保存信息（JSON，含时间戳和 running 标志）

        Args:
            session: 当前数据库会话（不在此方法内 commit）
            key: 元数据键
            value: 元数据值（字符串）
        """
        statement = select(SchedulerMetadataModel).where(
            SchedulerMetadataModel.key == key
        )
        existing = session.exec(statement).first()

        if existing:
            existing.value = value
            session.add(existing)
        else:
            session.add(SchedulerMetadataModel(key=key, value=value))

    def _get_metadata(self, session: Session, key: str, default: str = "") -> str:
        """获取元数据值（在事务中调用）。

        Args:
            session: 当前数据库会话
            key: 元数据键
            default: 键不存在时返回的默认值（默认空字符串）

        Returns:
            元数据值字符串，若键不存在返回 default
        """
        statement = select(SchedulerMetadataModel).where(
            SchedulerMetadataModel.key == key
        )
        result = session.exec(statement).first()
        return result.value if result else default

    def delete_job(self, job_id: str) -> None:
        """按 job_id 显式删除单条任务记录（取消任务的落库路径）。

        save_state 只做 upsert 不做删除（避免多进程写-写竞态吞掉
        其他进程新增的任务），取消任务必须通过此方法显式删除。

        Args:
            job_id: 要删除的任务 ID
        """
        try:
            with Session(self.engine) as session:
                statement = select(JobModel).where(JobModel.job_id == job_id)
                for entry in session.exec(statement).all():
                    session.delete(entry)
                session.commit()
        except Exception as e:
            logger.error(f"Failed to delete job {job_id} from database: {e}")

    def load_state(self) -> Optional[Dict[str, Any]]:
        """从数据库加载调度器完整状态。

        加载流程:
            1. 查询所有任务（scheduler_jobs 表）
            2. 查询最近 1000 条历史记录（按时间倒序，再反转恢复正序）
            3. 读取元数据:
               - job_counter: 任务 ID 计数器
               - statistics: 统计信息（JSON 解码）
               - last_save: 含 scheduler_running 标志

        返回 None 的情况:
            - 数据库中没有任务且没有历史记录（首次启动）
            - 加载过程中发生异常

        多进程模式:
            scheduler_running 标志从 last_save 元数据中读取，
            API 进程据此判断 worker 进程是否在运行。

        Returns:
            状态字典，包含:
                - jobs: 任务字典列表
                - history: 历史记录字典列表（按时间正序）
                - statistics: 统计信息字典
                - _job_counter: 任务 ID 计数器
                - scheduler_running: 调度器运行状态
            若无历史状态返回 None
        """
        try:
            with Session(self.engine) as session:
                # Load jobs
                statement = select(JobModel)
                jobs = session.exec(statement).all()
                job_dicts = [job.to_dict() for job in jobs]

                # Load history (limited)
                statement = (
                    select(JobHistoryModel)
                    .order_by(JobHistoryModel.timestamp.desc())
                    .limit(1000)
                )
                history = session.exec(statement).all()
                history_dicts = [h.to_dict() for h in reversed(history)]

                # Load metadata
                job_counter = int(self._get_metadata(session, "job_counter", "0"))
                stats_json = self._get_metadata(session, "statistics", "{}")
                statistics = json.loads(stats_json)

                # 读取 last_save 中的 scheduler_running 标志（多进程只读模式需要）
                last_save_json = self._get_metadata(session, "last_save", "{}")
                last_save = json.loads(last_save_json) if last_save_json else {}
                scheduler_running = last_save.get("scheduler_running", False)

                if not job_dicts and not history_dicts:
                    if not self.quiet:
                        logger.info("No previous state found, starting fresh")
                    return None

                return {
                    "jobs": job_dicts,
                    "history": history_dicts,
                    "statistics": statistics,
                    "_job_counter": job_counter,
                    "scheduler_running": scheduler_running,
                }

        except Exception as e:
            logger.error(f"Failed to load state from database: {e}")
            return None

    def save_history_entry(self, entry: Dict[str, Any]) -> None:
        """保存单条历史记录（用于增量更新）。

        与 save_state 中批量同步历史不同，此方法用于实时插入单条记录。
        适用于任务执行后立即记录历史，避免等待批量保存。

        Args:
            entry: 历史记录字典，包含:
                - job_id, func_name, status, timestamp
                - error, run_count, retry_count, execution_time
        """
        try:
            with Session(self.engine) as session:
                history_model = JobHistoryModel.from_dict(entry)
                session.add(history_model)
                session.commit()
        except Exception as e:
            logger.error(f"Failed to save history entry: {e}")

    def cleanup_history(self, max_history: int, retention_days: int) -> None:
        """清理过期的历史记录。

        按两个维度清理 scheduler_history 表:
            1. 时间维度: 删除超过 retention_days 天的记录
            2. 数量维度: 超过 max_history 条时，删除最旧的记录

        清理策略:
            - 先按时间删除过期记录
            - 再统计剩余记录数，若仍超限则按时间排序删除最旧的

        Args:
            max_history: 最大保留记录数
            retention_days: 保留天数（0 表示不按时间清理）
        """
        try:
            with Session(self.engine) as session:
                # Remove entries older than retention period
                if retention_days > 0:
                    cutoff_time = time.time() - (retention_days * 86400)
                    statement = select(JobHistoryModel).where(
                        JobHistoryModel.timestamp < cutoff_time
                    )
                    old_entries = session.exec(statement).all()
                    for entry in old_entries:
                        session.delete(entry)

                # Count remaining entries
                statement = select(JobHistoryModel)
                all_entries = session.exec(statement).all()

                # Remove oldest if over max
                if len(all_entries) > max_history:
                    # Sort by timestamp and delete oldest
                    sorted_entries = sorted(all_entries, key=lambda x: x.timestamp)
                    to_delete = sorted_entries[: len(all_entries) - max_history]
                    for entry in to_delete:
                        session.delete(entry)

                session.commit()

        except Exception as e:
            logger.error(f"Failed to cleanup history: {e}")

    def query_history_paged(
        self,
        func_name: Optional[str] = None,
        status: Optional[str] = None,
        keyword: Optional[str] = None,
        page: int = 1,
        page_size: int = 20,
    ) -> Dict[str, Any]:
        """直接从数据库分页查询历史记录（不依赖内存缓存）。

        支持按状态、任务名、关键词筛选，按时间倒序分页返回。
        状态过滤限定在 completed/failed/missed（与前端历史视图一致）。

        Args:
            func_name: 可选，按函数名精确过滤（值为 "all" 或 None 时不过滤）
            status:    可选，按状态过滤（值为 "all" 或 None 时过滤为终态集合）
            keyword:   可选，按函数名或错误信息模糊匹配（大小写不敏感）
            page:      页码（从 1 开始）
            page_size: 每页条数

        Returns:
            {"items": [...], "total": int, "page": int, "page_size": int}
        """
        try:
            with Session(self.engine) as session:
                # 构建基础查询条件
                conditions = []
                # 历史视图只展示终态记录
                if status and status != "all":
                    conditions.append(JobHistoryModel.status == status)
                else:
                    conditions.append(
                        JobHistoryModel.status.in_(("completed", "failed", "missed"))
                    )
                if func_name and func_name != "all":
                    conditions.append(JobHistoryModel.func_name == func_name)
                if keyword:
                    kw = f"%{keyword}%"
                    conditions.append(
                        or_(
                            JobHistoryModel.func_name.like(kw),
                            JobHistoryModel.error.like(kw),
                        )
                    )

                # 统计总数
                count_stmt = select(func.count(JobHistoryModel.id))
                for cond in conditions:
                    count_stmt = count_stmt.where(cond)
                total = session.exec(count_stmt).one()

                # 分页查询（时间倒序）
                query_stmt = select(JobHistoryModel)
                for cond in conditions:
                    query_stmt = query_stmt.where(cond)
                query_stmt = (
                    query_stmt.order_by(JobHistoryModel.timestamp.desc())
                    .offset((page - 1) * page_size)
                    .limit(page_size)
                )
                rows = session.exec(query_stmt).all()
                items = [r.to_dict() for r in rows]

                return {
                    "items": items,
                    "total": total,
                    "page": page,
                    "page_size": page_size,
                }
        except Exception as e:
            logger.error(f"Failed to query history paged: {e}")
            return {"items": [], "total": 0, "page": page, "page_size": page_size}

    def query_dead_letters_paged(
        self,
        keyword: Optional[str] = None,
        page: int = 1,
        page_size: int = 20,
    ) -> Dict[str, Any]:
        """直接从数据库分页查询死信队列记录。

        Args:
            keyword:   可选，按函数名或错误信息模糊匹配（大小写不敏感）
            page:      页码（从 1 开始）
            page_size: 每页条数

        Returns:
            {"items": [...], "total": int, "page": int, "page_size": int}
        """
        try:
            with Session(self.engine) as session:
                conditions = []
                if keyword:
                    kw = f"%{keyword}%"
                    conditions.append(
                        or_(
                            DeadLetterModel.func_name.like(kw),
                            DeadLetterModel.error.like(kw),
                        )
                    )

                # 统计总数
                count_stmt = select(func.count(DeadLetterModel.id))
                for cond in conditions:
                    count_stmt = count_stmt.where(cond)
                total = session.exec(count_stmt).one()

                # 分页查询（时间倒序）
                query_stmt = select(DeadLetterModel)
                for cond in conditions:
                    query_stmt = query_stmt.where(cond)
                query_stmt = (
                    query_stmt.order_by(DeadLetterModel.timestamp.desc())
                    .offset((page - 1) * page_size)
                    .limit(page_size)
                )
                rows = session.exec(query_stmt).all()
                items = [r.to_dict() for r in rows]

                return {
                    "items": items,
                    "total": total,
                    "page": page,
                    "page_size": page_size,
                }
        except Exception as e:
            logger.error(f"Failed to query dead letters paged: {e}")
            return {"items": [], "total": 0, "page": page, "page_size": page_size}

    def query_stats(self) -> Optional[Dict[str, Any]]:
        """从数据库历史记录表实时统计执行次数和失败次数。

        与内存计数器（self.stats）不同，此方法直接查询 scheduler_history 表，
        返回数据库层面的准确统计值，避免多进程模式下内存计数器不同步的问题。

        Returns:
            {"total_runs": int, "total_failures": int, "per_job": {...}}
            查询失败时返回 None（调用方应 fallback 到内存统计）。
        """
        try:
            with Session(self.engine) as session:
                # 全局统计：total_runs = completed + failed + missed
                #           total_failures = failed
                count_stmt = select(
                    JobHistoryModel.status,
                    func.count(JobHistoryModel.id),
                ).group_by(JobHistoryModel.status)
                rows = session.exec(count_stmt).all()

                status_counts = {status: cnt for status, cnt in rows}
                total_runs = sum(
                    status_counts.get(s, 0)
                    for s in ("completed", "failed", "missed")
                )
                total_failures = status_counts.get("failed", 0)

                # 每个任务的统计
                per_job_stmt = select(
                    JobHistoryModel.func_name,
                    JobHistoryModel.status,
                    func.count(JobHistoryModel.id),
                ).group_by(
                    JobHistoryModel.func_name,
                    JobHistoryModel.status,
                )
                per_job_rows = session.exec(per_job_stmt).all()

                per_job: Dict[str, Dict[str, int]] = {}
                for func_name, status, cnt in per_job_rows:
                    if func_name not in per_job:
                        per_job[func_name] = {
                            "completed": 0,
                            "failed": 0,
                            "total_runs": 0,
                        }
                    if status in ("completed", "failed"):
                        per_job[func_name][status] = cnt
                        per_job[func_name]["total_runs"] += cnt

                return {
                    "total_runs": total_runs,
                    "total_failures": total_failures,
                    "per_job": per_job,
                }
        except Exception as e:
            logger.error(f"Failed to query stats from database: {e}")
            return None

    def save_dead_letters(
        self, dead_letters: List[Dict[str, Any]], max_dead_letters: int
    ) -> None:
        """保存死信队列到数据库（全量替换）。

        清空 scheduler_dead_letters 表后，重新插入所有死信记录。
        采用"全量替换"策略确保内存与数据库一致。

        Args:
            dead_letters: 死信记录字典列表
            max_dead_letters: 最大保留数量（超过时只保留最新的）
        """
        try:
            with Session(self.engine) as session:
                # Clear existing and add new ones
                statement = select(DeadLetterModel)
                existing = session.exec(statement).all()
                for entry in existing:
                    session.delete(entry)

                # Add new entries (respect limit)
                for dl_dict in dead_letters[-max_dead_letters:]:
                    dl_model = DeadLetterModel.from_dict(dl_dict)
                    session.add(dl_model)

                session.commit()

        except Exception as e:
            logger.error(f"Failed to save dead letters: {e}")

    def add_dead_letter(self, entry: Dict[str, Any], max_dead_letters: int) -> None:
        """添加单条死信记录（增量方式）。

        与 save_dead_letters 的全量替换不同，此方法仅插入一条新记录，
        并在超过上限时删除最旧的记录。适用于任务失败后实时记录。

        Args:
            entry: 死信记录字典
            max_dead_letters: 最大保留数量（超过时删除最旧的）
        """
        try:
            with Session(self.engine) as session:
                # Add new entry
                dl_model = DeadLetterModel.from_dict(entry)
                session.add(dl_model)

                # Enforce limit
                statement = select(DeadLetterModel).order_by(DeadLetterModel.timestamp)
                all_entries = session.exec(statement).all()

                if len(all_entries) > max_dead_letters:
                    # Delete oldest entries
                    to_delete = all_entries[: len(all_entries) - max_dead_letters + 1]
                    for entry in to_delete:
                        session.delete(entry)

                session.commit()

        except Exception as e:
            logger.error(f"Failed to add dead letter: {e}")

    def load_dead_letters(self) -> List[Dict[str, Any]]:
        """从数据库加载死信队列。

        按时间正序返回所有死信记录（最旧的在前）。

        Returns:
            死信记录字典列表，若加载失败返回空列表
        """
        try:
            with Session(self.engine) as session:
                statement = select(DeadLetterModel).order_by(DeadLetterModel.timestamp)
                dead_letters = session.exec(statement).all()
                return [dl.to_dict() for dl in dead_letters]

        except Exception as e:
            logger.error(f"Failed to load dead letters: {e}")
            return []

    def clear_dead_letters(self) -> int:
        """清空所有死信记录。

        Returns:
            清除的记录数量（失败时返回 0）
        """
        try:
            with Session(self.engine) as session:
                statement = select(DeadLetterModel)
                entries = session.exec(statement).all()
                count = len(entries)

                for entry in entries:
                    session.delete(entry)

                session.commit()
                return count

        except Exception as e:
            logger.error(f"Failed to clear dead letters: {e}")
            return 0

    def close(self) -> None:
        """关闭数据库引擎，释放资源。

        调用 engine.dispose() 释放所有数据库连接池中的连接。
        应在调度器停止时调用，避免连接泄漏。

        注意:
            关闭后若再次使用会自动重建连接池，但建议在 stop() 时调用。
        """
        try:
            self.engine.dispose()
        except Exception as e:
            logger.error(f"Error disposing database engine: {e}")
