"""示例：数据库公共方法使用演示。

演示三种典型场景:
    1. 单库查询/写入
    2. 从项目配置读取连接串
    3. 多库操作（一次性连接多个库，自行处理跨库逻辑）

运行方式:
    python SchedulerTask/db_demo.py
"""
import sys
from pathlib import Path

# 脚本任务以子进程运行，需手动把项目根目录加入 sys.path
# 动态向上查找包含 SchedulerTask 子目录的祖先目录，避免文件位置变化导致路径错误
_PROJECT_ROOT = Path(__file__).resolve().parent
while _PROJECT_ROOT != _PROJECT_ROOT.parent:
    if (_PROJECT_ROOT / "SchedulerTask").is_dir() and (
        _PROJECT_ROOT / "Config"
    ).is_dir():
        break
    _PROJECT_ROOT = _PROJECT_ROOT.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from SchedulerTask.Utils.db_utils import connect, multi_connect


def demo_single_db():
    """场景 1：单库查询与写入。"""
    # 显式指定连接串
    from Config import get_database_url
    url = get_database_url("business")
    with connect(url) as db:
        # 查询
        rows = db.query("SELECT 1 AS val")
        print(f"[单库] 查询结果: {rows}")

        # 标量查询
        cnt = db.query_scalar("SELECT COUNT(*) FROM example_user_logs")
        print(f"[单库] example_user_logs 行数: {cnt}")


def demo_from_config():
    """场景 2：从项目配置读取连接串（不传 url）。"""
    with connect(config_name="scheduler") as db:
        cnt = db.query_scalar("SELECT COUNT(*) FROM scheduler_jobs")
        print(f"[配置] scheduler_jobs 行数: {cnt}")


def demo_multi_db():
    """场景 3：多库操作。

    一次性连接源库和目标库，业务逻辑自行处理：
    从 test 读取 example_user_logs → 给每行加 archived 标记 → 写入归档表。
    """
    from Config import get_database_url
    source_url = get_database_url("business")
    dest_url = get_database_url("business")

    # 方式 1：按位置返回（适合两个库）
    with multi_connect(source_url, dest_url) as (src, dst):
        # 确保目标表存在
        dst.execute(
            "CREATE TABLE IF NOT EXISTS example_user_logs_archive ("
            "  id INT, user_name VARCHAR(64), action VARCHAR(100), archived TINYINT DEFAULT 1"
            ")"
        )

        # 自行处理：读取 → 加标记 → 写入
        rows = src.query(
            "SELECT id, user_name, action FROM example_user_logs LIMIT 100"
        )
        archived_rows = [{**row, "archived": 1} for row in rows]
        written = dst.insert_batch("example_user_logs_archive", archived_rows)
        print(f"[多库] 读取 {len(rows)} 行, 写入 {written} 行")

    # 方式 2：按名字返回（适合多个库，避免顺序混淆）
    with multi_connect(
        ("src", source_url),
        ("dst", dest_url),
    ) as dbs:
        rows = dbs["src"].query("SELECT COUNT(*) AS n FROM example_user_logs")
        print(f"[多库-命名] src 表行数: {rows[0]['n']}")


if __name__ == "__main__":
    print("=== 场景 1: 单库操作 ===")
    demo_single_db()
    print("\n=== 场景 2: 从配置读取 ===")
    demo_from_config()
    print("\n=== 场景 3: 多库操作 ===")
    demo_multi_db()
