"""脚本文件任务示例二：统计目录磁盘占用并输出 Top-N 子目录。

演示要点：
    - 通过 argparse 接收注册时传入的 script_args
    - 业务失败时向 stderr 输出原因并返回非 0 退出码（触发调度器重试）
    - 需要引用项目内模块（如 db_utils）时，按 SchedulerTask/README.md
      在文件开头做 sys.path 引导即可，本示例未用到

可独立运行验证:
    python SchedulerTask/tasks/dir_size_report_task.py --path Log --top 5
"""
import argparse
import sys
from pathlib import Path


def dir_size(path: Path) -> int:
    """递归统计目录总字节数（忽略不可访问项）"""
    total = 0
    for p in path.rglob("*"):
        try:
            if p.is_file():
                total += p.stat().st_size
        except OSError:
            continue
    return total


def main() -> int:
    parser = argparse.ArgumentParser(description="统计目录磁盘占用 Top-N")
    parser.add_argument("--path", default=".", help="要统计的目录")
    parser.add_argument("--top", type=int, default=10, help="输出的子目录数量")
    args = parser.parse_args()

    root = Path(args.path)
    if not root.is_dir():
        print(f"目录不存在: {root}", file=sys.stderr)
        return 1

    children = [c for c in root.iterdir() if c.is_dir()]
    stats = sorted(
        ((dir_size(c), c.name) for c in children), reverse=True
    )
    total = sum(size for size, _ in stats)

    unit = 1024 * 1024
    print(f"目录 {root} 子目录占用 Top-{min(args.top, len(stats))}:")
    for size, name in stats[: args.top]:
        print(f"  {size / unit:>10.2f} MB  {name}")
    print(f"合计: {total / unit:.2f} MB")
    return 0


if __name__ == "__main__":
    sys.exit(main())
