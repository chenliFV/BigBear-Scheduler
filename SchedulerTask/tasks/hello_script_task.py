"""脚本文件任务示例一：打印调度上下文信息。

演示脚本任务的最小结构：
    - 以子进程方式被执行（python hello_script_task.py）
    - 退出码 0 表示成功，非 0 表示失败（失败会触发调度器重试）
    - 通过调度器注入的环境变量感知任务上下文

可独立运行验证: python SchedulerTask/tasks/hello_script_task.py
"""
import os
import sys


def main() -> int:
    job_id = os.environ.get("SCHEDULER_JOB_ID", "manual")
    retry_count = os.environ.get("SCHEDULER_JOB_RETRY_COUNT", "0")
    max_retries = os.environ.get("SCHEDULER_JOB_MAX_RETRIES", "0")

    print(
        f"Hello from script task! "
        f"(job_id={job_id}, retry={retry_count}/{max_retries})"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
