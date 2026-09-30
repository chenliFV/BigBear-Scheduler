#!/bin/bash
# 一键启动 Scheduler 双进程架构
#
# 用法:
#   ./start.sh [--python <解释器路径>] <命令>
#
# 参数:
#   --python   指定 Python 解释器路径（如 python3、/usr/bin/python3、.venv/bin/python）
#              未指定时按优先级自动检测：.venv/bin/python → PATH 中的 python3
#
# 命令:
#   start      启动 Worker + API 两个进程（默认）
#   worker     仅启动 Worker
#   api        仅启动 API
#   stop       停止所有进程
#   restart    重启所有进程
#   status     查看运行状态
#
# 示例:
#   ./start.sh                              # 自动检测 Python，启动两个进程
#   ./start.sh --python python3 start       # 使用 PATH 中的 python3
#   ./start.sh --python .venv/bin/python    # 使用虚拟环境
#   ./start.sh stop
#   ./start.sh status

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# 解析 --python 参数，剩余参数作为子命令
PYTHON=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        --python)
            PYTHON="$2"
            shift 2
            ;;
        *)
            break
            ;;
    esac
done

WORKER_PID_FILE="${SCRIPT_DIR}/.scheduler.pid"
API_PID_FILE="${SCRIPT_DIR}/.api.pid"
LOG_DIR="${SCRIPT_DIR}/Log"

mkdir -p "$LOG_DIR/worker" "$LOG_DIR/api"

# 确定 Python 解释器：优先使用 --python 传入的，其次自动检测
if [ -n "$PYTHON" ]; then
    # 用户显式指定，验证是否可用
    if ! "$PYTHON" --version >/dev/null 2>&1; then
        echo "[ERROR] 指定的 Python 解释器不可用: $PYTHON"
        exit 1
    fi
    echo "[INFO] 使用指定 Python: $PYTHON ($("$PYTHON" --version 2>&1))"
else
    VENV_PYTHON="${SCRIPT_DIR}/scheduler_env/bin/python"
    if [ -f "$VENV_PYTHON" ]; then
        PYTHON="$VENV_PYTHON"
        echo "[INFO] 使用虚拟环境 Python: $PYTHON"
    elif command -v python3 >/dev/null 2>&1; then
        PYTHON="$(command -v python3)"
        echo "[INFO] 使用系统 Python: $PYTHON ($("$PYTHON" --version 2>&1))"
    else
        echo "[ERROR] 未找到 Python 解释器"
        echo "        用法: ./start.sh --python <解释器路径> <命令>"
        echo "        或创建虚拟环境: python3 -m venv .venv"
        exit 1
    fi
fi

start_worker() {
    if [ -f "$WORKER_PID_FILE" ] && kill -0 "$(cat $WORKER_PID_FILE)" 2>/dev/null; then
        echo "[WARN] Worker 已在运行 (PID: $(cat $WORKER_PID_FILE))"
        return
    fi
    echo "[INFO] 启动 Scheduler Worker..."
    nohup "$PYTHON" SchedulerRun.py > /dev/null 2>> "$LOG_DIR/worker/worker.log" &
    WORKER_PID=$!
    echo $WORKER_PID > "$WORKER_PID_FILE"
    echo "[OK] Worker 已启动 (PID: $WORKER_PID)"
    echo "     日志: $LOG_DIR/worker/worker.log（按天轮转，保留30天）"
}

start_api() {
    if [ -f "$API_PID_FILE" ] && kill -0 "$(cat $API_PID_FILE)" 2>/dev/null; then
        echo "[WARN] API 已在运行 (PID: $(cat $API_PID_FILE))"
        return
    fi
    echo "[INFO] 启动 FastAPI..."
    nohup "$PYTHON" FastApiRun.py > /dev/null 2>&1 &
    API_PID=$!
    echo $API_PID > "$API_PID_FILE"
    echo "[OK] API 已启动 (PID: $API_PID)"
    echo "     日志: $LOG_DIR/api/api.log（按天轮转，保留30天）"
    echo "     访问: http://localhost:8000/scheduler/"
}

stop_process() {
    local name=$1
    local pid_file=$2
    if [ -f "$pid_file" ]; then
        local pid=$(cat "$pid_file")
        if kill -0 "$pid" 2>/dev/null; then
            echo "[INFO] 停止 $name (PID: $pid)..."
            kill -TERM "$pid"
            # 等待进程退出（最多 35 秒）
            for i in $(seq 1 35); do
                if ! kill -0 "$pid" 2>/dev/null; then
                    break
                fi
                sleep 1
            done
            # 如果还没退出，强制杀
            if kill -0 "$pid" 2>/dev/null; then
                echo "[WARN] $name 未响应，强制终止"
                kill -9 "$pid" 2>/dev/null
            fi
            echo "[OK] $name 已停止"
        else
            echo "[INFO] $name 未在运行"
        fi
        rm -f "$pid_file"
    else
        echo "[INFO] $name 未在运行"
    fi
}

show_status() {
    echo "=============================="
    echo " Scheduler 服务状态"
    echo "=============================="
    # Worker
    if [ -f "$WORKER_PID_FILE" ] && kill -0 "$(cat $WORKER_PID_FILE)" 2>/dev/null; then
        echo " Worker  (PID: $(cat $WORKER_PID_FILE))  [运行中]"
    else
        echo " Worker  [已停止]"
    fi
    # API
    if [ -f "$API_PID_FILE" ] && kill -0 "$(cat $API_PID_FILE)" 2>/dev/null; then
        echo " API     (PID: $(cat $API_PID_FILE))  [运行中]"
    else
        echo " API     [已停止]"
    fi
    echo "=============================="
}

case "${1:-all}" in
    worker)
        start_worker
        ;;
    api)
        start_api
        ;;
    all|start)
        start_worker
        sleep 2  # 等 worker 先初始化数据库
        start_api
        echo ""
        echo "[DONE] 两个服务已启动"
        echo "       Dashboard: http://localhost:8000/scheduler/"
        echo "       停止: ./start.sh stop"
        ;;
    stop)
        stop_process "API" "$API_PID_FILE"
        stop_process "Worker" "$WORKER_PID_FILE"
        ;;
    restart)
        $0 --python "$PYTHON" stop
        sleep 1
        $0 --python "$PYTHON" start
        ;;
    status)
        show_status
        ;;
    *)
        echo "用法: $0 {start|stop|restart|status|worker|api}"
        exit 1
        ;;
esac
