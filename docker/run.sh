#!/bin/bash
# 无 docker compose 插件环境的一键部署脚本（与 docker-compose.yml 逻辑等价）
#
# 用法: ./run.sh {start|stop|restart|status|logs|purge} [worker|api|mysql]
#
# 命令:
#   start     构建镜像（如缺失）并启动 MySQL + Worker + API（默认）
#   stop      停止并移除三个容器（MySQL 数据保留在数据卷）
#   restart   stop + start
#   status    查看容器状态
#   logs      跟踪指定服务日志（默认 worker）
#   purge     stop 基础上再删除 MySQL 数据卷（清空数据库，慎用）
#
# 环境变量（可覆盖，与 docker-compose.yml 默认值一致）:
#   MYSQL_ROOT_PASSWORD  MySQL root 密码（默认 root123456）
#   MYSQL_USER           业务账号名（默认 stkuser，需同步修改 initdb/01-init.sql）
#   MYSQL_PASSWORD       业务账号密码（默认 stkuser）
#   API_PORT             宿主机端口（默认 8000）
#
# 注意: 修改密码后需先 ./run.sh purge 清空旧数据卷再 start，否则沿用卷内旧密码。

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

IMAGE="bigbear-scheduler:latest"
NET="scheduler-net"
MYSQL_NAME="scheduler-mysql"
WORKER_NAME="scheduler-worker"
API_NAME="scheduler-api"
MYSQL_VOLUME="scheduler-mysql-data"

MYSQL_ROOT_PASSWORD="${MYSQL_ROOT_PASSWORD:-root123456}"
MYSQL_USER="${MYSQL_USER:-stkuser}"
MYSQL_PASSWORD="${MYSQL_PASSWORD:-stkuser}"
API_PORT="${API_PORT:-8000}"

# 容器名兜底（start 时被 network-alias mysql 取代，服务发现仍用 "mysql"）
exists_container() { docker ps -a --format '{{.Names}}' | grep -qx "$1"; }

# 基础镜像：docker.io 拉取失败时回退 daocloud 镜像源（部分内网环境需要）
pull_base() {
    local img="$1" mirror="docker.m.daocloud.io/library/$1"
    if docker image inspect "$img" >/dev/null 2>&1; then
        return 0
    fi
    echo "[INFO] 拉取基础镜像 $img ..."
    if ! docker pull "$img"; then
        echo "[WARN] docker.io 拉取失败，改用镜像源 $mirror"
        docker pull "$mirror"
        docker tag "$mirror" "$img"
    fi
}

build_image() {
    if docker image inspect "$IMAGE" >/dev/null 2>&1; then
        echo "[INFO] 镜像已存在: $IMAGE（强制重建请先执行 docker rmi $IMAGE）"
        return 0
    fi
    echo "[INFO] 构建镜像 $IMAGE（构建上下文为项目根目录）..."
    docker build -f "$SCRIPT_DIR/Dockerfile" -t "$IMAGE" "$PROJECT_ROOT"
}

wait_mysql() {
    echo "[INFO] 等待 MySQL 就绪（首次初始化约 10-30 秒）..."
    local i
    for i in $(seq 1 60); do
        if docker exec "$MYSQL_NAME" mysqladmin ping -h127.0.0.1 \
            -uroot -p"$MYSQL_ROOT_PASSWORD" --silent >/dev/null 2>&1; then
            echo "[OK] MySQL 就绪"
            return 0
        fi
        sleep 5
    done
    echo "[ERROR] MySQL 等待超时，排查: docker logs $MYSQL_NAME"
    exit 1
}

run_app() {
    local name="$1" extra="$2" cmd="$3"
    if exists_container "$name"; then
        echo "[INFO] $name 已存在，直接启动"
        docker start "$name" >/dev/null
        return
    fi
    echo "[INFO] 启动 $name ..."
    # shellcheck disable=SC2086
    docker run -d --name "$name" --network "$NET" $extra \
        -e TZ=Asia/Shanghai \
        -e DB_HOST=mysql \
        -e DB_USER="$MYSQL_USER" \
        -e DB_PASSWORD="$MYSQL_PASSWORD" \
        -v "$SCRIPT_DIR/database.json":/app/Config/database.json:ro \
        -v "$SCRIPT_DIR/log":/app/Log \
        "$IMAGE" $cmd >/dev/null
}

start() {
    pull_base python:3.12-slim
    pull_base mysql:8.0
    build_image
    mkdir -p "$SCRIPT_DIR/log/worker" "$SCRIPT_DIR/log/api"

    docker network inspect "$NET" >/dev/null 2>&1 || docker network create "$NET"

    if exists_container "$MYSQL_NAME"; then
        echo "[INFO] MySQL 容器已存在，直接启动"
        docker start "$MYSQL_NAME" >/dev/null
    else
        echo "[INFO] 启动 MySQL ..."
        docker run -d --name "$MYSQL_NAME" --network "$NET" --network-alias mysql \
            -e MYSQL_ROOT_PASSWORD="$MYSQL_ROOT_PASSWORD" \
            -e MYSQL_DATABASE=scheduler \
            -e MYSQL_USER="$MYSQL_USER" \
            -e MYSQL_PASSWORD="$MYSQL_PASSWORD" \
            -v "$MYSQL_VOLUME":/var/lib/mysql \
            -v "$SCRIPT_DIR/initdb":/docker-entrypoint-initdb.d:ro \
            mysql:8.0 \
            --character-set-server=utf8mb4 \
            --collation-server=utf8mb4_unicode_ci \
            --default-authentication-plugin=mysql_native_password >/dev/null
    fi
    wait_mysql

    run_app "$WORKER_NAME" "" "python SchedulerRun.py"
    run_app "$API_NAME" "-p ${API_PORT}:8000" "python FastApiRun.py"

    echo
    echo "[DONE] 三个服务已启动"
    echo "       Dashboard: http://localhost:${API_PORT}/scheduler/"
    echo "       查看日志:   $0 logs worker | $0 logs api"
    echo "       停止服务:   $0 stop"
}

stop() {
    # worker 优雅关闭：给足 90s 等待任务完成（对应 stop_grace_period）
    docker stop -t 90 "$API_NAME" "$WORKER_NAME" >/dev/null 2>&1 || true
    docker stop -t 30 "$MYSQL_NAME" >/dev/null 2>&1 || true
    docker rm "$API_NAME" "$WORKER_NAME" "$MYSQL_NAME" >/dev/null 2>&1 || true
    echo "[OK] 已停止并移除容器（MySQL 数据保留在卷 $MYSQL_VOLUME，彻底清空用 $0 purge）"
}

status() {
    docker ps -a --filter "name=^scheduler-" \
        --format 'table {{.Names}}\t{{.Status}}\t{{.Ports}}'
}

logs() {
    local svc="${1:-worker}"
    local name
    case "$svc" in
        worker) name="$WORKER_NAME" ;;
        api)    name="$API_NAME" ;;
        mysql)  name="$MYSQL_NAME" ;;
        *) echo "未知服务: $svc（可选 worker | api | mysql）"; exit 1 ;;
    esac
    docker logs -f "$name"
}

purge() {
    stop
    docker volume rm "$MYSQL_VOLUME" >/dev/null 2>&1 || true
    echo "[OK] 已删除数据卷 $MYSQL_VOLUME（数据库已清空）"
}

case "${1:-start}" in
    start)   start ;;
    stop)    stop ;;
    restart) stop; start ;;
    status)  status ;;
    logs)    logs "${2:-worker}" ;;
    purge)   purge ;;
    *)       echo "用法: $0 {start|stop|restart|status|logs|purge} [worker|api|mysql]"; exit 1 ;;
esac
