#!/bin/bash
# 全量单容器入口：启动 MySQL → 等待就绪 → 同时运行 Worker 与 API → 信号优雅关闭
set -e

DATA_DIR=/var/lib/mysql
APP_USER="${MYSQL_USER:-stkuser}"
APP_PASSWORD="${MYSQL_PASSWORD:-stkuser}"
SSH_PASSWORD="${SSH_PASSWORD:-root123456}"

# 应用进程的数据库连接参数（database.json 中 ${DB_HOST} 等占位符由此展开）
export DB_HOST=127.0.0.1
export DB_USER="$APP_USER"
export DB_PASSWORD="$APP_PASSWORD"

# 0) SSH 服务（宿主机映射 2222 端口后可 ssh root@<宿主机> -p 2222 进容器）
echo "root:${SSH_PASSWORD}" | chpasswd
/usr/sbin/sshd
echo "[ok] SSH 已启动（端口 22，root 密码取 SSH_PASSWORD 环境变量）"

start_mysqld() {
    # --bind-address=0.0.0.0：覆盖 Ubuntu 默认的 127.0.0.1，否则宿主机端口映射（如 3308）到不了容器 3306
    mysqld --user=mysql --daemonize \
        --bind-address=0.0.0.0 \
        --character-set-server=utf8mb4 \
        --collation-server=utf8mb4_unicode_ci \
        --default-authentication-plugin=mysql_native_password
}

# 1) 首次启动（以标记文件判断，兼容镜像/卷内已有残留数据目录的情况）：
#    清空并初始化数据目录，建库建账号；之后直接启动
if [ ! -f "$DATA_DIR/.initialized" ]; then
    echo "[init] 初始化 MySQL 数据目录..."
    rm -rf "$DATA_DIR"/* 2>/dev/null || true
    mysqld --initialize-insecure --datadir="$DATA_DIR" --user=mysql
    start_mysqld

    # root 走 caching_sha2 空密码（仅本机 socket 可连），业务账号走 mysql_native_password 供 TCP 连接
    mysql -uroot <<SQL
CREATE DATABASE IF NOT EXISTS scheduler CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE DATABASE IF NOT EXISTS business  CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE USER IF NOT EXISTS '${APP_USER}'@'%' IDENTIFIED WITH mysql_native_password BY '${APP_PASSWORD}';
GRANT ALL PRIVILEGES ON scheduler.* TO '${APP_USER}'@'%';
GRANT ALL PRIVILEGES ON business.*  TO '${APP_USER}'@'%';
FLUSH PRIVILEGES;
SQL
    touch "$DATA_DIR/.initialized"
    echo "[init] MySQL 初始化完成（scheduler / business 库已创建）"
else
    start_mysqld
fi

# 2) 等待 MySQL 就绪
for i in $(seq 1 60); do
    if mysqladmin -uroot ping --silent >/dev/null 2>&1; then
        echo "[ok] MySQL 就绪"
        break
    fi
    sleep 1
done

# 3) 先启动 Worker，等待其完成建表与任务落库后再启动 API
#    （两个进程并发 create_all / DESCRIBE 同一批表存在竞争，会导致其一崩溃重启）
python SchedulerRun.py &
WORKER_PID=$!

for i in $(seq 1 60); do
    if mysql -uroot -N -e "SELECT 1 FROM information_schema.tables \
        WHERE table_schema='scheduler' AND table_name='scheduler_jobs'" 2>/dev/null | grep -q 1; then
        break
    fi
    sleep 1
done
sleep 3  # 预留 flush_state 异步落库时间

python FastApiRun.py &
API_PID=$!

shutdown() {
    echo "[stop] 优雅关闭: Worker / API 等待任务完成 -> 关闭 MySQL"
    kill -TERM "$API_PID" "$WORKER_PID" 2>/dev/null || true
    wait "$API_PID" "$WORKER_PID" 2>/dev/null || true
    mysqladmin -uroot shutdown 2>/dev/null || true
    exit 0
}
trap shutdown TERM INT

echo "[ready] 全量单容器已启动：SSH + MySQL + Worker + API"
echo "        Dashboard: http://<宿主机IP>:8000/scheduler/"
echo "        MySQL:     宿主机 3308 -> 容器 3306（stkuser）"
echo "        SSH:       宿主机 2222 -> 容器 22（root）"
echo "        容器内可用工具: vim / zip / unzip / curl / less"

# 任一进程退出则整体停止（配合 restart 策略自动拉起）
wait -n
echo "[warn] 检测到应用进程退出，容器即将停止"
shutdown
