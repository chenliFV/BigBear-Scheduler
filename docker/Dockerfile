# 调度器镜像：同一镜像承载 Worker 与 API 两种进程
#   Worker（默认）: compose 中 command: python SchedulerRun.py
#   API:           compose 中 command: python FastApiRun.py
#
# 注意：构建上下文为项目根目录（见 docker-compose.yml 的 context: ..），
# 本文件内所有 COPY 路径均相对于项目根目录。
FROM python:3.12-slim

ENV TZ=Asia/Shanghai \
    PYTHONUNBUFFERED=1

# gcc / libc6-dev / libpq-dev: Requirements.txt 中的 psycopg2 为源码发行，
# 编译 C 扩展需要工具链、glibc 头文件与 PostgreSQL 客户端头文件
# tzdata: 为 zoneinfo 提供系统时区数据库（调度默认时区 Asia/Shanghai）
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libc6-dev \
    libpq-dev \
    tzdata \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# 先复制依赖清单，利用 Docker 层缓存加速重复构建
COPY Requirements.txt ./
RUN pip install --no-cache-dir -r Requirements.txt

# 复制项目代码
COPY . .

# 运行时日志目录（compose 中挂载到宿主机 docker/log 持久化）
RUN mkdir -p /app/Log
VOLUME ["/app/Log"]

# API 进程端口（Dashboard / REST API）
EXPOSE 8000

# 默认启动 Worker；API 服务通过 command 覆盖为: python FastApiRun.py
CMD ["python", "SchedulerRun.py"]
