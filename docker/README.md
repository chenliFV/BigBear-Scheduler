# Docker 部署指南

容器化部署文件全部集中在本目录，一条命令拉起 **MySQL + Worker + API** 三个服务。项目整体介绍见[根 README](../README.md)。

## 目录结构

```
docker/
├── README.md               # 本文档
├── Dockerfile              # 拆分部署镜像（Worker / API 共用，MySQL 独立容器）
├── Dockerfile.all          # 全量单容器镜像（MySQL + Worker + API 同容器，见下文）
├── Dockerfile.dockerignore # 构建排除清单（BuildKit 变体，见下方说明）
├── docker-compose.yml      # Docker Compose 编排（mysql / worker / api）
├── run.sh                  # 一键部署脚本（无 compose 插件时的等价替代，见下方说明）
├── start-all.sh            # 全量单容器入口脚本（初始化 MySQL → 启动 Worker + API）
├── fastapi.prod.json       # 生产配置（关闭热重载 api.reload=false，可挂载覆盖）
├── database.json           # Docker 数据库配置（连接参数为 ${DB_HOST} 等环境变量占位，
│                           #   挂载为容器内 Config/database.json，由 loader 的展开机制注入）
├── initdb/
│   └── 01-init.sql         # MySQL 容器首次启动自动执行（预建 business 业务库并授权）
└── log/                    # 容器运行日志挂载目录（worker/ api/，运行时自动生成）
```

> **关于 `Dockerfile.dockerignore`**：默认的 `.dockerignore` 必须位于构建上下文根目录（本项目为项目根），无法放入本目录。因此改用 BuildKit 提供的按 Dockerfile 命名变体——与 Dockerfile 同目录、命名为 `Dockerfile` + `.dockerignore`，优先级高于根目录 `.dockerignore`，从而与其余 Docker 文件集中在一起。注意它**仅在 BuildKit 构建器下生效**（Docker 20.10+ 默认启用）；`DOCKER_BUILDKIT=0` 的老构建器会静默忽略该清单——构建仍会成功，只是会把 `.git`、虚拟环境等发进构建上下文，变慢且镜像变大。清单内路径仍相对构建上下文根目录（项目根）。

## 前置要求

| 组件 | 版本要求 |
| --- | --- |
| Docker | 20.10+ |
| Docker Compose | v2（`docker compose` 子命令；v1 的 `docker-compose` 独立二进制亦可，命令等价替换） |
| BuildKit | 默认构建器即可（20.10+ 默认启用）；请勿设置 `DOCKER_BUILDKIT=0`，否则构建排除清单不生效 |

## 快速启动

### 方式一：docker compose（推荐，需 compose 插件）

```bash
cd docker

# 构建镜像并后台启动全部服务（首次会自动拉取 mysql:8.0 镜像）
docker compose up -d --build

# 查看服务状态
docker compose ps

# 查看日志（worker / api 的日志同时写入容器 stdout，可用 docker logs 查看）
docker compose logs -f worker
docker compose logs -f api
```

启动完成后访问 Dashboard：<http://localhost:8000/scheduler/>

> MySQL 首次初始化约需 10–30 秒（建库 + 执行 initdb 脚本），worker / api 会等待其健康检查（`mysqladmin ping`）通过后再启动，无需手动干预。

### 方式二：run.sh（无 compose 插件时的等价替代）

主机未安装 `docker compose` 插件（且无 sudo 权限安装）时，用 [run.sh](run.sh) 一键部署，编排逻辑与 docker-compose.yml 完全等价（同一网络、`--network-alias mysql` 服务发现、initdb 挂载、数据卷、日志挂载、worker 90s 优雅关闭），并额外提供基础镜像拉取失败时自动回退 `docker.m.daocloud.io` 镜像源的能力：

```bash
cd docker

./run.sh start           # 构建镜像（如缺失）并启动 MySQL + Worker + API
./run.sh status          # 查看容器状态
./run.sh logs worker     # 跟踪服务日志（worker | api | mysql）
./run.sh stop            # 停止并移除容器（MySQL 数据保留在数据卷）
./run.sh restart         # 重启
./run.sh purge           # 连同 MySQL 数据卷一起删除（清空数据库，慎用）
```

环境变量与方式一相同（`MYSQL_ROOT_PASSWORD` / `MYSQL_USER` / `MYSQL_PASSWORD` / `API_PORT`），支持前缀覆盖：

```bash
API_PORT=8080 MYSQL_PASSWORD=strong-pass ./run.sh start
```

> 注意：修改密码后需先 `./run.sh purge` 清空旧数据卷再 `start`，否则 MySQL 沿用卷内旧密码导致等待超时。

## 全量单容器部署（All-in-One）

把 **MySQL 8.0 + Python 环境 + Worker + API** 全部装进一个容器，适合快速体验、单机演示或离线分发。基于 `ubuntu:24.04`（官方归档自带 mysql-server 8.0 与 python3.12），镜像内已安装 `vim` / `zip` / `unzip` / `curl` / `less` / `procps` 基础工具。

与拆分部署的差异：

| 项 | 拆分部署（Dockerfile / compose / run.sh） | 全量单容器（Dockerfile.all） |
| --- | --- | --- |
| 容器数 | 3（mysql / worker / api） | 1 |
| MySQL 初始化 | compose initdb 挂载 / MYSQL_* 环境变量 | 入口脚本 start-all.sh 自动建库建号 |
| 数据库连接 | 服务名 `mysql`（网络别名） | 容器内 `127.0.0.1` |
| 热重载 | 需自行处理 | 镜像内已烘焙关闭热重载的 fastapi.prod.json |

构建与运行（在项目根目录执行）：

```bash
# 构建（内网 apt 慢可加 --build-arg APT_MIRROR=http://mirrors.tuna.tsinghua.edu.cn/ubuntu/，
#        注意必须用 http:// 源，基础镜像无 ca-certificates 无法校验 https）
docker build -f docker/Dockerfile.all -t bigbear-scheduler-all:latest .

# 运行：MySQL 数据落卷持久化；日志映射到宿主机 /Log；MySQL/SSH 暴露到宿主机
docker run -d --name scheduler-all \
    -p 8000:8000 \
    -p 3308:3306 \
    -p 2222:22 \
    -v scheduler-all-mysql:/var/lib/mysql \
    -v /Log:/app/Log \
    -v "$PWD/docker/fastapi.prod.json":/app/Config/fastapi.json:ro \
    --restart unless-stopped \
    bigbear-scheduler-all:latest

# 验证 / 运维
docker logs -f scheduler-all                 # 启动日志（SSH → MySQL 初始化 → Worker/API）
curl http://localhost:8000/scheduler/api/status
docker exec -it scheduler-all bash           # 容器内 vim / zip / unzip 可用
docker exec scheduler-all mysql -ustkuser -pstkuser scheduler -e "SHOW TABLES;"

# 外部访问
mysql -h 127.0.0.1 -P 3308 -ustkuser -pstkuser scheduler   # 宿主机直连 MySQL
ssh root@127.0.0.1 -p 2222                                 # SSH 进容器（密码见 SSH_PASSWORD）

# 停止（优雅关闭 Worker/API 后再停 MySQL）与清理
docker stop scheduler-all && docker rm scheduler-all
docker volume rm scheduler-all-mysql         # 彻底清空数据时执行
```

环境变量：`MYSQL_USER` / `MYSQL_PASSWORD`（默认 `stkuser` / `stkuser`，首启建号时生效）、`SSH_PASSWORD`（默认 `root123456`，SSH root 密码）；改密码需删卷重建。

**自动启动语义**：镜像入口为 [start-all.sh](start-all.sh)，配合 `--restart unless-stopped`，容器重启、Docker 服务重启、宿主机重启后都会自动依次拉起 SSH → MySQL → Worker → API，无需人工干预；容器内任一应用进程意外退出时整个容器停止并由 Docker 自动重启恢复。

**端口与挂载一览**：

| 宿主机 | 容器 | 用途 |
| --- | --- | --- |
| 8000 | 8000 | Dashboard / REST API |
| 3308 | 3306 | MySQL（stkuser 账号） |
| 2222 | 22 | SSH（root 账号，密码 `SSH_PASSWORD`） |
| `/Log` | `/app/Log` | 运行日志（worker/ api/，按天轮转） |
| 命名卷 `scheduler-all-mysql` | `/var/lib/mysql` | MySQL 数据持久化 |

> 单容器内多进程由 [start-all.sh](start-all.sh) 管理：初始化 MySQL → 建库建号（`scheduler` + `business`）→ 后台启动 Worker 与 API → `SIGTERM` 优雅关闭（Worker 等待任务完成后再停 MySQL）；任一进程意外退出则整个容器停止，配合 `--restart` 自动拉起。

## 服务与数据卷

| 服务 | 容器名 | 说明 |
| --- | --- | --- |
| mysql | `scheduler-mysql` | MySQL 8.0，utf8mb4 + `mysql_native_password` 认证（PyMySQL 免装 cryptography）；数据持久化于命名卷 `mysql-data` |
| worker | `scheduler-worker` | 调度循环与任务执行，默认启动命令 `python SchedulerRun.py`；`stop_grace_period: 90s` 保证优雅关闭 |
| api | `scheduler-api` | Dashboard + REST API，启动命令 `python FastApiRun.py`，映射宿主机 `${API_PORT}`（默认 8000）端口 |

| 数据卷 | 宿主机位置 | 内容 |
| --- | --- | --- |
| 命名卷 `mysql-data` | Docker 管理 | MySQL 数据文件 |
| 绑定挂载 `./log/` | `docker/log/` | worker / api 运行日志（按天轮转，保留 30 天） |
| 绑定挂载 `./database.json` | `docker/database.json` | 覆盖容器内 `/app/Config/database.json`（只读） |
| 绑定挂载 `./initdb/` | `docker/initdb/` | MySQL 首启初始化脚本（只读，仅首次建库时生效） |

镜像构建要点（见 [Dockerfile](Dockerfile)）：

- 基于 `python:3.12-slim`，安装 `gcc` / `libpq-dev`（Requirements.txt 中的 psycopg2 为源码发行需编译）与 `tzdata`（`zoneinfo` 系统时区库）
- 先复制 `Requirements.txt` 单独安装依赖，利用 Docker 层缓存加速重复构建
- `TZ=Asia/Shanghai`：容器内日志时间与 daily / cron 调度时刻与项目默认时区一致

## 自定义环境变量

在本目录创建 `.env` 文件（或直接在命令前缀）覆盖默认值：

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `MYSQL_ROOT_PASSWORD` | `root123456` | MySQL root 密码 |
| `MYSQL_USER` | `stkuser` | 业务账号名（修改需同步更新 [initdb/01-init.sql](initdb/01-init.sql) 中的授权语句） |
| `MYSQL_PASSWORD` | `stkuser` | 业务账号密码（worker / api 经 `DB_PASSWORD` 引用） |
| `API_PORT` | `8000` | Dashboard 宿主机映射端口 |

```bash
# 示例：自定义密码与端口
MYSQL_ROOT_PASSWORD=strong-root-pass MYSQL_PASSWORD=strong-pass API_PORT=8080 \
    docker compose up -d --build
```

`.env` 示例：

```env
MYSQL_ROOT_PASSWORD=change-me-root
MYSQL_PASSWORD=change-me
API_PORT=8000
```

## 建业务表与结构回滚（可选）

动态建表在容器内手动触发，与裸机用法一致（目标库为 `business`，已由 initdb 脚本预建并授权）：

```bash
# 用示例配置建表
docker compose run --rm worker \
    python SchedulerTable/TableGenerator.py --config SchedulerTable/tables_config_test.json

# 结构回滚（列出批次 / 预演 / 实际执行，连接串指向 compose 内的 mysql 服务）
docker compose run --rm worker \
    python SchedulerTable/RollbackSchema.py \
    --db "mysql+pymysql://stkuser:stkuser@mysql:3306/business" --list
```

## 常用运维命令

均在 `docker/` 目录执行：

```bash
docker compose ps           # 查看服务状态
docker compose logs -f api  # 跟踪指定服务日志
docker compose stop         # 停止全部服务（worker 优雅关闭，等待任务完成）
docker compose start        # 再次启动
docker compose restart      # 重启
docker compose down         # 停止并删除容器（保留 mysql-data 数据卷）
docker compose down -v      # 停止并连同数据卷一起删除（清空数据库，慎用）

# 代码更新后重建镜像并滚动重启
docker compose up -d --build
```

## 注意事项

- **认证插件**：compose 已为 MySQL 8.0 指定 `--default-authentication-plugin=mysql_native_password`，PyMySQL 无需安装 `cryptography`；如改用默认的 `caching_sha2_password`，需在 Dockerfile 中追加 `pip install cryptography`
- **数据持久化**：MySQL 数据在命名卷 `mysql-data` 中，`docker compose down` 不会删除；`down -v` 会连同删除
- **首次初始化**：initdb 脚本仅在 `mysql-data` 卷为空（首次启动）时执行，修改脚本后需 `docker compose down -v` 重建才会生效
- **热重载**：项目 [Config/fastapi.json](../Config/fastapi.json) 默认 `api.reload: true`，容器内无文件变更，生产环境建议改为 `false`（可加挂一份修改后的 fastapi.json）
- **外部数据库**：如需使用已有的外部 MySQL，删除 `mysql` 服务与 `mysql-data` 卷，并将 worker / api 环境中的 `DB_HOST` / `DB_USER` / `DB_PASSWORD` 指向外部实例
- **单机编排**：如需多机部署，可直接用本目录 Dockerfile 构建并分发 worker / api 镜像（构建上下文为项目根：`docker build -f docker/Dockerfile -t bigbear-scheduler ..`）
