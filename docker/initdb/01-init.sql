-- MySQL 容器首次启动时由官方镜像入口自动执行：
--   1. 预建业务库 business（调度库 scheduler 由 MYSQL_DATABASE 环境变量自动创建）
--   2. 授予业务账号 business 库权限，供动态建表（TableGenerator）使用
-- 注意：账号名与 compose 中 MYSQL_USER 默认值一致（stkuser），改名需同步修改此处
CREATE DATABASE IF NOT EXISTS `business`
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_unicode_ci;
GRANT ALL PRIVILEGES ON `business`.* TO 'stkuser'@'%';
FLUSH PRIVILEGES;
