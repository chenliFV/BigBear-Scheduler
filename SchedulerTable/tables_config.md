# 动态数据表配置文件说明

本文档描述 `tables_config/` 目录下 JSON 配置文件的字段定义。该目录由 `TableGenerator.py` 读取，需手动执行 `python SchedulerTable/TableGenerator.py --config <配置目录>`（或代码调用 `generate_tables()`）才会建表/迁移，调度器启动时不会自动执行。批次数据归集链路的表声明位于 [BatchDataCollection/tables_config/](../BatchDataCollection/tables_config)。

配置目录按目标数据库拆分多份配置，每份配置顶层的 `database` 字段指向 [Config/database.json](../Config/database.json) 中的逻辑库名：

- [BatchDataCollection/tables_config/business.json](../BatchDataCollection/tables_config/business.json)：DWD/DWS 报表表（7 张，`database = "business"`）
- [BatchDataCollection/tables_config/ods.json](../BatchDataCollection/tables_config/ods.json)：ODS 归档表（2 张，`database = "ods"`）

多份配置可以指向不同数据库（各库彼此独立），因此 ODS 表与 DWD 表分别声明，互不影响。
若某份配置的目标逻辑库未在 `Config/database.json` 中配置，则该份配置被跳过（只记日志，不报错），其余配置照常执行。

## 总体行为

- 表不存在 → 创建（含所有列、约束、索引）
- 表已存在 → 按最新配置进行迁移：
  - 新增缺失的列和索引
  - 配置声明 `bigint` 而库中为 `int` 的列 → 自动无损扩宽（MySQL/MariaDB `MODIFY COLUMN`，PostgreSQL `ALTER COLUMN TYPE`；SQLite 动态类型无宽度语义，跳过）
  - 根据 `drop_missing` 决定是否删除多余列/索引
- **除 int→bigint 无损扩宽外，已有列的类型不会被修改**（避免数据丢失，需手动迁移）
- 主键列永远不会被删除

---

## 顶层字段

| 字段 | 类型 | 默认值 | 必填 | 说明 |
|---|---|---|---|---|
| `database` | string | `"business"` | 否 | 业务表使用的数据库逻辑名，对应 [Config/database.json](../Config/database.json) 中 `databases` 的键。未配置时默认使用 `business`。也可通过函数参数覆盖：`generate_tables(database_url='...')` |
| `database_url` | string \| null | `null` | 否 | 业务表使用的数据库连接字符串（向后兼容）。`null` 时从 [Config/database.json](../Config/database.json) 统一生成；字符串时建在独立数据库中。也支持函数参数覆盖：`generate_tables(database_url='...')` |
| `drop_missing` | boolean | `false` | 否 | 是否自动删除配置中已不存在的列和索引。`false`：保留（默认，避免误删数据）；`true`：自动 `ALTER TABLE DROP COLUMN` / `DROP INDEX`。也支持函数参数覆盖：`generate_tables(drop_missing=True)` |
| `create_database` | boolean | `true` | 否 | 数据库不存在时是否自动创建。`true`（默认）：先 `CREATE DATABASE` 再建表（MySQL/MariaDB 按连接串 `charset` 建库，默认 `utf8mb4`；PostgreSQL 连接维护库 `postgres` 创建）；`false`：不创建，直接建表。SQLite 的库文件由驱动自动创建，无需该选项。也支持函数参数覆盖：`generate_tables(create_database=False)` |
| `tables` | array | — | 是 | 表定义数组，每个元素是一张表的完整定义 |

### 数据库配置说明

连接串统一由 [Config/database.json](../Config/database.json) 集中管理，通过 `Config.get_database_url(db_name)` 生成。在配置文件中通过 `database` 字段指定逻辑名：

```json
{
    "database": "business",
    "drop_missing": false,
    "tables": [ ... ]
}
```

如需直接指定连接串（覆盖统一配置），可使用 `database_url` 字段：

```
SQLite:        sqlite:///./business.db           (相对路径，项目根目录)
               sqlite:////tmp/business.db        (Linux 绝对路径，4 个斜杠)

MySQL:         mysql+pymysql://user:pwd@host:3306/dbname?charset=utf8mb4

PostgreSQL:    postgresql://user:pwd@host:5432/dbname
               postgresql+psycopg2://user:pwd@host:5432/dbname
```

---

## 表定义（`tables` 数组元素）

| 字段 | 类型 | 必填 | 说明 |
|---|---|---|---|
| `name` | string | 是 | 表名，如 `example_user_logs` |
| `comment` | string | 否 | 表注释，写入 DDL 的 `COMMENT` |
| `columns` | array | 是 | 列定义数组，至少一个元素 |
| `indexes` | array | 否 | 索引定义数组 |

---

## 列定义（`columns` 数组元素）

| 字段 | 类型 | 必填 | 默认值 | 适用条件 | 说明 |
|---|---|---|---|---|---|
| `name` | string | 是 | — | — | 列名，如 `id`、`user_name` |
| `type` | string | 是 | — | — | 字段类型，见下方[支持的类型](#支持的字段类型) |
| `length` | integer | 否 | — | `str` / `string` / `varchar` | 字符串最大长度。未指定时退化为 `TEXT` |
| `precision` | integer | 否 | — | `numeric` | 总位数，如价格 `10` 表示最多 10 位数字 |
| `scale` | integer | 否 | — | `numeric` | 小数位数，如价格 `2` 表示保留 2 位小数 |
| `primary_key` | boolean | 否 | `false` | — | 是否为主键 |
| `autoincrement` | boolean | 否 | `false` | 整数主键 | 是否自增 |
| `nullable` | boolean | 否 | `true`（主键列默认 `false`） | — | 是否允许 `NULL` |
| `unique` | boolean | 否 | `false` | — | 是否唯一（建表时创建唯一约束） |
| `default` | any | 否 | — | — | 默认值。字符串 `"now"`（不区分大小写）表示数据库当前时间 `CURRENT_TIMESTAMP`；其他字面值在 Python 端 + SQL 端同时设置。示例：`true` / `false` / `0` / `3.14` / `"active"` / `"now"` |
| `comment` | string | 否 | — | — | 列注释，写入 DDL 的 `COMMENT` |
| `index` | boolean | 否 | `false` | — | 是否对该列单独建索引（便捷写法，等价于在 `indexes` 中声明单列索引） |

---

## 索引定义（`indexes` 数组元素）

| 字段 | 类型 | 必填 | 默认值 | 说明 |
|---|---|---|---|---|
| `name` | string | 是 | — | 索引名（建议以 `idx_` 开头，便于辨识） |
| `columns` | array | 是 | — | 索引包含的列名数组，支持单列或多列（复合索引） |
| `unique` | boolean | 否 | `false` | 是否为唯一索引 |

---

## 支持的字段类型

类型名不区分大小写。

| 类型名 | 别名 | 对应 SQL 类型 | 适用场景 |
| --- | --- | --- | --- |
| `int` | `integer` | `INTEGER` | 整数，如 id、count |
| `bigint` | — | `BIGINT` | 大整数，如源库 bigint 主键的镜像列；迁移时库中同名列若为 `int` 会自动无损扩宽 |
| `str` | `string`、`varchar` | `VARCHAR(length)` 或 `TEXT` | 变长字符串，需配 `length`；不配则退化为 `TEXT` |
| `text` | — | `TEXT` | 长文本，如 description、content |
| `float` | — | `FLOAT` | 单精度浮点数 |
| `double` | — | `DOUBLE` | 双精度浮点数（SQLAlchemy 1.4+） |
| `numeric` | — | `NUMERIC(precision, scale)` | 定点小数，如货币、金额。需配 `precision` + `scale` |
| `bool` | `boolean` | `BOOLEAN` | 布尔值，存为 0/1 或 TRUE/FALSE |
| `json` | — | `JSON` | JSON 文档（MySQL 5.7+ / PostgreSQL / SQLite JSON） |
| `datetime` | — | `DATETIME` | 日期时间，如 created_at、updated_at |
| `bytes` | — | `BLOB` / `VARBINARY` | 二进制数据，如图片、序列化对象 |

---

## 完整示例

完整的可运行配置示例见 [BatchDataCollection/tables_config/](../BatchDataCollection/tables_config) 目录（按目标库拆分为 `business.json`、`ods.json` 等），执行时通过 `--config` 指定该目录。本文档仅为字段说明，程序不读取。
