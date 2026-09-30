"""按变更记录表回滚数据库结构。

读取目标数据库中的 table_schema_change_log 表，根据指定的 batch_id
把数据库结构恢复到"该批次执行之前"的状态：即回滚目标批次及其之后
的所有批次。

回滚方向（与 TableGenerator 的正向 DDL 相反）:
    create_table  → DROP TABLE
    add_column    → ALTER TABLE ... DROP COLUMN
    add_index     → DROP INDEX
    drop_column   → ALTER TABLE ... ADD COLUMN（列定义从历史 add_column
                    记录或 create_table 的 DDL 中还原）
    drop_index    → CREATE INDEX（索引定义从历史 add_index 记录中还原）
    create_database / error → 不自动处理，只给出提示

注意（不可逆）:
    回滚只能恢复"结构"，无法恢复被删除的数据。以下操作造成的数据
    丢失在回滚后无法找回：
        - drop_column：被删列的数据
        - drop_table ：被删表的全部数据
        - drop_index ：不丢数据，仅重建索引

用法:
    # 1) 查看所有批次，拿到要回滚到的 batch_id
    python SchedulerTable/RollbackSchema.py --db sqlite:////tmp/tg_mock_demo/demo.db --list

    # 2) 预演（默认行为，只打印将要执行的 SQL，不落库；--dry-run 可显式声明）
    python SchedulerTable/RollbackSchema.py --db <url> --batch-id <uuid> --dry-run

    # 3) 实际执行
    python SchedulerTable/RollbackSchema.py --db <url> --batch-id <uuid> --execute
"""
import argparse
import logging
import os
import sys
from typing import Any, Dict, List, Optional, Tuple

# 把项目根目录加入 sys.path，便于直接 python SchedulerTable/RollbackSchema.py
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from sqlalchemy import MetaData, inspect, select  # noqa: E402

from SchedulerTable.TableGenerator import (  # noqa: E402
    _CHANGE_LOG_TABLE_NAME,
    _OP_ADD_COLUMN,
    _OP_ADD_INDEX,
    _OP_CREATE_DATABASE,
    _OP_CREATE_TABLE,
    _OP_DROP_COLUMN,
    _OP_DROP_INDEX,
    _OP_TABLE_ERROR,
    _build_change_log_table,
    _mask_url,
)

logger = logging.getLogger("rollback_schema")
logger.addHandler(logging.NullHandler())

# 随建表一起创建时写入的备注，这类列/索引由 DROP TABLE 一并覆盖
_NOTE_CREATED_WITH_TABLE = "随新建表创建"

# 建表语句中属于约束、非列定义的行首关键字（小写）
_CONSTRAINT_KEYWORDS = (
    "primary key", "unique", "constraint", "foreign key", "check", "key",
    "index", "fulltext", "spatial",
)


# ==================== 读取变更记录 ====================

def load_change_log_records(engine) -> List[Dict[str, Any]]:
    """按 id 升序读取变更记录表的所有记录。

    Args:
        engine: 目标数据库的 SQLAlchemy Engine

    Returns:
        记录字典列表（含 id/batch_id/table_name/operation/ddl_sql 等字段）

    Raises:
        RuntimeError: 目标数据库中不存在变更记录表
    """
    if not inspect(engine).has_table(_CHANGE_LOG_TABLE_NAME):
        raise RuntimeError(
            f"目标数据库中不存在变更记录表 '{_CHANGE_LOG_TABLE_NAME}'，无法回滚。"
            "请确认 --db 指向的是执行过 TableGenerator 的数据库。"
        )

    metadata = MetaData()
    table = _build_change_log_table(metadata)
    columns = [
        table.c.id, table.c.batch_id, table.c.executed_at,
        table.c.database_name, table.c.table_name, table.c.operation,
        table.c.object_type, table.c.object_name, table.c.ddl_sql,
        table.c.success, table.c.note, table.c.operator,
    ]
    with engine.connect() as conn:
        rows = conn.execute(select(*columns).order_by(table.c.id)).mappings().all()
    return [dict(r) for r in rows]


def group_by_batch(records: List[Dict[str, Any]]) -> List[Tuple[str, List[Dict[str, Any]]]]:
    """按批次分组，保持批次首次出现的顺序（即执行先后顺序）。"""
    order: List[str] = []
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for rec in records:
        batch_id = rec["batch_id"]
        if batch_id not in grouped:
            grouped[batch_id] = []
            order.append(batch_id)
        grouped[batch_id].append(rec)
    return [(batch_id, grouped[batch_id]) for batch_id in order]


# ==================== 反向 DDL 还原 ====================

def _find_table_body_end(create_ddl: str, start: int) -> int:
    """从 '(' 开始做括号配对，返回匹配的 ')' 下标（忽略引号内的括号）。

    MySQL 的建表语句形如 ``)COMMENT='说明(含括号)'``，若直接取最后一个 ')'
    会被注释里的括号干扰，故必须做配对查找。
    """
    depth, quote, i = 0, None, start
    while i < len(create_ddl):
        ch = create_ddl[i]
        if quote:
            if ch == "\\" and quote != "`":  # 反斜杠转义（反引号内不转义）
                i += 2
                continue
            if ch == quote:
                # 引号内的双写表示转义，如 '' / ""
                if i + 1 < len(create_ddl) and create_ddl[i + 1] == quote:
                    i += 2
                    continue
                quote = None
        elif ch in ("'", '"', "`"):
            quote = ch
        elif ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
            if depth == 0:
                return i
        i += 1
    return -1


def _split_top_level(body: str) -> List[str]:
    """按顶层逗号切分表体，忽略括号与引号内的逗号。

    兼容 DECIMAL(10, 2)（括号内逗号）与 COMMENT 'a,b'（引号内逗号）。
    """
    parts: List[str] = []
    buf, depth, quote, i = "", 0, None, 0
    while i < len(body):
        ch = body[i]
        if quote:
            buf += ch
            if ch == "\\" and quote != "`":
                if i + 1 < len(body):
                    buf += body[i + 1]
                    i += 2
                    continue
            elif ch == quote:
                if i + 1 < len(body) and body[i + 1] == quote:
                    buf += body[i + 1]
                    i += 2
                    continue
                quote = None
        elif ch in ("'", '"', "`"):
            quote = ch
            buf += ch
        elif ch == "(":
            depth += 1
            buf += ch
        elif ch == ")":
            depth -= 1
            buf += ch
        elif ch == "," and depth == 0:
            parts.append(buf)
            buf = ""
        else:
            buf += ch
        i += 1
    if buf.strip():
        parts.append(buf)
    return parts


def _extract_column_from_create_ddl(create_ddl: str, column_name: str) -> Optional[str]:
    """从 CREATE TABLE 语句中提取指定列的定义片段（不含列名前的引号）。"""
    start = create_ddl.find("(")
    if start == -1:
        return None
    end = _find_table_body_end(create_ddl, start)
    if end == -1 or end <= start:
        return None
    body = create_ddl[start + 1:end]

    for part in _split_top_level(body):
        part = part.strip()
        if not part or part.lower().startswith(_CONSTRAINT_KEYWORDS):
            continue
        first_token = part.split(None, 1)[0]
        if first_token.strip('`"[]') == column_name:
            return part
    return None


def _recover_column_ddl(
    records: List[Dict[str, Any]], table_name: str, column_name: str, before_id: int
) -> Optional[str]:
    """还原被删列的 ADD COLUMN 语句（优先复用历史 add_column，其次解析建表 DDL）。"""
    best_add: Optional[str] = None
    best_create: Optional[str] = None
    for rec in records:
        if rec["id"] >= before_id:
            break
        if rec["table_name"] != table_name or not rec["ddl_sql"]:
            continue
        if rec["operation"] == _OP_ADD_COLUMN and rec["object_name"] == column_name:
            best_add = rec["ddl_sql"]
        elif rec["operation"] == _OP_CREATE_TABLE:
            best_create = rec["ddl_sql"]

    if best_add:
        return best_add
    if best_create:
        column_def = _extract_column_from_create_ddl(best_create, column_name)
        if column_def:
            return f"ALTER TABLE {table_name} ADD COLUMN {column_def}"
    return None


def _recover_index_ddl(
    records: List[Dict[str, Any]], table_name: str, index_name: str, before_id: int
) -> Optional[str]:
    """还原被删索引的 CREATE INDEX 语句（复用历史 add_index 记录）。"""
    best_add: Optional[str] = None
    for rec in records:
        if rec["id"] >= before_id:
            break
        if (rec["operation"] == _OP_ADD_INDEX and rec["table_name"] == table_name
                and rec["object_name"] == index_name and rec["ddl_sql"]):
            best_add = rec["ddl_sql"]
    return best_add


def build_reverse_statement(
    dialect_name: str, record: Dict[str, Any], records: List[Dict[str, Any]]
) -> Optional[str]:
    """为一条变更记录生成反向 DDL；无法生成时返回 None。"""
    operation = record["operation"]
    table = record["table_name"]
    name = record["object_name"]

    if operation == _OP_CREATE_TABLE:
        return f"DROP TABLE {table}"
    if operation == _OP_ADD_COLUMN:
        return f"ALTER TABLE {table} DROP COLUMN {name}"
    if operation == _OP_ADD_INDEX:
        # MySQL/MariaDB 删除索引需要带 ON <table>
        if dialect_name in ("mysql", "mariadb"):
            return f"DROP INDEX {name} ON {table}"
        return f"DROP INDEX {name}"
    if operation == _OP_DROP_COLUMN:
        return _recover_column_ddl(records, table, name, record["id"])
    if operation == _OP_DROP_INDEX:
        return _recover_index_ddl(records, table, name, record["id"])
    return None


# ==================== 回滚计划 ====================

def build_rollback_plan(
    records: List[Dict[str, Any]], batch_id: str, dialect_name: str
) -> List[Dict[str, Any]]:
    """构建回滚计划（不执行，仅生成按执行顺序排列的步骤）。

    计划包含目标批次及其之后的所有批次，按记录 id 倒序执行（后发生的先撤销）。
    对于在本批次范围内被建表覆盖的表，其逐列/逐索引记录会被跳过（由 DROP TABLE 覆盖）。

    Returns:
        步骤列表，每项含 record / sql / action / reason
            按执行顺序排列
    """
    target = [r for r in records if r["batch_id"] == batch_id]
    if not target:
        raise ValueError(f"变更记录表中不存在 batch_id={batch_id}")

    min_id = min(r["id"] for r in target)
    scope = [r for r in records if r["id"] >= min_id]

    # 范围内将被整体删除的表：其余对该表的记录无需单独撤销
    dropped_tables = {
        r["table_name"] for r in scope if r["operation"] == _OP_CREATE_TABLE
    }

    plan: List[Dict[str, Any]] = []
    for rec in sorted(scope, key=lambda r: r["id"], reverse=True):
        step = {"record": rec, "sql": None, "action": "execute", "reason": None}

        if not rec["success"]:
            step["action"] = "skip"
            step["reason"] = "该记录执行失败，未产生实际变更"
        elif rec["operation"] == _OP_TABLE_ERROR:
            step["action"] = "skip"
            step["reason"] = "错误记录，无对应结构变更"
        elif rec["operation"] == _OP_CREATE_DATABASE:
            step["action"] = "skip"
            step["reason"] = "不自动删除数据库，如需回滚请人工处理"
        elif rec["operation"] == _OP_CREATE_TABLE:
            step["sql"] = f"DROP TABLE {rec['table_name']}"
        elif (rec["table_name"] in dropped_tables
              or rec["note"] == _NOTE_CREATED_WITH_TABLE):
            step["action"] = "skip"
            step["reason"] = "该表将被整体删除，此变更被 DROP TABLE 覆盖"
        else:
            step["sql"] = build_reverse_statement(dialect_name, rec, records)
            if not step["sql"]:
                step["action"] = "skip"
                step["reason"] = "无法从历史记录还原反向 DDL（缺少列/索引定义）"

        plan.append(step)
    return plan


# ==================== 执行回滚 ====================

def _already_in_target_state(engine, record: Dict[str, Any]) -> Optional[str]:
    """检查对象是否已处于回滚目标状态，是则返回跳过原因，否则返回 None。"""
    operation = record["operation"]
    table = record["table_name"]
    name = record["object_name"]
    inspector = inspect(engine)

    if operation == _OP_CREATE_TABLE:
        return f"表 {table} 已不存在" if not inspector.has_table(table) else None

    if not inspector.has_table(table):
        return f"表 {table} 已不存在"

    if operation == _OP_ADD_COLUMN:
        if name not in {c["name"] for c in inspector.get_columns(table)}:
            return f"列 {table}.{name} 已不存在"
    elif operation == _OP_ADD_INDEX:
        if name not in {i["name"] for i in inspector.get_indexes(table)}:
            return f"索引 {table}.{name} 已不存在"
    elif operation == _OP_DROP_COLUMN:
        if name in {c["name"] for c in inspector.get_columns(table)}:
            return f"列 {table}.{name} 已存在"
    elif operation == _OP_DROP_INDEX:
        if name in {i["name"] for i in inspector.get_indexes(table)}:
            return f"索引 {table}.{name} 已存在"
    return None


def execute_rollback(
    engine, plan: List[Dict[str, Any]], dry_run: bool = True
) -> List[Dict[str, Any]]:
    """按计划执行（或预演）回滚，返回每步的执行结果。"""
    results: List[Dict[str, Any]] = []
    for step in plan:
        outcome = {"step": step, "status": "skipped", "detail": step["reason"]}

        if step["action"] != "execute":
            results.append(outcome)
            continue

        skip_reason = _already_in_target_state(engine, step["record"])
        if skip_reason:
            outcome["detail"] = skip_reason
            results.append(outcome)
            continue

        if dry_run:
            outcome["status"] = "pending"
            outcome["detail"] = "预演未执行"
            results.append(outcome)
            continue

        try:
            with engine.begin() as conn:
                conn.exec_driver_sql(step["sql"])
            outcome["status"] = "ok"
            outcome["detail"] = None
        except Exception as exc:  # noqa: BLE001 - 逐条回报，不中断整体回滚
            outcome["status"] = "failed"
            outcome["detail"] = str(exc)
        results.append(outcome)
    return results


# ==================== 命令行入口 ====================

def _print_batches(engine, records: List[Dict[str, Any]]) -> None:
    print("=" * 100)
    print(f"变更记录表: {_CHANGE_LOG_TABLE_NAME}    数据库: {_mask_url(str(engine.url))}")
    print("=" * 100)
    print(f"共 {len(records)} 条记录\n")
    for idx, (batch_id, rows) in enumerate(group_by_batch(records), 1):
        head = rows[0]
        ops: Dict[str, int] = {}
        for rec in rows:
            ops[rec["operation"]] = ops.get(rec["operation"], 0) + 1
        ops_str = "  ".join(f"{k}={v}" for k, v in sorted(ops.items()))
        print(f"批次 {idx}  batch_id={batch_id}")
        print(f"  执行时间: {head['executed_at']}   目标库: {head['database_name']}"
              f"   执行者: {head['operator'] or '-'}")
        print(f"  记录数: {len(rows)}   变更: {ops_str}")
        print()


def _print_plan(plan: List[Dict[str, Any]], dry_run: bool) -> None:
    print("-" * 100)
    print(f"回滚计划（{'预演，不执行' if dry_run else '实际执行'}）: 共 {len(plan)} 条记录，按 id 倒序")
    print("-" * 100)
    print(f"  {'id':>4}  {'表名':<28} {'operation':<14} {'object_name':<26} 反向 DDL / 说明")
    for step in plan:
        rec = step["record"]
        text = step["sql"] if step["sql"] else f"[跳过] {step['reason']}"
        print(f"  {rec['id']:>4}  {(rec['table_name'] or '-'):<28} {rec['operation']:<14} "
              f"{(rec['object_name'] or '-'):<26} {' '.join(text.split())}")
    print()


def _print_results(results: List[Dict[str, Any]]) -> None:
    counters: Dict[str, int] = {}
    print("=" * 100)
    print("执行结果")
    print("=" * 100)
    for item in results:
        rec = item["step"]["record"]
        counters[item["status"]] = counters.get(item["status"], 0) + 1
        detail = item["detail"] or ""
        print(f"  #{rec['id']:<4} {rec['operation']:<14} {(rec['table_name'] or '-'):<28} "
              f"{item['status']:<8} {' '.join(detail.split())}")
    print()
    summary = "  ".join(f"{k}={v}" for k, v in sorted(counters.items()))
    print(f"汇总: {summary}")


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="根据变更记录表（table_schema_change_log）的 batch_id 回滚数据库结构"
    )
    parser.add_argument("--db", required=True, help="数据库连接字符串（含变更记录表的库）")
    parser.add_argument("--batch-id", help="回滚目标批次：恢复到该批次执行之前（含该批次及其后所有批次）")
    parser.add_argument("--list", action="store_true", help="仅列出所有批次，不做回滚")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true",
                      help="仅预演并打印将要执行的 SQL，不落库（默认行为）")
    mode.add_argument("--execute", action="store_true", help="实际执行回滚")
    parser.add_argument("--echo", action="store_true", help="打印 SQL 日志")
    args = parser.parse_args(argv)

    if not args.list and not args.batch_id:
        parser.error("需要 --batch-id 指定回滚目标批次，或使用 --list 查看所有批次")

    from sqlalchemy import create_engine

    if args.db.startswith("sqlite"):
        engine = create_engine(
            args.db, echo=args.echo, connect_args={"check_same_thread": False}
        )
    else:
        engine = create_engine(args.db, echo=args.echo)

    try:
        records = load_change_log_records(engine)
    except RuntimeError as exc:
        print(f"[错误] {exc}")
        return 1

    if not records:
        print("变更记录表为空，无变更可回滚。")
        return 0

    if args.list:
        _print_batches(engine, records)
        return 0

    try:
        plan = build_rollback_plan(records, args.batch_id, engine.dialect.name)
    except ValueError as exc:
        print(f"[错误] {exc}")
        print("可执行 --list 查看所有可用批次。")
        return 1

    dry_run = not args.execute
    _print_plan(plan, dry_run=dry_run)
    results = execute_rollback(engine, plan, dry_run=dry_run)
    _print_results(results)

    failed = sum(1 for r in results if r["status"] == "failed")
    if failed:
        print(f"\n[警告] 有 {failed} 条反向 DDL 执行失败，请检查上方明细。")
        return 2
    if dry_run:
        print("\n以上为预演结果。确认无误后追加 --execute 实际执行。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
