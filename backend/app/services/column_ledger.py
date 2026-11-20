"""列变更清单：跨 DDL追踪 / Schema同步 共享的列定义台账。

DDL追踪把数据库里的原始列定义（含 DECIMAL(p,s)）写进来，Schema同步
从这里读取并生成目标语句；语句执行结果再回写到同一条记录，下一次
部署就能查到原始定义、当前生效版本和上一次执行结果。
"""
from __future__ import annotations

import re
from datetime import datetime
from typing import Any

from app.decimal_types import DecimalType
from app.pipeline import pipeline_bus


def record_original_definition(
    *,
    table: str,
    column: str,
    source_definition: str,
    ddl_record_id: str = "",
    database: str = "",
    target_table: str | None = None,
    mapping_type: str | None = None,
    mapping_version: int = 1,
    effective: bool = True,
) -> dict[str, Any]:
    """登记/更新一列的数据库原始定义，保留 DECIMAL(p,s) 原样。

    已存在的记录不会被新的原始定义覆盖掉历史：原始定义只在首次捕获时
    写入，后续重复解析只刷新 captured_at / ddl_record_id，避免把下一次
    部署要查询的原定义冲掉。
    """
    entry = pipeline_bus.column_changes[table].get(column)
    parsed = DecimalType.parse(source_definition) or _first_type_token(source_definition)
    if entry is None:
        entry = {
            "table": table,
            "database": database,
            "column": column,
            "source_definition": source_definition,
            "source_type": str(parsed) if parsed else source_definition,
            "original_precision": parsed.precision if parsed else None,
            "original_scale": parsed.scale if parsed else None,
            "target_table": target_table or "",
            "mapping_type": mapping_type or (str(parsed) if parsed else ""),
            "mapping_version": mapping_version,
            "effective": effective,
            "captured_at": datetime.now().isoformat(),
            "ddl_record_id": ddl_record_id,
            "execution_results": [],
            "last_result": None,
        }
        pipeline_bus.column_changes[table][column] = entry
    else:
        entry["captured_at"] = datetime.now().isoformat()
        if ddl_record_id:
            entry["ddl_record_id"] = ddl_record_id
        if database:
            entry["database"] = database
        if target_table:
            entry["target_table"] = target_table
    return dict(entry)


def write_execution_result(
    *,
    table: str,
    column: str,
    result: dict[str, Any],
    version: int,
    effective_type: str | None = None,
) -> dict[str, Any] | None:
    """把一次迁移执行结果回写到列变更清单。

    只有真正生效（effective=True）时才推进 mapping_type / mapping_version；
    阻塞或失败的结果只留痕，不改变当前线上映射。
    """
    entry = pipeline_bus.column_changes.get(table, {}).get(column)
    if entry is None:
        return None
    stamped = {"version": version, "recorded_at": datetime.now().isoformat(), **result}
    entry.setdefault("execution_results", []).append(stamped)
    entry["last_result"] = stamped
    if stamped.get("effective"):
        entry["mapping_type"] = effective_type or stamped.get("target_type") or entry.get("mapping_type")
        entry["mapping_version"] = version
        entry["effective"] = True
    return dict(entry)


def definition_for(table: str, column: str) -> dict[str, Any] | None:
    """下一次部署查询原定义用：返回该列最近一次捕获的原始定义。"""
    entry = pipeline_bus.column_changes.get(table, {}).get(column)
    return dict(entry) if entry else None


def write_execution_result(
    *,
    table: str,
    column: str,
    result: dict[str, Any],
    version: int,
    effective_type: str | None = None,
) -> dict[str, Any] | None:
    """把一次迁移执行结果回写到列变更清单。

    只有真正生效（effective=True）时才推进 mapping_type / mapping_version；
    阻塞或失败的结果只留痕，不改变当前线上映射。
    """
    entry = pipeline_bus.column_changes.get(table, {}).get(column)
    if entry is None:
        return None
    stamped = {"version": version, "recorded_at": datetime.now().isoformat(), **result}
    entry.setdefault("execution_results", []).append(stamped)
    entry["last_result"] = stamped
    if stamped.get("effective"):
        entry["mapping_type"] = effective_type or stamped.get("target_type") or entry.get("mapping_type")
        entry["mapping_version"] = version
        entry["effective"] = True
    return dict(entry)


def column_changes(table: str | None = None) -> dict[str, Any]:
    """查询列变更清单；可按表过滤。"""
    if table:
        return {table: dict(pipeline_bus.column_changes.get(table, {}))}
    return {
        name: dict(columns)
        for name, columns in pipeline_bus.column_changes.items()
    }


def clear_column_changes(table: str | None = None) -> None:
    """清空台账（演示/测试重置用）。"""
    if table is None:
        pipeline_bus.column_changes.clear()
    else:
        pipeline_bus.column_changes.pop(table, None)


def capture_from_ddl(
    *,
    database: str,
    table: str,
    ddl_statement: str,
    ddl_record_id: str = "",
    mapped_table: str | None = None,
) -> list[dict[str, Any]]:
    """DDL追踪的落点：从 DDL 语句里按原样捕获 DECIMAL(p,s) 列定义。

    这里刻意不经过 convert_type，数据库写的是什么精度就登记什么精度；
    目标表的列定义由 Schema同步 另行生成。返回本次捕获的列。
    """
    captured: list[dict[str, Any]] = []
    target_table = mapped_table or table
    for name, definition in iter_decimal_definitions(ddl_statement):
        entry = record_original_definition(
            table=target_table,
            column=name,
            source_definition=definition,
            ddl_record_id=ddl_record_id,
            database=database,
            target_table=target_table,
        )
        captured.append(entry)
    return captured


_DECIMAL_COLUMN_RE = re.compile(
    r"`?([A-Za-z_][\w]*)`?\s+(DECIMAL\s*(?:\(\s*\d+\s*(?:,\s*\d+\s*)?\))?)",
    re.IGNORECASE,
)


def iter_decimal_definitions(ddl_statement: str) -> list[tuple[str, str]]:
    """提取 DDL 中的 DECIMAL 列及其原始定义（类型原样保留）。"""
    results: list[tuple[str, str]] = []
    seen: set[str] = set()
    for match in _DECIMAL_COLUMN_RE.finditer(ddl_statement):
        name = match.group(1)
        if name.casefold() in seen:
            continue
        seen.add(name.casefold())
        definition = " ".join(ddl_statement[match.start(1):match.end(2)].split())
        results.append((name, definition))
    return results


def _first_type_token(definition: str) -> DecimalType | None:
    """从完整列定义里提取 DECIMAL(...) 片段。"""
    upper = definition.upper()
    index = upper.find("DECIMAL")
    if index < 0:
        return None
    return DecimalType.parse(definition[index:].split()[0])
