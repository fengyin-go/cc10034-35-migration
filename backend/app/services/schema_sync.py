"""Schema同步领域服务：管理源表到目标表的列级Schema同步。

上游：ddl_tracker 产出列变更分析结果 → schema_sync 消费并生成同步策略
下游：format_adapter 参考同步后的列映射进行格式适配
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from app.services.base import BaseModuleService
from app.services import column_ledger
from app.decimal_types import DecimalType
from app.pipeline import pipeline_bus


class SchemaSyncService(BaseModuleService):
    MODULE = "schema_sync"
    ENTITY = "同步任务"
    OPERATION_TERM = "转换"
    LIST_FIELDS = ["任务名称", "源表", "目标表", "列映射", "类型转换", "同步模式", "冲突策略"]
    REQUIRED_FIELDS = ["任务名称", "源表", "目标表"]
    STATUS_ORDER = ["草稿", "同步中", "已完成", "同步失败"]
    ACTION_RULES = {"开始同步": "草稿", "完成同步": "同步中", "标记失败": "同步中"}
    NEGATIVE_ACTIONS = []

    def build_sync_plan(self, config: dict[str, Any]) -> dict[str, Any]:
        """根据源Schema和DDL变更结果构建同步计划。

        读取 ddl_tracker 产出的 schema_registry / 列变更清单中已登记的列变更，
        结合类型转换策略、冲突策略生成同步任务。
        """
        source_table = config.get("源表", "")
        target_table = config.get("目标表", "")
        type_conversion = config.get("类型转换", "直接映射")
        conflict_strategy = config.get("冲突策略", "覆盖目标")

        source_cols = pipeline_bus.schema_registry.get(source_table, [])
        col_mapping: list[dict[str, Any]] = []
        for col_info in source_cols:
            source_type = col_info.get("type") or col_info.get("source_definition", "")
            target_type = self.convert_type(source_type, type_conversion)
            col_mapping.append({
                "source_column": col_info.get("column"),
                "source_type": source_type,
                "source_definition": col_info.get("source_definition", ""),
                "target_type": target_type,
                "action": col_info.get("action"),
            })

        plan = {
            "task_name": config.get("任务名称"),
            "source_table": source_table,
            "target_table": target_table,
            "column_mapping": col_mapping,
            "mapping_count": len(col_mapping),
            "conflict_strategy": conflict_strategy,
            "generated_ddl": self.generate_sync_ddl(col_mapping, target_table),
            "estimated_impact": self.estimate_sync_impact(col_mapping),
            "generated_at": datetime.now().isoformat(),
        }
        return plan

    def convert_type(self, source_type: str, strategy: str) -> str:
        """类型转换策略执行。

        支持的转换：mysql->hive/postgresql/doris 等目标格式。

        DECIMAL 不再固定成 DECIMAL(38,18)：从源定义里解析出完整的
        DECIMAL(p,s) 并保留，只有解析不到精度时才回落到显式默认值。
        """
        source_upper = source_type.upper()
        decimal_type = DecimalType.parse(source_type)
        if decimal_type is not None or "DECIMAL" in source_upper:
            if decimal_type is None:
                # 无参 DECIMAL 按 MySQL 默认 DECIMAL(10,0) 处理，而不是 38,18。
                decimal_type = DecimalType(10, 0)
            errors = decimal_type.definition_errors(hive=strategy.lower() in ("hive", "直接映射"))
            if errors:
                return source_type
            return str(decimal_type)

        type_map = {
            "VARCHAR": {"直接映射": "STRING", "hive": "STRING", "doris": "VARCHAR(65533)"},
            "INT": {"直接映射": "INT", "hive": "INT", "doris": "INT"},
            "BIGINT": {"直接映射": "BIGINT", "hive": "BIGINT", "doris": "BIGINT"},
            "DATETIME": {"直接映射": "TIMESTAMP", "hive": "TIMESTAMP", "doris": "DATETIME"},
            "TEXT": {"直接映射": "STRING", "hive": "STRING", "doris": "STRING"},
            "JSON": {"直接映射": "STRING", "hive": "STRING", "doris": "JSON"},
        }
        lower_strategy = strategy.lower()
        for base, targets in type_map.items():
            if base in source_upper:
                return targets.get(lower_strategy, targets.get("直接映射", source_type))
        return "STRING"

    def estimate_sync_impact(self, col_mapping: list[dict[str, Any]]) -> dict[str, Any]:
        """估算同步对下游的影响范围。"""
        add_count = sum(1 for c in col_mapping if c.get("action") == "add")
        drop_count = sum(1 for c in col_mapping if c.get("action") == "drop")
        modify_count = sum(1 for c in col_mapping if c.get("action") == "modify")
        risk_level = "低"
        if drop_count > 0:
            risk_level = "高"
        elif modify_count > 0:
            risk_level = "中"
        return {
            "columns_added": add_count,
            "columns_dropped": drop_count,
            "columns_modified": modify_count,
            "risk_level": risk_level,
            "breaking_change": drop_count > 0,
        }

    def generate_sync_ddl(self, col_mapping: list[dict[str, Any]], target_table: str) -> str:
        """根据列映射生成目标DDL（Hive 语法，DECIMAL 保留源精度）。"""
        cols: list[str] = []
        for cm in col_mapping:
            if cm.get("action") == "drop":
                continue
            cols.append(f"  {cm['source_column']} {cm['target_type']}")
        if not cols:
            return f"-- 无新增列，目标表 {target_table} 不变"
        return f"ALTER TABLE {target_table} ADD COLUMNS (\n" + ",\n".join(cols) + "\n);"

    def build_hive_column_statements(
        self,
        col_mapping: list[dict[str, Any]],
        target_table: str,
        *,
        existing_columns: dict[str, str] | None = None,
    ) -> list[str]:
        """为已有列生成 Hive CHANGE COLUMN 语句；新列走 ADD COLUMNS。

        与 generate_sync_ddl 的区别：这里按目标表现有列定义选择语法，
        已存在的 DECIMAL 列不会被追加成第二列，而是原地改类型。
        """
        existing_columns = existing_columns or {}
        add_cols: list[str] = []
        statements: list[str] = []
        for cm in col_mapping:
            if cm.get("action") == "drop":
                continue
            name = cm["source_column"]
            target_type = cm["target_type"]
            if name in existing_columns:
                statements.append(
                    f"ALTER TABLE {target_table} CHANGE COLUMN {name} {name} {target_type};"
                )
            else:
                add_cols.append(f"  {name} {target_type}")
        if add_cols:
            statements.append(
                f"ALTER TABLE {target_table} ADD COLUMNS (\n" + ",\n".join(add_cols) + "\n);"
            )
        if not statements:
            return [f"-- 无新增列，目标表 {target_table} 不变"]
        return statements

    def generate_decimal_migration_records(
        self,
        source_table: str,
        target_table: str,
        *,
        release_version: int = 2,
        existing_columns: dict[str, str] | None = None,
    ) -> list[dict[str, Any]]:
        """基于列变更清单里的原始 DECIMAL(p,s) 生成新版小数列迁移记录。

        上游 DDL追踪 已经把数据库原始定义写进列变更清单；这里只负责
        转成目标 SQL 和迁移记录，不再假定 DECIMAL(38,18)。
        """
        ledger = column_ledger.column_changes(target_table).get(target_table, {})
        records: list[dict[str, Any]] = []
        for column, entry in ledger.items():
            source_type = DecimalType.parse(entry.get("source_type"))
            if source_type is None:
                continue
            target_type = str(source_type)
            record = {
                "record_id": f"dcm_{source_table}_{column}_v{release_version}",
                "source_table": source_table,
                "target_table": target_table,
                "column": column,
                "source_definition": entry.get("source_definition"),
                "source_type": target_type,
                "original_precision": source_type.precision,
                "original_scale": source_type.scale,
                "legacy_target_type": entry.get("mapping_type", "DECIMAL(38,18)"),
                "target_type": target_type,
                "mapping_kind": "保留源精度",
                "release_version": release_version,
                "statement": f"ALTER TABLE {target_table} CHANGE COLUMN {column} {column} {target_type};",
                "status": "待执行",
                "effective": False,
            }
            records.append(record)
        pipeline_bus.decimal_migrations.extend(records)
        return records

    def detect_type_conflict(self, col_name: str, source_type: str, target_type: str) -> dict[str, Any]:
        """检测列类型冲突；DECIMAL 要比整数位和小数位，不能只看类型名。"""
        has_conflict = source_type.upper() != target_type.upper()
        resolution = ""
        if has_conflict:
            source_decimal = DecimalType.parse(source_type)
            target_decimal = DecimalType.parse(target_type)
            if "VARCHAR" in source_type.upper() and "STRING" in target_type.upper():
                resolution = "字符串宽度调整，无数据丢失"
            elif "DECIMAL" in source_type.upper() and "BIGINT" in target_type.upper():
                resolution = "精度可能丢失，建议保留DECIMAL"
            elif source_decimal and target_decimal:
                if target_decimal.integer_digits < source_decimal.integer_digits:
                    resolution = "目标整数位不足，存量值可能被截断，禁止迁移"
                elif target_decimal.scale != source_decimal.scale:
                    resolution = "目标小数位与源不一致，存量值可能被舍入，禁止迁移"
                else:
                    resolution = "整数位与小数位一致，可无损迁移"
            else:
                resolution = "建议人工确认类型转换"
        return {
            "column": col_name,
            "source_type": source_type,
            "target_type": target_type,
            "conflict": has_conflict,
            "resolution": resolution,
        }

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry, msg = super().run_action(entry_id, action)
        if entry and action == "开始同步":
            source_table = entry.get("源表", "")
            existing = pipeline_bus.schema_registry.get(source_table)
            if existing:
                pipeline_bus.write_audit(
                    "Schema同步", source_table,
                    f"检测到 {len(existing)} 条Schema记录，生成列映射同步计划",
                    "schema_sync", "同步中"
                )
        return entry, msg