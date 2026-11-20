"""小数列（DECIMAL）精度迁移：试运行、执行、续做与目标表核对。

背景：旧版 Schema同步 把 MySQL 的 DECIMAL 一律固定成 Hive 的
DECIMAL(38,18)。现在源库 introspection 能拿到完整精度，迁移目标是把
目标列改成和源一致的 DECIMAL(p,s)，同时保证已有存量值一个都不能被截断
或舍入。

关键约束：
1. 迁移前逐列检查整数位和小数位，任何超限的存量值都让该列阻塞；
2. 同一个发布批次里，存量保真和整批切换会冲突：整批切换模式下哪怕只有
   amount 阻塞，fee 也不能单独上线；逐列模式下 fee 可以独立上线；
3. 已上线的映射按原版本复查；续做时只重试未生效的列和语句；
4. 每次执行结果回写列变更清单，供下一次部署查询原定义。
"""
from __future__ import annotations

import copy
from datetime import datetime
from typing import Any

from app.decimal_seed import DEMO_BATCH
from app.decimal_types import DecimalType
from app.pipeline import pipeline_bus
from app.services import column_ledger

MAPPING_KIND = "保留源精度"

STATUS_PENDING = "待执行"
STATUS_BLOCKED = "阻塞"
STATUS_EFFECTIVE = "已生效"
STATUS_SKIPPED = "已跳过"
STATUS_FAILED = "执行失败"


def _now() -> str:
    return datetime.now().isoformat()


class DecimalMigrationService:
    """DECIMAL 列迁移编排器；状态保存在进程内，便于演示和接口复核。"""

    def __init__(self) -> None:
        self.reset()

    # ------------------------------------------------------------------ 状态
    def reset(self) -> None:
        self._batches: dict[str, dict[str, Any]] = {}
        self._targets: dict[str, dict[str, Any]] = {}
        self._records: dict[str, dict[str, Any]] = {}
        self._runs: dict[str, list[dict[str, Any]]] = {}
        pipeline_bus.decimal_migrations.clear()
        column_ledger.clear_column_changes()
        self.ensure_batch(copy.deepcopy(DEMO_BATCH))

    def ensure_batch(self, batch: dict[str, Any]) -> dict[str, Any]:
        """登记批次；首次登记时从存量目标表快照初始化目标状态。"""
        batch_id = str(batch.get("batch_id") or f"batch_{len(self._batches) + 1}")
        if batch_id in self._batches:
            return copy.deepcopy(self._batches[batch_id])

        normalized = {
            "batch_id": batch_id,
            "name": batch.get("name", batch_id),
            "source_table": batch.get("source_table", ""),
            "target_table": batch.get("target_table", ""),
            "release_version": int(batch.get("release_version", 2)),
            "ddl_record_id": batch.get("ddl_record_id", ""),
            "strict_batch": bool(batch.get("strict_batch", False)),
            "columns": [dict(column) for column in batch.get("columns", [])],
            "downstreams": [dict(item) for item in batch.get("downstreams", [])],
        }
        self._batches[batch_id] = normalized

        target_table = normalized["target_table"]
        if target_table not in self._targets:
            self._targets[target_table] = {
                "table": target_table,
                "columns": dict(batch.get("target_columns", {})),
                "rows": [dict(row) for row in batch.get("target_rows", [])],
            }

        # 旧版映射（v1，固定 DECIMAL(38,18)）视为已经上线，原始定义入台账。
        for column in normalized["columns"]:
            column_ledger.record_original_definition(
                table=target_table,
                column=column["column"],
                source_definition=column.get("source_definition")
                or f"`{column['column']}` {column.get('source_type', '')}",
                ddl_record_id=normalized["ddl_record_id"],
                database=normalized["source_table"].split(".")[0] if "." in normalized["source_table"] else "",
                mapping_type=column.get("legacy_target_type", "DECIMAL(38,18)"),
                mapping_version=1,
                effective=True,
            )
        return copy.deepcopy(normalized)

    def get_batch(self, batch_id: str | None = None) -> dict[str, Any]:
        batch = self._batches.get(batch_id or next(iter(self._batches)))
        if batch is None:
            raise KeyError(f"批次 {batch_id} 不存在")
        return batch

    def list_batches(self) -> list[dict[str, Any]]:
        return [copy.deepcopy(batch) for batch in self._batches.values()]

    # ------------------------------------------------------------ 源端 introspect
    def introspect_source(self, batch_id: str | None = None) -> dict[str, Any]:
        """读取源库列定义（完整 DECIMAL(p,s)），并刷新列变更台账。"""
        batch = self.get_batch(batch_id)
        target_table = batch["target_table"]
        columns: list[dict[str, Any]] = []
        for column in batch["columns"]:
            definition = column.get("source_definition") or f"`{column['column']}` {column.get('source_type', '')}"
            entry = column_ledger.record_original_definition(
                table=target_table,
                column=column["column"],
                source_definition=definition,
                ddl_record_id=batch["ddl_record_id"],
                database=batch["source_table"].split(".")[0] if "." in batch["source_table"] else "",
            )
            parsed = DecimalType.parse(entry["source_type"])
            columns.append({
                "column": column["column"],
                "source_definition": entry["source_definition"],
                "source_type": entry["source_type"],
                "precision": parsed.precision if parsed else None,
                "scale": parsed.scale if parsed else None,
                "legacy_target_type": column.get("legacy_target_type", "DECIMAL(38,18)"),
            })
        return {"source_table": batch["source_table"], "columns": columns}

    # ---------------------------------------------------------------- 迁移记录
    def _record_id(self, batch: dict[str, Any], column: str, version: int) -> str:
        return f"dcm_{batch['batch_id']}_{column}_v{version}"

    def generate_records(
        self,
        batch: dict[str, Any],
        columns: list[dict[str, Any]] | None = None,
        version: int | None = None,
    ) -> list[dict[str, Any]]:
        """生成新版小数列迁移记录（每个批次+列+版本一条，重复调用不新增）。"""
        version = int(version or batch["release_version"])
        created: list[dict[str, Any]] = []
        for column in columns or batch["columns"]:
            source_type = DecimalType.parse(column.get("source_type"))
            target_type = str(source_type) if source_type else column.get("source_type", "")
            record_id = self._record_id(batch, column["column"], version)
            record = self._records.get(record_id)
            if record is None:
                record = {
                    "record_id": record_id,
                    "batch_id": batch["batch_id"],
                    "release_version": version,
                    "source_table": batch["source_table"],
                    "target_table": batch["target_table"],
                    "column": column["column"],
                    "source_type": column.get("source_type", ""),
                    "source_definition": column.get("source_definition", ""),
                    "legacy_target_type": column.get("legacy_target_type", "DECIMAL(38,18)"),
                    "target_type": target_type,
                    "mapping_kind": MAPPING_KIND,
                    "status": STATUS_PENDING,
                    "effective": False,
                    "statement": self.build_statement(batch, column, target_type),
                    "rollback_statement": self.build_statement(
                        batch, column, column.get("legacy_target_type", "DECIMAL(38,18)"),
                    ),
                    "generated_at": _now(),
                    "executed_at": None,
                    "value_check": None,
                }
                self._records[record_id] = record
                pipeline_bus.decimal_migrations.append(record)
            created.append(copy.deepcopy(record))
        return created

    def migration_records(self, batch_id: str | None = None) -> list[dict[str, Any]]:
        records = list(self._records.values())
        if batch_id:
            records = [record for record in records if record["batch_id"] == batch_id]
        return copy.deepcopy(records)

    def build_statement(self, batch: dict[str, Any], column: dict[str, Any], target_type: str) -> str:
        """Hive 目标端语句：保留源 DECIMAL(p,s)，用 CHANGE COLUMN 原地改类型。"""
        name = column["column"]
        return f"ALTER TABLE {batch['target_table']} CHANGE COLUMN {name} {name} {target_type};"

    # ------------------------------------------------------------ 存量值检查
    def check_column_values(
        self,
        target: dict[str, Any],
        column: dict[str, Any],
        target_type: str,
    ) -> dict[str, Any]:
        """逐值检查整数位、小数位，不截断不舍入。"""
        decimal_type = DecimalType.parse(target_type)
        if decimal_type is None:
            return {
                "total": len(target["rows"]),
                "checked": 0,
                "blocked": 0,
                "failures": [],
                "ok": False,
                "reason": f"目标类型 {target_type} 不是可校验的 DECIMAL",
            }
        failures: list[dict[str, Any]] = []
        checked = 0
        for row in target["rows"]:
            if column["column"] not in row:
                continue
            checked += 1
            inspected = decimal_type.inspect_value(row[column["column"]])
            if not inspected["ok"]:
                failures.append({"row_id": row.get("id"), **inspected})
        return {
            "total": checked,
            "checked": checked,
            "blocked": len(failures),
            "failures": failures,
            "integer_checked_first": True,
            "ok": len(failures) == 0,
            "reason": "" if not failures else failures[0]["reason"],
        }

    # ------------------------------------------------------------- 下游依赖
    def downstreams_reading(self, batch: dict[str, Any], column: str) -> list[dict[str, Any]]:
        return [
            downstream for downstream in batch["downstreams"]
            if column in downstream.get("reads", [])
        ]

    def _plan_delivery(
        self,
        batch: dict[str, Any],
        column_reports: list[dict[str, Any]],
        *,
        strict: bool,
    ) -> dict[str, Any]:
        """计算本次发布实际能提供的新版列。"""
        passing = {report["column"] for report in column_reports if report["status"] != STATUS_BLOCKED}
        blocked = {report["column"] for report in column_reports if report["status"] == STATUS_BLOCKED}

        if strict and blocked:
            deliverable = set()
        elif strict:
            deliverable = set(passing)
        else:
            deliverable = {
                column for column in passing
                if all(
                    not (set(downstream.get("reads", [])) & blocked)
                    for downstream in self.downstreams_reading(batch, column)
                )
            }

        blocked_downstreams = [
            downstream["name"]
            for downstream in batch["downstreams"]
            if set(downstream.get("reads", [])) & blocked
        ]
        if strict and blocked:
            reason = "整批切换要求所有列同时生效，存在阻塞列时不发布任何新版列"
        elif strict:
            reason = ""
        elif blocked and not deliverable:
            reason = "所有可迁移列的下游都同时依赖阻塞列，无法安全切换"
        else:
            reason = ""
        return {
            "mode": "整批切换" if strict else "逐列切换",
            "deliverable_columns": sorted(deliverable),
            "blocked_columns": sorted(blocked),
            "blocked_downstreams": sorted(set(blocked_downstreams)),
            "reason": reason,
        }

    # --------------------------------------------------------------- 试运行
    def dry_run(
        self,
        batch_id: str | None = None,
        *,
        strict_batch: bool | None = None,
        target_table: str | None = None,
    ) -> dict[str, Any]:
        """试运行整批：检查位宽、生成新版迁移记录、给出可发布列。"""
        batch = self.get_batch(batch_id)
        strict = batch["strict_batch"] if strict_batch is None else bool(strict_batch)
        target = self._target_for(batch, target_table)
        self.introspect_source(batch["batch_id"])
        records = self.generate_records(batch)
        record_by_column = {record["column"]: record for record in records}

        column_reports: list[dict[str, Any]] = []
        for column in batch["columns"]:
            source_decimal = DecimalType.parse(column.get("source_type"))
            target_type = str(source_decimal) if source_decimal else column.get("source_type", "")
            definition_errors = source_decimal.definition_errors() if source_decimal else [
                f"{column.get('source_type')} 不是 DECIMAL"
            ]
            value_check = self.check_column_values(target, column, target_type)
            status = STATUS_PENDING
            if definition_errors or not value_check["ok"]:
                status = STATUS_BLOCKED
            record = record_by_column[column["column"]]
            record["value_check"] = {
                key: value for key, value in value_check.items() if key != "failures"
            }
            record["value_check"]["blocked_values"] = [
                {"row_id": failure["row_id"], "value": failure["value"],
                 "failed_check": failure["failed_check"], "reason": failure["reason"]}
                for failure in value_check["failures"]
            ]
            # 已生效的列不被新一轮试运行降级回「待执行」，否则续做会重复执行。
            already_effective = (
                record.get("effective")
                and target["columns"].get(column["column"]) == target_type
            )
            if status == STATUS_BLOCKED:
                record["status"] = STATUS_BLOCKED
            elif already_effective:
                record["status"] = STATUS_EFFECTIVE
            else:
                record["status"] = STATUS_PENDING
            record["definition_errors"] = definition_errors
            column_reports.append({
                "column": column["column"],
                "source_type": column.get("source_type"),
                "legacy_target_type": column.get("legacy_target_type", "DECIMAL(38,18)"),
                "target_type": target_type,
                "mapping_kind": MAPPING_KIND,
                "definition_errors": definition_errors,
                "value_check": value_check,
                "status": status,
                "downstreams": [item["name"] for item in self.downstreams_reading(batch, column["column"])],
                "record_id": record["record_id"],
                "statement": record["statement"],
            })

        delivery = self._plan_delivery(batch, column_reports, strict=strict)
        alternative = self._plan_delivery(batch, column_reports, strict=not strict)
        report = {
            "batch_id": batch["batch_id"],
            "batch_name": batch["name"],
            "source_table": batch["source_table"],
            "target_table": batch["target_table"],
            "release_version": batch["release_version"],
            "dry_run": True,
            "strict_batch": strict,
            "generated_at": _now(),
            "columns": column_reports,
            "delivery": delivery,
            "conflict": {
                "summary": (
                    "存量保真优先：阻塞列不做截断/舍入，发布能提供的列由切换模式决定"
                ),
                "strict_batch_alternative": alternative,
                "note": (
                    "fee 的下游只读 fee，因此逐列模式下 fee 可以独立上线；"
                    "整批模式下一旦 amount 阻塞，fee 也必须一起等待"
                ),
            },
            "migration_records": [copy.deepcopy(record_by_column[column["column"]]) for column in batch["columns"]],
        }
        return report

    # --------------------------------------------------------------- 执行
    def execute(
        self,
        batch_id: str | None = None,
        *,
        columns: list[str] | None = None,
        strict_batch: bool | None = None,
        target_table: str | None = None,
    ) -> dict[str, Any]:
        """执行迁移：默认只执行试运行判定为可发布的列。

        每一列执行前重新做位宽检查；阻塞列不写目标表、不改存量值，
        只把失败结果回写列变更清单。
        """
        batch = self.get_batch(batch_id)
        strict = batch["strict_batch"] if strict_batch is None else bool(strict_batch)
        target = self._target_for(batch, target_table)
        dry = self.dry_run(batch["batch_id"], strict_batch=strict, target_table=target_table)
        deliverable = set(dry["delivery"]["deliverable_columns"])
        blocked_columns = {report["column"] for report in dry["columns"] if report["status"] == STATUS_BLOCKED}
        if columns is None:
            # 默认把批次里的每一列都走一遍：可发布的列真正执行，阻塞列留失败痕迹，
            # 这样执行结果就是完整的「实际失败范围」。
            requested = [column["column"] for column in batch["columns"]]
        else:
            requested = list(columns)
        known_columns = {column["column"] for column in batch["columns"]}
        unknown = [column for column in requested if column not in known_columns]
        if unknown:
            raise ValueError(f"批次 {batch['batch_id']} 不含列：{'、'.join(unknown)}")

        version = int(batch["release_version"])
        results: list[dict[str, Any]] = []
        for column in batch["columns"]:
            name = column["column"]
            if name not in requested:
                continue
            record_id = self._record_id(batch, name, version)
            record = self._records[record_id]
            target_type = record["target_type"]
            current_type = target["columns"].get(name)
            value_check = self.check_column_values(target, column, target_type)
            definition_errors = record.get("definition_errors") or []

            if strict and name not in deliverable:
                reason = (
                    "整批切换模式下列不能单独上线"
                    f"（批次阻塞列：{'、'.join(sorted(blocked_columns)) or '无'}）"
                )
                result = self._result(name, STATUS_BLOCKED, target_type, current_type,
                                      False, reason, value_check)
            elif current_type == target_type and record.get("effective"):
                result = self._result(name, STATUS_SKIPPED, target_type, current_type,
                                      False, "该列在版本 v%d 已生效，跳过" % version, value_check)
            elif definition_errors or not value_check["ok"]:
                reason = definition_errors[0] if definition_errors else value_check["reason"]
                result = self._result(name, STATUS_BLOCKED, target_type, current_type,
                                      False, f"存量值不满足目标精度：{reason}", value_check)
            else:
                target["columns"][name] = target_type
                result = self._result(name, STATUS_EFFECTIVE, target_type, current_type,
                                      True, "列类型已按源精度生效，存量值未改写", value_check)

            result["statement"] = record["statement"]
            record["status"] = result["status"]
            # 「已跳过」表示这一列本来就已经生效，不能把 effective 打回 False，
            # 否则续做/重跑会把它当成未生效列重复执行。
            record["effective"] = result["effective"] or result["status"] == STATUS_SKIPPED
            record["executed_at"] = _now()
            record["current_type"] = current_type
            record["value_check"] = {
                "total": value_check["total"],
                "blocked": value_check["blocked"],
            }
            column_ledger.write_execution_result(
                table=batch["target_table"],
                column=name,
                result=result,
                version=version,
                effective_type=target_type if result["effective"] else None,
            )
            results.append(result)

        run = {
            "batch_id": batch["batch_id"],
            "version": version,
            "mode": "整批切换" if strict else "逐列切换",
            "executed_at": _now(),
            "requested": requested,
            "results": copy.deepcopy(results),
        }
        self._runs.setdefault(batch["batch_id"], []).append(run)
        return {
            "ok": all(result["status"] in (STATUS_EFFECTIVE, STATUS_SKIPPED) for result in results),
            "batch_id": batch["batch_id"],
            "release_version": version,
            "strict_batch": strict,
            "executed": [result for result in results if result["status"] == STATUS_EFFECTIVE],
            "skipped": [result for result in results if result["status"] == STATUS_SKIPPED],
            "blocked": [result for result in results if result["status"] == STATUS_BLOCKED],
            "results": results,
            "deliverable_columns": sorted(deliverable),
            "target": self.target_snapshot(batch["target_table"]),
        }

    def _result(
        self,
        column: str,
        status: str,
        target_type: str,
        current_type: str | None,
        effective: bool,
        message: str,
        value_check: dict[str, Any],
    ) -> dict[str, Any]:
        return {
            "column": column,
            "status": status,
            "target_type": target_type,
            "current_type": current_type,
            "effective": effective,
            "message": message,
            "value_check": {
                "total": value_check["total"],
                "blocked": value_check["blocked"],
                "failures": copy.deepcopy(value_check["failures"]),
            },
        }

    # --------------------------------------------------------------- 续做
    def recheck_online_mappings(self, batch_id: str | None = None) -> list[dict[str, Any]]:
        """按记录中的原版本复查已上线映射：类型是否仍是那一版的类型、值是否仍装得下。"""
        batch = self.get_batch(batch_id)
        target = self._target_for(batch)
        checks: list[dict[str, Any]] = []
        ledger = pipeline_bus.column_changes.get(batch["target_table"], {})
        for name, entry in ledger.items():
            declared = entry.get("mapping_type")
            version = entry.get("mapping_version")
            actual = target["columns"].get(name)
            decimal_type = DecimalType.parse(declared)
            value_ok = True
            blocked = 0
            if decimal_type is not None:
                check = self.check_column_values(target, {"column": name}, str(decimal_type))
                value_ok = check["ok"]
                blocked = check["blocked"]
            type_ok = declared == actual
            checks.append({
                "column": name,
                "mapping_version": version,
                "declared_type": declared,
                "actual_type": actual,
                "original_definition": entry.get("source_definition"),
                "type_ok": type_ok,
                "values_ok": value_ok,
                "blocked_values": blocked,
                "online": bool(type_ok and value_ok),
            })
        return checks

    def resume(
        self,
        batch_id: str | None = None,
        *,
        target_table: str | None = None,
    ) -> dict[str, Any]:
        """恢复：先按原版本复查，再只重试未生效的列和语句。"""
        batch = self.get_batch(batch_id)
        target = self._target_for(batch, target_table)
        checks = self.recheck_online_mappings(batch["batch_id"])
        version = int(batch["release_version"])
        retry: list[str] = []
        skipped: list[dict[str, Any]] = []
        for column in batch["columns"]:
            name = column["column"]
            record = self._records.get(self._record_id(batch, name, version))
            expected_type = record["target_type"] if record else column.get("source_type")
            if target["columns"].get(name) == expected_type and record and record.get("effective"):
                skipped.append({
                    "column": name,
                    "status": STATUS_SKIPPED,
                    "reason": "已在原版本生效，续做不重复执行",
                    "target_type": expected_type,
                })
            else:
                retry.append(name)

        execution = self.execute(
            batch["batch_id"], columns=retry, target_table=target_table,
        ) if retry else {
            "ok": True, "executed": [], "skipped": [], "blocked": [],
            "results": [],
        }
        return {
            "ok": execution["ok"],
            "batch_id": batch["batch_id"],
            "rechecked": checks,
            "retried_columns": retry,
            "skipped_columns": skipped,
            "execution": execution,
        }

    # --------------------------------------------------------------- 下游查询
    def query_downstream(
        self,
        downstream: str,
        batch_id: str | None = None,
    ) -> dict[str, Any]:
        """模拟下游按列查询：只返回该下游声明的列，值保持原样。"""
        batch = self.get_batch(batch_id)
        target = self._target_for(batch)
        match = next(
            (item for item in batch["downstreams"] if item["name"] == downstream),
            None,
        )
        if match is None:
            raise KeyError(f"下游 {downstream} 不在批次 {batch['batch_id']} 中")
        reads = list(match.get("reads", []))
        rows = [
            {"id": row.get("id"), **{column: row.get(column) for column in reads}}
            for row in target["rows"]
        ]
        mapping_status = {
            column: {
                "target_type": target["columns"].get(column),
                "source_type": next(
                    (item.get("source_type") for item in batch["columns"]
                     if item["column"] == column), None
                ),
                "effective": target["columns"].get(column) == next(
                    (str(DecimalType.parse(item.get("source_type")))
                     for item in batch["columns"] if item["column"] == column), None
                ),
            }
            for column in reads
        }
        return {
            "downstream": downstream,
            "reads": reads,
            "sql": f"SELECT {', '.join(reads)} FROM {batch['target_table']}",
            "mapping_status": mapping_status,
            "row_count": len(rows),
            "rows": rows,
        }

    # --------------------------------------------------------------- 目标核对
    def target_snapshot(self, table: str) -> dict[str, Any]:
        target = self._targets.get(table, {"table": table, "columns": {}, "rows": []})
        return copy.deepcopy(target)

    def verify_target(self, batch_id: str | None = None) -> dict[str, Any]:
        """核对目标表列定义、存量值、类型冲突。"""
        batch = self.get_batch(batch_id)
        target = self._target_for(batch)
        checks: list[dict[str, Any]] = []
        for column in batch["columns"]:
            name = column["column"]
            source_type = DecimalType.parse(column.get("source_type"))
            expected = str(source_type) if source_type else column.get("source_type")
            actual = target["columns"].get(name)
            value_check = self.check_column_values(target, column, expected)
            definition_errors = source_type.definition_errors() if source_type else []
            type_ok = actual == expected
            conflicts: list[str] = []
            if actual != expected:
                actual_type = DecimalType.parse(actual)
                if actual_type and source_type:
                    if actual_type.integer_digits < source_type.integer_digits:
                        conflicts.append(
                            f"目标整数位 {actual_type.integer_digits} 小于源 {source_type.integer_digits}，存量值可能截断"
                        )
                    if actual_type.scale != source_type.scale:
                        conflicts.append(
                            f"目标小数位 {actual_type.scale} 与源 {source_type.scale} 不一致，存在舍入风险"
                        )
                conflicts.append(f"目标类型 {actual} 未按源精度 {expected} 生效")
            if value_check["blocked"]:
                conflicts.append(
                    f"{value_check['blocked']} 条存量值装不进 {expected}，不得截断或舍入"
                )
            checks.append({
                "column": name,
                "source_type": expected,
                "declared_target_type": actual,
                "types_match": type_ok,
                "definition_errors": definition_errors,
                "value_check": {
                    "total": value_check["total"],
                    "blocked": value_check["blocked"],
                    "failures": value_check["failures"],
                },
                "conflicts": conflicts,
                "fidelity_ok": not conflicts,
            })
        return {
            "batch_id": batch["batch_id"],
            "target_table": batch["target_table"],
            "checked_at": _now(),
            "columns": checks,
            "row_count": len(target["rows"]),
            "rows": copy.deepcopy(target["rows"]),
            "all_fidelity_ok": all(check["fidelity_ok"] for check in checks),
        }

    def column_changes(self, table: str | None = None) -> dict[str, Any]:
        batch = self.get_batch()
        return column_ledger.column_changes(table or batch["target_table"])

    def runs(self, batch_id: str | None = None) -> list[dict[str, Any]]:
        if batch_id:
            return copy.deepcopy(self._runs.get(batch_id, []))
        return copy.deepcopy(self._runs)

    # ------------------------------------------------------------------ 内部
    def _target_for(self, batch: dict[str, Any], table: str | None = None) -> dict[str, Any]:
        name = table or batch["target_table"]
        if name not in self._targets:
            self._targets[name] = {"table": name, "columns": {}, "rows": []}
        return self._targets[name]


migration_service = DecimalMigrationService()
