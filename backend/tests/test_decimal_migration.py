"""小数列迁移的回归测试：位宽校验、台账、DDL 精度保留、批次冲突与续做。

用标准库 unittest，直接跑：
    cd backend && .venv/bin/python -m unittest discover -s tests -v
"""
from __future__ import annotations

import unittest
from decimal import Decimal

from app.decimal_types import DecimalType, fraction_digits_of, integer_digits_of
from app.pipeline import pipeline_bus
from app.services import column_ledger
from app.services.ddl_tracker import DdlTrackerService
from app.services.decimal_migration import migration_service
from app.services.schema_sync import SchemaSyncService


class DecimalTypeParseTest(unittest.TestCase):
    def test_parses_precision_and_scale(self):
        parsed = DecimalType.parse("DECIMAL(19,4)")
        self.assertIsNotNone(parsed)
        self.assertEqual((parsed.precision, parsed.scale), (19, 4))
        self.assertEqual(parsed.integer_digits, 15)
        self.assertEqual(str(parsed), "DECIMAL(19,4)")

    def test_bare_decimal_uses_mysql_default(self):
        parsed = DecimalType.parse("DECIMAL")
        self.assertEqual((parsed.precision, parsed.scale), (10, 0))

    def test_non_decimal_returns_none(self):
        self.assertIsNone(DecimalType.parse("VARCHAR(32)"))

    def test_hive_limit_rejects_65_30(self):
        errors = DecimalType.parse("DECIMAL(65,30)").definition_errors()
        self.assertTrue(any("超出 Hive 上限" in error for error in errors))


class ValueWidthTest(unittest.TestCase):
    def test_trailing_zeros_do_not_count_as_scale(self):
        self.assertEqual(fraction_digits_of(Decimal("1.2300")), 2)

    def test_integer_digits(self):
        self.assertEqual(integer_digits_of(Decimal("0.0001")), 0)
        self.assertEqual(integer_digits_of(Decimal("1000000000000000")), 16)

    def test_fits_boundary_values(self):
        target = DecimalType.parse("DECIMAL(19,4)")
        for value in ("123456789012345.6789", "-999999999999999.9999", "0.0001", "0"):
            with self.subTest(value=value):
                ok, reason = target.fits_value(value)
                self.assertTrue(ok, reason)

    def test_integer_overflow_is_reported_first(self):
        target = DecimalType.parse("DECIMAL(19,4)")
        ok, reason = target.fits_value("1234567890123456.7890")
        self.assertFalse(ok)
        self.assertIn("整数位 16", reason)

    def test_scale_overflow_is_not_rounded(self):
        target = DecimalType.parse("DECIMAL(19,4)")
        ok, reason = target.fits_value("12.34567")
        self.assertFalse(ok)
        self.assertIn("小数位 5", reason)

    def test_no_rounding_side_effect(self):
        """检查过程绝不能改写原值。"""
        value = Decimal("12.34567")
        DecimalType.parse("DECIMAL(19,4)").fits_value(value)
        self.assertEqual(value, Decimal("12.34567"))


class ColumnLedgerTest(unittest.TestCase):
    def setUp(self):
        column_ledger.clear_column_changes()

    def tearDown(self):
        column_ledger.clear_column_changes()

    def test_ddl_capture_keeps_original_precision(self):
        ddl = DdlTrackerService()
        ddl.record_ddl(
            "ods", "ods.order_detail",
            "ALTER TABLE ods.order_detail MODIFY COLUMN amount DECIMAL(19,4) NOT NULL, "
            "MODIFY COLUMN fee DECIMAL(10,2) NOT NULL",
        )
        entry = column_ledger.definition_for("ods.order_detail", "amount")
        self.assertEqual(entry["source_type"], "DECIMAL(19,4)")
        self.assertEqual(entry["original_precision"], 19)
        self.assertEqual(entry["original_scale"], 4)

    def test_original_definition_survives_reparse(self):
        column_ledger.capture_from_ddl(
            database="ods", table="ods.t", ddl_statement="ALTER TABLE ods.t ADD COLUMN a DECIMAL(19,4)",
        )
        column_ledger.capture_from_ddl(
            database="ods", table="ods.t", ddl_statement="ALTER TABLE ods.t ADD COLUMN a DECIMAL(20,6)",
        )
        entry = column_ledger.definition_for("ods.t", "a")
        self.assertEqual(entry["source_type"], "DECIMAL(19,4)")


class SchemaSyncDecimalTest(unittest.TestCase):
    def setUp(self):
        self.service = SchemaSyncService()

    def test_convert_type_preserves_precision(self):
        self.assertEqual(self.service.convert_type("DECIMAL(19,4)", "hive"), "DECIMAL(19,4)")
        self.assertEqual(self.service.convert_type("DECIMAL(10,2)", "直接映射"), "DECIMAL(10,2)")
        self.assertEqual(self.service.convert_type("DECIMAL", "hive"), "DECIMAL(10,0)")

    def test_conflict_flags_legacy_38_18(self):
        conflict = self.service.detect_type_conflict("amount", "DECIMAL(19,4)", "DECIMAL(38,18)")
        self.assertTrue(conflict["conflict"])
        self.assertIn("舍入", conflict["resolution"])

    def test_hive_statements_use_change_column_for_existing(self):
        statements = self.service.build_hive_column_statements(
            [{"source_column": "fee", "target_type": "DECIMAL(10,2)", "action": "modify"}],
            "dwd.dwd_order_detail",
            existing_columns={"fee": "DECIMAL(38,18)"},
        )
        self.assertEqual(
            statements,
            ["ALTER TABLE dwd.dwd_order_detail CHANGE COLUMN fee fee DECIMAL(10,2);"],
        )


class MigrationBatchTest(unittest.TestCase):
    def setUp(self):
        migration_service.reset()

    def tearDown(self):
        migration_service.reset()

    def test_dry_run_blocks_amount_and_clears_fee(self):
        report = migration_service.dry_run()
        by_column = {column["column"]: column for column in report["columns"]}
        self.assertEqual(by_column["amount"]["status"], "阻塞")
        self.assertEqual(by_column["fee"]["status"], "待执行")
        self.assertEqual(by_column["amount"]["value_check"]["blocked"], 3)
        self.assertEqual(by_column["fee"]["value_check"]["blocked"], 0)
        self.assertEqual(report["delivery"]["deliverable_columns"], ["fee"])

    def test_strict_batch_delivers_nothing(self):
        report = migration_service.dry_run(strict_batch=True)
        self.assertEqual(report["delivery"]["deliverable_columns"], [])
        self.assertEqual(
            report["conflict"]["strict_batch_alternative"]["deliverable_columns"], ["fee"],
        )

    def test_records_are_not_duplicated(self):
        migration_service.dry_run()
        migration_service.dry_run()
        records = migration_service.migration_records()
        self.assertEqual(len(records), 2)
        self.assertTrue(all(record["mapping_kind"] == "保留源精度" for record in records))

    def test_execute_column_mode_applies_only_fee(self):
        result = migration_service.execute()
        self.assertFalse(result["ok"])
        self.assertEqual([item["column"] for item in result["executed"]], ["fee"])
        self.assertEqual(result["target"]["columns"]["fee"], "DECIMAL(10,2)")
        self.assertEqual(result["target"]["columns"]["amount"], "DECIMAL(38,18)")
        # 存量值一条都不能被改写。
        amount_row = next(row for row in result["target"]["rows"] if row["id"] == 2)
        self.assertEqual(amount_row["amount"], "12.34567")

    def test_strict_execute_applies_nothing(self):
        result = migration_service.execute(strict_batch=True)
        self.assertEqual(result["executed"], [])
        self.assertEqual(result["target"]["columns"]["fee"], "DECIMAL(38,18)")

    def test_resume_retries_only_ineffective_columns(self):
        migration_service.execute()
        resumed = migration_service.resume()
        self.assertEqual(resumed["retried_columns"], ["amount"])
        self.assertEqual([item["column"] for item in resumed["skipped_columns"]], ["fee"])
        self.assertEqual(
            [item["column"] for item in resumed["execution"]["results"]], ["amount"],
        )
        self.assertEqual(resumed["execution"]["results"][0]["status"], "阻塞")

    def test_ledger_records_execution_and_version(self):
        migration_service.execute()
        ledger = migration_service.column_changes()["dwd.dwd_order_detail"]
        self.assertEqual(ledger["fee"]["mapping_type"], "DECIMAL(10,2)")
        self.assertEqual(ledger["fee"]["mapping_version"], 2)
        self.assertEqual(ledger["amount"]["mapping_type"], "DECIMAL(38,18)")
        self.assertEqual(ledger["amount"]["last_result"]["status"], "阻塞")

    def test_downstream_fee_query_reads_source_precision(self):
        migration_service.execute()
        payload = migration_service.query_downstream("dwd_order_fee_view")
        self.assertEqual(payload["reads"], ["fee"])
        self.assertTrue(payload["mapping_status"]["fee"]["effective"])
        self.assertEqual(payload["row_count"], 6)

    def test_repeated_execute_is_idempotent(self):
        migration_service.execute()
        second = migration_service.execute()
        by_column = {item["column"]: item for item in second["results"]}
        self.assertEqual(by_column["fee"]["status"], "已跳过")
        self.assertEqual(by_column["amount"]["status"], "阻塞")
        # 第三次仍不能被当成未生效列重复执行。
        third = migration_service.execute()
        self.assertEqual(
            {item["column"]: item["status"] for item in third["results"]}["fee"], "已跳过",
        )

    def test_verify_reports_amount_conflict(self):
        migration_service.execute()
        report = migration_service.verify_target()
        by_column = {column["column"]: column for column in report["columns"]}
        self.assertFalse(report["all_fidelity_ok"])
        self.assertFalse(by_column["amount"]["fidelity_ok"])
        self.assertTrue(by_column["fee"]["fidelity_ok"])


class PipelineIsolationTest(unittest.TestCase):
    def tearDown(self):
        migration_service.reset()
        pipeline_bus.schema_registry.clear()

    def test_schema_plan_consumes_ledger_precision(self):
        migration_service.reset()
        pipeline_bus.schema_registry.clear()
        ddl = DdlTrackerService()
        ddl.record_ddl(
            "ods", "ods.order_detail",
            "ALTER TABLE ods.order_detail MODIFY COLUMN amount DECIMAL(19,4), "
            "MODIFY COLUMN fee DECIMAL(10,2)",
        )
        plan = SchemaSyncService().build_sync_plan({
            "任务名称": "decimal", "源表": "ods.order_detail",
            "目标表": "dwd.dwd_order_detail", "类型转换": "hive",
        })
        mapping = {item["source_column"]: item["target_type"] for item in plan["column_mapping"]}
        self.assertEqual(mapping["amount"], "DECIMAL(19,4)")
        self.assertEqual(mapping["fee"], "DECIMAL(10,2)")


if __name__ == "__main__":
    unittest.main()
