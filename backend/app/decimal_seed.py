"""DECIMAL 小数列迁移的演示批次与目标表初始状态。

场景取自一次真实的发布批次：旧版 Schema同步 把 Hive 目标列固定成
DECIMAL(38,18)，而源库 introspection 已经能拿到完整精度
（amount DECIMAL(19,4)、fee DECIMAL(10,2)）。目标表里已有存量值，
其中 amount 的一部分历史值在新的 DECIMAL(19,4) 下装不下，
fee 的取值则全部可以无损迁移。

目标表 rows 里的值就是「存量值」：迁移检查只读这些值，不做任何
quantize/round，装不下就判失败。
"""
from __future__ import annotations

# 目标表初始状态：两列都还是旧映射的 DECIMAL(38,18)。
DEMO_TARGET_COLUMNS: dict[str, str] = {
    "id": "BIGINT",
    "amount": "DECIMAL(38,18)",
    "fee": "DECIMAL(38,18)",
    "created_at": "TIMESTAMP",
}

# id=2 的小数位 5 位、id=3 与 id=6 的整数位 16 位，都超出 DECIMAL(19,4)；
# 其余 amount 值和全部 fee 值都能无损落入源精度。
DEMO_TARGET_ROWS: list[dict[str, str | int]] = [
    {"id": 1, "amount": "123456789012345.6789", "fee": "12.34", "created_at": "2026-11-01 09:00:00"},
    {"id": 2, "amount": "12.34567", "fee": "99999999.99", "created_at": "2026-11-02 10:15:00"},
    {"id": 3, "amount": "1234567890123456.7890", "fee": "0.00", "created_at": "2026-11-03 11:30:00"},
    {"id": 4, "amount": "-999999999999999.9999", "fee": "-5.50", "created_at": "2026-11-04 12:45:00"},
    {"id": 5, "amount": "0.0001", "fee": "0.01", "created_at": "2026-11-05 13:00:00"},
    {"id": 6, "amount": "1000000000000000", "fee": "88.80", "created_at": "2026-11-06 14:20:00"},
]

DEMO_BATCH: dict[str, object] = {
    "batch_id": "batch_order_detail_decimal_v2",
    "name": "order_detail 小数列精度迁移 v2",
    "source_table": "ods.order_detail",
    "target_table": "dwd.dwd_order_detail",
    "release_version": 2,
    "ddl_record_id": "ddl_order_detail_decimal",
    "strict_batch": False,
    "columns": [
        {
            "column": "amount",
            "source_type": "DECIMAL(19,4)",
            "legacy_target_type": "DECIMAL(38,18)",
            "source_definition": "`amount` DECIMAL(19,4) NOT NULL COMMENT '订单金额'",
        },
        {
            "column": "fee",
            "source_type": "DECIMAL(10,2)",
            "legacy_target_type": "DECIMAL(38,18)",
            "source_definition": "`fee` DECIMAL(10,2) NOT NULL DEFAULT 0.00 COMMENT '手续费'",
        },
    ],
    "downstreams": [
        {"name": "dwd_order_fee_view", "reads": ["fee"]},
        {"name": "dwd_order_amount_report", "reads": ["amount"]},
    ],
    "target_columns": DEMO_TARGET_COLUMNS,
    "target_rows": DEMO_TARGET_ROWS,
}
