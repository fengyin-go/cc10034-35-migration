"""DECIMAL(p,s) 类型解析与存量值位宽校验。

迁移的第一道闸门：先看整数位、再看小数位，任何会被截断或舍入的存量值
都必须判为不可迁移，绝不能通过 quantize/round 把值改小。
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation

# Hive / Doris 的 DECIMAL 最大精度都是 38；MySQL 源端允许到 65。
HIVE_MAX_PRECISION = 38
HIVE_MAX_SCALE = 38

_DECIMAL_RE = re.compile(
    r"^\s*DECIMAL\s*(?:\(\s*(\d+)\s*(?:,\s*(\d+)\s*)?\))?\s*$",
    re.IGNORECASE,
)

# 这是 MySQL 的默认值：不带参数的 DECIMAL 等价于 DECIMAL(10,0)。
DEFAULT_PRECISION = 10
DEFAULT_SCALE = 0


@dataclass(frozen=True)
class DecimalType:
    """一个 DECIMAL(p,s) 定义，以及按位宽判断存量值是否装得下。"""

    precision: int
    scale: int

    @classmethod
    def parse(cls, text: object) -> "DecimalType | None":
        """解析 DECIMAL / DECIMAL(p) / DECIMAL(p,s)，非 DECIMAL 返回 None。"""
        match = _DECIMAL_RE.match(str(text or ""))
        if match is None:
            return None
        precision = int(match.group(1)) if match.group(1) is not None else DEFAULT_PRECISION
        scale = int(match.group(2)) if match.group(2) is not None else DEFAULT_SCALE
        return cls(precision=precision, scale=scale)

    @property
    def integer_digits(self) -> int:
        """整数位上限：p - s。"""
        return self.precision - self.scale

    def __str__(self) -> str:
        return f"DECIMAL({self.precision},{self.scale})"

    def definition_errors(self, *, hive: bool = True) -> list[str]:
        """校验定义本身是否合法（Hive 目标端上限 38）。"""
        errors: list[str] = []
        if self.precision < 1:
            errors.append("precision 必须大于 0")
        if self.scale < 0:
            errors.append("scale 不能为负数")
        if self.scale > self.precision:
            errors.append(f"scale {self.scale} 大于 precision {self.precision}")
        if hive and self.precision > HIVE_MAX_PRECISION:
            errors.append(f"precision {self.precision} 超出 Hive 上限 {HIVE_MAX_PRECISION}")
        if hive and self.scale > HIVE_MAX_SCALE:
            errors.append(f"scale {self.scale} 超出 Hive 上限 {HIVE_MAX_SCALE}")
        return errors

    def inspect_value(self, value: object) -> dict[str, object]:
        """逐值检查整数位与小数位，返回可复核的结构化结果。

        顺序固定为整数位 -> 小数位；两步都计算出来供展示，
        但失败原因只取第一个不满足的检查项。这里不做任何
        quantize/round，超过位宽就是失败。
        """
        try:
            number = value if isinstance(value, Decimal) else Decimal(str(value))
        except (InvalidOperation, ValueError, TypeError):
            return {
                "value": str(value),
                "ok": False,
                "integer_digits": None,
                "fraction_digits": None,
                "integer_ok": False,
                "fraction_ok": False,
                "failed_check": "解析",
                "reason": "无法解析为 DECIMAL",
            }

        if not number.is_finite():
            return {
                "value": str(value),
                "ok": False,
                "integer_digits": None,
                "fraction_digits": None,
                "integer_ok": False,
                "fraction_ok": False,
                "failed_check": "有限性",
                "reason": "非有限数值",
            }

        integer_digits = integer_digits_of(number)
        fraction_digits = fraction_digits_of(number)
        integer_ok = integer_digits <= self.integer_digits
        fraction_ok = fraction_digits <= self.scale

        failed_check = ""
        reason = ""
        if not integer_ok:
            failed_check = "整数位"
            reason = (
                f"整数位 {integer_digits} 超过上限 {self.integer_digits}"
                f"（{number} 会被截断）"
            )
        elif not fraction_ok:
            failed_check = "小数位"
            reason = (
                f"小数位 {fraction_digits} 超过上限 {self.scale}"
                f"（{number} 会被舍入）"
            )

        return {
            "value": str(value),
            "ok": integer_ok and fraction_ok,
            "integer_digits": integer_digits,
            "fraction_digits": fraction_digits,
            "integer_limit": self.integer_digits,
            "fraction_limit": self.scale,
            "integer_ok": integer_ok,
            "fraction_ok": fraction_ok,
            "failed_check": failed_check,
            "reason": reason,
        }

    def fits_value(self, value: object) -> tuple[bool, str]:
        """判断单个存量值能否无损写入本类型：先整数位，再小数位。"""
        inspected = self.inspect_value(value)
        return bool(inspected["ok"]), str(inspected["reason"])

    def narrower_than(self, other: "DecimalType") -> bool:
        """本类型是否比另一类型更窄（用于提示迁移方向）。"""
        return (
            self.integer_digits < other.integer_digits
            or self.scale < other.scale
        )


def integer_digits_of(number: Decimal) -> int:
    """整数位长度：0 记 1 位，其余按最高有效位计算。"""
    if number == 0:
        return 1
    return max(number.adjusted() + 1, 0)


def fraction_digits_of(number: Decimal) -> int:
    """有效小数位长度：先去掉尾随零，避免把 1.2300 误判成 4 位小数。"""
    sign, digits, exponent = number.as_tuple()
    digits = list(digits)
    while exponent < 0 and digits and digits[-1] == 0:
        digits.pop()
        exponent += 1
    if not digits:
        return 0
    return max(-exponent, 0)


def parse_decimal_type(text: object) -> DecimalType | None:
    """模块级便捷函数。"""
    return DecimalType.parse(text)
