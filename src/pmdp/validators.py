"""Pure-Python validators (unit-testable without Spark)."""

from __future__ import annotations

import re

_ISIN_RE = re.compile(r"^[A-Z]{2}[A-Z0-9]{9}[0-9]$")


def isin_check_digit(body: str) -> int:
    """ISO 6166 check digit: letters expand to two digits, then Luhn over the digit string."""
    digits = "".join(str(int(ch, 36)) for ch in body)
    total = 0
    for i, ch in enumerate(reversed(digits)):
        d = int(ch)
        if i % 2 == 0:
            d *= 2
        total += d // 10 + d % 10
    return (10 - total % 10) % 10


def is_valid_isin(value: str | None) -> bool:
    if value is None:
        return False
    if not _ISIN_RE.match(value):
        return False
    return isin_check_digit(value[:-1]) == int(value[-1])
