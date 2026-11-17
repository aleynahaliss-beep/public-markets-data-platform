"""Minimal weekday-only business calendar (no market holidays)."""

from __future__ import annotations

from datetime import date, timedelta


def is_business_day(d: date) -> bool:
    return d.weekday() < 5


def business_days(end: date, n: int) -> list[date]:
    """Return the last `n` business days up to and including `end`, ascending."""
    days: list[date] = []
    d = end
    while len(days) < n:
        if is_business_day(d):
            days.append(d)
        d -= timedelta(days=1)
    return days[::-1]


def add_business_days(d: date, n: int) -> date:
    step = 1 if n >= 0 else -1
    remaining = abs(n)
    while remaining:
        d += timedelta(days=step)
        if is_business_day(d):
            remaining -= 1
    return d
