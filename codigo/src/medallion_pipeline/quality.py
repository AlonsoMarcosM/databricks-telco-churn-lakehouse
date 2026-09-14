"""Pure quality helpers shared by declarative pipeline wiring and local tests."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime


def build_quarantine_expression(rules: Mapping[str, str]) -> str:
    """Build the SQL predicate that routes any failed rule to quarantine."""

    if not rules:
        raise ValueError("at least one quality rule is required")
    expressions = [expression.strip() for expression in rules.values()]
    if any(not expression for expression in expressions):
        raise ValueError("quality rule expressions cannot be empty")
    return "NOT (" + " AND ".join(expressions) + ")"


def label_is_available(prediction_time: datetime, label_available_time: datetime) -> bool:
    """Return whether a label can legally be used at prediction time."""

    if prediction_time.tzinfo is None or label_available_time.tzinfo is None:
        raise ValueError("point-in-time comparisons require timezone-aware datetimes")
    return label_available_time <= prediction_time

