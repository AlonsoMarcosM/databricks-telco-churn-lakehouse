import importlib.util
import random
from datetime import datetime, timezone
from pathlib import Path

import pytest

from src.medallion_pipeline.quality import build_quarantine_expression, label_is_available
from src.medallion_pipeline.rules.customers import get_customer_rules, get_usage_rules


ROOT = Path(__file__).resolve().parents[1]
GENERATOR_PATH = ROOT / "codigo/src/medallion_pipeline/utilities/generate.py"


def load_generator():
    spec = importlib.util.spec_from_file_location("telco_generator", GENERATOR_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader
    spec.loader.exec_module(module)
    return module


def test_quarantine_expression_contains_every_rule():
    rules = get_customer_rules()
    expression = build_quarantine_expression(rules)
    assert expression.startswith("NOT (")
    assert all(rule in expression for rule in rules.values())


def test_empty_quality_rules_are_rejected():
    with pytest.raises(ValueError):
        build_quarantine_expression({})


def test_usage_rules_cover_non_negative_measures():
    rules = get_usage_rules()
    assert "data_consumed_gb >= 0" in rules.values()
    assert "bill_amount >= 0" in rules.values()


def test_point_in_time_guard():
    prediction = datetime(2025, 1, 31, tzinfo=timezone.utc)
    past_label = datetime(2025, 1, 30, tzinfo=timezone.utc)
    future_label = datetime(2025, 2, 1, tzinfo=timezone.utc)
    assert label_is_available(prediction, past_label)
    assert not label_is_available(prediction, future_label)


def test_small_generation_is_deterministic():
    generator = load_generator()
    random.seed(generator.SEED)
    first = generator.generate_customers(4)
    random.seed(generator.SEED)
    second = generator.generate_customers(4)
    assert first == second
    assert len({row["customer_id"] for row in first}) == 4

