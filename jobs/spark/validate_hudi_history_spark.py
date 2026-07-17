#!/usr/bin/env python3
"""Validate a Hudi snapshot using the Hudi reader, not physical Parquet files."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


CRITICAL_FIELDS = (
    "listed_price", "promo_price", "current_price", "effective_unit_price",
    "comparison_unit", "unit_price_publishable", "package_total_base_quantity",
    "discount_amount", "discount_percent", "currency", "is_on_promotion",
    "is_price_discount",
)
NUMERIC_FIELDS = {
    "listed_price", "promo_price", "current_price", "effective_unit_price",
    "package_total_base_quantity", "discount_amount", "discount_percent",
}
BOOLEAN_FIELDS = {"unit_price_publishable", "is_on_promotion", "is_price_discount"}
TOLERANCE = 0.0001
GRAIN = ("snapshot_date", "retailer_id", "store_code", "retailer_product_id")


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate a historical Hudi Gold snapshot.")
    parser.add_argument("--table-path", required=True, help="Persistent Hudi table directory for one retailer.")
    parser.add_argument("--snapshot-date", required=True, help="Business date to validate, YYYY-MM-DD.")
    parser.add_argument("--compare-gold-file", help="Optional Python Gold JSONL reference for the same snapshot date.")
    return parser.parse_args(argv)


def mismatch_condition(field: str) -> Any:
    expected = F.col(f"expected.{field}")
    actual = F.col(f"actual.{field}")
    both_null = expected.isNull() & actual.isNull()
    one_null = expected.isNull() != actual.isNull()
    if field in NUMERIC_FIELDS:
        return one_null | (~both_null & (F.abs(expected.cast("double") - actual.cast("double")) > F.lit(TOLERANCE)))
    if field in BOOLEAN_FIELDS:
        return one_null | (~both_null & (expected.cast("boolean") != actual.cast("boolean")))
    return one_null | (~both_null & (expected.cast("string") != actual.cast("string")))


def validate(spark: SparkSession, table_path: str, snapshot_date: str, compare_gold_file: str | None) -> dict[str, Any]:
    history = spark.read.format("hudi").load(table_path)
    rows = history.filter(F.col("snapshot_date") == F.lit(snapshot_date))
    row_count = rows.count()
    distinct_ids = rows.select("price_snapshot_id").distinct().count()
    duplicate_grains = rows.groupBy(*GRAIN).count().filter(F.col("count") > 1).count()
    required_keys = ("price_snapshot_id", "retailer_key", "store_key", "retailer_product_key")
    null_key_condition = F.col(required_keys[0]).isNull()
    for field in required_keys[1:]:
        null_key_condition = null_key_condition | F.col(field).isNull()

    report: dict[str, Any] = {
        "status": "passed",
        "table_path": table_path,
        "snapshot_date": snapshot_date,
        "snapshot_rows": int(row_count),
        "distinct_price_snapshot_ids": int(distinct_ids),
        "duplicate_grain_groups": int(duplicate_grains),
        "null_required_key_rows": int(rows.filter(null_key_condition).count()),
    }
    if row_count != distinct_ids or duplicate_grains or report["null_required_key_rows"]:
        report["status"] = "failed"

    if compare_gold_file:
        expected = spark.read.json(compare_gold_file).select("price_snapshot_id", *CRITICAL_FIELDS).alias("expected")
        actual = rows.select("price_snapshot_id", *CRITICAL_FIELDS).alias("actual")
        expected_ids = expected.select("price_snapshot_id").distinct()
        actual_ids = actual.select("price_snapshot_id").distinct()
        missing = expected_ids.join(actual_ids, "price_snapshot_id", "left_anti").count()
        extra = actual_ids.join(expected_ids, "price_snapshot_id", "left_anti").count()
        joined = expected.join(actual, "price_snapshot_id", "inner")
        mismatches = {field: int(joined.filter(mismatch_condition(field)).count()) for field in CRITICAL_FIELDS}
        report["reference_comparison"] = {
            "expected_rows": int(expected_ids.count()),
            "missing_in_hudi": int(missing),
            "extra_in_hudi": int(extra),
            "field_mismatch_counts": mismatches,
        }
        if missing or extra or any(mismatches.values()):
            report["status"] = "failed"
    return report


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    spark = SparkSession.builder.appName("supermarket_hudi_history_validation").getOrCreate()
    try:
        report = validate(spark, args.table_path, args.snapshot_date, args.compare_gold_file)
        print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
        return 0 if report["status"] == "passed" else 1
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
