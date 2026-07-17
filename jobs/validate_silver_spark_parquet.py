#!/usr/bin/env python3
"""Compare Spark Silver Parquet with the Python Silver JSONL reference."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

import pandas as pd


PRODUCT_KEY = "retailer_product_id"
OBSERVATION_KEY = "observation_id"
PRODUCT_FIELDS = ["product_name", "retailer_id", "source_product_id", "package_parse_status", "unit_price_publishable"]
OBSERVATION_FIELDS = ["retailer_product_id", "store_code", "current_price", "listed_price", "is_on_promotion", "is_price_discount", "data_quality_status"]


def read_parquet(path: Path) -> pd.DataFrame:
    return pd.read_parquet(path)


def compare(reference: Path, actual: Path, key: str, fields: list[str]) -> dict[str, Any]:
    expected = pd.read_json(reference, lines=True)
    got = read_parquet(actual)
    expected_ids = set(expected[key].dropna().astype(str))
    got_ids = set(got[key].dropna().astype(str))
    result: dict[str, Any] = {
        "reference_rows": len(expected),
        "spark_rows": len(got),
        "reference_distinct_keys": len(expected_ids),
        "spark_distinct_keys": len(got_ids),
        "missing_keys": len(expected_ids - got_ids),
        "extra_keys": len(got_ids - expected_ids),
        "duplicate_keys": int(got[key].duplicated().sum()),
        "field_mismatch_counts": {},
    }
    left = expected.set_index(key).sort_index()
    right = got.set_index(key).sort_index()
    common = left.index.intersection(right.index)
    for field in fields:
        if field not in left or field not in right:
            result["field_mismatch_counts"][field] = "missing_column"
            continue
        a = left.loc[common, field]
        b = right.loc[common, field]
        is_boolean = pd.api.types.is_bool_dtype(a) or pd.api.types.is_bool_dtype(b)
        if not is_boolean and (pd.api.types.is_numeric_dtype(a) or pd.api.types.is_numeric_dtype(b)):
            mismatch = ~(pd.to_numeric(a, errors="coerce").fillna(-999999999).sub(pd.to_numeric(b, errors="coerce").fillna(-999999999)).abs() <= 0.0001)
        else:
            mismatch = a.fillna("<NULL>").astype(str) != b.fillna("<NULL>").astype(str)
        result["field_mismatch_counts"][field] = int(mismatch.sum())
    result["passed"] = (
        result["reference_rows"] == result["spark_rows"]
        and result["missing_keys"] == 0
        and result["extra_keys"] == 0
        and result["duplicate_keys"] == 0
        and all(value == 0 for value in result["field_mismatch_counts"].values() if isinstance(value, int))
    )
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Validate Spark Silver Parquet against Python Silver JSONL.")
    parser.add_argument("--python-products", required=True)
    parser.add_argument("--spark-products", required=True)
    parser.add_argument("--python-observations", required=True)
    parser.add_argument("--spark-observations", required=True)
    args = parser.parse_args(argv or sys.argv[1:])
    report = {
        "products": compare(Path(args.python_products), Path(args.spark_products), PRODUCT_KEY, PRODUCT_FIELDS),
        "observations": compare(Path(args.python_observations), Path(args.spark_observations), OBSERVATION_KEY, OBSERVATION_FIELDS),
    }
    report["passed"] = report["products"]["passed"] and report["observations"]["passed"]
    print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
