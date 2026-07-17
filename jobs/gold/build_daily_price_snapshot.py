#!/usr/bin/env python3
"""Build Gold daily price snapshot from Silver products and observations."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


CONTRACT_VERSION = 1
UNIT_PRICE_TOLERANCE = 0.0001


def canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                rows.append(json.loads(line))
    return rows


def write_jsonl(path: Path, rows: list[dict[str, Any]]) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(canonical_json(row) + "\n")
    return sha256_file(path)


def date_key(snapshot_date: str) -> int:
    return int(snapshot_date.replace("-", ""))


def snapshot_key(snapshot_date: str, retailer_id: str, store_code: str, retailer_product_id: str) -> str:
    return sha256_text("|".join([snapshot_date, retailer_id, store_code, retailer_product_id]))


def output_paths(out_dir: Path, retailer_id: str, snapshot_date: str, run_id: str) -> dict[str, Path]:
    base = out_dir / "gold" / "fact_price_snapshot_daily" / f"store={retailer_id}" / f"date={snapshot_date}" / f"run_id={run_id}"
    return {
        "base": base,
        "snapshot": base / "price_snapshot_daily.jsonl",
        "manifest": base / "manifest.json",
        "validation": base / "validation_report.json",
    }


def index_products(products: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {row["retailer_product_id"]: row for row in products}


def choose_latest_observations(observations: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, int]]:
    grouped: dict[tuple[str, str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in observations:
        key = (
            str(row.get("observation_date")),
            str(row.get("retailer_id")),
            str(row.get("store_code")),
            str(row.get("retailer_product_id")),
        )
        grouped[key].append(row)

    selected: list[dict[str, Any]] = []
    duplicate_groups = 0
    observations_dropped = 0
    for rows in grouped.values():
        if len(rows) > 1:
            duplicate_groups += 1
            observations_dropped += len(rows) - 1
        selected.append(
            sorted(
                rows,
                key=lambda row: (
                    str(row.get("observed_at") or ""),
                    str(row.get("normalized_at") or ""),
                    str(row.get("observation_id") or ""),
                ),
                reverse=True,
            )[0]
        )
    return selected, {"duplicate_observation_groups": duplicate_groups, "observations_dropped": observations_dropped}


def calculate_effective_unit_price(observation: dict[str, Any], product: dict[str, Any]) -> tuple[float | None, str | None]:
    if not product.get("unit_price_publishable"):
        return None, None
    denominator = product.get("package_total_base_quantity")
    current_price = observation.get("current_price")
    comparison_unit = product.get("measurement_base_unit")
    if not isinstance(denominator, (int, float)) or denominator <= 0:
        return None, None
    if not isinstance(current_price, (int, float)) or current_price <= 0:
        return None, None
    if not comparison_unit:
        return None, None
    return round(current_price / denominator, 6), comparison_unit


def build_snapshot_row(observation: dict[str, Any], product: dict[str, Any], built_at: str, run_status: str) -> dict[str, Any]:
    snapshot_date = observation["observation_date"]
    store_code = observation.get("store_code") or "unknown_store"
    effective_unit_price, comparison_unit = calculate_effective_unit_price(observation, product)
    price_snapshot_id = snapshot_key(
        snapshot_date=snapshot_date,
        retailer_id=observation["retailer_id"],
        store_code=store_code,
        retailer_product_id=observation["retailer_product_id"],
    )

    return {
        "price_snapshot_id": price_snapshot_id,
        "contract_version": CONTRACT_VERSION,
        "date_key": date_key(snapshot_date),
        "snapshot_date": snapshot_date,
        "retailer_id": observation["retailer_id"],
        "store_code": store_code,
        "store_group_code": observation.get("store_group_code"),
        "region": observation.get("region"),
        "retailer_product_id": observation["retailer_product_id"],
        "observation_id": observation["observation_id"],
        "product_name": product.get("product_name"),
        "brand": product.get("brand"),
        "category_raw": product.get("category_raw"),
        "listed_price": observation.get("listed_price"),
        "promo_price": observation.get("promo_price"),
        "current_price": observation.get("current_price"),
        "currency": observation.get("currency") or "VND",
        "effective_unit_price": effective_unit_price,
        "comparison_unit": comparison_unit,
        "measurement_type": product.get("measurement_type"),
        "unit_price_publishable": bool(product.get("unit_price_publishable")),
        "package_total_base_quantity": product.get("package_total_base_quantity"),
        "discount_amount": observation.get("discount_amount"),
        "discount_percent": observation.get("discount_percent"),
        "availability_status": observation.get("availability_status"),
        "is_on_promotion": bool(observation.get("is_on_promotion")),
        "is_price_discount": bool(observation.get("is_price_discount")),
        "has_promo_mechanic": bool(observation.get("has_promo_mechanic")),
        "observed_at": observation.get("observed_at"),
        "run_status": run_status,
        "source_run_id": observation.get("source_run_id"),
        "source_bronze_record_key": observation.get("source_bronze_record_key"),
        "source_file": observation.get("source_file"),
        "source_line_number": observation.get("source_line_number"),
        "silver_data_quality_status": observation.get("data_quality_status"),
        "built_at": built_at,
    }


def validate_snapshots(rows: list[dict[str, Any]]) -> dict[str, Any]:
    issues: list[dict[str, Any]] = []
    issue_counts: Counter[str] = Counter()
    grain_counts: Counter[tuple[str, str, str, str]] = Counter()

    for row in rows:
        grain = (row["snapshot_date"], row["retailer_id"], row["store_code"], row["retailer_product_id"])
        grain_counts[grain] += 1

        def add(rule_id: str, severity: str, message: str) -> None:
            issue_counts[rule_id] += 1
            issues.append(
                {
                    "rule_id": rule_id,
                    "severity": severity,
                    "price_snapshot_id": row.get("price_snapshot_id"),
                    "retailer_product_id": row.get("retailer_product_id"),
                    "message": message,
                }
            )

        current_price = row.get("current_price")
        if current_price is None:
            add("missing_current_price", "BLOCK_PUBLISH", "current_price is missing")
        elif not isinstance(current_price, (int, float)) or current_price <= 0:
            add("non_positive_current_price", "BLOCK_PUBLISH", "current_price must be greater than zero")

        if row.get("date_key") != date_key(row["snapshot_date"]):
            add("date_key_mismatch", "BLOCK_PUBLISH", "date_key does not match snapshot_date")

        if not row.get("source_run_id") or not row.get("observation_id") or not row.get("source_bronze_record_key"):
            add("missing_lineage", "BLOCK_PUBLISH", "source_run_id, observation_id, or source_bronze_record_key is missing")

        if row.get("unit_price_publishable"):
            denominator = row.get("package_total_base_quantity")
            effective_unit_price = row.get("effective_unit_price")
            if not isinstance(denominator, (int, float)) or denominator <= 0 or effective_unit_price is None:
                add("invalid_unit_price_formula", "BLOCK_PUBLISH", "publishable unit price is missing denominator or output")
            else:
                expected = current_price / denominator
                if abs(expected - effective_unit_price) > UNIT_PRICE_TOLERANCE:
                    add("invalid_unit_price_formula", "BLOCK_PUBLISH", "effective_unit_price formula mismatch")
        elif row.get("effective_unit_price") is not None or row.get("comparison_unit") is not None:
            add("invalid_unit_price_formula", "BLOCK_PUBLISH", "unit price must be null when unit_price_publishable is false")
        else:
            add("unit_price_not_publishable", "WARN", "total price can publish, unit price unavailable")

        if row.get("listed_price") is None:
            add("missing_listed_price", "WARN", "listed_price is missing")

        if row.get("availability_status") == "unknown":
            add("unknown_availability", "WARN", "availability_status is unknown")

        if row.get("silver_data_quality_status") == "warning":
            add("upstream_quality_warning", "WARN", "selected Silver observation has warning status")

    for (snapshot_date, retailer_id, store_code, retailer_product_id), count in grain_counts.items():
        if count > 1:
            issue_counts["duplicate_snapshot_grain"] += 1
            issues.append(
                {
                    "rule_id": "duplicate_snapshot_grain",
                    "severity": "BLOCK_PUBLISH",
                    "price_snapshot_id": None,
                    "retailer_product_id": retailer_product_id,
                    "message": f"duplicate grain {snapshot_date}/{retailer_id}/{store_code}/{retailer_product_id}",
                }
            )

    block_count = sum(1 for issue in issues if issue["severity"] == "BLOCK_PUBLISH")
    warn_count = sum(1 for issue in issues if issue["severity"] == "WARN")
    return {
        "status": "passed" if block_count == 0 else "failed",
        "rows_checked": len(rows),
        "block_publish_issues": block_count,
        "warn_issues": warn_count,
        "issue_counts": dict(sorted(issue_counts.items())),
        "issues_sample": issues[:50],
    }


def build_gold(
    products_file: Path,
    observations_file: Path,
    out_dir: Path,
    built_at: str | None = None,
    run_status: str = "success",
) -> dict[str, Any]:
    products = read_jsonl(products_file)
    observations = read_jsonl(observations_file)
    product_by_id = index_products(products)
    selected_observations, selection_stats = choose_latest_observations(observations)
    build_time = built_at or utc_now_iso()

    rows: list[dict[str, Any]] = []
    skipped_counts: Counter[str] = Counter()
    for observation in selected_observations:
        product = product_by_id.get(observation["retailer_product_id"])
        if not product:
            skipped_counts["missing_product"] += 1
            continue
        if observation.get("data_quality_status") == "quarantined":
            skipped_counts["quarantined_observation"] += 1
            continue
        rows.append(build_snapshot_row(observation, product, build_time, run_status))

    rows = sorted(rows, key=lambda row: row["price_snapshot_id"])
    if rows:
        retailer_id = rows[0]["retailer_id"]
        snapshot_date = rows[0]["snapshot_date"]
        source_run_id = rows[0]["source_run_id"]
    else:
        retailer_id = "unknown"
        snapshot_date = "unknown"
        source_run_id = "unknown"

    paths = output_paths(out_dir, retailer_id, snapshot_date, source_run_id)
    paths["base"].mkdir(parents=True, exist_ok=True)
    snapshot_hash = write_jsonl(paths["snapshot"], rows)
    validation_report = validate_snapshots(rows)

    with paths["validation"].open("w", encoding="utf-8") as handle:
        json.dump(validation_report, handle, ensure_ascii=False, indent=2, sort_keys=True)
        handle.write("\n")

    manifest = {
        "status": "success" if validation_report["status"] == "passed" else "validation_failed",
        "contract_version": CONTRACT_VERSION,
        "source_products_file": products_file.as_posix(),
        "source_observations_file": observations_file.as_posix(),
        "price_snapshot_file": paths["snapshot"].as_posix(),
        "validation_report_file": paths["validation"].as_posix(),
        "retailer_id": retailer_id,
        "snapshot_date": snapshot_date,
        "source_run_id": source_run_id,
        "products_read": len(products),
        "observations_read": len(observations),
        "snapshots_written": len(rows),
        "skipped_counts": dict(sorted(skipped_counts.items())),
        "selection_stats": selection_stats,
        "unit_price_publishable_counts": dict(
            sorted(Counter(str(row["unit_price_publishable"]).lower() for row in rows).items())
        ),
        "validation_status": validation_report["status"],
        "block_publish_issues": validation_report["block_publish_issues"],
        "warn_issues": validation_report["warn_issues"],
        "price_snapshot_file_sha256": snapshot_hash,
        "built_at": build_time,
    }
    with paths["manifest"].open("w", encoding="utf-8") as handle:
        json.dump(manifest, handle, ensure_ascii=False, indent=2, sort_keys=True)
        handle.write("\n")

    return manifest


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build Gold daily price snapshot from Silver JSONL.")
    parser.add_argument("--products-file", required=True, help="Silver retailer_products JSONL.")
    parser.add_argument("--observations-file", required=True, help="Silver product_observations JSONL.")
    parser.add_argument("--out-dir", default="warehouse", help="Output root directory.")
    parser.add_argument("--built-at", help="Override build timestamp for deterministic tests.")
    parser.add_argument("--run-status", default="success", help="Run completeness status to write into Gold rows.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    manifest = build_gold(
        products_file=Path(args.products_file),
        observations_file=Path(args.observations_file),
        out_dir=Path(args.out_dir),
        built_at=args.built_at,
        run_status=args.run_status,
    )
    print(json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if manifest["status"] == "success" else 2


if __name__ == "__main__":
    sys.exit(main())
