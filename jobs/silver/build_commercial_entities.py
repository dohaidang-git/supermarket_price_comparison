#!/usr/bin/env python3
"""Materialize conservative Silver store, category and product-offer entities."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


CONTRACT_VERSION = 1


def stable_id(prefix: str, *parts: Any) -> str:
    raw = "|".join(str(part or "") for part in parts)
    return f"{prefix}_{hashlib.sha256(raw.encode('utf-8')).hexdigest()[:24]}"


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    with path.open(encoding="utf-8") as handle:
        return [json.loads(line) for line in handle if line.strip()]


def write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def category_slug(value: str) -> str:
    return " ".join(value.casefold().split())


def category_value(value: Any) -> str | None:
    if isinstance(value, str) and value.strip():
        return value.strip()
    if isinstance(value, list):
        strings = [item.strip() for item in value if isinstance(item, str) and item.strip()]
        return strings[-1] if strings else None
    return None


def meaningful_offer_text(value: Any, product_name: Any) -> bool:
    if not isinstance(value, str) or not value.strip():
        return False
    normalized_value = " ".join(value.casefold().split())
    normalized_name = " ".join(str(product_name or "").casefold().split())
    return normalized_value != normalized_name


def output_paths(out_dir: Path, retailer_id: str, date_part: str, run_part: str) -> dict[str, Path]:
    base = out_dir / "silver" / "commercial_entities" / f"store={retailer_id}" / date_part / run_part
    return {
        "stores": base / "retailer_stores.jsonl",
        "categories": base / "category_mapping.jsonl",
        "promotions": base / "promotions.jsonl",
        "promotion_items": base / "promotion_items.jsonl",
        "manifest": base / "manifest.json",
    }


def promotion_type(observation: dict[str, Any]) -> str:
    if observation.get("is_price_discount"):
        return "price_discount"
    if observation.get("has_promo_mechanic"):
        return "promo_mechanic_unparsed"
    return "promotion_flag_only"


def validate(
    stores: list[dict[str, Any]],
    categories: list[dict[str, Any]],
    promotions: list[dict[str, Any]],
    items: list[dict[str, Any]],
    product_ids: set[str],
) -> dict[str, Any]:
    issues: list[dict[str, str]] = []

    def unique(rows: list[dict[str, Any]], field: str, entity: str) -> None:
        values = [row.get(field) for row in rows]
        if any(value in (None, "") for value in values):
            issues.append({"rule": f"{entity}_null_key", "severity": "BLOCK_PUBLISH"})
        if len(values) != len(set(values)):
            issues.append({"rule": f"{entity}_duplicate_key", "severity": "BLOCK_PUBLISH"})

    unique(stores, "retailer_store_id", "store")
    unique(categories, "category_mapping_id", "category")
    unique(promotions, "promotion_id", "promotion")
    unique(items, "promotion_item_id", "promotion_item")
    promotion_ids = {row["promotion_id"] for row in promotions}
    for item in items:
        if item["promotion_id"] not in promotion_ids:
            issues.append({"rule": "promotion_item_missing_promotion", "severity": "BLOCK_PUBLISH"})
        if item["retailer_product_id"] not in product_ids:
            issues.append({"rule": "promotion_item_missing_product", "severity": "BLOCK_PUBLISH"})
    for promotion in promotions:
        if promotion["promotion_type"] == "price_discount" and promotion["current_price"] is None:
            issues.append({"rule": "price_discount_missing_current_price", "severity": "BLOCK_PUBLISH"})
    severity_counts = Counter(issue["severity"] for issue in issues)
    return {
        "status": "failed" if severity_counts["BLOCK_PUBLISH"] else "success",
        "issues": issues,
        "severity_counts": dict(severity_counts),
    }


def build_entities(
    bronze_rows: list[dict[str, Any]],
    products: list[dict[str, Any]],
    observations: list[dict[str, Any]],
    built_at: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    product_by_id = {row["retailer_product_id"]: row for row in products}
    observation_by_key = {row["source_bronze_record_key"]: row for row in observations}
    stores: dict[str, dict[str, Any]] = {}
    categories: dict[str, dict[str, Any]] = {}
    promotions: dict[str, dict[str, Any]] = {}
    items: dict[str, dict[str, Any]] = {}

    for bronze in bronze_rows:
        payload = bronze["raw_payload"]
        retailer_id = bronze["retailer_id"]
        observation = observation_by_key.get(bronze["bronze_record_key"])
        product = product_by_id.get(observation.get("retailer_product_id")) if observation else None
        store_code = bronze.get("store_code") or "unknown_store"
        store_id = stable_id("retstore", retailer_id, store_code)
        stores[store_id] = {
            "retailer_store_id": store_id,
            "contract_version": CONTRACT_VERSION,
            "retailer_id": retailer_id,
            "store_code": store_code,
            "store_group_code": bronze.get("store_group_code"),
            "store_name": bronze.get("store_name") or payload.get("branch_name"),
            "region": bronze.get("region"),
            "address_context": payload.get("address_context"),
            "source_run_id": bronze["run_id"],
            "source_bronze_record_key": bronze["bronze_record_key"],
            "built_at": built_at,
        }

        category_raw = category_value(payload.get("source_category_name") or payload.get("category_raw") or (product or {}).get("category_raw"))
        if category_raw:
            normalized = category_slug(category_raw)
            mapping_id = stable_id("catmap", retailer_id, normalized)
            categories[mapping_id] = {
                "category_mapping_id": mapping_id,
                "contract_version": CONTRACT_VERSION,
                "retailer_id": retailer_id,
                "category_raw": category_raw,
                "category_normalized": normalized,
                "canonical_category_id": None,
                "mapping_status": "unmapped",
                "mapping_method": "normalized_raw_only",
                "source_run_id": bronze["run_id"],
                "source_bronze_record_key": bronze["bronze_record_key"],
                "built_at": built_at,
            }

        if not observation or not product or not observation.get("is_on_promotion"):
            continue
        offer_type = promotion_type(observation)
        offer_text = payload.get("promotion_text_raw")
        promotion_id = stable_id(
            "promo", retailer_id, store_code, product["retailer_product_id"], observation["observation_date"],
            offer_type, observation.get("listed_price"), observation.get("promo_price"), observation.get("current_price"), offer_text,
        )
        promotions[promotion_id] = {
            "promotion_id": promotion_id,
            "contract_version": CONTRACT_VERSION,
            "retailer_id": retailer_id,
            "store_code": store_code,
            "promotion_scope": "product_offer",
            "promotion_type": offer_type,
            "offer_text_raw": offer_text,
            "offer_text_available": meaningful_offer_text(offer_text, product.get("product_name")),
            "promotion_start_date": payload.get("promotion_start_date"),
            "promotion_end_date": payload.get("promotion_end_date"),
            "observation_date": observation["observation_date"],
            "listed_price": observation.get("listed_price"),
            "promo_price": observation.get("promo_price"),
            "current_price": observation.get("current_price"),
            "currency": observation.get("currency"),
            "source_run_id": bronze["run_id"],
            "source_bronze_record_key": bronze["bronze_record_key"],
            "built_at": built_at,
        }
        item_id = stable_id("promoitem", promotion_id, product["retailer_product_id"])
        items[item_id] = {
            "promotion_item_id": item_id,
            "contract_version": CONTRACT_VERSION,
            "promotion_id": promotion_id,
            "retailer_id": retailer_id,
            "store_code": store_code,
            "retailer_product_id": product["retailer_product_id"],
            "observation_id": observation["observation_id"],
            "observation_date": observation["observation_date"],
            "source_run_id": bronze["run_id"],
            "source_bronze_record_key": bronze["bronze_record_key"],
            "built_at": built_at,
        }
    return list(stores.values()), list(categories.values()), list(promotions.values()), list(items.values())


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build Silver commercial entities from one retailer run.")
    parser.add_argument("--bronze-file", required=True)
    parser.add_argument("--products-file", required=True)
    parser.add_argument("--observations-file", required=True)
    parser.add_argument("--out-dir", default="warehouse")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    bronze_rows = read_jsonl(Path(args.bronze_file))
    products = read_jsonl(Path(args.products_file))
    observations = read_jsonl(Path(args.observations_file))
    if not bronze_rows:
        raise ValueError("Bronze input is empty")
    first = bronze_rows[0]
    paths = output_paths(Path(args.out_dir), first["retailer_id"], first["observed_at"][:10].join(("date=", "")), f"run_id={first['run_id']}")
    built_at = utc_now()
    stores, categories, promotions, items = build_entities(bronze_rows, products, observations, built_at)
    validation = validate(stores, categories, promotions, items, {row["retailer_product_id"] for row in products})
    if validation["status"] != "success":
        print(json.dumps(validation, ensure_ascii=False, indent=2), file=sys.stderr)
        return 1
    for key, rows in (("stores", stores), ("categories", categories), ("promotions", promotions), ("promotion_items", items)):
        write_jsonl(paths[key], sorted(rows, key=lambda row: str(next(iter(row.values())))))
    manifest = {
        "status": "success", "contract_version": CONTRACT_VERSION, "retailer_id": first["retailer_id"],
        "run_date": first["observed_at"][:10], "run_id": first["run_id"], "built_at": built_at,
        "inputs": {"bronze": args.bronze_file, "products": args.products_file, "observations": args.observations_file},
        "outputs": {key: str(path) for key, path in paths.items() if key != "manifest"},
        "counts": {"retailer_stores": len(stores), "category_mapping": len(categories), "promotions": len(promotions), "promotion_items": len(items)},
        "validation": validation,
    }
    paths["manifest"].parent.mkdir(parents=True, exist_ok=True)
    paths["manifest"].write_text(json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
