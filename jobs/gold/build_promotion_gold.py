#!/usr/bin/env python3
"""Build Gold promotion dimension and product-offer fact from Silver entities."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def key(prefix: str, *parts: Any) -> str:
    return f"{prefix}_{hashlib.sha256('|'.join(str(v or '') for v in parts).encode()).hexdigest()[:24]}"


def read(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.open(encoding="utf-8") if line.strip()]


def write(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build Gold promotion dimension and fact.")
    parser.add_argument("--promotions-file", required=True)
    parser.add_argument("--promotion-items-file", required=True)
    parser.add_argument("--out-dir", default="warehouse")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    promotions = read(Path(args.promotions_file))
    items = read(Path(args.promotion_items_file))
    if not promotions:
        raise ValueError("No promotions to publish")
    promotion_by_id = {row["promotion_id"]: row for row in promotions}
    first = promotions[0]
    date = first["observation_date"]
    run_id = first["source_run_id"]
    retailer_id = first["retailer_id"]
    built_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    dim_rows = []
    for row in promotions:
        dim_rows.append({
            "promotion_key": key("promo", row["promotion_id"]), "contract_version": 1,
            "promotion_id": row["promotion_id"], "retailer_id": row["retailer_id"], "store_code": row["store_code"],
            "promotion_scope": row["promotion_scope"], "promotion_type": row["promotion_type"],
            "offer_text_raw": row.get("offer_text_raw"), "offer_text_available": row["offer_text_available"],
            "promotion_start_date": row.get("promotion_start_date"), "promotion_end_date": row.get("promotion_end_date"),
            "observation_date": row["observation_date"],
            "source_run_id": row["source_run_id"], "built_at": built_at,
        })
    fact_rows = []
    for item in items:
        promotion = promotion_by_id.get(item["promotion_id"])
        if not promotion:
            raise ValueError(f"promotion_item missing promotion: {item['promotion_item_id']}")
        fact_rows.append({
            "promotion_item_fact_id": key("promofact", item["promotion_item_id"]), "contract_version": 1,
            "date_key": int(item["observation_date"].replace("-", "")), "observation_date": item["observation_date"],
            "promotion_key": key("promo", item["promotion_id"]), "promotion_id": item["promotion_id"],
            "retailer_key": key("ret", item["retailer_id"]), "store_key": key("store", item["retailer_id"], item["store_code"]),
            "retailer_product_key": key("retprod", item["retailer_id"], item["retailer_product_id"]),
            "retailer_id": item["retailer_id"], "store_code": item["store_code"], "retailer_product_id": item["retailer_product_id"],
            "promotion_type": promotion["promotion_type"], "listed_price": promotion.get("listed_price"),
            "promo_price": promotion.get("promo_price"), "current_price": promotion.get("current_price"), "currency": promotion["currency"],
            "source_run_id": item["source_run_id"], "source_bronze_record_key": item["source_bronze_record_key"], "built_at": built_at,
        })
    if len({row['promotion_key'] for row in dim_rows}) != len(dim_rows) or len({row['promotion_item_fact_id'] for row in fact_rows}) != len(fact_rows):
        raise ValueError("Duplicate Gold promotion key")
    base = Path(args.out_dir) / "gold"
    dim_path = base / "dim_promotion" / f"store={retailer_id}" / f"date={date}" / f"run_id={run_id}" / "dim_promotion.jsonl"
    fact_path = base / "fact_promotion_item" / f"store={retailer_id}" / f"date={date}" / f"run_id={run_id}" / "fact_promotion_item.jsonl"
    write(dim_path, dim_rows)
    write(fact_path, fact_rows)
    report = {"status": "success", "retailer_id": retailer_id, "run_date": date, "run_id": run_id, "dim_promotions": len(dim_rows), "fact_promotion_items": len(fact_rows), "dim_path": str(dim_path), "fact_path": str(fact_path)}
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
