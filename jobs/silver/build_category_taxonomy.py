#!/usr/bin/env python3
"""Apply reviewed, retailer-specific category taxonomy rules to Silver products."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
import unicodedata
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml


CONTRACT_VERSION = 1


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def normalized(value: Any) -> str:
    text = str(value or "").strip().casefold()
    text = "".join(char for char in unicodedata.normalize("NFD", text) if unicodedata.category(char) != "Mn")
    return " ".join(text.replace("đ", "d").split())


def stable_id(*parts: str) -> str:
    value = "|".join(parts)
    return f"catmap_{hashlib.sha256(value.encode('utf-8')).hexdigest()[:24]}"


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def match_rule(row: dict[str, Any], rules: list[dict[str, Any]]) -> tuple[str | None, str]:
    retailer_id = row["retailer_id"]
    leaf = normalized(row.get("category_raw"))
    path = [normalized(value) for value in row.get("category_path_raw") or []]
    for rule in rules:
        if rule.get("retailer_id") != retailer_id:
            continue
        value = normalized(rule.get("value"))
        if rule.get("match_type") == "exact_leaf" and leaf == value:
            return rule["canonical_category_id"], "curated_exact_leaf_v1"
        if rule.get("match_type") == "path_prefix" and path and path[0] == value:
            return rule["canonical_category_id"], "curated_path_prefix_v1"
    return None, "unmapped"


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build reviewed category mapping from Silver products.")
    parser.add_argument("--products-files", nargs="+", required=True)
    parser.add_argument("--taxonomy-config", default="configs/category_taxonomy.yaml")
    parser.add_argument("--out-dir", default="warehouse")
    parser.add_argument("--run-date", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--built-at")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    config = yaml.safe_load(Path(args.taxonomy_config).read_text(encoding="utf-8")) or {}
    category_ids = {item["canonical_category_id"] for item in config.get("canonical_categories", [])}
    rules = config.get("mappings", [])
    if any(rule.get("canonical_category_id") not in category_ids for rule in rules):
        raise ValueError("Taxonomy rule references an undeclared canonical_category_id")

    built_at = args.built_at or utc_now()
    candidates: dict[tuple[str, str, tuple[str, ...]], dict[str, Any]] = {}
    for file_name in args.products_files:
        for product in read_jsonl(Path(file_name)):
            category_raw = product.get("category_raw")
            if not isinstance(category_raw, str) or not category_raw.strip():
                continue
            path = product.get("category_path_raw")
            category_path = [value for value in path if isinstance(value, str) and value.strip()] if isinstance(path, list) else [category_raw]
            key = (product["retailer_id"], normalized(category_raw), tuple(category_path))
            candidates.setdefault(key, product)

    rows: list[dict[str, Any]] = []
    for (_, category_normalized, category_path), product in sorted(candidates.items()):
        canonical_id, method = match_rule(product, rules)
        rows.append({
            "category_mapping_id": stable_id(product["retailer_id"], category_normalized, " > ".join(category_path)),
            "contract_version": CONTRACT_VERSION,
            "retailer_id": product["retailer_id"],
            "category_raw": product["category_raw"],
            "category_path_raw": list(category_path),
            "category_normalized": category_normalized,
            "canonical_category_id": canonical_id,
            "mapping_status": "mapped" if canonical_id else "unmapped",
            "mapping_method": method,
            "source_run_id": product["source_run_id"],
            "source_bronze_record_key": product["source_bronze_record_key"],
            "built_at": built_at,
        })

    base = Path(args.out_dir) / "silver" / "category_taxonomy" / f"date={args.run_date}" / f"run_id={args.run_id}"
    base.mkdir(parents=True, exist_ok=True)
    mapping_path = base / "category_mapping.jsonl"
    with mapping_path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")
    manifest = {
        "status": "success",
        "contract_version": CONTRACT_VERSION,
        "taxonomy_config": args.taxonomy_config,
        "mapping_file": mapping_path.as_posix(),
        "categories_written": len(rows),
        "mapping_status_counts": dict(Counter(row["mapping_status"] for row in rows)),
        "mapping_method_counts": dict(Counter(row["mapping_method"] for row in rows)),
        "built_at": built_at,
    }
    (base / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
