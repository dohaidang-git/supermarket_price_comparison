#!/usr/bin/env python3
"""Build conservative cross-retailer product identity mappings from Silver."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
import unicodedata
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


CONTRACT_VERSION = 1
VALID_BARCODE_LENGTHS = {8, 12, 13, 14}


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            if not line.strip():
                continue
            value = json.loads(line)
            if not isinstance(value, dict):
                raise ValueError(f"{path}:{line_number} must contain a JSON object")
            rows.append(value)
    return rows


def normalize_text(value: Any) -> str:
    text = str(value or "").strip().lower()
    text = "".join(char for char in unicodedata.normalize("NFD", text) if unicodedata.category(char) != "Mn")
    text = text.replace("đ", "d")
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9]+", " ", text)).strip()


def valid_barcode(value: Any) -> str | None:
    """Return a checksum-valid GTIN, excluding arbitrary numeric SKUs."""
    text = str(value or "").strip()
    if not text.isdigit() or len(text) not in VALID_BARCODE_LENGTHS:
        return None

    weighted_sum = sum(
        int(digit) * (3 if (len(text) - 2 - index) % 2 == 0 else 1)
        for index, digit in enumerate(text[:-1])
    )
    return text if (10 - (weighted_sum % 10)) % 10 == int(text[-1]) else None


def stable_id(prefix: str, value: str) -> str:
    digest = hashlib.sha256(f"{prefix}|{value}".encode("utf-8")).hexdigest()
    return f"prod_{digest[:24]}"


def attribute_key(row: dict[str, Any]) -> str:
    values = (
        normalize_text(row.get("brand")),
        normalize_text(row.get("product_name")),
        normalize_text(row.get("measurement_type")),
        str(row.get("measurement_base_quantity") or ""),
        normalize_text(row.get("measurement_base_unit")),
        str(row.get("selling_unit_count") or ""),
    )
    return "|".join(values)


def output_paths(out_dir: Path, run_date: str, run_id: str) -> dict[str, Path]:
    base = out_dir / "silver" / "product_identity_mapping" / f"date={run_date}" / f"run_id={run_id}"
    return {
        "base": base,
        "mapping": base / "product_identity_mapping.jsonl",
        "manifest": base / "manifest.json",
    }


def write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")


def infer_context(rows: list[dict[str, Any]], input_files: list[str]) -> tuple[str, str]:
    dates = sorted(str(row.get("observation_date") or "")[:10] for row in rows if row.get("observation_date"))
    run_ids = sorted(str(row.get("source_run_id") or "") for row in rows if row.get("source_run_id"))
    if not dates:
        path_dates = sorted(
            part.split("=", 1)[1]
            for filename in input_files
            for part in Path(filename).parts
            if part.startswith("date=")
        )
        dates = path_dates
    if not run_ids:
        path_runs = sorted(
            part.split("=", 1)[1]
            for filename in input_files
            for part in Path(filename).parts
            if part.startswith("run_id=")
        )
        run_ids = path_runs
    return (dates[-1] if dates else "unknown", run_ids[-1] if run_ids else "unknown")


def build_mapping(rows: list[dict[str, Any]], built_at: str) -> tuple[list[dict[str, Any]], dict[str, int]]:
    barcode_groups: dict[str, list[int]] = defaultdict(list)
    attribute_groups: dict[str, list[int]] = defaultdict(list)
    for index, row in enumerate(rows):
        barcode = valid_barcode(row.get("barcode"))
        if barcode:
            barcode_groups[barcode].append(index)
        key = attribute_key(row)
        if key.strip("|"):
            attribute_groups[key].append(index)

    mapping_rows: list[dict[str, Any]] = []
    statuses: Counter[str] = Counter()
    methods: Counter[str] = Counter()
    for index, row in enumerate(rows):
        retailer_product_id = row.get("retailer_product_id")
        retailer_id = row.get("retailer_id")
        barcode = valid_barcode(row.get("barcode"))
        matched_indices: list[int] = []
        method = "none"
        reason = "No exact cross-retailer identity evidence."

        if barcode:
            candidates = barcode_groups[barcode]
            if len({rows[item]["retailer_id"] for item in candidates}) > 1:
                matched_indices = candidates
                method = "barcode_exact"
                reason = "Valid barcode appears in multiple retailers."

        if not matched_indices:
            key = attribute_key(row)
            candidates = attribute_groups.get(key, [])
            if key.strip("|") and len({rows[item]["retailer_id"] for item in candidates}) > 1:
                matched_indices = candidates
                method = "attributes_exact"
                reason = "Brand, normalized name, measurement and pack attributes match across retailers."

        canonical_product_id = None
        status = "unmatched"
        if matched_indices:
            identity_value = barcode if method == "barcode_exact" else attribute_key(row)
            canonical_product_id = stable_id(method, identity_value)
            status = "matched"

        output = {
            "mapping_id": stable_id("mapping", str(retailer_product_id)),
            "contract_version": CONTRACT_VERSION,
            "retailer_product_id": retailer_product_id,
            "retailer_id": retailer_id,
            "canonical_product_id": canonical_product_id,
            "match_method": method,
            "match_score": 100 if status == "matched" else 0,
            "match_confidence": "high" if method == "barcode_exact" else ("medium" if status == "matched" else "none"),
            "review_status": status,
            "match_reason": reason,
            "source_run_id": row.get("source_run_id"),
            "matched_at": built_at,
        }
        mapping_rows.append(output)
        statuses[status] += 1
        methods[method] += 1

    return mapping_rows, {**{f"status_{k}": v for k, v in statuses.items()}, **{f"method_{k}": v for k, v in methods.items()}}


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build conservative product identity mappings from Silver JSONL files.")
    parser.add_argument("--products-files", nargs="+", required=True, help="One or more Silver retailer_products JSONL files.")
    parser.add_argument("--out-dir", default="warehouse", help="Warehouse root directory.")
    parser.add_argument("--run-date", help="Override output date partition, YYYY-MM-DD.")
    parser.add_argument("--run-id", help="Override source run id.")
    parser.add_argument("--built-at", help="Override mapping timestamp for deterministic tests.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    rows = [row for filename in args.products_files for row in read_jsonl(Path(filename))]
    if not rows:
        raise ValueError("No Silver product rows found")
    built_at = args.built_at or utc_now_iso()
    mapping_rows, stats = build_mapping(rows, built_at)
    run_date, run_id = infer_context(rows, args.products_files)
    run_date = args.run_date or run_date
    run_id = args.run_id or run_id
    paths = output_paths(Path(args.out_dir), run_date, run_id)
    paths["base"].mkdir(parents=True, exist_ok=True)
    write_jsonl(paths["mapping"], mapping_rows)
    manifest = {
        "status": "success",
        "engine": "python",
        "contract_version": CONTRACT_VERSION,
        "source_products_files": [str(Path(filename)) for filename in args.products_files],
        "mapping_file": str(paths["mapping"]),
        "products_read": len(rows),
        "mappings_written": len(mapping_rows),
        "run_date": run_date,
        "source_run_id": run_id,
        "mapping_stats": stats,
        "built_at": built_at,
    }
    paths["manifest"].write_text(json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
