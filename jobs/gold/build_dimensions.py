#!/usr/bin/env python3
"""Build Phase A Gold dimensions from Silver JSONL reference datasets."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any


CONTRACT_VERSION = 1
RETAILER_NAMES = {
    "bachhoaxanh": "Bach Hoa Xanh",
    "go": "GO!",
    "lottemart": "Lotte Mart",
    "mmvietnam": "MM Mega Market",
    "winmart": "WinMart",
}


def stable_key(prefix: str, *parts: Any) -> str:
    raw = "|".join(str(part or "") for part in parts)
    return f"{prefix}_{hashlib.sha256(raw.encode('utf-8')).hexdigest()[:24]}"


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    with path.open(encoding="utf-8") as handle:
        return [json.loads(line) for line in handle if line.strip()]


def write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def infer_context(rows: list[dict[str, Any]], files: list[str]) -> tuple[str, str]:
    run_ids = sorted(str(row.get("source_run_id") or "") for row in rows if row.get("source_run_id"))
    dates = sorted(str(row.get("observation_date") or "")[:10] for row in rows if row.get("observation_date"))
    if not dates:
        dates = sorted(part[5:] for filename in files for part in Path(filename).parts if part.startswith("date="))
    return (dates[-1] if dates else "unknown", run_ids[-1] if run_ids else "unknown")


def paths(out_dir: Path, run_date: str, run_id: str) -> dict[str, Path]:
    base = out_dir / "gold"
    result = {"manifest": base / "dimension_build" / f"date={run_date}" / f"run_id={run_id}" / "manifest.json"}
    for name in ("dim_retailer", "dim_date", "dim_store", "dim_retailer_product", "dim_product"):
        directory = base / name / f"date={run_date}" / f"run_id={run_id}"
        result[name] = directory / f"{name}.jsonl"
    return result


def build_dimensions(products: list[dict[str, Any]], observations: list[dict[str, Any]], mappings: list[dict[str, Any]], built_at: str) -> dict[str, list[dict[str, Any]]]:
    mapping_by_product = {row["retailer_product_id"]: row for row in mappings if row.get("review_status") == "matched"}
    retailers = sorted({row["retailer_id"] for row in products} | {row["retailer_id"] for row in observations})
    dim_retailer = [{
        "retailer_key": stable_key("ret", retailer_id), "contract_version": CONTRACT_VERSION,
        "retailer_id": retailer_id, "retailer_name": RETAILER_NAMES.get(retailer_id, retailer_id),
        "is_active": True, "source_run_id": next((row.get("source_run_id") for row in products if row.get("retailer_id") == retailer_id), None), "built_at": built_at,
    } for retailer_id in retailers]
    store_values = {(row["retailer_id"], row.get("store_code") or "unknown_store", row.get("store_group_code"), row.get("region")) for row in observations}
    dim_store = [{
        "store_key": stable_key("store", retailer_id, store_code), "contract_version": CONTRACT_VERSION,
        "retailer_id": retailer_id, "store_code": store_code, "store_group_code": group, "region": region,
        "is_active": True, "source_run_id": next((row.get("source_run_id") for row in observations if row.get("retailer_id") == retailer_id and (row.get("store_code") or "unknown_store") == store_code), None), "built_at": built_at,
    } for retailer_id, store_code, group, region in sorted(store_values)]
    date_values = sorted({str(row.get("observation_date")) for row in observations if row.get("observation_date")})
    dim_date = []
    for value in date_values:
        parsed = date.fromisoformat(value)
        dim_date.append({"date_key": int(parsed.strftime("%Y%m%d")), "contract_version": CONTRACT_VERSION, "calendar_date": value, "year": parsed.year, "month": parsed.month, "day": parsed.day, "day_of_week": parsed.isoweekday(), "is_weekend": parsed.isoweekday() >= 6})
    dim_retailer_product = []
    products_by_canonical: dict[str, list[dict[str, Any]]] = {}
    for row in products:
        mapping = mapping_by_product.get(row["retailer_product_id"])
        canonical_id = mapping.get("canonical_product_id") if mapping else None
        dim_retailer_product.append({
            "retailer_product_key": stable_key("retprod", row["retailer_id"], row["retailer_product_id"]), "contract_version": CONTRACT_VERSION,
            "retailer_product_id": row["retailer_product_id"], "canonical_product_id": canonical_id, "retailer_id": row["retailer_id"],
            "product_name": row["product_name"], "barcode": row.get("barcode"), "source_url": row.get("source_url"), "image_url": row.get("image_url"),
            "source_run_id": row.get("source_run_id"), "built_at": built_at,
        })
        if canonical_id:
            products_by_canonical.setdefault(canonical_id, []).append(row)
    dim_product = []
    for canonical_id, members in sorted(products_by_canonical.items()):
        selected = sorted(members, key=lambda row: (not bool(row.get("barcode")), row["product_name"], row["retailer_id"]))[0]
        dim_product.append({
            "product_key": stable_key("prod", canonical_id), "contract_version": CONTRACT_VERSION, "canonical_product_id": canonical_id,
            "canonical_name": selected["product_name"], "brand": selected.get("brand"), "product_type": None,
            "measurement_type": selected.get("measurement_type"), "package_total_base_quantity": selected.get("package_total_base_quantity"),
            "measurement_base_unit": selected.get("measurement_base_unit"), "canonical_image_url": selected.get("image_url"),
            "source_run_id": selected.get("source_run_id"), "built_at": built_at,
        })
    return {"dim_retailer": dim_retailer, "dim_store": dim_store, "dim_date": dim_date, "dim_retailer_product": dim_retailer_product, "dim_product": dim_product}


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build Phase A Gold dimensions.")
    parser.add_argument("--products-files", nargs="+", required=True)
    parser.add_argument("--observations-files", nargs="+", required=True)
    parser.add_argument("--mapping-file", required=True)
    parser.add_argument("--out-dir", default="warehouse")
    parser.add_argument("--built-at")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    products = [row for path in args.products_files for row in read_jsonl(Path(path))]
    observations = [row for path in args.observations_files for row in read_jsonl(Path(path))]
    mappings = read_jsonl(Path(args.mapping_file))
    built_at = args.built_at or now_iso()
    run_date, run_id = infer_context(observations or products, args.observations_files + args.products_files)
    output = paths(Path(args.out_dir), run_date, run_id)
    dimensions = build_dimensions(products, observations, mappings, built_at)
    for name, rows in dimensions.items():
        output[name].parent.mkdir(parents=True, exist_ok=True)
        write_jsonl(output[name], sorted(rows, key=lambda row: str(next(iter(row.values())))))
    output["manifest"].parent.mkdir(parents=True, exist_ok=True)
    manifest = {"status": "success", "contract_version": CONTRACT_VERSION, "built_at": built_at, "run_date": run_date, "source_run_id": run_id, "mapping_file": args.mapping_file, "counts": {name: len(rows) for name, rows in dimensions.items()}, "outputs": {name: str(path) for name, path in output.items() if name != "manifest"}}
    output["manifest"].write_text(json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
