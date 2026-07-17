#!/usr/bin/env python3
"""Normalize Bronze product records into Silver product and observation JSONL."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
import unicodedata
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


CONTRACT_VERSION = 1
DEFAULT_PRODUCT_DATASET = "products_enriched"

SELLING_UNIT_MAP = {
    "bich": "pack",
    "bo": "set",
    "chai": "bottle",
    "can": "can",
    "cai": "each",
    "cay": "each",
    "chiec": "each",
    "cuon": "roll",
    "goi": "pack",
    "hop": "box",
    "kg": "kg",
    "loc": "multi_pack",
    "lon": "can",
    "thung": "case",
    "tui": "bag",
    "vi": "blister",
    "vien": "piece",
}

COUNT_WORDS = (
    "cai",
    "chiec",
    "chai",
    "lon",
    "goi",
    "hop",
    "cuon",
    "vien",
)


def valid_gtin(value: Any) -> str | None:
    """Return a checksum-valid GTIN; do not promote arbitrary retailer SKUs."""
    text = str(value or "").strip()
    if not text.isdigit() or len(text) not in {8, 12, 13, 14}:
        return None
    weighted_sum = sum(
        int(digit) * (3 if (len(text) - 2 - index) % 2 == 0 else 1)
        for index, digit in enumerate(text[:-1])
    )
    return text if (10 - (weighted_sum % 10)) % 10 == int(text[-1]) else None


def canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                records.append(json.loads(line))
    return records


def write_jsonl(path: Path, rows: list[dict[str, Any]]) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(canonical_json(row) + "\n")
    return sha256_file(path)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def parse_number(value: str) -> float:
    return float(value.replace(",", "."))


def strip_vietnamese_accents(value: str) -> str:
    normalized = unicodedata.normalize("NFD", value)
    without_marks = "".join(char for char in normalized if unicodedata.category(char) != "Mn")
    return without_marks.replace("đ", "d").replace("Đ", "D")


def normalize_text(value: Any) -> str:
    return strip_vietnamese_accents(str(value).strip().lower())


def normalize_selling_unit(unit: Any) -> str | None:
    if unit is None:
        return None
    text = normalize_text(unit)
    return SELLING_UNIT_MAP.get(text, text if text else None)


def extract_raw_product(payload: dict[str, Any]) -> dict[str, Any]:
    raw_product = payload.get("raw_product")
    return raw_product if isinstance(raw_product, dict) else {}


def first_non_empty(*values: Any) -> Any:
    for value in values:
        if value not in (None, ""):
            return value
    return None


def category_fields(*values: Any) -> tuple[str | None, list[str] | None]:
    """Return a scalar category leaf and preserve any source breadcrumb path."""
    for value in values:
        if isinstance(value, str) and value.strip():
            category = value.strip()
            return category, [category]
        if isinstance(value, list):
            path = [str(item).strip() for item in value if isinstance(item, str) and item.strip()]
            if path:
                return path[-1], path
    return None, None


def parse_selling_count(text: str) -> tuple[float, str | None]:
    lower = normalize_text(text)

    pack_match = re.search(r"\b(?:loc|thung|vi|bo|hop)\s*(\d{1,3})\b", lower)
    if pack_match:
        count = float(pack_match.group(1))
        if count > 1:
            return count, "pack_count"

    count_words = "|".join(COUNT_WORDS)
    count_match = re.search(rf"\b(\d{{1,3}})\s*(?:{count_words})\b", lower)
    if count_match:
        count = float(count_match.group(1))
        if count > 1:
            return count, "inline_count"

    return 1.0, None


def parse_area(text: str) -> dict[str, Any] | None:
    lower = text.lower().replace("×", "x")
    match = re.search(r"\b(\d{2,4}(?:[,.]\d+)?)\s*[x*]\s*(\d{2,4}(?:[,.]\d+)?)(?:\s*cm)?\b", lower)
    if not match:
        return None
    length = parse_number(match.group(1))
    width = parse_number(match.group(2))
    if length <= 0 or width <= 0:
        return None
    return {
        "dimension_length_cm": length,
        "dimension_width_cm": width,
        "measurement_type": "area",
        "measurement_quantity": length * width,
        "measurement_unit": "cm2",
        "measurement_base_quantity": length * width,
        "measurement_base_unit": "cm2",
        "package_parse_status": "parsed_area",
        "package_parse_confidence": "medium",
        "package_parse_source": "product_name_raw",
    }


def parse_mass_or_volume(text: str) -> dict[str, Any] | None:
    lower = text.lower()
    matches = list(re.finditer(r"(\d+(?:[,.]\d+)?)\s*(kg|g|gr|gram|ml|l|lit|lít)\b", lower))
    if not matches:
        return None

    match = matches[-1]
    quantity = parse_number(match.group(1))
    unit = match.group(2)
    if quantity <= 0:
        return None

    if unit == "kg":
        base_quantity = quantity * 1000
        base_unit = "g"
        measurement_type = "mass"
    elif unit in {"g", "gr", "gram"}:
        unit = "g"
        base_quantity = quantity
        base_unit = "g"
        measurement_type = "mass"
    elif unit in {"l", "lit", "lít"}:
        unit = "l"
        base_quantity = quantity * 1000
        base_unit = "ml"
        measurement_type = "volume"
    else:
        unit = "ml"
        base_quantity = quantity
        base_unit = "ml"
        measurement_type = "volume"

    return {
        "measurement_type": measurement_type,
        "measurement_quantity": quantity,
        "measurement_unit": unit,
        "measurement_base_quantity": base_quantity,
        "measurement_base_unit": base_unit,
        "package_parse_status": "parsed_measurement",
        "package_parse_confidence": "medium",
        "package_parse_source": "product_name_raw",
    }


def parse_package(payload: dict[str, Any]) -> dict[str, Any]:
    raw_product = extract_raw_product(payload)
    product_name = str(first_non_empty(payload.get("product_name_raw"), raw_product.get("name"), "") or "")
    package_size_raw = payload.get("package_size_raw")
    selling_unit_raw = first_non_empty(payload.get("unit_raw"), raw_product.get("uomName"), raw_product.get("uom"))
    selling_unit_normalized = normalize_selling_unit(selling_unit_raw)
    combined_text = " ".join(str(part) for part in (package_size_raw, product_name) if part)
    selling_count, count_source = parse_selling_count(combined_text)

    parsed = parse_area(combined_text) or parse_mass_or_volume(combined_text)
    if parsed:
        total_base_quantity = parsed["measurement_base_quantity"] * selling_count
        return {
            "selling_unit_raw": selling_unit_raw,
            "selling_unit_normalized": selling_unit_normalized,
            "selling_unit_count": selling_count,
            "measurement_type": parsed["measurement_type"],
            "measurement_quantity": parsed["measurement_quantity"],
            "measurement_unit": parsed["measurement_unit"],
            "measurement_base_quantity": parsed["measurement_base_quantity"],
            "measurement_base_unit": parsed["measurement_base_unit"],
            "package_total_base_quantity": total_base_quantity,
            "package_parse_source": parsed["package_parse_source"],
            "package_parse_confidence": parsed["package_parse_confidence"],
            "package_parse_status": parsed["package_parse_status"],
            "unit_price_publishable": True,
            "dimension_length_cm": parsed.get("dimension_length_cm"),
            "dimension_width_cm": parsed.get("dimension_width_cm"),
        }

    if selling_unit_normalized:
        confidence = "medium" if selling_unit_normalized in {"each", "roll", "set", "blister"} else "low"
        return {
            "selling_unit_raw": selling_unit_raw,
            "selling_unit_normalized": selling_unit_normalized,
            "selling_unit_count": selling_count,
            "measurement_type": "count",
            "measurement_quantity": 1.0,
            "measurement_unit": "each",
            "measurement_base_quantity": 1.0,
            "measurement_base_unit": "each",
            "package_total_base_quantity": selling_count,
            "package_parse_source": count_source or "selling_unit_raw",
            "package_parse_confidence": confidence,
            "package_parse_status": "selling_unit_fallback",
            "unit_price_publishable": confidence != "low",
            "dimension_length_cm": None,
            "dimension_width_cm": None,
        }

    return {
        "selling_unit_raw": selling_unit_raw,
        "selling_unit_normalized": None,
        "selling_unit_count": None,
        "measurement_type": "unknown",
        "measurement_quantity": None,
        "measurement_unit": None,
        "measurement_base_quantity": None,
        "measurement_base_unit": None,
        "package_total_base_quantity": None,
        "package_parse_source": None,
        "package_parse_confidence": "none",
        "package_parse_status": "parse_failed",
        "unit_price_publishable": False,
        "dimension_length_cm": None,
        "dimension_width_cm": None,
    }


def retailer_product_id(retailer_id: str, payload: dict[str, Any]) -> str:
    raw_product = extract_raw_product(payload)
    source_id = str(first_non_empty(payload.get("item_no"), raw_product.get("itemNo"), payload.get("source_product_id"), raw_product.get("id")))
    sku = str(first_non_empty(raw_product.get("sku"), payload.get("sku"), ""))
    uom = str(first_non_empty(payload.get("unit_raw"), raw_product.get("uom"), raw_product.get("uomName"), ""))
    return sha256_text("|".join([retailer_id, source_id, sku, uom]))


def source_product_id(payload: dict[str, Any]) -> str | None:
    raw_product = extract_raw_product(payload)
    return first_non_empty(payload.get("item_no"), raw_product.get("itemNo"), payload.get("source_product_id"), raw_product.get("id"))


def observation_date(observed_at: str | None) -> str | None:
    if not observed_at:
        return None
    return observed_at[:10]


def bool_value(value: Any) -> bool:
    return bool(value) if value is not None else False


def quality_status(bronze_status: str, package_parse_status: str) -> str:
    if bronze_status == "QUARANTINE":
        return "quarantined"
    if bronze_status == "WARN" or package_parse_status in {"parse_failed", "selling_unit_fallback"}:
        return "warning"
    return "valid"


def build_product_record(bronze: dict[str, Any], normalized_at: str) -> dict[str, Any]:
    payload = bronze["raw_payload"]
    raw_product = extract_raw_product(payload)
    retailer_id = bronze["retailer_id"]
    package = parse_package(payload)
    product_id = retailer_product_id(retailer_id, payload)
    source_id = source_product_id(payload)
    product_name = first_non_empty(payload.get("product_name_raw"), raw_product.get("name"), raw_product.get("description"))
    source_barcode = valid_gtin(raw_product.get("barcode"))
    sku_barcode = valid_gtin(first_non_empty(raw_product.get("sku"), payload.get("sku")))
    barcode = source_barcode or sku_barcode
    category_raw, category_path_raw = category_fields(
        payload.get("source_category_name"), payload.get("category_raw"), raw_product.get("categoryName")
    )

    return {
        "retailer_product_id": product_id,
        "contract_version": CONTRACT_VERSION,
        "retailer_id": retailer_id,
        "source_product_id": source_id,
        "item_no": first_non_empty(payload.get("item_no"), raw_product.get("itemNo")),
        "sku": raw_product.get("sku"),
        "barcode": barcode,
        "barcode_source": "source_barcode" if source_barcode else ("sku_gtin_checksum" if sku_barcode else None),
        "product_name": product_name,
        "brand": first_non_empty(payload.get("brand_raw"), raw_product.get("brandName")),
        "category_raw": category_raw,
        "category_path_raw": category_path_raw,
        "image_url": first_non_empty(
            payload.get("image_url"),
            payload.get("image"),
            payload.get("thumbnail_url"),
            payload.get("thumbnail"),
            raw_product.get("image_url"),
            raw_product.get("image"),
            raw_product.get("mediaUrl"),
            raw_product.get("thumbnail_url"),
            raw_product.get("thumbnail"),
        ),
        "source_url": first_non_empty(
            payload.get("product_url"),
            payload.get("url"),
            raw_product.get("product_url"),
            raw_product.get("url"),
            payload.get("source_url"),
        ),
        "source_run_id": bronze["run_id"],
        "source_bronze_record_key": bronze["bronze_record_key"],
        "source_file": bronze["source_file"],
        "source_line_number": bronze["source_line_number"],
        "first_seen_at": bronze.get("observed_at"),
        "last_seen_at": bronze.get("observed_at"),
        "is_active": True,
        "normalized_at": normalized_at,
        **package,
    }


def build_observation_record(bronze: dict[str, Any], product: dict[str, Any], normalized_at: str) -> dict[str, Any]:
    payload = bronze["raw_payload"]
    observed_at = bronze.get("observed_at") or payload.get("crawled_at") or bronze.get("ingested_at")
    current_price = payload.get("current_price")
    listed_price = payload.get("listed_price")
    promo_price = payload.get("promo_price")
    discount_amount = None
    discount_percent = None
    if isinstance(current_price, (int, float)) and isinstance(listed_price, (int, float)) and listed_price > 0:
        discount_amount = max(listed_price - current_price, 0)
        discount_percent = round((discount_amount / listed_price) * 100, 4)

    obs_key = sha256_text(
        "|".join(
            [
                product["retailer_product_id"],
                str(bronze.get("store_code")),
                str(observed_at),
                bronze["bronze_record_key"],
            ]
        )
    )

    stock_quantity = payload.get("stock_quantity")
    availability_status = "unknown"
    if isinstance(stock_quantity, (int, float)):
        availability_status = "in_stock" if stock_quantity > 0 else "out_of_stock"

    is_price_discount = bool_value(payload.get("is_price_discount"))
    has_promo_mechanic = bool_value(payload.get("has_promo_mechanic"))
    is_on_promotion = bool_value(payload.get("is_on_promotion")) or is_price_discount or has_promo_mechanic

    return {
        "observation_id": obs_key,
        "contract_version": CONTRACT_VERSION,
        "retailer_product_id": product["retailer_product_id"],
        "retailer_id": bronze["retailer_id"],
        "store_code": bronze.get("store_code"),
        "store_group_code": bronze.get("store_group_code"),
        "region": bronze.get("region"),
        "observed_at": observed_at,
        "observation_date": observation_date(observed_at),
        "listed_price": listed_price,
        "promo_price": promo_price,
        "current_price": current_price,
        "currency": payload.get("currency") or "VND",
        "discount_amount": discount_amount,
        "discount_percent": discount_percent,
        "stock_quantity": stock_quantity,
        "availability_status": availability_status,
        "is_price_discount": is_price_discount,
        "has_promo_mechanic": has_promo_mechanic,
        "is_on_promotion": is_on_promotion,
        "source_url": payload.get("source_url"),
        "source_run_id": bronze["run_id"],
        "source_bronze_record_key": bronze["bronze_record_key"],
        "source_file": bronze["source_file"],
        "source_line_number": bronze["source_line_number"],
        "data_quality_status": quality_status(bronze["quality_status"], product["package_parse_status"]),
        "normalized_at": normalized_at,
    }


def output_paths(out_dir: Path, retailer_id: str, run_date: str, run_id: str) -> dict[str, Path]:
    base = out_dir / "silver" / f"store={retailer_id}" / f"date={run_date}" / f"run_id={run_id}"
    return {
        "base": base,
        "products": base / "retailer_products.jsonl",
        "observations": base / "product_observations.jsonl",
        "manifest": base / "manifest.json",
    }


def infer_run_context(records: list[dict[str, Any]]) -> tuple[str, str, str]:
    if not records:
        raise ValueError("No Bronze records found")
    first = records[0]
    retailer_id = str(first["retailer_id"])
    run_id = str(first["run_id"])
    observed_at = first.get("observed_at") or ""
    if observed_at:
        run_date = observed_at[:10]
    else:
        raw_run_dir = str(first.get("raw_run_dir") or "")
        date_parts = [part for part in raw_run_dir.split("/") if part.startswith("date=")]
        run_date = date_parts[-1].split("=", 1)[1] if date_parts else "unknown"
    return retailer_id, run_date, run_id


def resolve_product_datasets(bronze_records: list[dict[str, Any]], product_dataset: str) -> set[str]:
    available = {str(record.get("source_dataset")) for record in bronze_records if record.get("source_dataset")}
    if product_dataset != "auto":
        return {product_dataset}
    for preferred in ("products_enriched", "products"):
        if preferred in available:
            return {preferred}
    external = {dataset for dataset in available if "_promotions_" in dataset}
    if external:
        return external
    return available


def normalize(bronze_file: Path, out_dir: Path, product_dataset: str = DEFAULT_PRODUCT_DATASET, normalized_at: str | None = None) -> dict[str, Any]:
    bronze_records = read_jsonl(bronze_file)
    retailer_id, run_date, run_id = infer_run_context(bronze_records)
    paths = output_paths(out_dir, retailer_id, run_date, run_id)
    paths["base"].mkdir(parents=True, exist_ok=True)
    normalize_time = normalized_at or utc_now_iso()

    product_rows: list[dict[str, Any]] = []
    observation_rows: list[dict[str, Any]] = []
    skipped_counts: Counter[str] = Counter()
    parse_status_counts: Counter[str] = Counter()
    measurement_type_counts: Counter[str] = Counter()
    unit_price_publishable_counts: Counter[str] = Counter()
    selected_datasets = resolve_product_datasets(bronze_records, product_dataset)

    seen_products: dict[str, dict[str, Any]] = {}
    for bronze in bronze_records:
        if bronze.get("source_dataset") not in selected_datasets:
            skipped_counts[f"dataset:{bronze.get('source_dataset')}"] += 1
            continue
        if bronze.get("quality_status") == "QUARANTINE":
            skipped_counts["quarantine"] += 1
            continue

        payload = bronze.get("raw_payload") or {}
        if payload.get("current_price") is None:
            skipped_counts["missing_current_price"] += 1
            continue

        product = build_product_record(bronze, normalize_time)
        observation = build_observation_record(bronze, product, normalize_time)
        parse_status_counts[product["package_parse_status"]] += 1
        measurement_type_counts[product["measurement_type"]] += 1
        unit_price_publishable_counts[str(product["unit_price_publishable"]).lower()] += 1
        seen_products[product["retailer_product_id"]] = product
        observation_rows.append(observation)

    product_rows = sorted(seen_products.values(), key=lambda row: row["retailer_product_id"])
    observation_rows = sorted(observation_rows, key=lambda row: row["observation_id"])

    products_hash = write_jsonl(paths["products"], product_rows)
    observations_hash = write_jsonl(paths["observations"], observation_rows)

    manifest = {
        "status": "success",
        "contract_version": CONTRACT_VERSION,
        "source_bronze_file": bronze_file.as_posix(),
        "product_dataset": product_dataset,
        "selected_product_datasets": sorted(selected_datasets),
        "run_id": run_id,
        "retailer_id": retailer_id,
        "run_date": run_date,
        "retailer_products_file": paths["products"].as_posix(),
        "product_observations_file": paths["observations"].as_posix(),
        "retailer_products_written": len(product_rows),
        "product_observations_written": len(observation_rows),
        "skipped_counts": dict(sorted(skipped_counts.items())),
        "package_parse_status_counts": dict(sorted(parse_status_counts.items())),
        "measurement_type_counts": dict(sorted(measurement_type_counts.items())),
        "unit_price_publishable_counts": dict(sorted(unit_price_publishable_counts.items())),
        "retailer_products_file_sha256": products_hash,
        "product_observations_file_sha256": observations_hash,
        "normalized_at": normalize_time,
    }

    with paths["manifest"].open("w", encoding="utf-8") as handle:
        json.dump(manifest, handle, ensure_ascii=False, indent=2, sort_keys=True)
        handle.write("\n")

    return manifest


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Normalize Bronze product records into Silver JSONL.")
    parser.add_argument("--bronze-file", required=True, help="Bronze raw_records JSONL file.")
    parser.add_argument("--out-dir", default="warehouse", help="Output root directory.")
    parser.add_argument("--product-dataset", default="auto", help="Bronze source_dataset to normalize, or auto for retailer crawler outputs.")
    parser.add_argument("--normalized-at", help="Override normalization timestamp for deterministic tests.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    manifest = normalize(
        bronze_file=Path(args.bronze_file),
        out_dir=Path(args.out_dir),
        product_dataset=args.product_dataset,
        normalized_at=args.normalized_at,
    )
    print(json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
