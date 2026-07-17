#!/usr/bin/env python3
"""Merge WinMart API product records with UI promo card mechanics."""

from __future__ import annotations

import argparse
import json
import sys
import unicodedata
from collections import defaultdict
from copy import deepcopy
from pathlib import Path
from typing import Any

from winmart_scraper import (
    STORE_ID,
    STORE_NAME,
    append_jsonl,
    build_output_paths,
    now_local_iso,
    write_json,
)


def read_jsonl(path: Path, required: bool = True) -> list[dict[str, Any]]:
    if not path.exists():
        if required:
            raise FileNotFoundError(f"Missing input file: {path}")
        return []

    records: list[dict[str, Any]] = []
    with path.open(encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            if not line.strip():
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"Invalid JSONL at {path}:{line_number}: {exc}") from exc
            if not isinstance(record, dict):
                raise ValueError(f"Expected JSON object at {path}:{line_number}")
            records.append(record)
    return records


def strip_accents(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value)
    return "".join(char for char in normalized if not unicodedata.combining(char))


def normalize_uom(value: Any) -> str | None:
    if value in (None, ""):
        return None
    return strip_accents(str(value)).lower().strip()


def product_item_no(product: dict[str, Any]) -> str | None:
    raw_product = product.get("raw_product")
    if isinstance(raw_product, dict):
        for key in ("itemNo", "item_no", "itemNumber"):
            value = raw_product.get(key)
            if value not in (None, ""):
                return str(value)

    for key in ("item_no", "source_item_no"):
        value = product.get(key)
        if value not in (None, ""):
            return str(value)
    return None


def product_identity_key(product: dict[str, Any]) -> str:
    item_no = product_item_no(product)
    if item_no:
        return f"itemNo:{item_no}"
    raw_product = product.get("raw_product")
    if isinstance(raw_product, dict):
        for key in ("sku", "barcode", "id"):
            value = raw_product.get(key)
            if value not in (None, ""):
                return f"{key}:{value}"
    return json.dumps(product, ensure_ascii=False, sort_keys=True, default=str)


def is_price_discount(product: dict[str, Any]) -> bool:
    listed_price = product.get("listed_price")
    promo_price = product.get("promo_price")
    return (
        isinstance(listed_price, (int, float))
        and isinstance(promo_price, (int, float))
        and promo_price < listed_price
    )


def product_uoms(product: dict[str, Any]) -> set[str]:
    found: set[str] = set()
    for value in (product.get("unit_raw"),):
        normalized = normalize_uom(value)
        if normalized:
            found.add(normalized)

    raw_product = product.get("raw_product")
    if not isinstance(raw_product, dict):
        return found

    for key in ("uomName", "uom"):
        normalized = normalize_uom(raw_product.get(key))
        if normalized:
            found.add(normalized)

    raw_uoms = raw_product.get("uoms")
    if isinstance(raw_uoms, list):
        for uom in raw_uoms:
            if not isinstance(uom, dict):
                continue
            for key in ("uomName", "uom"):
                normalized = normalize_uom(uom.get(key))
                if normalized:
                    found.add(normalized)
    return found


def promo_uom_hint(promo: dict[str, Any]) -> str | None:
    mechanic = promo.get("promo_mechanic")
    if not isinstance(mechanic, dict):
        return None
    for key in ("buy_uom", "bundle_uom", "gift_uom"):
        normalized = normalize_uom(mechanic.get(key))
        if normalized:
            return normalized
    return None


def concise_promo(promo: dict[str, Any], product_uom_values: set[str]) -> dict[str, Any]:
    uom_hint = promo_uom_hint(promo)
    if not uom_hint:
        uom_status = "no_uom_hint"
    elif not product_uom_values:
        uom_status = "no_product_uoms"
    elif uom_hint in product_uom_values:
        uom_status = "matched"
    else:
        uom_status = "unmatched"

    return {
        "item_no": promo.get("item_no"),
        "promo_mechanic_text": promo.get("promo_mechanic_text"),
        "promo_mechanic_type": promo.get("promo_mechanic_type"),
        "promo_mechanic": promo.get("promo_mechanic"),
        "promo_product_url": promo.get("product_url"),
        "promo_source_url": promo.get("source_url"),
        "promo_source_category_code": promo.get("source_category_code"),
        "promo_source_category_name": promo.get("source_category_name"),
        "promo_source_category_slug": promo.get("source_category_slug"),
        "promo_raw_payload_path": promo.get("raw_payload_path"),
        "match_confidence": promo.get("match_confidence"),
        "uom_hint": uom_hint,
        "uom_match_status": uom_status,
    }


def enrich_products(
    products: list[dict[str, Any]],
    promos: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    promos_by_item_no: dict[str, list[dict[str, Any]]] = defaultdict(list)
    promos_without_item_no: list[dict[str, Any]] = []
    for promo in promos:
        item_no = promo.get("item_no")
        if item_no in (None, ""):
            promos_without_item_no.append(promo)
            continue
        promos_by_item_no[str(item_no)].append(promo)

    enriched: list[dict[str, Any]] = []
    matched_promo_ids: set[int] = set()
    products_with_promo = 0
    total_matches = 0
    products_with_price_discount = 0
    products_on_promotion = 0
    uom_status_counts: dict[str, int] = defaultdict(int)

    for product in products:
        item_no = product_item_no(product)
        matches = promos_by_item_no.get(item_no or "", [])
        product_uom_values = product_uoms(product)
        promo_matches = [concise_promo(promo, product_uom_values) for promo in matches]

        record = deepcopy(product)
        record["item_no"] = item_no
        record["promo_card_match_count"] = len(promo_matches)
        record["promo_mechanic_texts"] = [match.get("promo_mechanic_text") for match in promo_matches]
        record["promo_mechanics"] = promo_matches
        record["has_promo_mechanic"] = bool(promo_matches)
        record["is_price_discount"] = is_price_discount(record)
        record["is_on_promotion"] = record["is_price_discount"] or record["has_promo_mechanic"]
        if record["is_price_discount"]:
            products_with_price_discount += 1
        if record["is_on_promotion"]:
            products_on_promotion += 1

        if promo_matches:
            products_with_promo += 1
            total_matches += len(promo_matches)
            for promo in matches:
                matched_promo_ids.add(id(promo))
            for match in promo_matches:
                uom_status_counts[str(match.get("uom_match_status") or "unknown")] += 1

        enriched.append(record)

    unmatched = [
        promo
        for promo in promos
        if id(promo) not in matched_promo_ids
    ]
    summary = {
        "products_read": len(products),
        "promo_cards_read": len(promos),
        "products_with_price_discount": products_with_price_discount,
        "products_with_promo_cards": products_with_promo,
        "products_on_promotion": products_on_promotion,
        "promo_card_matches": total_matches,
        "promo_cards_unmatched": len(unmatched),
        "promo_cards_without_item_no": len(promos_without_item_no),
        "uom_match_status_counts": dict(sorted(uom_status_counts.items())),
    }
    return enriched, unmatched, summary


def write_jsonl_replace(path: Path, records: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("", encoding="utf-8")
    for record in records:
        append_jsonl(path, record)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Merge WinMart API products with promo_cards.jsonl.")
    parser.add_argument("--run-dir", default=None, help="Existing run directory containing products.jsonl and promo_cards.jsonl.")
    parser.add_argument("--out-dir", default="raw", help="Raw output directory if --run-dir is not used.")
    parser.add_argument("--run-id", default=None, help="Run id if --run-dir is not used.")
    parser.add_argument(
        "--products-file",
        default=None,
        help="Override product input path. Defaults to products.jsonl plus promo_hydrated_products.jsonl when present.",
    )
    parser.add_argument(
        "--hydrated-products-file",
        default=None,
        help="Optional hydrated products input. Defaults to promo_hydrated_products.jsonl when present.",
    )
    parser.add_argument("--promo-cards-file", default=None, help="Override promo_cards.jsonl path.")
    parser.add_argument("--output-file", default=None, help="Output enriched JSONL path.")
    parser.add_argument("--unmatched-output-file", default=None, help="Output unmatched promo cards JSONL path.")
    parser.add_argument("--metadata-name", default="metadata_merge.json", help="Metadata filename inside run directory.")
    return parser.parse_args()


def resolve_run_dir(args: argparse.Namespace) -> Path:
    if args.run_dir:
        return Path(args.run_dir)
    if args.run_id:
        return build_output_paths(Path(args.out_dir), args.run_id).run_dir
    if args.products_file:
        return Path(args.products_file).resolve().parent
    raise ValueError("Pass --run-dir, --run-id, or --products-file")


def read_product_inputs(
    products_file: Path,
    hydrated_products_file: Path | None,
) -> tuple[list[dict[str, Any]], list[str]]:
    records = read_jsonl(products_file, required=True)
    sources = [str(products_file)]
    seen = {product_identity_key(record) for record in records}

    if hydrated_products_file and hydrated_products_file.exists():
        for record in read_jsonl(hydrated_products_file, required=False):
            key = product_identity_key(record)
            if key in seen:
                continue
            seen.add(key)
            records.append(record)
        sources.append(str(hydrated_products_file))

    return records, sources


def main() -> int:
    args = parse_args()
    run_dir = resolve_run_dir(args)
    products_file = Path(args.products_file) if args.products_file else run_dir / "products.jsonl"
    hydrated_products_file = (
        Path(args.hydrated_products_file)
        if args.hydrated_products_file
        else run_dir / "promo_hydrated_products.jsonl"
    )
    promo_cards_file = Path(args.promo_cards_file) if args.promo_cards_file else run_dir / "promo_cards.jsonl"
    output_file = Path(args.output_file) if args.output_file else run_dir / "products_enriched.jsonl"
    unmatched_file = (
        Path(args.unmatched_output_file)
        if args.unmatched_output_file
        else run_dir / "unmatched_promo_cards.jsonl"
    )

    crawl_timestamp = now_local_iso()
    status = "success"
    error_message = None
    summary: dict[str, Any] = {}
    product_input_files: list[str] = []

    try:
        products, product_input_files = read_product_inputs(products_file, hydrated_products_file)
        promos = read_jsonl(promo_cards_file, required=False)
        enriched, unmatched, summary = enrich_products(products, promos)
        write_jsonl_replace(output_file, enriched)
        write_jsonl_replace(unmatched_file, unmatched)
    except Exception as exc:  # noqa: BLE001 - persist merge failure.
        status = "failed"
        error_message = str(exc)

    metadata = {
        "store_id": STORE_ID,
        "store_name": STORE_NAME,
        "crawler": "merge_promos",
        "crawl_timestamp": crawl_timestamp,
        "status": status,
        "error_message": error_message,
        "run_dir": str(run_dir),
        "products_file": str(products_file),
        "hydrated_products_file": str(hydrated_products_file),
        "product_input_files": product_input_files,
        "promo_cards_file": str(promo_cards_file),
        "products_enriched_file": str(output_file),
        "unmatched_promo_cards_file": str(unmatched_file),
        **summary,
    }
    write_json(run_dir / args.metadata_name, metadata)
    print(json.dumps(metadata, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if status == "success" else 1


if __name__ == "__main__":
    sys.exit(main())
