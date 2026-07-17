#!/usr/bin/env python3
"""Crawl WinMart categories from configs/winmart_categories.yaml.

This is the operational scraper for known WinMart category APIs. It reads the
fixed MVP store profile, crawls enabled categories by pageNumber, writes all
product candidates for downstream promo-card matching, and keeps the legacy
discounted product output for direct price-discount use cases.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Any
from urllib.parse import urlencode, urlparse, urlunparse, parse_qsl

import requests
import yaml

from winmart_scraper import (
    DEFAULT_USER_AGENT,
    STORE_ID,
    STORE_NAME,
    append_jsonl,
    build_output_paths,
    extract_product_candidates,
    first_value,
    money_to_number,
    normalize_product,
    now_local_iso,
    safe_name,
    write_json,
)


DEFAULT_CONFIG = "configs/winmart_categories.yaml"
DEFAULT_API_TEMPLATE = (
    "https://api-crownx.winmart.vn/it/api/web/v3/item/category"
    "?orderByDesc=true&pageNumber={page_number}&pageSize={page_size}"
    "&slug={slug}&storeCode={store_code}"
)


def load_config(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8") as handle:
        data = yaml.safe_load(handle)
    if not isinstance(data, dict):
        raise ValueError(f"Config must be a YAML object: {path}")
    return data


def enabled_categories(config: dict[str, Any]) -> list[dict[str, Any]]:
    categories = config.get("categories") or []
    if not isinstance(categories, list):
        raise ValueError("Config field categories must be a list")
    return [
        category
        for category in categories
        if isinstance(category, dict) and category.get("enabled") is True
    ]


def append_query_params(url: str, params: dict[str, str | None]) -> str:
    clean_params = {key: value for key, value in params.items() if value not in (None, "")}
    if not clean_params:
        return url
    parsed = urlparse(url)
    query = dict(parse_qsl(parsed.query, keep_blank_values=True))
    query.update(clean_params)
    return urlunparse(parsed._replace(query=urlencode(query)))


def category_api_url(
    template: str,
    category: dict[str, Any],
    profile: dict[str, Any],
    page_number: int,
    page_size: int,
) -> str:
    url = template.format(
        page_number=page_number,
        page_size=page_size,
        slug=category["slug"],
        store_code=profile.get("store_code"),
        store_group_code=profile.get("store_group_code") or "",
    )
    if profile.get("store_group_code") and "storeGroupCode=" not in url:
        url = append_query_params(url, {"storeGroupCode": str(profile["store_group_code"])})
    return url


def is_discounted(raw_product: dict[str, Any]) -> bool:
    listed_price = money_to_number(
        first_value(raw_product, ("price", "listedPrice", "listPrice", "originalPrice", "oldPrice"))
    )
    promo_price = money_to_number(
        first_value(raw_product, ("salePrice", "promoPrice", "discountPrice", "promotionPrice", "specialPrice"))
    )
    return listed_price is not None and promo_price is not None and promo_price < listed_price


def is_direct_price_discount(record: dict[str, Any]) -> bool:
    listed_price = record.get("listed_price")
    promo_price = record.get("promo_price")
    return (
        isinstance(listed_price, (int, float))
        and isinstance(promo_price, (int, float))
        and promo_price < listed_price
    )


def raw_product_key(raw_product: dict[str, Any], dedupe_keys: list[str]) -> str:
    for key in dedupe_keys:
        value = raw_product.get(key)
        if value not in (None, ""):
            return f"{key}:{value}"
    fallback = {
        "id": raw_product.get("id"),
        "name": raw_product.get("name"),
        "sku": raw_product.get("sku"),
        "barcode": raw_product.get("barcode"),
    }
    return json.dumps(fallback, ensure_ascii=False, sort_keys=True, default=str)


def enrich_record(
    record: dict[str, Any],
    raw_product: dict[str, Any],
    category: dict[str, Any],
    profile: dict[str, Any],
) -> dict[str, Any]:
    listed_price = record.get("listed_price")
    promo_price = record.get("promo_price")
    record["is_price_discount"] = is_direct_price_discount(record)
    record["is_on_promotion"] = record["is_price_discount"]
    if record["is_price_discount"]:
        record["discount_amount"] = listed_price - promo_price
        record["discount_percent"] = round((listed_price - promo_price) / listed_price * 100, 2)

    quantity = raw_product.get("quantity")
    if quantity not in (None, ""):
        record["availability_raw"] = f"quantity={quantity}"
        record["stock_quantity"] = quantity

    record.update(
        {
            "store_code": profile.get("store_code"),
            "store_group_code": profile.get("store_group_code"),
            "address_context": profile.get("address_context"),
            "branch_name": profile.get("branch_name") or record.get("branch_name"),
            "source_category_code": category.get("code"),
            "source_category_name": category.get("name"),
            "source_category_slug": category.get("slug"),
            "source_category_level": category.get("level"),
            "source_category_parent_code": category.get("parent_code"),
        }
    )
    return record


def fetch_json(session: requests.Session, url: str, timeout: int) -> tuple[dict[str, Any], requests.Response]:
    response = session.get(url, timeout=timeout)
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict):
        raise ValueError(f"Expected JSON object from {url}")
    return payload, response


def crawl_category(
    session: requests.Session,
    category: dict[str, Any],
    config: dict[str, Any],
    paths: Any,
    args: argparse.Namespace,
    crawl_timestamp: str,
    seen_all_products: set[str],
    seen_discounted_products: set[str],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    profile = config["crawl_profile"]
    options = config.get("crawl_options", {})
    api_template = (
        config.get("api_templates", {}).get("category_items")
        or DEFAULT_API_TEMPLATE
    )
    page_size = args.page_size or int(options.get("page_size") or 8)
    max_pages = args.max_pages or int(options.get("max_pages_per_category") or 50)
    stop_after = args.stop_after_no_discount_pages or int(options.get("stop_after_no_discount_pages") or 3)
    only_discounted = bool(options.get("only_discounted_products", True))
    collect_all_products = bool(options.get("collect_all_products", False))
    stop_on_no_discount_pages = bool(options.get("stop_on_no_discount_pages", not collect_all_products))
    dedupe_keys = list(options.get("dedupe_keys") or ["barcode", "sku", "itemNo", "id"])

    all_records: list[dict[str, Any]] = []
    discounted_records: list[dict[str, Any]] = []
    pages_requested = 0
    pages_with_items = 0
    pages_with_discount = 0
    no_discount_pages = 0
    total_candidates = 0
    total_discount_candidates = 0
    stop_reason = "max_pages"

    for page_number in range(1, max_pages + 1):
        url = category_api_url(api_template, category, profile, page_number, page_size)
        pages_requested += 1
        payload, response = fetch_json(session, url, args.timeout)

        payload_path = (
            paths.raw_payloads_dir
            / safe_name(category["slug"], "category")
            / f"page_{page_number:04d}.json"
        )
        write_json(payload_path, payload)

        candidates = extract_product_candidates(payload)
        discounted = [item for item in candidates if is_discounted(item)]
        total_candidates += len(candidates)
        total_discount_candidates += len(discounted)

        if candidates:
            pages_with_items += 1
        if discounted:
            pages_with_discount += 1
            no_discount_pages = 0
        else:
            no_discount_pages += 1

        append_jsonl(
            paths.log_jsonl,
            {
                "event": "category_page",
                "category_code": category.get("code"),
                "category_name": category.get("name"),
                "category_slug": category.get("slug"),
                "page_number": page_number,
                "status_code": response.status_code,
                "url": url,
                "payload_path": str(payload_path),
                "product_candidates": len(candidates),
                "discount_candidates": len(discounted),
                "records_all_before_dedupe": len(candidates),
                "records_discounted_before_dedupe": len(discounted if only_discounted else candidates),
                "timestamp": now_local_iso(),
            },
        )

        if not candidates:
            stop_reason = "empty_page"
            break

        for raw_product in candidates:
            key = raw_product_key(raw_product, dedupe_keys)
            record = None
            if collect_all_products and key not in seen_all_products:
                seen_all_products.add(key)
                record = normalize_product(
                    raw_product,
                    url,
                    profile.get("region") or "",
                    crawl_timestamp,
                    str(payload_path),
                )
                all_records.append(enrich_record(record, raw_product, category, profile))

            if only_discounted and not is_discounted(raw_product):
                continue
            if key in seen_discounted_products:
                continue
            seen_discounted_products.add(key)
            if record is None:
                record = normalize_product(
                    raw_product,
                    url,
                    profile.get("region") or "",
                    crawl_timestamp,
                    str(payload_path),
                )
                record = enrich_record(record, raw_product, category, profile)
            discounted_records.append(record)

        if only_discounted and stop_on_no_discount_pages and no_discount_pages >= stop_after:
            stop_reason = "no_discount_pages"
            break

        if args.delay > 0:
            time.sleep(args.delay)

    summary = {
        "category_code": category.get("code"),
        "category_name": category.get("name"),
        "category_slug": category.get("slug"),
        "pages_requested": pages_requested,
        "pages_with_items": pages_with_items,
        "pages_with_discount": pages_with_discount,
        "product_candidates": total_candidates,
        "discount_candidates": total_discount_candidates,
        "records_all_written": len(all_records),
        "records_discounted_written": len(discounted_records),
        "records_written": len(discounted_records),
        "collect_all_products": collect_all_products,
        "stop_on_no_discount_pages": stop_on_no_discount_pages,
        "stop_reason": stop_reason,
    }
    append_jsonl(paths.log_jsonl, {"event": "category_summary", **summary, "timestamp": now_local_iso()})
    return all_records, discounted_records, summary


def write_records(path: Path, records: list[dict[str, Any]]) -> None:
    for record in records:
        append_jsonl(path, record)
    if not path.exists():
        path.touch()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Crawl enabled WinMart categories from YAML config.")
    parser.add_argument("--config", default=DEFAULT_CONFIG, help="Path to WinMart category YAML config.")
    parser.add_argument("--out-dir", default="raw", help="Raw output directory.")
    parser.add_argument("--run-id", default=None, help="Optional run id.")
    parser.add_argument("--timeout", type=int, default=20, help="HTTP timeout in seconds.")
    parser.add_argument("--delay", type=float, default=0.5, help="Delay between category page requests.")
    parser.add_argument("--max-pages", type=int, default=None, help="Override max pages per category.")
    parser.add_argument("--page-size", type=int, default=None, help="Override page size.")
    parser.add_argument(
        "--stop-after-no-discount-pages",
        type=int,
        default=None,
        help="Override consecutive no-discount pages before stopping a category.",
    )
    parser.add_argument("--limit-categories", type=int, default=None, help="Only crawl first N enabled categories.")
    parser.add_argument("--dry-run", action="store_true", help="Print config summary and exit without crawling.")
    parser.add_argument("--user-agent", default=DEFAULT_USER_AGENT, help="HTTP User-Agent.")
    parser.add_argument(
        "--metadata-name",
        default="metadata.json",
        help="Metadata filename inside the run directory.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    config_path = Path(args.config)
    config = load_config(config_path)
    profile = config.get("crawl_profile")
    if not isinstance(profile, dict):
        raise ValueError("Config field crawl_profile must be an object")

    categories = enabled_categories(config)
    if args.limit_categories is not None:
        categories = categories[: args.limit_categories]

    if args.dry_run:
        print(
            json.dumps(
                {
                    "config": str(config_path),
                    "retailer_id": config.get("retailer_id"),
                    "profile": profile,
                    "enabled_categories": len(categories),
                    "enabled_category_codes": [category.get("code") for category in categories],
                    "enabled_category_names": [category.get("name") for category in categories],
                },
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
            )
        )
        return 0

    paths = build_output_paths(Path(args.out_dir), args.run_id)
    crawl_timestamp = now_local_iso()
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": args.user_agent,
            "Accept": "application/json,text/plain,*/*",
            "Accept-Language": "vi,en;q=0.8",
        }
    )

    status = "success"
    error_message = None
    all_product_records: list[dict[str, Any]] = []
    discounted_product_records: list[dict[str, Any]] = []
    summaries: list[dict[str, Any]] = []
    category_errors: list[dict[str, Any]] = []
    seen_all_products: set[str] = set()
    seen_discounted_products: set[str] = set()

    try:
        append_jsonl(
            paths.log_jsonl,
            {
                "event": "config_loaded",
                "config_path": str(config_path),
                "enabled_categories": len(categories),
                "store_code": profile.get("store_code"),
                "store_group_code": profile.get("store_group_code"),
                "region": profile.get("region"),
                "timestamp": now_local_iso(),
            },
        )
        for category in categories:
            try:
                all_records, discounted_records, summary = crawl_category(
                    session,
                    category,
                    config,
                    paths,
                    args,
                    crawl_timestamp,
                    seen_all_products,
                    seen_discounted_products,
                )
                all_product_records.extend(all_records)
                discounted_product_records.extend(discounted_records)
                summaries.append(summary)
            except Exception as exc:  # noqa: BLE001 - keep the rest of the configured crawl moving.
                category_error = {
                    "category_code": category.get("code"),
                    "category_name": category.get("name"),
                    "category_slug": category.get("slug"),
                    "error": str(exc),
                }
                category_errors.append(category_error)
                append_jsonl(
                    paths.log_jsonl,
                    {
                        "event": "category_failed",
                        **category_error,
                        "timestamp": now_local_iso(),
                    },
                )
    except Exception as exc:  # noqa: BLE001 - persist crawl failure in metadata/log.
        status = "failed"
        error_message = str(exc)
        append_jsonl(
            paths.log_jsonl,
            {
                "event": "scrape_failed",
                "error": error_message,
                "timestamp": now_local_iso(),
            },
        )

    options = config.get("crawl_options") or {}
    collect_all_products = bool(options.get("collect_all_products", False))
    products_all_jsonl = paths.run_dir / "products_all.jsonl"
    products_discounted_jsonl = paths.run_dir / "products_discounted.jsonl"
    if collect_all_products:
        write_records(products_all_jsonl, all_product_records)
    write_records(paths.products_jsonl, discounted_product_records)
    write_records(products_discounted_jsonl, discounted_product_records)
    if category_errors and status == "success":
        status = "partial_failed"
        error_message = f"{len(category_errors)} categories failed; see category_errors and scrape_log.jsonl"

    metadata = {
        "store_id": STORE_ID,
        "store_name": STORE_NAME,
        "crawler": "config_api",
        "config_path": str(config_path),
        "crawl_timestamp": crawl_timestamp,
        "status": status,
        "error_message": error_message,
        "region": profile.get("region"),
        "store_code": profile.get("store_code"),
        "store_group_code": profile.get("store_group_code"),
        "branch_name": profile.get("branch_name"),
        "address_context": profile.get("address_context"),
        "enabled_categories": len(categories),
        "categories_completed": len(summaries),
        "categories_failed": len(category_errors),
        "category_errors": category_errors,
        "products_all_jsonl": str(products_all_jsonl) if collect_all_products else None,
        "products_discounted_jsonl": str(products_discounted_jsonl),
        "products_jsonl": str(paths.products_jsonl),
        "products_all_written": sum(1 for _ in products_all_jsonl.open(encoding="utf-8"))
        if products_all_jsonl.exists()
        else 0,
        "products_discounted_written": sum(1 for _ in products_discounted_jsonl.open(encoding="utf-8")),
        "products_written": sum(1 for _ in paths.products_jsonl.open(encoding="utf-8")),
        "category_summaries": summaries,
        "run_dir": str(paths.run_dir),
        "note": (
            "Promotion-first mode writes products.jsonl/products_discounted.jsonl for direct price discounts. "
            "If collect_all_products is enabled, products_all.jsonl is also written for debug/backfill."
        ),
    }
    write_json(paths.run_dir / args.metadata_name, metadata)
    print(json.dumps(metadata, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if status in {"success", "partial_failed"} else 1


if __name__ == "__main__":
    sys.exit(main())
