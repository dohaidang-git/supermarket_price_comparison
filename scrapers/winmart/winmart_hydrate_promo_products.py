#!/usr/bin/env python3
"""Hydrate only WinMart promo-card products missing from products.jsonl."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

import requests
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright

from winmart_config_scraper import append_query_params, enrich_record, load_config
from winmart_merge_promos import product_item_no, read_jsonl, write_jsonl_replace
from winmart_scraper import (
    DEFAULT_USER_AGENT,
    STORE_ID,
    STORE_NAME,
    append_jsonl,
    build_output_paths,
    can_fetch,
    extract_next_data,
    extract_product_candidates,
    normalize_product,
    now_local_iso,
    safe_name,
    write_json,
)


DEFAULT_CONFIG = "configs/winmart_categories.yaml"


def raw_item_no(raw_product: dict[str, Any]) -> str | None:
    for key in ("itemNo", "item_no", "itemNumber"):
        value = raw_product.get(key)
        if value not in (None, ""):
            return str(value)
    return None


def promo_item_no(promo: dict[str, Any]) -> str | None:
    value = promo.get("item_no")
    return None if value in (None, "") else str(value)


def product_keys(products: list[dict[str, Any]]) -> set[str]:
    found: set[str] = set()
    for product in products:
        item_no = product_item_no(product)
        if item_no:
            found.add(item_no)
    return found


def promo_context_category(promo: dict[str, Any]) -> dict[str, Any]:
    return {
        "code": promo.get("source_category_code"),
        "name": promo.get("source_category_name"),
        "slug": promo.get("source_category_slug"),
        "level": promo.get("source_category_level"),
        "parent_code": None,
    }


def normalize_hydrated_product(
    raw_product: dict[str, Any],
    promo: dict[str, Any],
    profile: dict[str, Any],
    crawl_timestamp: str,
    raw_payload_path: str,
    hydrate_source: str,
) -> dict[str, Any]:
    record = normalize_product(
        raw_product,
        promo.get("source_url") or promo.get("product_url") or "",
        profile.get("region") or "",
        crawl_timestamp,
        raw_payload_path,
    )
    record = enrich_record(record, raw_product, promo_context_category(promo), profile)
    record["hydrate_source"] = hydrate_source
    record["hydrated_from_promo_card"] = True
    record["hydrate_promo_item_no"] = promo_item_no(promo)
    record["hydrate_promo_product_url"] = promo.get("product_url")
    record["hydrate_promo_mechanic_text"] = promo.get("promo_mechanic_text")
    record["is_on_promotion"] = True
    return record


def find_in_existing_payloads(
    run_dir: Path,
    wanted_item_nos: set[str],
) -> dict[str, tuple[dict[str, Any], str]]:
    found: dict[str, tuple[dict[str, Any], str]] = {}
    for payload_path in sorted((run_dir / "raw_payloads").rglob("*.json")):
        if len(found) == len(wanted_item_nos):
            break
        try:
            payload = json.loads(payload_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            continue
        for candidate in extract_product_candidates(payload):
            item_no = raw_item_no(candidate)
            if item_no in wanted_item_nos and item_no not in found:
                found[item_no] = (candidate, str(payload_path))
    return found


def rewrite_product_url(url: str, profile: dict[str, Any]) -> str:
    return append_query_params(
        url,
        {
            "storeCode": str(profile.get("store_code") or ""),
            "storeGroupCode": str(profile.get("store_group_code") or ""),
        },
    )


def extract_json_response(response: Any) -> dict[str, Any] | None:
    headers = response.headers or {}
    content_type = headers.get("content-type", "")
    if "json" not in content_type.lower():
        return None
    try:
        payload = response.json()
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def hydrate_from_detail_pages(
    promos_by_item_no: dict[str, dict[str, Any]],
    profile: dict[str, Any],
    paths: Any,
    args: argparse.Namespace,
    crawl_timestamp: str,
    already_found: set[str],
) -> tuple[dict[str, tuple[dict[str, Any], str]], list[dict[str, Any]]]:
    remaining = [
        item_no
        for item_no in promos_by_item_no
        if item_no not in already_found and promos_by_item_no[item_no].get("product_url")
    ]
    if args.max_hydrate_items is not None:
        remaining = remaining[: args.max_hydrate_items]

    found: dict[str, tuple[dict[str, Any], str]] = {}
    errors: list[dict[str, Any]] = []
    if not remaining or args.skip_detail_pages:
        return found, errors

    session = requests.Session()
    session.headers.update({"User-Agent": args.user_agent})

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=not args.headed)
        context = browser.new_context(
            user_agent=args.user_agent,
            locale="vi-VN",
            viewport={"width": 1366, "height": 900},
        )
        page = context.new_page()

        for item_no in remaining:
            promo = promos_by_item_no[item_no]
            url = rewrite_product_url(str(promo.get("product_url")), profile)
            response_index = 0
            response_payloads: list[tuple[dict[str, Any], str]] = []

            def on_response(response: Any) -> None:
                nonlocal response_index
                payload = extract_json_response(response)
                if payload is None:
                    return
                response_index += 1
                response_path = (
                    paths.raw_payloads_dir
                    / "promo_hydrated"
                    / safe_name(item_no, "item")
                    / f"response_{response_index:03d}.json"
                )
                write_json(response_path, payload)
                response_payloads.append((payload, str(response_path)))

            page.on("response", on_response)
            try:
                if not args.skip_robots_check:
                    allowed, reason = can_fetch(session, url, args.user_agent, timeout=20)
                    append_jsonl(
                        paths.log_jsonl,
                        {
                            "event": "hydrate_robots_check",
                            "item_no": item_no,
                            "url": url,
                            "allowed": allowed,
                            "message": reason,
                            "timestamp": now_local_iso(),
                        },
                    )
                    if not allowed:
                        raise RuntimeError(reason)

                page.goto(url, wait_until="domcontentloaded", timeout=args.timeout_ms)
                page.wait_for_timeout(args.wait_ms)

                html_path = (
                    paths.raw_payloads_dir
                    / "promo_hydrated"
                    / safe_name(item_no, "item")
                    / "page.html"
                )
                html_path.parent.mkdir(parents=True, exist_ok=True)
                html_path.write_text(page.content(), encoding="utf-8")

                for payload, payload_path in response_payloads:
                    for candidate in extract_product_candidates(payload):
                        if raw_item_no(candidate) == item_no:
                            found[item_no] = (candidate, payload_path)
                            break
                    if item_no in found:
                        break

                if item_no not in found:
                    next_data = extract_next_data(page.content())
                    if next_data is not None:
                        next_path = html_path.with_name("next_data.json")
                        write_json(next_path, next_data)
                        for candidate in extract_product_candidates(next_data):
                            if raw_item_no(candidate) == item_no:
                                found[item_no] = (candidate, str(next_path))
                                break

                append_jsonl(
                    paths.log_jsonl,
                    {
                        "event": "hydrate_detail_page",
                        "item_no": item_no,
                        "url": url,
                        "matched": item_no in found,
                        "timestamp": now_local_iso(),
                    },
                )
            except (PlaywrightTimeoutError, Exception) as exc:  # noqa: BLE001 - keep following items moving.
                error = {"item_no": item_no, "url": url, "error": str(exc)}
                errors.append(error)
                append_jsonl(paths.log_jsonl, {"event": "hydrate_detail_failed", **error, "timestamp": now_local_iso()})
            finally:
                page.remove_listener("response", on_response)

        context.close()
        browser.close()

    return found, errors


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Hydrate WinMart products for unmatched promo cards only.")
    parser.add_argument("--config", default=DEFAULT_CONFIG, help="Path to WinMart category YAML config.")
    parser.add_argument("--run-dir", default=None, help="Existing run directory.")
    parser.add_argument("--out-dir", default="raw", help="Raw output directory if --run-dir is not used.")
    parser.add_argument("--run-id", default=None, help="Run id if --run-dir is not used.")
    parser.add_argument("--products-file", default=None, help="Input discounted products file.")
    parser.add_argument("--promo-cards-file", default=None, help="Input promo cards file.")
    parser.add_argument("--output-file", default=None, help="Output hydrated products JSONL.")
    parser.add_argument("--metadata-name", default="metadata_hydrate_promos.json", help="Metadata filename inside run dir.")
    parser.add_argument("--max-hydrate-items", type=int, default=None, help="Maximum detail pages to hydrate.")
    parser.add_argument("--timeout-ms", type=int, default=45000, help="Playwright timeout in milliseconds.")
    parser.add_argument("--wait-ms", type=int, default=1800, help="Wait after product detail load.")
    parser.add_argument("--headed", action="store_true", help="Run browser headed.")
    parser.add_argument("--skip-detail-pages", action="store_true", help="Only search existing raw API payloads.")
    parser.add_argument("--skip-robots-check", action="store_true", help="Skip robots.txt checks for detail pages.")
    parser.add_argument("--user-agent", default=DEFAULT_USER_AGENT, help="HTTP User-Agent.")
    return parser.parse_args()


def resolve_run_dir(args: argparse.Namespace) -> Path:
    if args.run_dir:
        return Path(args.run_dir)
    if args.run_id:
        return build_output_paths(Path(args.out_dir), args.run_id).run_dir
    raise ValueError("Pass --run-dir or --run-id")


def main() -> int:
    args = parse_args()
    run_dir = resolve_run_dir(args)
    paths = build_output_paths(Path(args.out_dir), run_dir.name.removeprefix("run_id="))
    if args.run_dir:
        paths.run_dir = run_dir
        paths.raw_payloads_dir = run_dir / "raw_payloads"
        paths.products_jsonl = run_dir / "products.jsonl"
        paths.log_jsonl = run_dir / "scrape_log.jsonl"

    config = load_config(Path(args.config))
    profile = config.get("crawl_profile") or {}
    if not isinstance(profile, dict):
        raise ValueError("Config field crawl_profile must be an object")
    options = config.get("crawl_options") or {}
    if args.max_hydrate_items is None:
        args.max_hydrate_items = options.get("max_hydrate_items")
    if options.get("hydrate_detail_pages") is False:
        args.skip_detail_pages = True

    products_file = Path(args.products_file) if args.products_file else run_dir / "products.jsonl"
    promo_cards_file = Path(args.promo_cards_file) if args.promo_cards_file else run_dir / "promo_cards.jsonl"
    output_file = Path(args.output_file) if args.output_file else run_dir / "promo_hydrated_products.jsonl"
    crawl_timestamp = now_local_iso()

    status = "success"
    error_message = None
    hydrated_records: list[dict[str, Any]] = []
    hydrate_errors: list[dict[str, Any]] = []
    existing_payload_hits: dict[str, tuple[dict[str, Any], str]] = {}
    detail_hits: dict[str, tuple[dict[str, Any], str]] = {}
    missing_item_nos: list[str] = []

    try:
        products = read_jsonl(products_file, required=True)
        promos = read_jsonl(promo_cards_file, required=False)
        existing_item_nos = product_keys(products)
        promos_by_item_no: dict[str, dict[str, Any]] = {}
        for promo in promos:
            item_no = promo_item_no(promo)
            if item_no and item_no not in existing_item_nos and item_no not in promos_by_item_no:
                promos_by_item_no[item_no] = promo

        wanted = set(promos_by_item_no)
        existing_payload_hits = find_in_existing_payloads(run_dir, wanted)

        found_item_nos = set(existing_payload_hits)
        detail_hits, hydrate_errors = hydrate_from_detail_pages(
            promos_by_item_no,
            profile,
            paths,
            args,
            crawl_timestamp,
            found_item_nos,
        )
        all_hits = {**existing_payload_hits, **detail_hits}

        for item_no, (raw_product, payload_path) in all_hits.items():
            hydrated_records.append(
                normalize_hydrated_product(
                    raw_product,
                    promos_by_item_no[item_no],
                    profile,
                    crawl_timestamp,
                    payload_path,
                    "existing_payload" if item_no in existing_payload_hits else "detail_page",
                )
            )

        missing_item_nos = sorted(wanted - set(all_hits))
        write_jsonl_replace(output_file, hydrated_records)
        if hydrate_errors:
            status = "partial_failed"
            error_message = f"{len(hydrate_errors)} detail pages failed; see hydrate_errors"
    except Exception as exc:  # noqa: BLE001 - persist hydrate failure.
        status = "failed"
        error_message = str(exc)
        write_jsonl_replace(output_file, hydrated_records)

    metadata = {
        "store_id": STORE_ID,
        "store_name": STORE_NAME,
        "crawler": "hydrate_promo_products",
        "crawl_timestamp": crawl_timestamp,
        "status": status,
        "error_message": error_message,
        "run_dir": str(run_dir),
        "products_file": str(products_file),
        "promo_cards_file": str(promo_cards_file),
        "promo_hydrated_products_file": str(output_file),
        "existing_payload_hits": len(existing_payload_hits),
        "detail_page_hits": len(detail_hits),
        "hydrated_products_written": len(hydrated_records),
        "missing_item_nos": missing_item_nos,
        "missing_item_nos_count": len(missing_item_nos),
        "hydrate_errors": hydrate_errors,
        "max_hydrate_items": args.max_hydrate_items,
        "skip_detail_pages": args.skip_detail_pages,
    }
    write_json(run_dir / args.metadata_name, metadata)
    print(json.dumps(metadata, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if status in {"success", "partial_failed"} else 1


if __name__ == "__main__":
    sys.exit(main())

