#!/usr/bin/env python3
"""Capture WinMart promotion mechanic text from rendered product cards.

The category API is still the source of truth for product, price, stock and
promotion codes. This scraper complements it by reading UI text rendered on
product cards, such as "Mua 5 Goi duoc tang 1 goi..." or "Mua 3 goi voi gia
12.000 d", then linking that text back to products through itemNo in the card
URL.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import unicodedata
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import requests
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright

from winmart_config_scraper import append_query_params, enabled_categories, load_config
from winmart_scraper import (
    DEFAULT_USER_AGENT,
    STORE_ID,
    STORE_NAME,
    append_jsonl,
    build_output_paths,
    can_fetch,
    money_to_number,
    now_local_iso,
    safe_name,
    write_json,
)


DEFAULT_CONFIG = "configs/winmart_categories.yaml"
DEFAULT_URL = "https://winmart.vn/gia-sieu-re--c114?cate2=&brands=&order=km&storeCode=1682&storeGroupCode=1998"


def strip_accents(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value)
    return "".join(char for char in normalized if not unicodedata.combining(char))


def compact_text(value: str | None) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def item_no_from_href(href: str | None) -> str | None:
    if not href:
        return None
    match = re.search(r"--s(\d+)(?:[/?#]|$)", href)
    return match.group(1) if match else None


def promo_lines_from_card_text(card_text: str) -> list[str]:
    lines = [compact_text(line) for line in card_text.splitlines()]
    lines = [line for line in lines if line]
    found: list[str] = []
    seen: set[str] = set()

    def add_if_promo(phrase: str) -> None:
        phrase = compact_text(phrase)
        if not phrase or len(phrase) > 320:
            return
        normalized = strip_accents(phrase).lower()
        has_buy = re.search(r"\bmua\s+\d+", normalized) is not None
        has_mechanic = any(
            keyword in normalized
            for keyword in (
                "duoc tang",
                "tang",
                "voi gia",
                "tinh tien",
                "dong gia",
            )
        )
        if not (has_buy and has_mechanic):
            return
        if normalized not in seen:
            found.append(phrase)
            seen.add(normalized)

    for line in lines:
        add_if_promo(line)

    if found:
        return found

    for window in (2, 3):
        for index in range(len(lines)):
            phrase = compact_text(" ".join(lines[index : index + window]))
            add_if_promo(phrase)

    return found


def parse_promo_mechanic(text: str) -> dict[str, Any]:
    normalized = strip_accents(text).lower()
    parsed: dict[str, Any] = {
        "promo_mechanic_text": text,
        "promo_mechanic_type": "unknown",
    }

    buy_get = re.search(
        r"\bmua\s+(\d+)\s*([a-z0-9]+)?\b.{0,100}?(?:duoc\s+)?tang\s+(\d+)\s*([a-z0-9]+)?",
        normalized,
    )
    if buy_get:
        parsed.update(
            {
                "promo_mechanic_type": "buy_x_get_y",
                "buy_quantity": int(buy_get.group(1)),
                "buy_uom": buy_get.group(2),
                "gift_quantity": int(buy_get.group(3)),
                "gift_uom": buy_get.group(4),
            }
        )
        return parsed

    bundle_price = re.search(
        r"\bmua\s+(\d+)\s*([a-z0-9]+)?\b.{0,80}?voi\s+gia\s+([\d.,]+)",
        normalized,
    )
    if bundle_price:
        quantity = int(bundle_price.group(1))
        price = money_to_number(bundle_price.group(3))
        parsed.update(
            {
                "promo_mechanic_type": "bundle_price",
                "bundle_quantity": quantity,
                "bundle_uom": bundle_price.group(2),
                "bundle_price": price,
                "effective_unit_price": round(price / quantity, 2)
                if isinstance(price, (int, float)) and quantity
                else None,
            }
        )
        return parsed

    buy_pay = re.search(r"\bmua\s+(\d+)\b.{0,80}?tinh\s+tien\s+(\d+)", normalized)
    if buy_pay:
        parsed.update(
            {
                "promo_mechanic_type": "buy_x_pay_y",
                "buy_quantity": int(buy_pay.group(1)),
                "pay_quantity": int(buy_pay.group(2)),
            }
        )
        return parsed

    return parsed


def guess_product_name(raw_card: dict[str, Any], promo_lines: list[str]) -> str | None:
    anchor_text = compact_text(raw_card.get("anchorText"))
    if anchor_text:
        return anchor_text

    promo_texts = {strip_accents(text).lower() for text in promo_lines}
    for line in (raw_card.get("cardText") or "").splitlines():
        candidate = compact_text(line)
        normalized = strip_accents(candidate).lower()
        if not candidate or normalized in promo_texts:
            continue
        if "mua " in normalized or " voi gia " in normalized or " duoc tang " in normalized:
            continue
        if "₫" in candidate or re.search(r"\d+[.,]\d{3}\s*d\b", normalized):
            continue
        if len(candidate) >= 8:
            return candidate
    return None


def category_urls_from_config(config_path: Path, limit_categories: int | None) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    config = load_config(config_path)
    profile = config.get("crawl_profile") or {}
    if not isinstance(profile, dict):
        raise ValueError("Config field crawl_profile must be an object")

    categories = enabled_categories(config)
    if limit_categories is not None:
        categories = categories[:limit_categories]

    urls: list[dict[str, Any]] = []
    for category in categories:
        source_url = category.get("source_url")
        if not source_url:
            continue
        url = append_query_params(
            str(source_url),
            {
                "storeCode": str(profile.get("store_code") or ""),
                "storeGroupCode": str(profile.get("store_group_code") or ""),
            },
        )
        urls.append(
            {
                "url": url,
                "category_code": category.get("code"),
                "category_name": category.get("name"),
                "category_slug": category.get("slug"),
                "category_level": category.get("level"),
            }
        )
    return urls, profile


def direct_urls(urls: list[str]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    return [{"url": url} for url in urls], {}


def extract_cards(page: Any) -> list[dict[str, Any]]:
    return page.evaluate(
        """
        () => {
          const anchors = Array.from(document.querySelectorAll('a[href]'))
            .filter((anchor) => /\\/products\\/.*--s\\d+/.test(anchor.href || anchor.getAttribute('href') || ''));
          const seen = new Set();
          const cards = [];

          const visibleText = (element) => {
            if (!element) return '';
            return (element.innerText || '').replace(/[ \\t]+/g, ' ').trim();
          };

          const chooseContainer = (anchor) => {
            let element = anchor;
            let best = anchor;
            for (let depth = 0; element && depth < 8; depth += 1, element = element.parentElement) {
              const text = visibleText(element);
              const rect = element.getBoundingClientRect();
              if (text.length >= 20 && text.length <= 1800 && rect.width >= 80 && rect.height >= 40) {
                best = element;
                const lowered = text.toLowerCase();
                if (lowered.includes('mua ') || lowered.includes('₫') || lowered.includes('đ')) {
                  return element;
                }
              }
            }
            return best;
          };

          for (const anchor of anchors) {
            const href = anchor.href || anchor.getAttribute('href') || '';
            if (seen.has(href)) continue;
            seen.add(href);

            const card = chooseContainer(anchor);
            const rect = card.getBoundingClientRect();
            cards.push({
              href,
              anchorText: visibleText(anchor),
              cardText: visibleText(card),
              x: Math.round(rect.x),
              y: Math.round(rect.y),
              width: Math.round(rect.width),
              height: Math.round(rect.height),
            });
          }

          return cards;
        }
        """
    )


def normalize_card(
    raw_card: dict[str, Any],
    source_url: str,
    source_context: dict[str, Any],
    profile: dict[str, Any],
    crawl_timestamp: str,
    raw_payload_path: str,
) -> list[dict[str, Any]]:
    href = raw_card.get("href")
    item_no = item_no_from_href(href)
    promo_lines = promo_lines_from_card_text(raw_card.get("cardText") or "")
    product_name = guess_product_name(raw_card, promo_lines)

    records: list[dict[str, Any]] = []
    for promo_line in promo_lines:
        parsed = parse_promo_mechanic(promo_line)
        records.append(
            {
                "store_id": STORE_ID,
                "store_name": STORE_NAME,
                "region": profile.get("region"),
                "store_code": profile.get("store_code"),
                "store_group_code": profile.get("store_group_code"),
                "branch_name": profile.get("branch_name"),
                "source_url": source_url,
                "product_url": href,
                "item_no": item_no,
                "product_name_raw": product_name,
                "promo_mechanic_text": promo_line,
                "promo_mechanic_type": parsed.get("promo_mechanic_type"),
                "promo_mechanic": parsed,
                "match_key": f"itemNo:{item_no}" if item_no else f"url:{href}",
                "match_confidence": "high" if item_no else "medium",
                "promo_source": "dom_card",
                "crawl_timestamp": crawl_timestamp,
                "raw_payload_path": raw_payload_path,
                "source_category_code": source_context.get("category_code"),
                "source_category_name": source_context.get("category_name"),
                "source_category_slug": source_context.get("category_slug"),
                "source_category_level": source_context.get("category_level"),
                "raw_card": raw_card,
            }
        )
    return records


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Crawl WinMart promo mechanic text from rendered product cards.")
    parser.add_argument("--config", default=DEFAULT_CONFIG, help="Path to WinMart category YAML config.")
    parser.add_argument("--url", action="append", help="Category URL to crawl. Can be passed multiple times.")
    parser.add_argument("--out-dir", default="raw", help="Raw output directory.")
    parser.add_argument("--run-id", default=None, help="Optional run id.")
    parser.add_argument("--region", default=None, help="Override region when using --url without config.")
    parser.add_argument("--store-code", default=None, help="Override storeCode when using --url without config.")
    parser.add_argument("--store-group-code", default=None, help="Override storeGroupCode when using --url without config.")
    parser.add_argument("--limit-categories", type=int, default=None, help="Only crawl first N enabled config categories.")
    parser.add_argument("--limit-records", type=int, default=1000, help="Maximum promo records to write.")
    parser.add_argument("--timeout-ms", type=int, default=45000, help="Playwright timeout in milliseconds.")
    parser.add_argument("--wait-ms", type=int, default=1800, help="Wait after load and each scroll.")
    parser.add_argument("--scrolls", type=int, default=8, help="Number of scrolls per URL.")
    parser.add_argument("--headed", action="store_true", help="Open a visible browser for debugging.")
    parser.add_argument("--screenshot", action="store_true", help="Save full page screenshots.")
    parser.add_argument("--user-agent", default=DEFAULT_USER_AGENT, help="HTTP User-Agent.")
    parser.add_argument("--skip-robots-check", action="store_true", help="Skip robots.txt checks for target URLs.")
    parser.add_argument(
        "--metadata-name",
        default="metadata.json",
        help="Metadata filename inside the run directory.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    paths = build_output_paths(Path(args.out_dir), args.run_id)
    promo_cards_jsonl = paths.run_dir / "promo_cards.jsonl"
    crawl_timestamp = now_local_iso()

    if args.url:
        targets, profile = direct_urls(args.url)
        profile.update(
            {
                "region": args.region,
                "store_code": args.store_code,
                "store_group_code": args.store_group_code,
            }
        )
    else:
        targets, profile = category_urls_from_config(Path(args.config), args.limit_categories)

    if not targets:
        raise ValueError("No target category URLs found")

    status = "success"
    error_message = None
    cards_seen = 0
    promo_records_written = 0
    urls_completed = 0
    url_errors: list[dict[str, Any]] = []
    seen_records: set[str] = set()

    session = requests.Session()
    session.headers.update({"User-Agent": args.user_agent})

    try:
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=not args.headed)
            context = browser.new_context(
                user_agent=args.user_agent,
                locale="vi-VN",
                viewport={"width": 1366, "height": 900},
            )
            page = context.new_page()

            for index, target in enumerate(targets, start=1):
                url = target["url"]
                if promo_records_written >= args.limit_records:
                    break

                try:
                    if not args.skip_robots_check:
                        allowed, reason = can_fetch(session, url, args.user_agent, timeout=20)
                        append_jsonl(
                            paths.log_jsonl,
                            {
                                "event": "robots_check",
                                "url": url,
                                "allowed": allowed,
                                "message": reason,
                                "timestamp": now_local_iso(),
                            },
                        )
                        if not allowed:
                            raise RuntimeError(reason)

                    append_jsonl(
                        paths.log_jsonl,
                        {
                            "event": "browser_goto",
                            "url": url,
                            "category_slug": target.get("category_slug"),
                            "timestamp": now_local_iso(),
                        },
                    )
                    page.goto(url, wait_until="domcontentloaded", timeout=args.timeout_ms)
                    page.wait_for_timeout(args.wait_ms)

                    for scroll_index in range(args.scrolls):
                        page.mouse.wheel(0, 1400)
                        page.wait_for_timeout(args.wait_ms)
                        append_jsonl(
                            paths.log_jsonl,
                            {
                                "event": "scroll",
                                "url": url,
                                "scroll_index": scroll_index + 1,
                                "timestamp": now_local_iso(),
                            },
                        )

                    payload_name = f"{index:03d}_{safe_name(target.get('category_slug') or urlparse(url).path, 'category')}"
                    html_path = paths.raw_payloads_dir / "promo_cards" / f"{payload_name}.html"
                    html_path.parent.mkdir(parents=True, exist_ok=True)
                    html_path.write_text(page.content(), encoding="utf-8")

                    if args.screenshot:
                        screenshot_path = paths.raw_payloads_dir / "promo_cards" / f"{payload_name}.png"
                        page.screenshot(path=str(screenshot_path), full_page=True)

                    cards = extract_cards(page)
                    cards_seen += len(cards)
                    records_for_url = 0
                    for card in cards:
                        if promo_records_written >= args.limit_records:
                            break
                        records = normalize_card(card, url, target, profile, crawl_timestamp, str(html_path))
                        for record in records:
                            record_key = json.dumps(
                                {
                                    "item_no": record.get("item_no"),
                                    "text": record.get("promo_mechanic_text"),
                                    "url": record.get("product_url"),
                                },
                                ensure_ascii=False,
                                sort_keys=True,
                            )
                            if record_key in seen_records:
                                continue
                            seen_records.add(record_key)
                            append_jsonl(promo_cards_jsonl, record)
                            promo_records_written += 1
                            records_for_url += 1

                    urls_completed += 1
                    append_jsonl(
                        paths.log_jsonl,
                        {
                            "event": "promo_cards_page",
                            "url": url,
                            "category_slug": target.get("category_slug"),
                            "raw_payload_path": str(html_path),
                            "cards_seen": len(cards),
                            "promo_records_written": records_for_url,
                            "timestamp": now_local_iso(),
                        },
                    )
                except Exception as exc:  # noqa: BLE001 - keep following URLs moving.
                    url_error = {
                        "url": url,
                        "category_slug": target.get("category_slug"),
                        "error": str(exc),
                    }
                    url_errors.append(url_error)
                    append_jsonl(paths.log_jsonl, {"event": "promo_cards_url_failed", **url_error, "timestamp": now_local_iso()})

            context.close()
            browser.close()

    except PlaywrightTimeoutError as exc:
        status = "failed"
        error_message = f"Playwright timeout: {exc}"
    except Exception as exc:  # noqa: BLE001 - persist run-level failure.
        status = "failed"
        error_message = str(exc)

    if url_errors and status == "success":
        status = "partial_failed"
        error_message = f"{len(url_errors)} URLs failed; see url_errors and scrape_log.jsonl"
    if not promo_cards_jsonl.exists():
        promo_cards_jsonl.touch()

    metadata = {
        "store_id": STORE_ID,
        "store_name": STORE_NAME,
        "crawler": "playwright_promo_cards",
        "config_path": None if args.url else str(Path(args.config)),
        "crawl_timestamp": crawl_timestamp,
        "status": status,
        "error_message": error_message,
        "region": profile.get("region"),
        "store_code": profile.get("store_code"),
        "store_group_code": profile.get("store_group_code"),
        "target_urls": len(targets),
        "urls_completed": urls_completed,
        "urls_failed": len(url_errors),
        "url_errors": url_errors,
        "cards_seen": cards_seen,
        "promo_records_written": sum(1 for _ in promo_cards_jsonl.open(encoding="utf-8")),
        "promo_cards_jsonl": str(promo_cards_jsonl),
        "run_dir": str(paths.run_dir),
        "note": (
            "promo_cards.jsonl contains UI promotion mechanic text from rendered product cards. "
            "Merge with products.jsonl by item_no/raw_product.itemNo, and by UOM when the text includes a unit."
        ),
    }
    write_json(paths.run_dir / args.metadata_name, metadata)
    print(json.dumps(metadata, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if status in {"success", "partial_failed"} else 1


if __name__ == "__main__":
    sys.exit(main())
