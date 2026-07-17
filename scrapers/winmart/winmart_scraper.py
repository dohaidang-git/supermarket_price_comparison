#!/usr/bin/env python3
"""Small, polite WinMart crawl probe for raw data ingestion.

The scraper intentionally starts with public page HTML and __NEXT_DATA__.
WinMart product grids are often hydrated client-side, so pass an observed
JSON endpoint from DevTools with --api-url when you want product records.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib import robotparser
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup


DEFAULT_URL = "https://winmart.vn/gia-sieu-re--c114?storeCode=1535"
DEFAULT_USER_AGENT = "supermarket-discount-research/0.1 (+local educational crawl)"
STORE_ID = "winmart"
STORE_NAME = "WinMart"


@dataclass
class OutputPaths:
    run_dir: Path
    products_jsonl: Path
    log_jsonl: Path
    raw_payloads_dir: Path
    metadata_json: Path


def now_local_iso() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


def today_partition() -> str:
    configured_date = os.environ.get("WINMART_RUN_DATE")
    if configured_date:
        datetime.fromisoformat(configured_date)
        return configured_date
    return datetime.now().astimezone().date().isoformat()


def run_id() -> str:
    return datetime.now().astimezone().strftime("%Y%m%d_%H%M%S")


def build_output_paths(base_dir: Path, run: str | None = None) -> OutputPaths:
    run = run or run_id()
    run_dir = base_dir / f"store={STORE_ID}" / f"date={today_partition()}" / f"run_id={run}"
    raw_payloads_dir = run_dir / "raw_payloads"
    raw_payloads_dir.mkdir(parents=True, exist_ok=True)
    return OutputPaths(
        run_dir=run_dir,
        products_jsonl=run_dir / "products.jsonl",
        log_jsonl=run_dir / "scrape_log.jsonl",
        raw_payloads_dir=raw_payloads_dir,
        metadata_json=run_dir / "metadata.json",
    )


def append_jsonl(path: Path, record: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n")


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")


def safe_name(value: str, fallback: str) -> str:
    text = re.sub(r"[^A-Za-z0-9._-]+", "_", value.strip())
    text = text.strip("._-")
    return text[:120] or fallback


def robots_url_for(url: str) -> str:
    parsed = urlparse(url)
    return f"{parsed.scheme}://{parsed.netloc}/robots.txt"


def can_fetch(session: requests.Session, url: str, user_agent: str, timeout: int) -> tuple[bool, str]:
    robots_url = robots_url_for(url)
    parser = robotparser.RobotFileParser()
    try:
        response = session.get(robots_url, timeout=timeout)
        response.raise_for_status()
    except requests.RequestException as exc:
        return False, f"Could not read robots.txt at {robots_url}: {exc}"

    parser.parse(response.text.splitlines())
    allowed = parser.can_fetch(user_agent, url)
    if not allowed:
        return False, f"Blocked by robots.txt: {url}"
    return True, "Allowed by robots.txt"


def fetch(
    session: requests.Session,
    url: str,
    timeout: int,
    user_agent: str,
    log_path: Path,
    check_robots: bool = True,
) -> requests.Response:
    if check_robots:
        allowed, reason = can_fetch(session, url, user_agent, timeout)
        append_jsonl(
            log_path,
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

    response = session.get(url, timeout=timeout)
    append_jsonl(
        log_path,
        {
            "event": "http_fetch",
            "url": url,
            "status_code": response.status_code,
            "content_type": response.headers.get("content-type"),
            "content_length": len(response.content),
            "timestamp": now_local_iso(),
        },
    )
    response.raise_for_status()
    return response


def extract_next_data(html: str) -> dict[str, Any] | None:
    soup = BeautifulSoup(html, "html.parser")
    script = soup.find("script", id="__NEXT_DATA__")
    if not script or not script.string:
        return None
    return json.loads(script.string)


def walk_json(value: Any) -> list[Any]:
    found: list[Any] = []
    stack = [value]
    while stack:
        current = stack.pop()
        found.append(current)
        if isinstance(current, dict):
            stack.extend(current.values())
        elif isinstance(current, list):
            stack.extend(current)
    return found


def first_value(payload: dict[str, Any], keys: tuple[str, ...]) -> Any:
    lower_map = {str(key).lower(): key for key in payload.keys()}
    for key in keys:
        actual = lower_map.get(key.lower())
        if actual is not None and payload.get(actual) not in (None, ""):
            return payload.get(actual)
    return None


def money_to_number(value: Any) -> int | float | None:
    if value is None or value == "":
        return None
    if isinstance(value, (int, float)):
        return value
    if isinstance(value, str):
        cleaned = re.sub(r"[^\d.,-]", "", value)
        if not cleaned:
            return None
        if "," in cleaned and "." in cleaned:
            cleaned = cleaned.replace(".", "").replace(",", ".")
        else:
            cleaned = cleaned.replace(",", "").replace(".", "")
        try:
            parsed = float(cleaned)
        except ValueError:
            return None
        return int(parsed) if parsed.is_integer() else parsed
    return None


def looks_like_product(payload: dict[str, Any]) -> bool:
    keys = {str(key).lower() for key in payload.keys()}
    has_name = bool(keys & {"name", "productname", "product_name", "title", "displayname"})
    has_product_id = bool(keys & {"id", "productid", "product_id", "sku", "code", "itemid", "item_id"})
    has_price = any("price" in key for key in keys) or bool(keys & {"saleprice", "sellingprice", "finalprice"})
    has_product_hint = any("product" in key or "sku" in key for key in keys)
    return has_name and (has_price or (has_product_id and has_product_hint))


def extract_product_candidates(payload: Any) -> list[dict[str, Any]]:
    products: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in walk_json(payload):
        if not isinstance(item, dict) or not looks_like_product(item):
            continue
        fingerprint = json.dumps(item, ensure_ascii=False, sort_keys=True, default=str)
        if fingerprint in seen:
            continue
        seen.add(fingerprint)
        products.append(item)
    return products


def normalize_product(
    raw_product: dict[str, Any],
    source_url: str,
    region: str,
    crawl_timestamp: str,
    raw_payload_path: str,
) -> dict[str, Any]:
    product_name = first_value(raw_product, ("productName", "product_name", "name", "title", "displayName"))
    raw_price = money_to_number(first_value(raw_product, ("price", "sellingPrice", "finalPrice", "retailPrice")))
    listed_price = money_to_number(
        first_value(raw_product, ("listedPrice", "listPrice", "originalPrice", "oldPrice", "basePrice", "regularPrice"))
    )
    promo_price = money_to_number(
        first_value(raw_product, ("promoPrice", "salePrice", "discountPrice", "promotionPrice", "specialPrice"))
    )
    if listed_price is None and promo_price is not None and raw_price is not None and raw_price != promo_price:
        listed_price = raw_price
    current_price = money_to_number(first_value(raw_product, ("currentPrice",)))
    if current_price is None:
        current_price = promo_price or raw_price or listed_price
    discount_amount = money_to_number(first_value(raw_product, ("discountAmount", "discount", "saveAmount")))
    discount_percent = money_to_number(first_value(raw_product, ("discountPercent", "discountRate", "percentDiscount")))

    return {
        "store_id": STORE_ID,
        "store_name": STORE_NAME,
        "region": region,
        "branch_name": first_value(raw_product, ("branchName", "storeName")),
        "source_url": source_url,
        "source_product_id": first_value(raw_product, ("productId", "product_id", "id", "sku", "code", "itemId")),
        "product_name_raw": product_name,
        "brand_raw": first_value(raw_product, ("brandName", "brand", "manufacturer")),
        "category_raw": first_value(raw_product, ("category", "categoryName", "cateName")),
        "package_size_raw": first_value(raw_product, ("packageSize", "size", "volume", "weight", "capacity")),
        "unit_raw": first_value(raw_product, ("unit", "uomName", "uom", "measureUnit")),
        "listed_price": listed_price,
        "promo_price": promo_price,
        "current_price": current_price,
        "currency": "VND",
        "discount_amount": discount_amount,
        "discount_percent": discount_percent,
        "promotion_text_raw": first_value(raw_product, ("promotionText", "promotion", "promoText", "description")),
        "promotion_start_date": first_value(raw_product, ("promotionStartDate", "startDate")),
        "promotion_end_date": first_value(raw_product, ("promotionEndDate", "endDate")),
        "availability_raw": first_value(raw_product, ("availability", "stockStatus", "status", "inventoryStatus")),
        "image_url": first_value(raw_product, ("imageUrl", "mediaUrl", "image", "thumbnail", "thumbnailUrl", "avatar")),
        "crawl_timestamp": crawl_timestamp,
        "scrape_status": "success",
        "raw_payload_path": raw_payload_path,
        "raw_product": raw_product,
    }


def category_metadata_from_next_data(next_data: dict[str, Any] | None) -> dict[str, Any]:
    if not next_data:
        return {}
    page_props = next_data.get("props", {}).get("pageProps", {})
    category = page_props.get("data")
    if isinstance(category, dict):
        return {
            "category_code": category.get("categoryCode"),
            "category_name": category.get("name"),
            "category_description": category.get("description"),
            "category_image_url": category.get("imageUrl"),
            "slug": page_props.get("slug"),
        }
    return {}


def scrape_page(
    session: requests.Session,
    url: str,
    paths: OutputPaths,
    args: argparse.Namespace,
    crawl_timestamp: str,
) -> list[dict[str, Any]]:
    response = fetch(session, url, args.timeout, args.user_agent, paths.log_jsonl)
    payload_name = safe_name(urlparse(url).path.replace("/", "_") or "home", "page") + ".html"
    payload_path = paths.raw_payloads_dir / payload_name
    payload_path.write_text(response.text, encoding=response.encoding or "utf-8")

    next_data = extract_next_data(response.text)
    metadata = category_metadata_from_next_data(next_data)
    candidates = extract_product_candidates(next_data or {})
    records = [
        normalize_product(candidate, url, args.region, crawl_timestamp, str(payload_path))
        for candidate in candidates[: args.limit]
    ]

    append_jsonl(
        paths.log_jsonl,
        {
            "event": "html_parse",
            "url": url,
            "next_data_found": next_data is not None,
            "category_metadata": metadata,
            "product_candidates": len(candidates),
            "products_written": len(records),
            "timestamp": now_local_iso(),
        },
    )
    return records


def scrape_api(
    session: requests.Session,
    url: str,
    paths: OutputPaths,
    args: argparse.Namespace,
    crawl_timestamp: str,
    index: int,
) -> list[dict[str, Any]]:
    response = fetch(session, url, args.timeout, args.user_agent, paths.log_jsonl)
    try:
        payload = response.json()
    except ValueError:
        payload = {"raw_text": response.text}

    payload_path = paths.raw_payloads_dir / f"api_{index}_{safe_name(urlparse(url).path, 'endpoint')}.json"
    write_json(payload_path, payload)

    candidates = extract_product_candidates(payload)
    records = [
        normalize_product(candidate, url, args.region, crawl_timestamp, str(payload_path))
        for candidate in candidates[: args.limit]
    ]
    append_jsonl(
        paths.log_jsonl,
        {
            "event": "api_parse",
            "url": url,
            "product_candidates": len(candidates),
            "products_written": len(records),
            "timestamp": now_local_iso(),
        },
    )
    return records


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Crawl thử WinMart và lưu raw JSONL/log.")
    parser.add_argument("--url", default=DEFAULT_URL, help="Trang WinMart public để crawl thử.")
    parser.add_argument(
        "--api-url",
        action="append",
        default=[],
        help="JSON endpoint lấy từ DevTools Network. Có thể truyền nhiều lần.",
    )
    parser.add_argument("--out-dir", default="raw", help="Thư mục raw output.")
    parser.add_argument("--region", default="HCMC", help="Khu vực crawl, ví dụ HCMC.")
    parser.add_argument("--limit", type=int, default=50, help="Số product tối đa ghi mỗi nguồn.")
    parser.add_argument("--timeout", type=int, default=20, help="HTTP timeout, giây.")
    parser.add_argument("--delay", type=float, default=1.0, help="Delay giữa các request API, giây.")
    parser.add_argument("--run-id", default=None, help="Run id tùy chọn.")
    parser.add_argument("--user-agent", default=DEFAULT_USER_AGENT, help="HTTP User-Agent.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    paths = build_output_paths(Path(args.out_dir), args.run_id)
    crawl_timestamp = now_local_iso()

    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": args.user_agent,
            "Accept": "text/html,application/json;q=0.9,*/*;q=0.8",
            "Accept-Language": "vi,en;q=0.8",
        }
    )

    all_records: list[dict[str, Any]] = []
    status = "success"
    error_message = None

    try:
        all_records.extend(scrape_page(session, args.url, paths, args, crawl_timestamp))
        for index, api_url in enumerate(args.api_url, start=1):
            time.sleep(args.delay)
            all_records.extend(scrape_api(session, api_url, paths, args, crawl_timestamp, index))
    except Exception as exc:  # noqa: BLE001 - log and return non-zero for crawl probe.
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

    for record in all_records:
        append_jsonl(paths.products_jsonl, record)

    if not paths.products_jsonl.exists():
        paths.products_jsonl.touch()

    metadata = {
        "store_id": STORE_ID,
        "store_name": STORE_NAME,
        "region": args.region,
        "source_url": args.url,
        "api_urls": args.api_url,
        "crawl_timestamp": crawl_timestamp,
        "status": status,
        "error_message": error_message,
        "products_written": len(all_records),
        "note": (
            "If products_written is 0, the public HTML did not include product JSON. "
            "Open DevTools > Network > Fetch/XHR on WinMart, copy the product JSON endpoint, "
            "then rerun with --api-url."
        ),
        "run_dir": str(paths.run_dir),
    }
    write_json(paths.metadata_json, metadata)

    print(json.dumps(metadata, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if status == "success" else 1


if __name__ == "__main__":
    sys.exit(main())
