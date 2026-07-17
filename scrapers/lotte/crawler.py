import argparse
import json
import math
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from scrapers.common import adapt_products


BASE_URL = "https://www.lottemart.vn"
STORE_CODE = "tbh"
LOCALE_STORE = f"vi_{STORE_CODE}"
PROMOTION_URL = (
    f"{BASE_URL}/vi-{STORE_CODE}/promotion?tab=special_price&categories=0"
)
API_URL = f"{BASE_URL}/v1/p/mart/es/{LOCALE_STORE}/homepage/promotion"
VIETNAM_TZ = timezone(timedelta(hours=7), name="Asia/Ho_Chi_Minh")


def clean_text(value: Any) -> str:
    return " ".join(str(value or "").split())


def as_int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)) and value > 0:
        return int(value)
    if isinstance(value, str):
        digits = "".join(char for char in value if char.isdigit())
        return int(digits) if digits else None
    return None


def timestamp_to_iso(value: Any) -> str:
    timestamp = as_int(value)
    if not timestamp:
        return ""
    return datetime.fromtimestamp(timestamp, VIETNAM_TZ).isoformat()


def build_api_url(page_number: int, page_size: int, category_id: str) -> str:
    query = urllib.parse.urlencode(
        {
            "page_number": page_number,
            "page_size": page_size,
            "filter[value]": "special_price",
            "filter[field]": "benefits_filter",
            "filter[category_id]": category_id,
            "sortDefault": "mostSold",
        }
    )
    return f"{API_URL}?{query}"


def fetch_json(url: str, timeout: int, retries: int, delay: float) -> dict[str, Any]:
    headers = {
        "Accept": "application/json, text/plain, */*",
        "Referer": PROMOTION_URL,
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/126.0.0.0 Safari/537.36"
        ),
    }
    last_error: Exception | None = None
    for attempt in range(retries + 1):
        try:
            request = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(request, timeout=timeout) as response:
                return json.loads(response.read().decode("utf-8"))
        except Exception as error:
            last_error = error
            if attempt < retries:
                time.sleep(delay * (attempt + 1))
    raise RuntimeError(f"Không tải được API Lotte: {url}") from last_error


def price_info(item: dict[str, Any]) -> dict[str, Any]:
    price = item.get("price")
    if not isinstance(price, dict):
        return {}
    vnd = price.get("VND")
    return vnd if isinstance(vnd, dict) else {}


def labels_to_names(labels: Any) -> list[str]:
    if not isinstance(labels, list):
        return []
    names: list[str] = []
    for label in labels:
        if isinstance(label, dict):
            name = clean_text(label.get("name"))
            if name:
                names.append(name)
    return names


def product_from_item(item: dict[str, Any]) -> dict[str, Any] | None:
    price = price_info(item)
    sale_price = as_int(price.get("special_price") or price.get("default"))
    original_price = as_int(price.get("price") or price.get("default_original"))
    image = clean_text(item.get("image_url") or item.get("thumbnail_url"))
    url = clean_text(item.get("url"))
    name = clean_text(item.get("name"))

    if not name or not sale_price:
        return None
    if not image.startswith(("http://", "https://")):
        return None
    if not url.startswith(BASE_URL) or "/product/" not in url:
        return None

    custom_attribute = item.get("custom_attribute")
    if not isinstance(custom_attribute, dict):
        custom_attribute = {}

    return {
        "name": name,
        "url": url,
        "image": image,
        "sale_price": sale_price,
        "original_price": original_price if original_price != sale_price else None,
        "discount": clean_text(price.get("default_discount_label")),
        "promotion": "",
        "promotion_type": "special_price",
        "sku": clean_text(item.get("sku")),
        "product_id": item.get("id"),
        "unit": clean_text(custom_attribute.get("unit") or item.get("unit")),
        "category_ids": item.get("category_ids") or [],
        "category_full_path": item.get("category_full_path") or [],
        "labels": labels_to_names(item.get("label")),
        "in_stock": bool(item.get("in_stock")),
        "stock_qty": as_int(item.get("stock_qty")),
        "sale_start_at": timestamp_to_iso(price.get("special_from_date")),
        "sale_end_at": timestamp_to_iso(price.get("special_to_date")),
        "sale_timezone": "Asia/Ho_Chi_Minh",
        "sale_time_source": "lotte_price_special_date",
        "source": "network_json",
        "site": "lottemart",
        "store_code": STORE_CODE,
        "source_url": PROMOTION_URL,
        "crawled_at": datetime.now(VIETNAM_TZ).isoformat(),
    }


def product_key(product: dict[str, Any]) -> str:
    sku = clean_text(product.get("sku")).lower()
    if sku:
        return f"sku:{sku}"
    return clean_text(product.get("url")).split("?")[0].rstrip("/").lower()


def merge_products(products: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged: dict[str, dict[str, Any]] = {}
    for product in products:
        key = product_key(product)
        if key:
            merged[key] = product
    return sorted(merged.values(), key=lambda item: clean_text(item.get("name")).lower())


def crawl(
    page_size: int,
    category_id: str,
    timeout: int,
    retries: int,
    delay: float,
    max_pages: int | None,
) -> list[dict[str, Any]]:
    products: list[dict[str, Any]] = []
    first_payload = fetch_json(
        build_api_url(1, page_size, category_id), timeout, retries, delay
    )
    data = first_payload.get("data") if isinstance(first_payload, dict) else {}
    if not isinstance(data, dict):
        raise RuntimeError("API Lotte không trả về field data hợp lệ.")

    total_items = as_int(data.get("total_items")) or 0
    total_pages = max(1, math.ceil(total_items / page_size)) if total_items else 1
    if max_pages:
        total_pages = min(total_pages, max_pages)

    for page_number in range(1, total_pages + 1):
        if page_number == 1:
            page_data = data
        else:
            payload = fetch_json(
                build_api_url(page_number, page_size, category_id),
                timeout,
                retries,
                delay,
            )
            page_data = payload.get("data") if isinstance(payload, dict) else {}

        items = page_data.get("items") if isinstance(page_data, dict) else []
        if not isinstance(items, list) or not items:
            print(f"Trang {page_number}: không có item, dừng crawl.")
            break

        page_products = [
            product
            for item in items
            if isinstance(item, dict)
            for product in [product_from_item(item)]
            if product
        ]
        products.extend(page_products)
        print(f"Trang {page_number}/{total_pages}: {len(page_products)} sản phẩm")
        time.sleep(delay)

    return merge_products(products)


def write_jsonl(products: list[dict[str, Any]], output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    crawl_date = datetime.now(VIETNAM_TZ).date().isoformat()
    output_path = output_dir / f"lotte_promotions_{crawl_date}.jsonl"
    temp_path = output_path.with_suffix(".tmp")

    with temp_path.open("w", encoding="utf-8") as file:
        for product in adapt_products(products, "lottemart"):
            file.write(json.dumps(product, ensure_ascii=False) + "\n")
    temp_path.replace(output_path)
    return output_path


def main() -> None:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")

    parser = argparse.ArgumentParser(
        description="Crawl sản phẩm khuyến mãi Giá Tốt từ lottemart.vn"
    )
    parser.add_argument("--output-dir", type=Path, default=Path("data"))
    parser.add_argument("--page-size", type=int, default=100)
    parser.add_argument("--category-id", default="0")
    parser.add_argument("--timeout", type=int, default=30)
    parser.add_argument("--retries", type=int, default=2)
    parser.add_argument("--delay", type=float, default=0.3)
    parser.add_argument(
        "--max-pages",
        type=int,
        default=None,
        help="Giới hạn số trang để test nhanh; bỏ trống để crawl toàn bộ.",
    )
    args = parser.parse_args()

    products = crawl(
        page_size=args.page_size,
        category_id=args.category_id,
        timeout=args.timeout,
        retries=args.retries,
        delay=args.delay,
        max_pages=args.max_pages,
    )
    output_path = write_jsonl(products, args.output_dir)
    print(f"Đã thu thập {len(products)} sản phẩm khuyến mãi Lotte.")
    print(f"JSONL: {output_path.resolve()}")


if __name__ == "__main__":
    main()
