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


BASE_URL = "https://khuyenmai.mmvietnam.com"
API_BASE_URL = "https://ebrochure-admin.digityze.asia/api/v2/products"
PROMOTION_URL = f"{BASE_URL}/products?page=1"
DEFAULT_STORE_CODE = "10010"
DEFAULT_LANGUAGE = "vn"
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


def parse_mmv_datetime(value: Any) -> str:
    text = clean_text(value)
    if not text:
        return ""
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(text, fmt).replace(tzinfo=VIETNAM_TZ).isoformat()
        except ValueError:
            pass
    return text


def build_api_url(page_number: int, sort: str) -> str:
    query = urllib.parse.urlencode({"sort": sort, "page": page_number})
    return f"{API_BASE_URL}?{query}"


def fetch_json(
    url: str,
    store_code: str,
    language: str,
    timeout: int,
    retries: int,
    delay: float,
) -> dict[str, Any]:
    headers = {
        "Accept": "application/json, text/plain, */*",
        "App-Language": language,
        "Store-Code": store_code,
        "Referer": BASE_URL + "/",
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
    raise RuntimeError(f"Không tải được API Mega Market: {url}") from last_error


def discount_label(sale_price: int | None, original_price: int | None) -> str:
    if not sale_price or not original_price or original_price <= sale_price:
        return ""
    percent = round((original_price - sale_price) * 100 / original_price)
    return f"-{percent}%"


def promotion_records(promotions: Any) -> list[dict[str, Any]]:
    if not isinstance(promotions, list):
        return []
    records: list[dict[str, Any]] = []
    for promotion in promotions:
        if not isinstance(promotion, dict):
            continue
        records.append(
            {
                "id": promotion.get("id"),
                "name": clean_text(promotion.get("name")),
                "name_en": clean_text(promotion.get("name_en")),
                "slug": clean_text(promotion.get("slug")),
                "start_at": parse_mmv_datetime(promotion.get("start_date")),
                "end_at": parse_mmv_datetime(promotion.get("end_date")),
                "image": clean_text(
                    promotion.get("img_id") or promotion.get("mobile_image_id")
                ),
            }
        )
    return records


def category_records(categories: Any) -> list[dict[str, Any]]:
    if not isinstance(categories, list):
        return []
    records: list[dict[str, Any]] = []
    for category in categories:
        if not isinstance(category, dict):
            continue
        records.append(
            {
                "id": category.get("id"),
                "name": clean_text(category.get("name")),
                "name_en": clean_text(category.get("name_en")),
                "slug": clean_text(category.get("slug")),
            }
        )
    return records


def product_from_item(
    item: dict[str, Any],
    store_code: str,
) -> dict[str, Any] | None:
    name = clean_text(item.get("name"))
    slug = clean_text(item.get("slug"))
    image = clean_text(item.get("thumbnail_image"))
    sale_price = as_int(item.get("sale_price"))
    original_price = as_int(item.get("unit_price"))
    mcard_price = as_int(item.get("mcard_price"))
    if mcard_price and (not sale_price or mcard_price < sale_price):
        sale_price = mcard_price

    if not name or not slug or not sale_price:
        return None
    if not image.startswith(("http://", "https://")):
        return None
    if original_price and original_price <= sale_price:
        original_price = None

    promotions = promotion_records(item.get("promotions"))
    sale_start_values = [record["start_at"] for record in promotions if record["start_at"]]
    sale_end_values = [record["end_at"] for record in promotions if record["end_at"]]

    brand = item.get("brand")
    if not isinstance(brand, dict):
        brand = {}

    return {
        "name": name,
        "name_en": clean_text(item.get("name_en")),
        "url": f"{BASE_URL}/products/{slug}",
        "image": image,
        "sale_price": sale_price,
        "original_price": original_price,
        "discount": discount_label(sale_price, original_price),
        "promotion": "; ".join(record["name"] for record in promotions if record["name"]),
        "promotion_type": "promotion_listing",
        "promotions": promotions,
        "sku": clean_text(item.get("sku")),
        "product_id": item.get("id"),
        "unit": clean_text(item.get("unit")),
        "mcard_price": mcard_price,
        "discount_amount": as_int(item.get("discount")),
        "discount_type": clean_text(item.get("discount_type")),
        "categories": category_records(item.get("categories")),
        "brand": clean_text(brand.get("name")),
        "brand_en": clean_text(brand.get("name_en")),
        "sale_start_at": min(sale_start_values) if sale_start_values else "",
        "sale_end_at": max(sale_end_values) if sale_end_values else "",
        "sale_timezone": "Asia/Ho_Chi_Minh",
        "sale_time_source": "mmvietnam_promotions",
        "source": "network_json",
        "site": "mmvietnam",
        "store_code": store_code,
        "source_url": PROMOTION_URL,
        "crawled_at": datetime.now(VIETNAM_TZ).isoformat(),
    }


def product_key(product: dict[str, Any]) -> str:
    sku = clean_text(product.get("sku")).lower()
    if sku:
        return f"sku:{sku}"
    product_id = clean_text(product.get("product_id"))
    if product_id:
        return f"id:{product_id}"
    return clean_text(product.get("url")).split("?")[0].rstrip("/").lower()


def merge_products(products: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged: dict[str, dict[str, Any]] = {}
    for product in products:
        key = product_key(product)
        if key:
            merged[key] = product
    return sorted(merged.values(), key=lambda item: clean_text(item.get("name")).lower())


def crawl(
    store_code: str,
    language: str,
    sort: str,
    timeout: int,
    retries: int,
    delay: float,
    max_pages: int | None,
) -> list[dict[str, Any]]:
    products: list[dict[str, Any]] = []
    first_payload = fetch_json(
        build_api_url(1, sort), store_code, language, timeout, retries, delay
    )
    meta = first_payload.get("meta") if isinstance(first_payload, dict) else {}
    if not isinstance(meta, dict):
        meta = {}
    total = as_int(meta.get("total")) or 0
    per_page = as_int(meta.get("per_page")) or 18
    last_page = as_int(meta.get("last_page")) or max(1, math.ceil(total / per_page))
    if max_pages:
        last_page = min(last_page, max_pages)

    for page_number in range(1, last_page + 1):
        if page_number == 1:
            payload = first_payload
        else:
            payload = fetch_json(
                build_api_url(page_number, sort),
                store_code,
                language,
                timeout,
                retries,
                delay,
            )
        items = payload.get("data") if isinstance(payload, dict) else []
        if not isinstance(items, list) or not items:
            print(f"Trang {page_number}: không có item, dừng crawl.")
            break

        page_products = [
            product
            for item in items
            if isinstance(item, dict)
            for product in [product_from_item(item, store_code)]
            if product
        ]
        products.extend(page_products)
        print(f"Trang {page_number}/{last_page}: {len(page_products)} sản phẩm")
        time.sleep(delay)

    return merge_products(products)


def write_jsonl(products: list[dict[str, Any]], output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    crawl_date = datetime.now(VIETNAM_TZ).date().isoformat()
    output_path = output_dir / f"mmvietnam_promotions_{crawl_date}.jsonl"
    temp_path = output_path.with_suffix(".tmp")
    with temp_path.open("w", encoding="utf-8") as file:
        for product in adapt_products(products, "mmvietnam"):
            file.write(json.dumps(product, ensure_ascii=False) + "\n")
    temp_path.replace(output_path)
    return output_path


def main() -> None:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")

    parser = argparse.ArgumentParser(
        description="Crawl sản phẩm khuyến mãi từ MM Mega Market Việt Nam"
    )
    parser.add_argument("--output-dir", type=Path, default=Path("data"))
    parser.add_argument("--store-code", default=DEFAULT_STORE_CODE)
    parser.add_argument("--language", default=DEFAULT_LANGUAGE)
    parser.add_argument("--sort", default="price_asc")
    parser.add_argument("--timeout", type=int, default=30)
    parser.add_argument("--retries", type=int, default=2)
    parser.add_argument("--delay", type=float, default=0.2)
    parser.add_argument(
        "--max-pages",
        type=int,
        default=None,
        help="Giới hạn số trang để test nhanh; bỏ trống để crawl toàn bộ.",
    )
    args = parser.parse_args()

    products = crawl(
        store_code=args.store_code,
        language=args.language,
        sort=args.sort,
        timeout=args.timeout,
        retries=args.retries,
        delay=args.delay,
        max_pages=args.max_pages,
    )
    output_path = write_jsonl(products, args.output_dir)
    print(f"Đã thu thập {len(products)} sản phẩm khuyến mãi MM Mega Market.")
    print(f"JSONL: {output_path.resolve()}")


if __name__ == "__main__":
    main()
