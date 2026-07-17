import argparse
import asyncio
import json
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from playwright.async_api import Page, Response, async_playwright

from scrapers.common import adapt_products


BASE_URL = "https://sieuthi-go.vn"
CATEGORIES_URL = f"{BASE_URL}/categories"
PRODUCT_API_URL = f"{BASE_URL}/api/order2_listProduct?platform=2&lang=vi"
VIETNAM_TZ = timezone(timedelta(hours=7), name="Asia/Ho_Chi_Minh")
DEFAULT_STORE_SITE_CODE = 102
CATEGORY_ID_RE = re.compile(r"-i\.(\d+)(?:-|$)")


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


def first_image(item: dict[str, Any]) -> str:
    thumbnail = item.get("thumbnail")
    if isinstance(thumbnail, list) and thumbnail:
        image = thumbnail[0]
    else:
        image = thumbnail
    if not image:
        meta = item.get("meta")
        if isinstance(meta, dict):
            image = meta.get("image")
    return clean_text(image)


def category_id_from_href(href: str) -> int | None:
    match = CATEGORY_ID_RE.search(href)
    return int(match.group(1)) if match else None


def product_url(item: dict[str, Any]) -> str:
    alias = clean_text(item.get("alias"))
    product_id = as_int(item.get("id"))
    if alias and product_id:
        return f"{BASE_URL}/products/{alias}-i.{product_id}"
    dynamic_link = clean_text(item.get("dynamic_link"))
    return dynamic_link


def detail_value(item: dict[str, Any], keys: tuple[str, ...]) -> str:
    detail = item.get("detail")
    if not isinstance(detail, list):
        return ""
    wanted = {key.lower() for key in keys}
    for row in detail:
        if not isinstance(row, dict):
            continue
        name = clean_text(row.get("name")).lower()
        if any(key in name for key in wanted):
            return clean_text(row.get("value"))
    return ""


def is_sale_product(item: dict[str, Any]) -> bool:
    price = as_int(item.get("price"))
    original_price = as_int(item.get("promotion_price"))
    member_price = as_int(item.get("member_price"))
    if price and original_price and original_price > price:
        return True
    if price and member_price and member_price < price:
        return True
    gift = item.get("gift")
    if isinstance(gift, dict) and gift.get("list"):
        return True
    return False


def product_from_item(
    item: dict[str, Any],
    category_id: int,
    category_name: str,
) -> dict[str, Any] | None:
    name = clean_text(item.get("name"))
    sale_price = as_int(item.get("price"))
    original_price = as_int(item.get("promotion_price"))
    image = first_image(item)
    url = product_url(item)

    if not name or not sale_price:
        return None
    if not image.startswith(("http://", "https://")):
        return None
    if not is_sale_product(item):
        return None

    promotion = ""
    gift = item.get("gift")
    if isinstance(gift, dict):
        promotion = clean_text(gift.get("title"))
    if not promotion:
        promotion = clean_text(item.get("message_product") or item.get("short_detail_note"))

    member_price = as_int(item.get("member_price"))
    effective_original = original_price
    if effective_original == sale_price:
        effective_original = None

    return {
        "name": name,
        "url": url,
        "image": image,
        "sale_price": member_price if member_price and member_price < sale_price else sale_price,
        "original_price": effective_original,
        "discount": discount_label(sale_price, effective_original),
        "promotion": promotion,
        "promotion_type": "category_promotion",
        "barcode": clean_text(item.get("barcode")),
        "product_id": item.get("id"),
        "unit": detail_value(item, ("trọng lượng", "dung tích")),
        "brand": detail_value(item, ("thương hiệu",)),
        "origin": detail_value(item, ("xuất xứ", "sản xuất tại")),
        "category_id": category_id,
        "category_name": category_name,
        "in_stock": item.get("status") == 1,
        "member_price": member_price,
        "sale_start_at": clean_text(
            item.get("member_price_start_date") or item.get("member_price_start_time")
        ),
        "sale_end_at": clean_text(
            item.get("member_price_end_date") or item.get("member_price_end_time")
        ),
        "sale_timezone": "Asia/Ho_Chi_Minh",
        "sale_time_source": "go_member_price_fields",
        "source": "network_json",
        "site": "go",
        "store_site_code": DEFAULT_STORE_SITE_CODE,
        "source_url": CATEGORIES_URL,
        "crawled_at": datetime.now(VIETNAM_TZ).isoformat(),
    }


def discount_label(sale_price: int | None, original_price: int | None) -> str:
    if not sale_price or not original_price or original_price <= sale_price:
        return ""
    percent = round((original_price - sale_price) * 100 / original_price)
    return f"-{percent}%"


def product_key(product: dict[str, Any]) -> str:
    barcode = clean_text(product.get("barcode")).lower()
    if barcode:
        return f"barcode:{barcode}"
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


async def discover_categories(page) -> list[dict[str, Any]]:
    raw_categories = await page.evaluate(
        """
        () => {
          const seen = new Set();
          const categories = [];
          for (const anchor of document.querySelectorAll('a[href*="/categories/"]')) {
            const href = new URL(anchor.getAttribute('href'), location.origin).href;
            const text = (anchor.innerText || anchor.getAttribute('title') || '')
              .replace(/\\s+/g, ' ').trim();
            if (!text || seen.has(href)) continue;
            seen.add(href);
            categories.push({name: text.replace(/\\s*\\|.*$/, ''), url: href});
          }
          return categories;
        }
        """
    )
    categories: list[dict[str, Any]] = []
    for category in raw_categories:
        href = clean_text(category.get("url"))
        category_id = category_id_from_href(href)
        name = clean_text(category.get("name"))
        if category_id and name:
            categories.append({"id": category_id, "name": name, "url": href})
    return categories


async def crawl(
    headless: bool,
    timeout_ms: int,
    delay_ms: int,
    max_categories: int | None,
    max_scrolls: int,
) -> list[dict[str, Any]]:
    all_products: list[dict[str, Any]] = []
    active_category: dict[str, Any] | None = None
    active_products: list[dict[str, Any]] = []

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=headless)
        context = await browser.new_context(
            locale="vi-VN",
            timezone_id="Asia/Ho_Chi_Minh",
            viewport={"width": 1440, "height": 1000},
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/126.0.0.0 Safari/537.36"
            ),
        )
        page = await context.new_page()

        async def handle_response(response: Response) -> None:
            nonlocal active_category, active_products
            if active_category is None:
                return
            if "order2_listProduct" not in response.url:
                return
            try:
                payload = await response.json()
            except Exception:
                return
            items = payload.get("products") if isinstance(payload, dict) else []
            if not isinstance(items, list):
                return
            for item in items:
                if not isinstance(item, dict):
                    continue
                product = product_from_item(
                    item,
                    active_category["id"],
                    active_category["name"],
                )
                if product:
                    active_products.append(product)

        page.on("response", handle_response)
        await page.goto(CATEGORIES_URL, wait_until="domcontentloaded", timeout=timeout_ms)
        await page.wait_for_load_state("networkidle", timeout=timeout_ms)
        await page.wait_for_timeout(1500)

        categories = await discover_categories(page)
        if max_categories:
            categories = categories[:max_categories]
        if not categories:
            raise RuntimeError("Không tìm thấy category GO trên trang.")
        print(f"Tìm thấy {len(categories)} category GO.")

        for category in categories:
            active_category = category
            active_products = []
            await page.goto(category["url"], wait_until="domcontentloaded", timeout=timeout_ms)
            await page.wait_for_timeout(2500)

            stable_rounds = 0
            previous_count = 0
            for _ in range(max_scrolls):
                await page.mouse.wheel(0, 1400)
                await page.wait_for_timeout(delay_ms)
                current_count = len(active_products)
                if current_count == previous_count:
                    stable_rounds += 1
                else:
                    stable_rounds = 0
                previous_count = current_count
                if stable_rounds >= 4:
                    break

            unique_category_products = merge_products(active_products)
            all_products.extend(unique_category_products)
            print(
                f"{category['name']}: {len(unique_category_products)} "
                "sản phẩm khuyến mãi"
            )

        active_category = None

        await browser.close()

    return merge_products(all_products)


def write_jsonl(products: list[dict[str, Any]], output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    crawl_date = datetime.now(VIETNAM_TZ).date().isoformat()
    output_path = output_dir / f"go_promotions_{crawl_date}.jsonl"
    temp_path = output_path.with_suffix(".tmp")
    with temp_path.open("w", encoding="utf-8") as file:
        for product in adapt_products(products, "go"):
            file.write(json.dumps(product, ensure_ascii=False) + "\n")
    temp_path.replace(output_path)
    return output_path


async def run_once(args: argparse.Namespace) -> list[dict[str, Any]]:
    products = await crawl(
        headless=not args.show_browser,
        timeout_ms=args.timeout,
        delay_ms=args.delay,
        max_categories=args.max_categories,
        max_scrolls=args.max_scrolls,
    )
    output_path = write_jsonl(products, args.output_dir)
    print(f"Đã thu thập {len(products)} sản phẩm khuyến mãi GO.")
    print(f"JSONL: {output_path.resolve()}")
    return products


def main() -> None:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")

    parser = argparse.ArgumentParser(
        description="Crawl sản phẩm khuyến mãi từ sieuthi-go.vn/categories"
    )
    parser.add_argument("--output-dir", type=Path, default=Path("data"))
    parser.add_argument("--show-browser", action="store_true")
    parser.add_argument("--timeout", type=int, default=60_000)
    parser.add_argument("--delay", type=int, default=250)
    parser.add_argument(
        "--max-categories",
        type=int,
        default=None,
        help="Giới hạn số category để test nhanh; bỏ trống để crawl toàn bộ.",
    )
    parser.add_argument("--max-scrolls", type=int, default=30)
    args = parser.parse_args()
    asyncio.run(run_once(args))


if __name__ == "__main__":
    main()
