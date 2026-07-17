import argparse
import asyncio
import json
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlparse

from playwright.async_api import Page, Response, async_playwright

from scrapers.common import adapt_products


BASE_URL = "https://www.bachhoaxanh.com"
PROMOTION_URL = f"{BASE_URL}/khuyen-mai"
PRICE_RE = re.compile(r"(\d[\d.\s,]*)\s*(?:₫|đ|VND)", re.IGNORECASE)
SLOT_RE = re.compile(
    r"^\s*(\d{1,2})\s*h\s*[-–—]\s*(\d{1,2})\s*h\s*$", re.IGNORECASE
)
VIETNAM_TZ = timezone(timedelta(hours=7), name="Asia/Ho_Chi_Minh")
EXPECTED_FLASH_SLOTS = (
    "10h - 12h",
    "12h - 14h",
    "14h - 16h",
    "16h - 18h",
    "18h - 20h",
    "20h - 22h",
    "22h - 00h",
)
NON_PRODUCT_ROUTES = {
    "khuyen-mai",
    "thuong-hieu",
    "he-thong-sieu-thi",
    "chinh-sach",
    "kinh-nghiem-hay",
    "tuyen-dung",
}


def clean_text(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def parse_price(value: Any) -> int | None:
    if isinstance(value, dict):
        value = first_value(
            value,
            (
                "value",
                "amount",
                "price",
                "salePrice",
                "sale_price",
                "discountPrice",
            ),
        )
    if isinstance(value, (int, float)) and value > 0:
        return int(value)
    match = PRICE_RE.search(clean_text(value))
    if not match:
        digits = re.sub(r"\D", "", clean_text(value))
        return int(digits) if digits else None
    digits = re.sub(r"\D", "", match.group(1))
    return int(digits) if digits else None


def first_value(data: dict[str, Any], keys: tuple[str, ...]) -> Any:
    lowered = {str(key).lower(): value for key, value in data.items()}
    for key in keys:
        value = lowered.get(key.lower())
        if value not in (None, "", [], {}):
            return value
    return None


def product_from_dict(data: dict[str, Any]) -> dict[str, Any] | None:
    name = first_value(
        data, ("productName", "product_name", "name", "title", "displayName")
    )
    product_prices = data.get("productPrices")
    selected_price: dict[str, Any] | None = None
    if isinstance(product_prices, list):
        candidate_prices = [
            item for item in product_prices
            if isinstance(item, dict) and parse_price(item.get("price"))
        ]
        sale_candidates = [
            item for item in candidate_prices
            if (
                parse_price(item.get("sysPrice"))
                and parse_price(item.get("price"))
                and parse_price(item.get("sysPrice")) > parse_price(item.get("price"))
            )
        ]
        if sale_candidates:
            selected_price = min(
                sale_candidates,
                key=lambda item: parse_price(item.get("price")) or 0,
            )
        elif candidate_prices:
            selected_price = min(
                candidate_prices,
                key=lambda item: parse_price(item.get("price")) or 0,
            )

    sale_price = (
        selected_price.get("price")
        if selected_price
        else first_value(
        data,
        (
            "salePrice",
            "sale_price",
            "discountPrice",
            "discount_price",
            "finalPrice",
            "price",
        ),
    )
    )
    url = first_value(data, ("url", "productUrl", "product_url", "link", "href", "slug"))
    image = first_value(
        data, ("image", "imageUrl", "image_url", "thumbnail", "picture", "avatar")
    )

    # A product record normally has a name plus at least a price, URL, or image.
    if not isinstance(name, str) or not clean_text(name):
        return None
    if sale_price is None and url is None and image is None:
        return None

    original_price = (
        selected_price.get("sysPrice")
        if selected_price
        else first_value(
        data,
        (
            "originalPrice",
            "original_price",
            "oldPrice",
            "old_price",
            "listPrice",
            "marketPrice",
        ),
    )
    )
    discount = first_value(
        data, ("discount", "discountPercent", "discount_percent", "promotion")
    )
    if selected_price and not discount:
        discount = selected_price.get("discountPercent")
    product_url = clean_text(url)
    if product_url and not product_url.startswith(("http://", "https://")):
        product_url = urljoin(BASE_URL, product_url)

    if isinstance(image, dict):
        image = first_value(image, ("url", "src", "large", "medium", "small"))
    if isinstance(image, list) and image:
        image = image[0]
        if isinstance(image, dict):
            image = first_value(image, ("url", "src"))

    return {
        "name": clean_text(name),
        "url": product_url,
        "image": clean_text(image),
        "sale_price": parse_price(sale_price),
        "original_price": parse_price(original_price),
        "discount": clean_text(discount),
        "promotion": clean_text(
            first_value(data, ("promotionText", "promotion_text", "campaign", "label"))
        ),
        "unit": clean_text(first_value(data, ("unit", "unitName", "quantity"))),
        "source": "network_json",
    }


def _append_unique_text(values: list[str], value: Any) -> None:
    text = clean_text(value)
    if text and text not in values:
        values.append(text)


def summarize_promotion_object(value: Any) -> str:
    if isinstance(value, dict):
        for key in (
            "name",
            "title",
            "label",
            "promotionText",
            "textPromotion",
            "textPromtionNonBlue",
            "description",
            "content",
        ):
            text = clean_text(value.get(key))
            if text:
                return text
        compact = {
            key: item
            for key, item in value.items()
            if item not in (None, "", [], {})
            and key
            in {
                "crmProgramId",
                "timeLineId",
                "maxQuantityOrder",
                "remainingQuantity",
                "startTime",
                "dueTime",
            }
        }
        return clean_text(json.dumps(compact, ensure_ascii=False)) if compact else ""
    return clean_text(value)


def bhx_promotion_texts(
    data: dict[str, Any],
    selected_price: dict[str, Any] | None = None,
) -> list[str]:
    texts: list[str] = []
    for key in (
        "promotionText",
        "promotionTextFS",
        "textPromotion",
        "textPromtionNonBlue",
        "textPromtionNonBlueFS",
        "label",
        "promotion",
        "giftText",
        "gift_text",
    ):
        _append_unique_text(texts, data.get(key))

    if selected_price:
        for key in ("label", "promotionText", "discountPercent"):
            _append_unique_text(texts, selected_price.get(key))
        if selected_price.get("isBuyTogether"):
            _append_unique_text(texts, "Khuyến mãi mua kèm")
        if selected_price.get("quantityCondition"):
            _append_unique_text(texts, "Khuyến mãi mua nhiều")

    for key in ("listCombo", "combos", "gifts", "giftProducts", "lstGift"):
        value = data.get(key)
        if isinstance(value, list):
            for item in value[:5]:
                _append_unique_text(texts, summarize_promotion_object(item))
        else:
            _append_unique_text(texts, summarize_promotion_object(value))

    return texts


def has_bhx_non_price_promotion(
    data: dict[str, Any],
    prices: list[Any],
    selected_price: dict[str, Any] | None,
) -> bool:
    if bhx_promotion_texts(data, selected_price):
        return True
    for key in (
        "listCombo",
        "combos",
        "gifts",
        "giftProducts",
        "lstGift",
        "lstCampaingInfo",
    ):
        value = data.get(key)
        if isinstance(value, list) and value:
            return True
        if isinstance(value, dict) and value:
            return True
    if data.get("isShowPopupPromotion"):
        return True
    for key in ("comboType", "productProType", "promotionType", "giftType"):
        value = data.get(key)
        if value not in (None, "", 0, "0", False):
            return True
    for price in prices:
        if not isinstance(price, dict):
            continue
        if price.get("isBuyTogether") or price.get("quantityCondition"):
            return True
        if clean_text(price.get("label")) or clean_text(price.get("promotionText")):
            return True
        if parse_price(price.get("salePriceNonBlue")):
            return True
    return False


def product_from_bhx_api_item(
    data: dict[str, Any],
    promotion_type: str = "category_sale",
    category_name: str = "",
) -> dict[str, Any] | None:
    product = product_from_dict(data)
    if not product:
        return None
    prices = data.get("productPrices")
    if not isinstance(prices, list):
        return None
    candidate_prices = [
        price for price in prices
        if isinstance(price, dict)
        and parse_price(price.get("price"))
    ]
    if not candidate_prices:
        return None
    sale_prices = [
        price for price in candidate_prices
        if parse_price(price.get("sysPrice"))
        and parse_price(price.get("sysPrice")) > parse_price(price.get("price"))
    ]
    selected_price = min(
        sale_prices or candidate_prices,
        key=lambda item: parse_price(item.get("price")) or 0,
    )
    sale_price = parse_price(selected_price.get("price"))
    sys_price = parse_price(selected_price.get("sysPrice"))
    has_price_discount = bool(sale_price and sys_price and sys_price > sale_price)
    has_other_promotion = has_bhx_non_price_promotion(
        data,
        prices,
        selected_price,
    )
    if not sale_price or (not has_price_discount and not has_other_promotion):
        return None
    original_price = sys_price if has_price_discount else None
    promotion_texts = bhx_promotion_texts(data, selected_price)
    actual_promotion_type = (
        promotion_type if has_price_discount else "category_promotion"
    )

    category = data.get("category")
    if isinstance(category, dict):
        category_name = category_name or clean_text(category.get("name"))

    product.update(
        {
            "sale_price": sale_price,
            "original_price": original_price,
            "discount": (
                clean_text(selected_price.get("discountPercent"))
                or discount_label(sale_price, original_price)
                if has_price_discount
                else ""
            ),
            "promotion_type": actual_promotion_type,
            "category_name": category_name,
            "product_id": data.get("id"),
            "sku": clean_text(data.get("productCode")),
            "brand": clean_text(data.get("brandName")),
            "promotion": "; ".join(promotion_texts),
            "sale_timezone": "Asia/Ho_Chi_Minh",
            "sale_time_source": "bhx_category_api",
        }
    )

    campaigns = data.get("lstCampaingInfo")
    if isinstance(campaigns, list):
        starts = [
            clean_text(item.get("startTime"))
            for item in campaigns
            if isinstance(item, dict) and clean_text(item.get("startTime"))
        ]
        ends = [
            clean_text(item.get("dueTime"))
            for item in campaigns
            if isinstance(item, dict) and clean_text(item.get("dueTime"))
        ]
        if starts:
            product["sale_start_at"] = min(starts)
        if ends:
            product["sale_end_at"] = max(ends)
        product["promotions"] = [
            {
                "crm_program_id": item.get("crmProgramId"),
                "timeline_id": item.get("timeLineId"),
                "start_at": clean_text(item.get("startTime")),
                "end_at": clean_text(item.get("dueTime")),
                "remaining_quantity": item.get("remainingQuantity"),
                "max_quantity_order": item.get("maxQuantityOrder"),
            }
            for item in campaigns
            if isinstance(item, dict)
        ]
    combos = data.get("listCombo")
    if isinstance(combos, list) and combos:
        product["combos"] = [
            summarize_promotion_object(item)
            for item in combos
            if summarize_promotion_object(item)
        ]
    return product


def discount_label(sale_price: int | None, original_price: int | None) -> str:
    if not sale_price or not original_price or original_price <= sale_price:
        return ""
    percent = round((original_price - sale_price) * 100 / original_price)
    return f"{percent}%"


def walk_json(value: Any):
    if isinstance(value, dict):
        product = product_from_dict(value)
        if product:
            yield product
        for child in value.values():
            yield from walk_json(child)
    elif isinstance(value, list):
        for child in value:
            yield from walk_json(child)


def collect_menu_categories(value: Any) -> list[dict[str, Any]]:
    categories: list[dict[str, Any]] = []

    def visit(node: Any, parent_name: str = "") -> None:
        if isinstance(node, dict):
            url = clean_text(node.get("url"))
            name = clean_text(node.get("name"))
            path = clean_text(node.get("path"))
            children = node.get("childrens") or node.get("children")
            if url and name and path in {"GroupV2", ""}:
                categories.append(
                    {
                        "id": clean_text(node.get("id")),
                        "name": name,
                        "url": url.strip("/"),
                        "parent_name": parent_name,
                    }
                )
            if isinstance(children, list):
                for child in children:
                    visit(child, name or parent_name)
            for key, child in node.items():
                if key in {"childrens", "children"}:
                    continue
                if isinstance(child, (list, dict)):
                    visit(child, name or parent_name)
        elif isinstance(node, list):
            for child in node:
                visit(child, parent_name)

    visit(value)
    unique: dict[str, dict[str, Any]] = {}
    for category in categories:
        unique[category["url"]] = category
    return list(unique.values())


def category_url(category: dict[str, Any]) -> str:
    return urljoin(BASE_URL, "/" + clean_text(category.get("url")).strip("/"))


def extract_category_sale_products(
    payload: Any,
    category_name: str,
) -> list[dict[str, Any]]:
    products: list[dict[str, Any]] = []

    def visit(node: Any) -> None:
        if isinstance(node, dict):
            product = product_from_bhx_api_item(
                node,
                promotion_type="category_sale",
                category_name=category_name,
            )
            if product:
                products.append(product)
                return
            for child in node.values():
                visit(child)
        elif isinstance(node, list):
            for child in node:
                visit(child)

    visit(payload)
    return products


async def extract_dom_products(
    page: Page, slot_label: str | None = None
) -> list[dict[str, Any]]:
    raw_products = await page.evaluate(
        """
        (slotLabel) => {
          const money = /(\\d[\\d.,\\s]*)\\s*(₫|đ|VND)/ig;
          const results = [];
          const seen = new Set();
          const normalize = value => (value || '').replace(/\\s+/g, ' ').trim();
          const isProductHref = href => {
            try {
              const parts = new URL(href, location.origin).pathname
                .split('/').filter(Boolean);
              return parts.length === 2;
            } catch {
              return false;
            }
          };

          let root = document;
          if (slotLabel) {
            const slot = [...document.querySelectorAll('button, a, div, span, li')]
              .filter(el => normalize(el.innerText).includes(normalize(slotLabel)))
              .sort((a, b) => normalize(a.innerText).length -
                normalize(b.innerText).length)[0];
            if (!slot) return [];
            const slotBar = slot.parentElement;
            const section = slotBar?.parentElement;
            const candidates = [...(section?.children || [])]
              .filter(el => el !== slotBar)
              .sort((a, b) =>
                b.querySelectorAll('img').length - a.querySelectorAll('img').length
              );
            if (!candidates.length || !candidates[0].querySelector('img')) return [];
            root = candidates[0];
          }

          for (const anchor of root.querySelectorAll('a[href]')) {
            let node = anchor.closest(
              '[class*="product"], [class*="item"], article, li'
            ) || anchor.parentElement;
            if (!node) continue;
            if (!node.getClientRects().length) continue;

            const text = normalize(node.innerText);
            const prices = [...text.matchAll(money)].map(m => m[0]);
            const image = node.querySelector('img');
            const href = new URL(anchor.getAttribute('href'), location.origin).href;
            if (!prices.length || !image || seen.has(href)) continue;

            const nameNode = node.querySelector(
              '[class*="name"], [class*="title"], h2, h3, h4'
            );
            let name = (nameNode?.innerText || anchor.getAttribute('title') ||
                        image.getAttribute('alt') || '').replace(/\\s+/g, ' ').trim();
            if (!name || name.length > 250) continue;

            seen.add(href);
            results.push({
              name,
              url: href,
              image: image.currentSrc || image.src || image.getAttribute('data-src') || '',
              prices,
              text
            });
          }
          return results;
        }
        """,
        slot_label,
    )

    products: list[dict[str, Any]] = []
    for item in raw_products:
        prices = [parse_price(price) for price in item.get("prices", [])]
        prices = [price for price in prices if price]
        if not prices:
            continue

        text = clean_text(item.get("text"))
        discount_match = re.search(r"(?:giảm|-)\s*(\d{1,3}\s*%)", text, re.IGNORECASE)
        products.append(
            {
                "name": clean_text(item.get("name")),
                "url": clean_text(item.get("url")),
                "image": clean_text(item.get("image")),
                "sale_price": min(prices),
                "original_price": max(prices) if len(set(prices)) > 1 else None,
                "discount": discount_match.group(1) if discount_match else "",
                "promotion": text[:500],
                "unit": "",
                "source": "dom",
            }
        )
    return products


def product_key(product: dict[str, Any]) -> str:
    url = clean_text(product.get("url")).split("?")[0].rstrip("/").lower()
    slot = clean_text(product.get("sale_start_at"))
    promotion_type = clean_text(product.get("promotion_type"))
    if url:
        return f"url:{url}:type:{promotion_type}:slot:{slot}"
    return (
        f"name:{clean_text(product.get('name')).lower()}:"
        f"{product.get('sale_price')}:type:{promotion_type}:slot:{slot}"
    )


def is_valid_image_url(value: Any) -> bool:
    try:
        parsed = urlparse(clean_text(value))
        return parsed.scheme in {"http", "https"} and bool(parsed.netloc)
    except ValueError:
        return False


def is_product_url(value: Any) -> bool:
    try:
        parsed = urlparse(clean_text(value))
    except ValueError:
        return False

    if parsed.scheme not in {"http", "https"}:
        return False
    if parsed.netloc.lower() not in {"bachhoaxanh.com", "www.bachhoaxanh.com"}:
        return False

    parts = [part for part in parsed.path.split("/") if part]
    return (
        len(parts) == 2
        and parts[0].lower() not in NON_PRODUCT_ROUTES
        and bool(parts[1])
    )


def is_valid_sale_product(product: dict[str, Any]) -> bool:
    sale_price = product.get("sale_price")
    return (
        isinstance(sale_price, int)
        and sale_price > 0
        and is_valid_image_url(product.get("image"))
        and is_product_url(product.get("url"))
    )


def merge_products(products: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged: dict[str, dict[str, Any]] = {}
    for product in products:
        if not product.get("name") or not is_product_url(product.get("url")):
            continue
        key = product_key(product)
        if key not in merged:
            merged[key] = product
            continue
        current = merged[key]
        for field, value in product.items():
            if current.get(field) in (None, "") and value not in (None, ""):
                current[field] = value
        if current.get("source") != product.get("source"):
            current["source"] = "network_json+dom"

    crawled_at = datetime.now(timezone.utc).isoformat()
    output = []
    for product in merged.values():
        if not is_valid_sale_product(product):
            continue
        product["crawled_at"] = crawled_at
        product["page_url"] = PROMOTION_URL
        output.append(product)
    return sorted(output, key=lambda item: clean_text(item.get("name")).lower())


def slot_datetimes(
    label: str, now: datetime | None = None
) -> tuple[datetime, datetime]:
    match = SLOT_RE.match(clean_text(label))
    if not match:
        raise ValueError(f"Nhãn sale period không hợp lệ: {label}")

    local_now = (now or datetime.now(VIETNAM_TZ)).astimezone(VIETNAM_TZ)
    start_hour, end_hour = map(int, match.groups())
    start = local_now.replace(
        hour=start_hour % 24, minute=0, second=0, microsecond=0
    )
    end = local_now.replace(hour=end_hour % 24, minute=0, second=0, microsecond=0)
    if end_hour <= start_hour:
        end += timedelta(days=1)
    return start, end


def slot_metadata(label: str) -> dict[str, Any]:
    start, end = slot_datetimes(label)
    return {
        "promotion_type": "flash_sale",
        "sale_slot": clean_text(label),
        "sale_start_at": start.isoformat(),
        "sale_end_at": end.isoformat(),
        "sale_timezone": "Asia/Ho_Chi_Minh",
        "sale_end_source": "flash_sale_slot",
        "sale_end_inferred": True,
    }


async def discover_sale_slots(page: Page) -> list[dict[str, Any]]:
    slots = await page.evaluate(
        """
        () => {
          const pattern = /\\b\\d{1,2}\\s*h\\s*[-–—]\\s*\\d{1,2}\\s*h\\b/ig;
          const normalize = value => (value || '').replace(/\\s+/g, ' ').trim();
          const results = [];
          const seen = new Set();
          const bodyText = normalize(document.body.innerText);
          for (const match of bodyText.matchAll(pattern)) {
            const label = normalize(match[0]);
            if (seen.has(label)) continue;
            const element = [...document.querySelectorAll('button, a, div, span, li')]
              .filter(el => normalize(el.innerText).includes(label))
              .sort((a, b) => normalize(a.innerText).length -
                normalize(b.innerText).length)[0];
            const surroundingText = normalize(element?.innerText || element?.parentElement?.innerText);
            const status = surroundingText
              .match(/(Đang diễn ra|Sắp diễn ra|Đã kết thúc)/i)?.[1] || '';
            seen.add(label);
            results.push({label, status});
          }
          return results;
        }
        """
    )
    return slots


async def click_slot(page: Page, label: str) -> None:
    match = SLOT_RE.match(label)
    if not match:
        raise RuntimeError(f"Nhãn sale period không hợp lệ: {label}")
    clicked = await page.evaluate(
        """
        (label) => {
          const normalize = value => (value || '').replace(/\\s+/g, ' ').trim();
          const candidates = [...document.querySelectorAll(
            'button, a, [role="button"], div, span, li'
          )].filter(el => normalize(el.innerText).includes(normalize(label)));
          candidates.sort((a, b) =>
            normalize(a.innerText).length - normalize(b.innerText).length
          );
          const target = candidates[0];
          if (!target) return false;
          target.scrollIntoView({block: 'center', inline: 'center'});
          target.click();
          return true;
        }
        """,
        label,
    )
    if not clicked:
        raise RuntimeError(f"Không tìm thấy sale period {label}")
    await page.wait_for_timeout(1500)


async def click_promotion_tab(page: Page, label: str) -> None:
    clicked = await page.evaluate(
        """
        (label) => {
          const normalize = value => (value || '').replace(/\\s+/g, ' ').trim();
          const candidates = [...document.querySelectorAll('main *')]
            .filter(el => normalize(el.innerText) === label)
            .sort((a, b) => a.children.length - b.children.length);
          const textNode = candidates[0];
          if (!textNode) return false;
          const target = textNode.closest(
            'button, a, [role="button"], [class*="cursor-pointer"]'
          ) || textNode.parentElement;
          if (!target) return false;
          target.scrollIntoView({block: 'center', inline: 'center'});
          target.click();
          return true;
        }
        """,
        label,
    )
    if not clicked:
        raise RuntimeError(f"Không tìm thấy tab {label}")
    await page.wait_for_timeout(1800)


async def count_product_cards(page: Page) -> int:
    return await page.evaluate(
        """
        () => {
          const money = /(\\d[\\d.,\\s]*)\\s*(₫|đ|VND)/i;
          const urls = new Set();
          for (const anchor of document.querySelectorAll('main a[href]')) {
            let parsed;
            try {
              parsed = new URL(anchor.href, location.origin);
            } catch {
              continue;
            }
            const parts = parsed.pathname.split('/').filter(Boolean);
            if (parts.length !== 2) continue;
            const card = anchor.closest(
              '[class*="product"], [class*="item"], article, li'
            ) || anchor.parentElement;
            if (!card || !card.querySelector('img')) continue;
            if (!money.test((card.innerText || '').replace(/\\s+/g, ' '))) continue;
            urls.add(parsed.origin + parsed.pathname);
          }
          return urls.size;
        }
        """
    )


async def fetch_menu_categories(page: Page) -> list[dict[str, Any]]:
    payload = await page.evaluate(
        """
        async () => {
          const response = await fetch(
            'https://api.bachhoaxanh.com/gw/Menu/GetMenuV2?ProvinceId=1027&WardId=0&StoreId=2546',
            {
              headers: {
                'Accept': 'application/json, text/plain, */*',
                'Referer': location.href
              }
            }
          );
          if (!response.ok) {
            throw new Error(`Menu API HTTP ${response.status}`);
          }
          return await response.json();
        }
        """
    )
    return collect_menu_categories(payload.get("data") if isinstance(payload, dict) else {})


async def scroll_until_stable(page: Page, max_scrolls: int) -> int:
    stable_rounds = 0
    previous_count = await count_product_cards(page)
    await page.evaluate("window.scrollTo(0, 0)")
    await page.wait_for_timeout(300)

    for _ in range(max_scrolls):
        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        await page.wait_for_timeout(800)
        current_count = await count_product_cards(page)
        if current_count <= previous_count:
            stable_rounds += 1
        else:
            stable_rounds = 0
            previous_count = current_count
        if stable_rounds >= 3:
            break
    return previous_count


async def crawl(
    headless: bool,
    timeout_ms: int,
    max_scrolls: int,
    max_categories: int | None = None,
) -> list[dict[str, Any]]:
    slot_network_products: list[dict[str, Any]] = []
    active_slot: dict[str, Any] | None = None
    all_products: list[dict[str, Any]] = []
    menu_categories: list[dict[str, Any]] = []

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
            try:
                content_type = (await response.all_headers()).get("content-type", "")
                if "json" not in content_type.lower():
                    return
                payload = await response.json()
                if "Menu/GetMenuV2" in response.url:
                    menu_categories.extend(collect_menu_categories(payload.get("data")))
                if active_slot is None:
                    return
                if active_slot.get("promotion_type") == "category_sale":
                    if (
                        "Category/V2/GetCate" in response.url
                        or "Category/GetCateVegetable" in response.url
                    ):
                        slot_network_products.extend(
                            extract_category_sale_products(
                                payload,
                                clean_text(active_slot.get("category_name")),
                            )
                        )
                    return
                for product in walk_json(payload):
                    product.update(active_slot)
                    slot_network_products.append(product)
            except Exception:
                return

        page.on("response", handle_response)
        await page.goto(PROMOTION_URL, wait_until="domcontentloaded", timeout=timeout_ms)
        await page.wait_for_timeout(2500)
        slots = await discover_sale_slots(page)
        if not slots:
            raise RuntimeError("Không tìm thấy khung giờ Flash Sale trên trang.")
        print(
            "Sale periods: "
            + ", ".join(slot["label"] for slot in slots)
        )
        discovered_labels = {slot["label"] for slot in slots}
        missing_slots = [
            label for label in EXPECTED_FLASH_SLOTS
            if label not in discovered_labels
        ]
        if missing_slots:
            print(
                "Cảnh báo: website không còn hiển thị các slot: "
                + ", ".join(missing_slots)
                + ". Dữ liệu cũ trong file cùng ngày sẽ được giữ nếu có."
            )

        for slot in slots:
            metadata = slot_metadata(slot["label"])
            active_slot = metadata
            slot_network_products.clear()
            try:
                await click_slot(page, slot["label"])
            except Exception as error:
                print(f"Bỏ qua {slot['label']}: {error}")
                continue
            loaded_count = await scroll_until_stable(page, max_scrolls)
            dom_products = await extract_dom_products(page, slot["label"])
            for product in dom_products:
                product.update(metadata)
            valid_network_products = merge_products(slot_network_products)
            if valid_network_products:
                all_products.extend(valid_network_products)
            else:
                all_products.extend(dom_products)
            print(
                f"{slot['label']}: loaded {loaded_count}, "
                f"extracted {len(dom_products)} DOM, "
                f"{len(valid_network_products)} valid JSON records"
            )

        active_slot = {"promotion_type": "all_promotions"}
        slot_network_products.clear()
        try:
            await click_promotion_tab(page, "TẤT CẢ KHUYẾN MÃI")
            loaded_count = await scroll_until_stable(page, max_scrolls)
            promotion_products = await extract_dom_products(page)
            for product in promotion_products:
                product.update(active_slot)
            valid_network_products = merge_products(slot_network_products)
            if valid_network_products:
                all_products.extend(valid_network_products)
            all_products.extend(promotion_products)
            print(
                "Tất cả khuyến mãi: "
                f"loaded {loaded_count}, extracted {len(promotion_products)} DOM"
            )
        except Exception as error:
            print(f"Không crawl được tab Tất cả khuyến mãi: {error}")

        if not menu_categories:
            try:
                menu_categories.extend(await fetch_menu_categories(page))
            except Exception as error:
                print(f"Không lấy được danh sách category BHX: {error}")

        categories_by_url: dict[str, dict[str, Any]] = {}
        for category in menu_categories:
            url = clean_text(category.get("url"))
            if url and url not in NON_PRODUCT_ROUTES:
                categories_by_url[url] = category
        categories = list(categories_by_url.values())
        if max_categories:
            categories = categories[:max_categories]
        print(f"Crawl sale theo category: {len(categories)} category")
        for index, category in enumerate(categories, start=1):
            active_slot = {
                "promotion_type": "category_sale",
                "category_name": clean_text(category.get("name")),
                "category_url": category_url(category),
            }
            slot_network_products.clear()
            try:
                await page.goto(
                    category_url(category),
                    wait_until="domcontentloaded",
                    timeout=timeout_ms,
                )
                await page.wait_for_timeout(1800)
                await scroll_until_stable(page, max_scrolls)
            except Exception as error:
                print(f"Bỏ qua category {clean_text(category.get('name'))}: {error}")
                continue
            category_products = merge_products(slot_network_products)
            all_products.extend(category_products)
            print(
                f"[{index}/{len(categories)}] "
                f"{clean_text(category.get('name'))}: {len(category_products)} sale"
            )

        active_slot = None
        await browser.close()

    return merge_products(all_products)


def write_outputs(products: list[dict[str, Any]], output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    crawl_date = datetime.now(VIETNAM_TZ).date().isoformat()
    jsonl_path = output_dir / f"bachhoaxanh_promotions_{crawl_date}.jsonl"
    temp_path = jsonl_path.with_suffix(".tmp")

    combined: dict[str, dict[str, Any]] = {}
    if jsonl_path.exists():
        with jsonl_path.open("r", encoding="utf-8-sig") as file:
            for line in file:
                try:
                    existing_product = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if is_valid_sale_product(existing_product):
                    combined[product_key(existing_product)] = existing_product

    for product in adapt_products(products, "bachhoaxanh"):
        combined[product_key(product)] = product
    output_products = sorted(
        combined.values(),
        key=lambda item: (
            clean_text(item.get("promotion_type")),
            clean_text(item.get("sale_start_at")),
            clean_text(item.get("name")).lower(),
        ),
    )

    with temp_path.open("w", encoding="utf-8") as file:
        for product in output_products:
            file.write(json.dumps(product, ensure_ascii=False) + "\n")
    temp_path.replace(jsonl_path)

    print(
        f"Đã thu thập {len(products)} record trong lượt này; "
        f"file ngày hiện có {len(output_products)} record."
    )
    print(f"JSONL: {jsonl_path.resolve()}")


def seconds_until_next_daily_run(hour: int, minute: int) -> float:
    now = datetime.now(VIETNAM_TZ)
    next_run = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if next_run <= now:
        next_run += timedelta(days=1)
    return max(5.0, (next_run - now).total_seconds())


async def run_once(args: argparse.Namespace) -> list[dict[str, Any]]:
    products = await crawl(
        headless=not args.show_browser,
        timeout_ms=args.timeout,
        max_scrolls=args.max_scrolls,
        max_categories=args.max_categories,
    )
    write_outputs(products, args.output_dir)
    return products


async def run_watcher(args: argparse.Namespace) -> None:
    while True:
        try:
            await run_once(args)
            delay = seconds_until_next_daily_run(
                args.daily_hour, args.daily_minute
            )
            next_run = datetime.now(VIETNAM_TZ) + timedelta(seconds=delay)
            print(f"Lần crawl tiếp theo: {next_run.isoformat()}")
        except Exception as error:
            delay = 300
            print(f"Crawl thất bại: {error}. Thử lại sau 5 phút.")
        await asyncio.sleep(delay)


def main() -> None:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")

    parser = argparse.ArgumentParser(
        description="Crawl sản phẩm khuyến mãi từ bachhoaxanh.com"
    )
    parser.add_argument("--show-browser", action="store_true")
    parser.add_argument("--timeout", type=int, default=60_000)
    parser.add_argument("--max-scrolls", type=int, default=40)
    parser.add_argument(
        "--max-categories",
        type=int,
        default=None,
        help="Giới hạn số category BHX để test nhanh; bỏ trống để crawl toàn bộ.",
    )
    parser.add_argument("--output-dir", type=Path, default=Path("data"))
    parser.add_argument(
        "--watch",
        action="store_true",
        help="Chạy liên tục và crawl toàn bộ sale period một lần mỗi ngày",
    )
    parser.add_argument(
        "--daily-hour",
        type=int,
        choices=range(24),
        default=0,
        metavar="0-23",
        help="Giờ crawl hằng ngày theo giờ Việt Nam (mặc định: 0)",
    )
    parser.add_argument(
        "--daily-minute",
        type=int,
        choices=range(60),
        default=5,
        metavar="0-59",
        help="Phút crawl hằng ngày (mặc định: 5)",
    )
    args = parser.parse_args()

    if args.watch:
        asyncio.run(run_watcher(args))
    else:
        asyncio.run(run_once(args))


if __name__ == "__main__":
    main()
