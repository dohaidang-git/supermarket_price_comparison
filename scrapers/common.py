"""Shared output adapter for retailer crawlers.

Each retailer keeps its source-specific fields, while this adapter adds the
small generic envelope expected by Bronze and Silver.
"""

from __future__ import annotations

from typing import Any


def as_number(value: Any) -> int | float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return value
    if isinstance(value, str):
        digits = "".join(char for char in value if char.isdigit())
        return int(digits) if digits else None
    return None


def to_bronze_payload(product: dict[str, Any], retailer_id: str) -> dict[str, Any]:
    """Add cross-retailer fields without discarding source-specific data."""
    sale_price = as_number(product.get("sale_price"))
    listed_price = as_number(product.get("original_price"))
    promotion_text = str(product.get("promotion") or "").strip()
    is_price_discount = bool(sale_price and listed_price and listed_price > sale_price)
    has_promo_mechanic = bool(promotion_text) and not is_price_discount
    generic = {
        "retailer_id": retailer_id,
        "store_code": product.get("store_code") or product.get("store_site_code") or "online_default",
        "store_group_code": product.get("store_group_code"),
        "source_product_id": product.get("product_id") or product.get("sku") or product.get("barcode"),
        "product_name_raw": product.get("name"),
        "brand_raw": product.get("brand"),
        "category_raw": product.get("category_name") or product.get("category_full_path"),
        "unit_raw": product.get("unit"),
        "current_price": sale_price,
        "listed_price": listed_price,
        "promo_price": sale_price if is_price_discount else None,
        "currency": "VND",
        "is_price_discount": is_price_discount,
        "has_promo_mechanic": has_promo_mechanic,
        "is_on_promotion": True,
        # Source crawlers use different names for the product page and image.
        # Prefer the product page URL over a category/promotion source URL.
        "source_url": (
            product.get("product_url")
            or product.get("url")
            or product.get("source_url")
        ),
        "image_url": (
            product.get("image_url")
            or product.get("image")
            or product.get("thumbnail_url")
            or product.get("thumbnail")
        ),
        "observed_at": product.get("crawled_at"),
        "raw_product": dict(product),
    }
    return {**product, **generic}


def adapt_products(products: list[dict[str, Any]], retailer_id: str) -> list[dict[str, Any]]:
    return [to_bronze_payload(product, retailer_id) for product in products]
