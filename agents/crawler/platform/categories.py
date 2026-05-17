"""Category-aware crawling profiles — fashion, electronics, beauty, travel, etc."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List

MERCHANT_CATEGORY: Dict[str, str] = {
    "myntra": "fashion",
    "ajio": "fashion",
    "nykaa": "beauty",
    "flipkart": "electronics",
    "amazon": "electronics",
    "meesho": "fashion",
    "tatacliq": "fashion",
    "lenskart": "beauty",
    "firstcry": "fashion",
    "pharmeasy": "health",
    "snapdeal": "electronics",
    "jiomart": "grocery",
    "makemytrip": "travel",
    "goibibo": "travel",
    "cleartrip": "travel",
}


@dataclass(frozen=True)
class CategoryProfile:
    name: str
    volatility: float  # 1.0 = normal; >1 = more frequent crawls on hot
    sale_keywords: tuple
    preferred_sources: tuple  # prioritize these competitors
    max_cashback_typical: float  # anomaly baseline hint


PROFILES: Dict[str, CategoryProfile] = {
    "fashion": CategoryProfile(
        "fashion",
        volatility=1.35,
        sale_keywords=("end of season", "fashion sale", "wardrobe", "style fest"),
        preferred_sources=("CashKaro", "CouponDunia", "GoPaisa"),
        max_cashback_typical=15.0,
    ),
    "beauty": CategoryProfile(
        "beauty",
        volatility=1.25,
        sale_keywords=("beauty fest", "glow sale", "skincare"),
        preferred_sources=("CashKaro", "CouponDunia", "PaisaWapas"),
        max_cashback_typical=12.0,
    ),
    "electronics": CategoryProfile(
        "electronics",
        volatility=1.5,
        sale_keywords=("big billion", "great indian festival", "electronic sale", "mobile fest"),
        preferred_sources=("CashKaro", "GoPaisa", "DesiDime"),
        max_cashback_typical=10.0,
    ),
    "travel": CategoryProfile(
        "travel",
        volatility=1.2,
        sale_keywords=("flight sale", "hotel deal", "travel fest"),
        preferred_sources=("CashKaro", "CouponDunia"),
        max_cashback_typical=8.0,
    ),
    "health": CategoryProfile(
        "health",
        volatility=1.1,
        sale_keywords=("pharma", "wellness"),
        preferred_sources=("CashKaro", "CouponDunia"),
        max_cashback_typical=10.0,
    ),
    "grocery": CategoryProfile(
        "grocery",
        volatility=1.15,
        sale_keywords=("grocery", "daily essentials"),
        preferred_sources=("CashKaro", "GoPaisa"),
        max_cashback_typical=8.0,
    ),
    "general": CategoryProfile(
        "general",
        volatility=1.0,
        sale_keywords=("sale", "offer"),
        preferred_sources=("CashKaro", "CouponDunia", "GoPaisa"),
        max_cashback_typical=12.0,
    ),
}


def merchant_category(merchant_slug: str) -> str:
    return MERCHANT_CATEGORY.get(merchant_slug.lower(), "general")


def category_profile(merchant_slug: str) -> CategoryProfile:
    return PROFILES.get(merchant_category(merchant_slug), PROFILES["general"])
