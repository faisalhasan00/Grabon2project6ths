"""Site profiles, merchant aliases, and intent-based target selection."""
from __future__ import annotations

import os
import re
from dataclasses import dataclass
from typing import List, Optional, Tuple
import urllib.parse

# Slug used in coupon-site URLs (lowercase, no spaces)
MERCHANT_ALIASES = {
    "myntra": "myntra",
    "amazon": "amazon",
    "amazon india": "amazon",
    "flipkart": "flipkart",
    "ajio": "ajio",
    "nykaa": "nykaa",
    "meesho": "meesho",
    "tatacliq": "tatacliq",
    "tata cliq": "tatacliq",
    "jiomart": "jiomart",
    "snapdeal": "snapdeal",
    "firstcry": "firstcry",
    "lenskart": "lenskart",
    "pharmeasy": "pharmeasy",
}

MERCHANT_DISPLAY = {
    "myntra": "Myntra",
    "amazon": "Amazon",
    "flipkart": "Flipkart",
    "ajio": "AJIO",
    "nykaa": "Nykaa",
    "meesho": "Meesho",
    "tatacliq": "Tata CLiQ",
    "jiomart": "JioMart",
    "snapdeal": "Snapdeal",
    "firstcry": "FirstCry",
    "lenskart": "Lenskart",
    "pharmeasy": "Pharmeasy",
}

# Coupon / cashback competitors — store pages for a merchant slug
COMPETITOR_STORE_URLS = {
    "CashKaro": "https://cashkaro.com/stores/{slug}",
    "CouponDunia": "https://www.coupondunia.in/{slug}-coupons",
    "GoPaisa": "https://www.gopaisa.com/{slug}-coupons",
    "PaisaWapas": "https://www.paisawapas.com/{slug}-coupons",
    "CouponRaja": "https://www.couponraja.in/{slug}-coupons",
    "Zingoy": "https://www.zingoy.com/{slug}-coupons",
    "DesiDime": "https://www.desidime.com/stores/{slug}",
    "CouponDekho": "https://www.coupondekho.co.in/{slug}-coupons",
    "Cashaly": "https://www.cashaly.com/store/{slug}",
    "CouponMoto": "https://www.couponmoto.com/stores/{slug}-coupons",
    "Zoutons": "https://zoutons.com/{slug}-coupons",
    "Freekaamaal": "https://freekaamaal.com/stores/{slug}",
}

DEFAULT_COMPETITORS = list(COMPETITOR_STORE_URLS.keys())


def competitor_count_for_run(*, full_scrape: bool = False) -> int:
    """How many competitor sites to hit per merchant (default: all configured)."""
    total = len(DEFAULT_COMPETITORS)
    if full_scrape:
        return total
    return min(total, int(os.getenv("CRAWLER_MAX_COMPETITORS", str(total))))

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
]


@dataclass(frozen=True)
class CrawlTarget:
    source_name: str
    url: str
    target_type: str  # "competitor" | "merchant"


def normalize_query(raw: str) -> str:
    """Turn orchestrator text into a merchant slug."""
    text = raw.lower().strip()
    for token in ("analyze", "revise:", "coupons", "coupon", "deals", "offers", "search for", "focus on"):
        text = text.replace(token, " ")
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    text = " ".join(text.split())
    if text in MERCHANT_ALIASES:
        return MERCHANT_ALIASES[text]
    for alias, slug in MERCHANT_ALIASES.items():
        if alias in text or text in alias:
            return slug
    # First token fallback
    return text.split()[0] if text else "myntra"


def merchant_display(slug: str) -> str:
    return MERCHANT_DISPLAY.get(slug, slug.title())
