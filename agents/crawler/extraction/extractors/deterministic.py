"""Regex / pattern extraction before any LLM call."""
from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Tuple

from agents.crawler.extraction.confidence import is_blocked_text
from agents.crawler.extraction.validation import best_cashback_from_text, is_junk_offer_line

CASHBACK_RE = re.compile(
    r"(\d+(?:\.\d+)?)\s*%\s*(?:cash\s*back|cashback|cb)",
    re.IGNORECASE,
)
COUPON_CODE_RE = re.compile(
    r"(?:code|coupon|promo)[:\s]+['\"]?([A-Z0-9]{4,20})['\"]?",
    re.IGNORECASE,
)
FLAT_OFF_RE = re.compile(
    r"(?:flat|save|upto|up to)\s*(?:rs\.?|₹)?\s*(\d{2,5})\s*(?:off)?",
    re.IGNORECASE,
)
PERCENT_OFF_RE = re.compile(r"(\d{1,2})\s*%\s*off", re.IGNORECASE)
BANK_OFF_RE = re.compile(
    r"(\d+(?:\.\d+)?)\s*%\s*(?:off|cashback)?\s*(?:on|with|via)\s+([A-Za-z][A-Za-z\s]{2,30}?)(?:\s+card|\s+bank|$)",
    re.IGNORECASE,
)


def extract_from_text(raw_text: str, merchant: str, source_name: str) -> Dict[str, Any]:
    text = raw_text or ""
    blocked = is_blocked_text(text)

    if blocked:
        return {
            "merchant": merchant,
            "source_name": source_name,
            "cashback_rate": None,
            "offers": [],
            "blocked": True,
            "extraction": {"method": "deterministic", "confidence": 0.12},
        }

    offers: List[str] = []
    seen = set()

    for m in CASHBACK_RE.finditer(text):
        line = f"[Cashback] {m.group(1)}% cashback"
        if line not in seen:
            seen.add(line)
            offers.append(line)

    for m in COUPON_CODE_RE.finditer(text):
        line = f"[Coupon] Code: {m.group(1)}"
        if line not in seen:
            seen.add(line)
            offers.append(line)

    for m in FLAT_OFF_RE.finditer(text):
        line = f"[Sale] Flat Rs.{m.group(1)} off"
        if line not in seen:
            seen.add(line)
            offers.append(line)

    for m in PERCENT_OFF_RE.finditer(text):
        pct = int(m.group(1))
        if pct > 30:
            continue
        line = f"[Sale] {pct}% off"
        if line not in seen:
            seen.add(line)
            offers.append(line)

    for m in BANK_OFF_RE.finditer(text):
        line = f"[Bank] {m.group(1)}% via {m.group(2).strip()}"
        if line not in seen:
            seen.add(line)
            offers.append(line)

    for line in text.splitlines():
        line = line.strip()
        if len(line) < 12 or len(line) > 220:
            continue
        if is_junk_offer_line(line):
            continue
        lower = line.lower()
        if any(k in lower for k in ("coupon", "cashback", "% off", "bank offer", "promo", "flat rs")):
            if line not in seen and len(offers) < 15:
                seen.add(line)
                offers.append(f"[Offer] {line[:200]}")

    cashback_rate = best_cashback_from_text(text)
    confidence = 0.35
    if offers:
        confidence = min(0.92, 0.72 + 0.03 * min(len(offers), 6))

    return {
        "merchant": merchant,
        "source_name": source_name,
        "cashback_rate": cashback_rate,
        "offers": offers[:15],
        "blocked": False,
        "extraction": {"method": "deterministic", "confidence": round(confidence, 3)},
    }

