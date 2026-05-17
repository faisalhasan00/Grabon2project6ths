"""Validate scraped data — reject navigation noise and impossible cashback rates."""
from __future__ import annotations

import os
import re
from statistics import median
from typing import Any, Dict, List, Optional, Tuple

MAX_CASHBACK_PCT = float(os.getenv("CRAWLER_MAX_CASHBACK_PCT", "30"))
MIN_CASHBACK_PCT = 0.5

CASHBACK_STRICT_RE = re.compile(
    r"(\d+(?:\.\d+)?)\s*%\s*(?:cash\s*back|cashback|cb)\b",
    re.IGNORECASE,
)

JUNK_SUBSTRINGS = (
    "sign in", "sign up", "log in", "login", "download app", "get app",
    "privacy policy", "terms of", "cookie", "copyright", "all rights",
    "customer care", "help center", "follow us", "subscribe", "newsletter",
    "play store", "app store", "javascript", "enable cookies",
    "captcha", "access denied", "404", "page not found",
    "rewards rate", "reward rate",  # often generic program text, not merchant offer
)

OFFER_REQUIRED_ANY = (
    "cashback", "coupon", "promo", "code", "% off", "flat", "bank",
    "offer", "deal", "discount", "save", "upto", "up to",
)


def _parse_pct(value: Optional[str]) -> Optional[float]:
    if not value:
        return None
    m = re.search(r"(\d+(?:\.\d+)?)", str(value))
    if not m:
        return None
    pct = float(m.group(1))
    if pct < MIN_CASHBACK_PCT or pct > MAX_CASHBACK_PCT:
        return None
    return pct


def extract_valid_cashback_rates(text: str) -> List[float]:
    """Only explicit 'X% cashback' phrases within sane bounds."""
    rates: List[float] = []
    for m in CASHBACK_STRICT_RE.finditer(text or ""):
        pct = float(m.group(1))
        if MIN_CASHBACK_PCT <= pct <= MAX_CASHBACK_PCT:
            rates.append(pct)
    return rates


def best_cashback_from_text(text: str) -> Optional[str]:
    rates = extract_valid_cashback_rates(text)
    if not rates:
        return None
    return f"{max(rates):g}%"


def is_junk_offer_line(line: str) -> bool:
    lower = line.lower().strip()
    if len(lower) < 10:
        return True
    if any(j in lower for j in JUNK_SUBSTRINGS):
        return True
    # Lines that are only a huge percent with no offer context
    if re.fullmatch(r"\d+\s*%", lower):
        return True
    if re.search(r"\b(\d{2,3})\s*%\b", lower) and not any(k in lower for k in OFFER_REQUIRED_ANY):
        return True
    pct_m = re.search(r"(\d+(?:\.\d+)?)\s*%", lower)
    if pct_m:
        pct = float(pct_m.group(1))
        if pct > MAX_CASHBACK_PCT:
            return True
        if "cashback" not in lower and "off" not in lower and "save" not in lower:
            return True
    return False


def sanitize_offers(offers: List[str]) -> Tuple[List[str], List[str]]:
    """Returns (kept, rejected)."""
    kept: List[str] = []
    rejected: List[str] = []
    seen = set()
    for raw in offers:
        line = raw.strip()
        if not line or line in seen:
            continue
        # Strip wrapper prefix for junk check
        body = re.sub(r"^\[[^\]]+\]\s*", "", line)
        if is_junk_offer_line(body):
            rejected.append(line)
            continue
        seen.add(line)
        kept.append(line)
    return kept[:15], rejected


def sanitize_source_record(record: Dict[str, Any], raw_text: str = "") -> Dict[str, Any]:
    """Clean one source; recompute cashback from strict rules only."""
    offers, rejected_offers = sanitize_offers(record.get("offers") or [])
    rate = best_cashback_from_text(raw_text)
    if rate is None:
        old = _parse_pct(record.get("cashback_rate"))
        if old is not None:
            rate = record.get("cashback_rate")
        else:
            rate = None
            record["cashback_rate_rejected"] = record.get("cashback_rate")

    extraction = dict(record.get("extraction") or {})
    if rejected_offers or record.get("cashback_rate_rejected"):
        extraction["validation"] = "noise_filtered"
        extraction["confidence"] = min(extraction.get("confidence", 0.5), 0.75)
    else:
        extraction["validation"] = "passed"

    out = {
        **record,
        "offers": offers,
        "cashback_rate": rate,
        "extraction": extraction,
        "rejected_offer_count": len(rejected_offers),
    }
    if rejected_offers:
        out["rejected_offers_sample"] = rejected_offers[:3]
    return out


def consensus_cashback(sources: List[Dict[str, Any]]) -> Tuple[Optional[str], Dict[str, Any]]:
    """
    Pick market cashback only from validated per-source rates.
    Prefer agreement across 2+ sources.
    """
    meta: Dict[str, Any] = {"source_rates": {}, "method": "none"}
    by_source: List[Tuple[str, float]] = []

    for src in sources:
        if src.get("blocked"):
            continue
        name = src.get("source_name", "?")
        pct = _parse_pct(src.get("cashback_rate"))
        if pct is not None:
            by_source.append((name, pct))
            meta["source_rates"][name] = f"{pct:g}%"

    if not by_source:
        meta["method"] = "no_valid_rate"
        return None, meta

    values = [v for _, v in by_source]
    if len(values) >= 2:
        values_sorted = sorted(values)
        spread = values_sorted[-1] - values_sorted[0]
        if spread <= 5:
            chosen = median(values)
            meta["method"] = "multi_source_consensus"
            meta["sources_agreeing"] = len(values)
            return f"{chosen:g}%", meta
        # Wild disagreement — use median, lower confidence
        chosen = median(values)
        meta["method"] = "multi_source_median_high_spread"
        meta["spread"] = spread
        return f"{chosen:g}%", meta

    chosen = values[0]
    meta["method"] = "single_source"
    meta["single_source"] = by_source[0][0]
    return f"{chosen:g}%", meta


def validate_intelligence_payload(
    sources: List[Dict[str, Any]],
    raw_texts: Optional[Dict[str, str]] = None,
) -> Tuple[List[Dict[str, Any]], Optional[str], Dict[str, Any]]:
    """Validate all sources and compute consensus market rate."""
    raw_texts = raw_texts or {}
    cleaned: List[Dict[str, Any]] = []
    for src in sources:
        name = src.get("source_name", "")
        cleaned.append(sanitize_source_record(src, raw_texts.get(name, "")))

    rate, meta = consensus_cashback(cleaned)
    meta["max_cashback_cap"] = MAX_CASHBACK_PCT
    meta["real_data_only"] = True
    return cleaned, rate, meta
