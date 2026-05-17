"""Self-healing parsers — selector chains, drift detection, automatic recovery."""
from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Tuple

from agents.crawler.intelligence.change_detection import fingerprint_dom
from agents.crawler.extraction.extractors.deterministic import extract_from_text
from agents.crawler.platform.memory import CrawlerMemory

# Per-source CSS fallback chains (coupon-aggregator layouts)
DEFAULT_SELECTOR_CHAINS: Dict[str, List[str]] = {
    "CashKaro": [
        "[class*='cashback']", "[class*='Cashback']", ".store-cb", ".cb-rate",
        "h1", "h2", ".offer-title", "[data-testid*='cashback']",
    ],
    "CouponDunia": [
        ".cashback", ".cb-percent", "[class*='cashback']", ".deal-title", "h1",
    ],
    "GoPaisa": [
        ".cashback-percent", "[class*='cashback']", ".offer-box h2", ".store-rate",
    ],
    "PaisaWapas": [
        "[class*='cashback']", ".rate-box", "h1", ".coupon-title",
    ],
    "default": [
        "[class*='cashback']", "[class*='offer']", "h1", "h2",
        "[class*='deal']", "[class*='coupon']",
    ],
}


def _chain_for_source(source_name: str, memory: CrawlerMemory, merchant_slug: str) -> List[str]:
    learned = memory.get_selector_chain(merchant_slug, source_name)
    defaults = DEFAULT_SELECTOR_CHAINS.get(source_name, DEFAULT_SELECTOR_CHAINS["default"])
    # Learned selectors that succeeded go first
    merged: List[str] = []
    for s in learned + defaults:
        if s not in merged:
            merged.append(s)
    return merged


def _extract_via_selectors(html: str, selectors: List[str]) -> Tuple[List[str], Optional[str], Optional[str]]:
    try:
        from bs4 import BeautifulSoup
    except ImportError:
        return [], None, None

    soup = BeautifulSoup(html or "", "html.parser")
    offers: List[str] = []
    used: Optional[str] = None
    for sel in selectors:
        try:
            nodes = soup.select(sel)
        except Exception:
            continue
        if not nodes:
            continue
        used = sel
        for node in nodes[:12]:
            text = node.get_text(" ", strip=True)
            if 8 < len(text) < 300:
                offers.append(text)
        if offers:
            break

    cashback = None
    for o in offers:
        m = re.search(r"(\d+(?:\.\d+)?)\s*%\s*(?:cashback|cash\s*back)?", o, re.I)
        if m:
            cashback = f"{float(m.group(1)):g}%"
            break
    return offers[:15], cashback, used


def extract_with_healing(
    memory: CrawlerMemory,
    *,
    merchant_slug: str,
    merchant_name: str,
    source_name: str,
    raw_text: str,
    html: str = "",
) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    """
    Parse with fallback chain + memory. Returns (extraction_dict, side_events).
    """
    events: List[Dict[str, Any]] = []
    dom_fp = fingerprint_dom(raw_text or html)
    prev_fp = memory.last_dom_fingerprint(merchant_slug, source_name)

    drift = bool(prev_fp and dom_fp != prev_fp)
    result = extract_from_text(raw_text, merchant_name, source_name)
    selector_used: Optional[str] = "regex_deterministic"
    offers = list(result.get("offers") or [])

    if html and (len(offers) < 2 or drift):
        css_offers, css_rate, selector_used = _extract_via_selectors(
            html, _chain_for_source(source_name, memory, merchant_slug)
        )
        if css_offers:
            result["offers"] = list(dict.fromkeys(offers + css_offers))[:20]
            result["extraction"] = {
                "method": "self_healing_css",
                "confidence": min(0.9, 0.55 + 0.05 * len(result["offers"])),
                "selector": selector_used,
            }
            if css_rate and not result.get("cashback_rate"):
                result["cashback_rate"] = css_rate

    if drift and len(result.get("offers") or []) == 0:
        events.append({
            "type": "parser_drift_detected",
            "source": source_name,
            "merchant": merchant_slug,
            "previous_fingerprint": prev_fp[:16] if prev_fp else None,
            "current_fingerprint": dom_fp[:16],
            "detector": "dom_fingerprint",
        })
        # Recovery attempt: broader regex on full html text
        if html:
            from bs4 import BeautifulSoup
            plain = BeautifulSoup(html, "html.parser").get_text(" ", strip=True)
            recovery = extract_from_text(plain[:12000], merchant_name, source_name)
            if recovery.get("offers"):
                result = recovery
                result["extraction"] = {
                    "method": "self_healing_recovery",
                    "confidence": 0.45,
                }
                selector_used = "recovery_fulltext"
                events.append({
                    "type": "parser_recovered",
                    "source": source_name,
                    "merchant": merchant_slug,
                    "method": "fulltext_fallback",
                })

    success = len(result.get("offers") or []) > 0 and not result.get("blocked")
    memory.record_parser_result(
        merchant_slug,
        source_name,
        selector_used=selector_used,
        dom_fingerprint=dom_fp,
        success=success,
        offer_count=len(result.get("offers") or []),
    )

    if drift and success:
        events.append({
            "type": "parser_drift_adapted",
            "source": source_name,
            "merchant": merchant_slug,
            "selector": selector_used,
        })

    result["dom_fingerprint"] = dom_fp
    return result, events
