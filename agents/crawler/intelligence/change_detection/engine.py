"""
Change Detection Engine — DOM, cashback deltas, screenshots, offer lifecycle.

Compares current crawl state against persisted snapshots to emit grounded
market events (e.g. 5% → 12% → cashback_spike_detected).
"""
from __future__ import annotations

import hashlib
import os
import re
from dataclasses import dataclass, field
from difflib import SequenceMatcher
from typing import Any, Dict, List, Optional, Set, Tuple

from agents.crawler.platform.store import IntelligenceStore
from agents.crawler.crawl.visual_intelligence import compare_screenshots

# Cashback spike: 5% → 12% must fire (7pp > default thresholds)
SPIKE_MIN_POINTS = float(os.getenv("CRAWLER_SPIKE_MIN_POINTS", "2.0"))
SPIKE_MIN_RATIO = float(os.getenv("CRAWLER_SPIKE_MIN_RATIO", "1.2"))  # 20% relative lift
DROP_MIN_POINTS = float(os.getenv("CRAWLER_DROP_MIN_POINTS", "1.5"))

DOM_CHANGE_THRESHOLD = float(os.getenv("CRAWLER_DOM_CHANGE_THRESHOLD", "0.88"))
DOM_MIN_CHAR_DELTA = int(os.getenv("CRAWLER_DOM_MIN_CHAR_DELTA", "120"))

SALE_KEYWORDS = (
    "sale", "billion", "festive", "diwali", "black friday",
    "big billion", "end of season", "eos", "clearance",
)

EXCLUSIVE_MARKERS = ("exclusive", "only on", "limited time", "special offer")


def parse_rate_pct(rate: Optional[str]) -> Optional[float]:
    if not rate:
        return None
    m = re.search(r"(\d+(?:\.\d+)?)", str(rate))
    if not m:
        return None
    return float(m.group(1))


def normalize_offer(text: str) -> str:
    t = re.sub(r"\s+", " ", (text or "").lower().strip())
    t = re.sub(r"[^\w\s%₹$.-]", "", t)
    return t[:240]


def offer_key(text: str) -> str:
    return hashlib.sha256(normalize_offer(text).encode("utf-8")).hexdigest()[:16]


def fingerprint_dom(raw_text: str) -> str:
    """Stable hash of normalized visible DOM text."""
    normalized = re.sub(r"\s+", " ", (raw_text or "").lower().strip())
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def fingerprint_offers(offers: List[str]) -> str:
    keys = sorted(offer_key(o) for o in offers if o and o.strip())
    blob = "|".join(keys)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def dom_similarity(old_text: str, new_text: str) -> float:
    a = re.sub(r"\s+", " ", (old_text or "").lower().strip())
    b = re.sub(r"\s+", " ", (new_text or "").lower().strip())
    if not a and not b:
        return 1.0
    if not a or not b:
        return 0.0
    return SequenceMatcher(None, a, b).ratio()


def extract_offer_lines(raw_text: str) -> List[str]:
    """Lines likely to contain promotional content (for DOM delta detail)."""
    lines: List[str] = []
    markers = ("cashback", "coupon", "%", "off", "save", "deal", "offer", "bank", "promo")
    for line in (raw_text or "").splitlines():
        low = line.lower().strip()
        if len(low) < 8:
            continue
        if any(m in low for m in markers):
            lines.append(low[:200])
    return lines


def is_cashback_spike(old_pct: float, new_pct: float) -> bool:
    if new_pct <= old_pct:
        return False
    delta = new_pct - old_pct
    if delta >= SPIKE_MIN_POINTS:
        return True
    if old_pct > 0 and (new_pct / old_pct) >= SPIKE_MIN_RATIO and delta >= 1.0:
        return True
    return False


def is_cashback_drop(old_pct: float, new_pct: float) -> bool:
    return old_pct - new_pct >= DROP_MIN_POINTS


@dataclass
class SourceState:
    source_name: str
    cashback_rate: Optional[str]
    rate_pct: Optional[float]
    offers: List[str]
    blocked: bool
    confidence: float
    dom_fingerprint: str
    offer_fingerprint: str
    screenshot_hash: Optional[str]
    perceptual_hash: Optional[str] = None
    hero_perceptual_hash: Optional[str] = None
    raw_text: str = ""


@dataclass
class ChangeDetectionResult:
    events: List[Dict[str, Any]] = field(default_factory=list)
    offer_lifecycle: List[Dict[str, Any]] = field(default_factory=list)


class ChangeDetectionEngine:
    def __init__(self, store: IntelligenceStore):
        self.store = store

    def process(
        self,
        merchant_slug: str,
        sources: List[Dict[str, Any]],
        *,
        raw_texts: Optional[Dict[str, str]] = None,
    ) -> List[Dict[str, Any]]:
        raw_texts = raw_texts or {}
        prev_all = self.store.get_snapshots(merchant_slug)
        result = ChangeDetectionResult()
        current_states: List[SourceState] = []

        for src in sources:
            name = src.get("source_name", "")
            offers = list(src.get("offers") or [])
            raw = raw_texts.get(name, "")
            evidence = src.get("evidence") or {}
            vi = src.get("visual_intelligence") or {}
            shot_hash = None
            if evidence.get("screenshot_hash"):
                shot_hash = str(evidence["screenshot_hash"]).replace("sha256:", "")
            phash = evidence.get("perceptual_hash") or vi.get("perceptual_hash")
            hero_phash = evidence.get("hero_perceptual_hash") or vi.get("hero_perceptual_hash")

            state = SourceState(
                source_name=name,
                cashback_rate=src.get("cashback_rate"),
                rate_pct=parse_rate_pct(src.get("cashback_rate")),
                offers=offers,
                blocked=bool(src.get("blocked")),
                confidence=float((src.get("extraction") or {}).get("confidence", 0) or 0),
                dom_fingerprint=fingerprint_dom(raw),
                offer_fingerprint=fingerprint_offers(offers),
                screenshot_hash=shot_hash,
                perceptual_hash=phash,
                hero_perceptual_hash=hero_phash,
                raw_text=raw,
            )
            current_states.append(state)
            prev = prev_all.get(name, {})
            self._diff_source(merchant_slug, state, prev, result)

        self._diff_merchant_aggregate(merchant_slug, current_states, prev_all, result)
        self._baseline_if_needed(merchant_slug, prev_all, current_states, result)

        if result.events:
            self.store.insert_events(merchant_slug, result.events)
        if result.offer_lifecycle:
            self.store.record_offer_lifecycle(merchant_slug, result.offer_lifecycle)

        for state in current_states:
            self.store.upsert_snapshot(
                merchant_slug,
                state.source_name,
                cashback_rate=state.cashback_rate,
                offers=state.offers,
                confidence=state.confidence,
                blocked=state.blocked,
                dom_fingerprint=state.dom_fingerprint,
                offer_fingerprint=state.offer_fingerprint,
                screenshot_hash=state.screenshot_hash,
                perceptual_hash=state.perceptual_hash,
                hero_perceptual_hash=state.hero_perceptual_hash,
                rate_pct=state.rate_pct,
                dom_text_sample=state.raw_text[:8000] if state.raw_text else None,
            )

        return result.events

    def _diff_source(
        self,
        merchant_slug: str,
        cur: SourceState,
        prev: Dict[str, Any],
        result: ChangeDetectionResult,
    ) -> None:
        name = cur.source_name
        if not prev:
            if cur.offers:
                result.events.append({
                    "type": "baseline_collected",
                    "source": name,
                    "merchant": merchant_slug,
                    "offer_count": len(cur.offers),
                    "rate": cur.cashback_rate,
                })
            return

        old_rate = prev.get("rate_pct")
        if old_rate is None:
            old_rate = parse_rate_pct(prev.get("cashback_rate"))
        new_rate = cur.rate_pct

        if old_rate is not None and new_rate is not None:
            if is_cashback_spike(old_rate, new_rate):
                result.events.append({
                    "type": "cashback_spike_detected",
                    "source": name,
                    "merchant": merchant_slug,
                    "from": f"{old_rate:g}%",
                    "to": f"{new_rate:g}%",
                    "delta_points": round(new_rate - old_rate, 2),
                    "detector": "cashback_delta",
                })
            elif is_cashback_drop(old_rate, new_rate):
                result.events.append({
                    "type": "cashback_drop_detected",
                    "source": name,
                    "merchant": merchant_slug,
                    "from": f"{old_rate:g}%",
                    "to": f"{new_rate:g}%",
                    "delta_points": round(old_rate - new_rate, 2),
                    "detector": "cashback_delta",
                })

        old_dom_fp = prev.get("dom_fingerprint") or ""
        old_dom_text = prev.get("dom_text_sample") or ""
        if cur.dom_fingerprint and old_dom_fp and cur.dom_fingerprint != old_dom_fp:
            sim = dom_similarity(old_dom_text, cur.raw_text)
            char_delta = abs(len(cur.raw_text) - len(old_dom_text))
            if sim < DOM_CHANGE_THRESHOLD or char_delta >= DOM_MIN_CHAR_DELTA:
                old_lines = extract_offer_lines(old_dom_text)
                new_lines = extract_offer_lines(cur.raw_text)
                added_lines = [ln for ln in new_lines if ln not in set(old_lines)][:8]
                result.events.append({
                    "type": "dom_structure_changed",
                    "source": name,
                    "merchant": merchant_slug,
                    "similarity": round(sim, 3),
                    "char_delta": char_delta,
                    "promo_lines_added": added_lines,
                    "detector": "dom_diff",
                })

        old_phash = prev.get("perceptual_hash") or ""
        new_phash = cur.perceptual_hash or ""
        if old_phash and new_phash:
            comp = compare_screenshots(new_phash, old_phash)
            if comp.visually_changed:
                result.events.append({
                    "type": "visual_campaign_changed",
                    "source": name,
                    "merchant": merchant_slug,
                    "hamming_distance": comp.hamming_distance,
                    "detector": "perceptual_hash",
                })
            if comp.hero_changed:
                result.events.append({
                    "type": "hero_banner_changed",
                    "source": name,
                    "merchant": merchant_slug,
                    "hero_hamming": comp.hero_hamming,
                    "detector": "hero_phash",
                })
        else:
            old_shot = (prev.get("screenshot_hash") or "").replace("sha256:", "")
            new_shot = (cur.screenshot_hash or "").replace("sha256:", "")
            if old_shot and new_shot and old_shot != new_shot:
                result.events.append({
                    "type": "visual_campaign_changed",
                    "source": name,
                    "merchant": merchant_slug,
                    "previous_hash": f"sha256:{old_shot[:12]}…",
                    "current_hash": f"sha256:{new_shot[:12]}…",
                    "detector": "screenshot_sha256",
                })

        old_offers = prev.get("offers") or []
        self._diff_offers(merchant_slug, name, old_offers, cur.offers, result)

        old_offer_fp = prev.get("offer_fingerprint") or ""
        if (
            old_offer_fp
            and cur.offer_fingerprint
            and old_offer_fp != cur.offer_fingerprint
            and not any(e["type"].startswith("offer_") for e in result.events if e.get("source") == name)
        ):
            result.events.append({
                "type": "offer_set_changed",
                "source": name,
                "merchant": merchant_slug,
                "detector": "offer_fingerprint",
            })

        if _has_sale_signal(cur.offers) and not _has_sale_signal(old_offers):
            result.events.append({
                "type": "campaign_started",
                "source": name,
                "merchant": merchant_slug,
                "signal": "sale_keywords",
                "detector": "keyword",
            })
        elif len(old_offers) == 0 and len(cur.offers) >= 4:
            result.events.append({
                "type": "campaign_started",
                "source": name,
                "merchant": merchant_slug,
                "offer_count": len(cur.offers),
                "detector": "offer_count",
            })

        offer_text = " ".join(cur.offers).lower()
        if any(m in offer_text for m in EXCLUSIVE_MARKERS):
            if not any(m in " ".join(old_offers).lower() for m in EXCLUSIVE_MARKERS):
                result.events.append({
                    "type": "exclusive_offer_detected",
                    "source": name,
                    "merchant": merchant_slug,
                    "detector": "keyword",
                })

    def _diff_offers(
        self,
        merchant_slug: str,
        source_name: str,
        old_offers: List[str],
        new_offers: List[str],
        result: ChangeDetectionResult,
    ) -> None:
        old_map = {offer_key(o): normalize_offer(o) for o in old_offers if o.strip()}
        new_map = {offer_key(o): normalize_offer(o) for o in new_offers if o.strip()}
        old_keys = set(old_map)
        new_keys = set(new_map)

        added_keys = new_keys - old_keys
        removed_keys = old_keys - new_keys

        for key in added_keys:
            text = new_map[key]
            result.offer_lifecycle.append({
                "offer_key": key,
                "source_name": source_name,
                "offer_text": text,
                "change": "added",
            })
            result.events.append({
                "type": "offer_added",
                "source": source_name,
                "merchant": merchant_slug,
                "offer": text[:160],
                "offer_key": key,
                "detector": "offer_lifecycle",
            })

        for key in removed_keys:
            text = old_map[key]
            result.offer_lifecycle.append({
                "offer_key": key,
                "source_name": source_name,
                "offer_text": text,
                "change": "removed",
            })
            result.events.append({
                "type": "offer_removed",
                "source": source_name,
                "merchant": merchant_slug,
                "offer": text[:160],
                "offer_key": key,
                "detector": "offer_lifecycle",
            })

        if len(added_keys) >= 3:
            result.events.append({
                "type": "new_offers_detected",
                "source": source_name,
                "merchant": merchant_slug,
                "new_count": len(added_keys),
                "detector": "offer_lifecycle",
            })

    def _diff_merchant_aggregate(
        self,
        merchant_slug: str,
        states: List[SourceState],
        prev_all: Dict[str, Dict[str, Any]],
        result: ChangeDetectionResult,
    ) -> None:
        def best_rate(states_or_prev, from_states: bool) -> Tuple[Optional[float], Optional[str]]:
            best: Optional[float] = None
            src: Optional[str] = None
            if from_states:
                for s in states:
                    if s.rate_pct is not None and (best is None or s.rate_pct > best):
                        best = s.rate_pct
                        src = s.source_name
            else:
                for name, p in states_or_prev.items():
                    r = p.get("rate_pct")
                    if r is None:
                        r = parse_rate_pct(p.get("cashback_rate"))
                    if r is not None and (best is None or r > best):
                        best = r
                        src = name
            return best, src

        old_best, _ = best_rate(prev_all, from_states=False)
        new_best, new_src = best_rate(states, from_states=True)

        if old_best is not None and new_best is not None and is_cashback_spike(old_best, new_best):
            already = any(
                e.get("type") == "cashback_spike_detected"
                and e.get("from") == f"{old_best:g}%"
                and e.get("to") == f"{new_best:g}%"
                for e in result.events
            )
            if not already:
                result.events.append({
                    "type": "cashback_spike_detected",
                    "source": new_src,
                    "merchant": merchant_slug,
                    "from": f"{old_best:g}%",
                    "to": f"{new_best:g}%",
                    "delta_points": round(new_best - old_best, 2),
                    "detector": "merchant_aggregate",
                    "scope": "merchant_best",
                })

        if new_best and 5 <= new_best <= 30:
            result.events.append({
                "type": "high_cashback_observed",
                "source": new_src,
                "merchant": merchant_slug,
                "rate": f"{new_best:g}%",
                "detector": "aggregate",
            })

    def _baseline_if_needed(
        self,
        merchant_slug: str,
        prev_all: Dict[str, Dict[str, Any]],
        states: List[SourceState],
        result: ChangeDetectionResult,
    ) -> None:
        if prev_all:
            return
        total = sum(len(s.offers) for s in states)
        if total > 0 and not any(e.get("type") == "baseline_collected" for e in result.events):
            result.events.append({
                "type": "baseline_collected",
                "merchant": merchant_slug,
                "offer_count": total,
                "detector": "first_run",
            })


def _has_sale_signal(offers: List[str]) -> bool:
    blob = " ".join(offers).lower()
    return any(k in blob for k in SALE_KEYWORDS)
