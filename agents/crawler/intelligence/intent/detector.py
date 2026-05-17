"""Competitive intent detection — infer strategy from offers and events."""
from __future__ import annotations

import re
from typing import Any, Dict, List

INTENT_RULES = [
    (
        "aggressive_acquisition",
        ("new user", "first order", "first purchase", "sign up", "welcome"),
        ("cashback_spike_detected", "high_cashback_observed", "new_offers_detected"),
    ),
    (
        "festive_retention_push",
        ("diwali", "festive", "eid", "christmas", "black friday", "big billion", "great indian"),
        ("campaign_started", "visual_sale_detected", "visual_campaign_detected"),
    ),
    (
        "wallet_partnership_expansion",
        ("paytm", "phonepe", "amazon pay", "wallet", "upi", "bank offer"),
        ("new_offers_detected", "exclusive_offer_detected"),
    ),
    (
        "competitor_push",
        ("exclusive", "limited time", "only on", "beat", "extra cashback"),
        ("cashback_spike_detected", "competitor_push_detected"),
    ),
]


class IntentDetector:
    def detect(
        self,
        merchant_slug: str,
        sources: List[Dict[str, Any]],
        events: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        blob = " ".join(
            " ".join(s.get("offers") or []) for s in sources
        ).lower()
        event_types = {e.get("type") for e in events}
        signals: List[Dict[str, Any]] = []

        for intent, keywords, event_hints in INTENT_RULES:
            kw_hit = sum(1 for k in keywords if k in blob)
            ev_hit = bool(event_types & set(event_hints))
            if kw_hit >= 2 or (kw_hit >= 1 and ev_hit):
                confidence = min(0.92, 0.45 + 0.15 * kw_hit + (0.2 if ev_hit else 0))
                signals.append({
                    "type": "competitive_intent_detected",
                    "merchant": merchant_slug,
                    "intent": intent,
                    "confidence": round(confidence, 2),
                    "keyword_hits": kw_hit,
                    "event_corroboration": ev_hit,
                })

        rates = []
        for s in sources:
            m = re.search(r"(\d+(?:\.\d+)?)\s*%", str(s.get("cashback_rate") or ""))
            if m:
                rates.append(float(m.group(1)))
        if rates and max(rates) >= 10 and "cashback_spike_detected" in event_types:
            if not any(x.get("intent") == "aggressive_acquisition" for x in signals):
                signals.append({
                    "type": "competitive_intent_detected",
                    "merchant": merchant_slug,
                    "intent": "aggressive_acquisition",
                    "confidence": 0.75,
                    "keyword_hits": 0,
                    "event_corroboration": True,
                })

        return signals
