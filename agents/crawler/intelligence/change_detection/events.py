"""Market event detection — delegates to ChangeDetectionEngine."""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from agents.crawler.intelligence.change_detection.engine import (
    ChangeDetectionEngine,
    parse_rate_pct,
)
from agents.crawler.platform.store import IntelligenceStore


def detect_events(
    merchant_slug: str,
    sources: List[Dict[str, Any]],
    store: IntelligenceStore,
    *,
    raw_texts: Optional[Dict[str, str]] = None,
) -> List[Dict[str, Any]]:
    return ChangeDetectionEngine(store).process(
        merchant_slug, sources, raw_texts=raw_texts
    )


def pick_best_cashback(sources: List[Dict[str, Any]]) -> Optional[str]:
    best: Optional[float] = None
    label: Optional[str] = None
    for s in sources:
        r = parse_rate_pct(s.get("cashback_rate"))
        if r is not None and (best is None or r > best):
            best = r
            label = s.get("cashback_rate")
    return label
