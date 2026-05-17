"""Change detection — DOM diff, cashback deltas, screenshots, offer deltas."""
from agents.crawler.intelligence.change_detection.engine import (
    ChangeDetectionEngine,
    ChangeDetectionResult,
    SourceState,
    fingerprint_dom,
    normalize_offer,
    offer_key,
    parse_rate_pct,
)
from agents.crawler.intelligence.change_detection.events import (
    detect_events,
    pick_best_cashback,
)

__all__ = [
    "ChangeDetectionEngine",
    "ChangeDetectionResult",
    "SourceState",
    "detect_events",
    "pick_best_cashback",
    "fingerprint_dom",
    "normalize_offer",
    "offer_key",
    "parse_rate_pct",
]
