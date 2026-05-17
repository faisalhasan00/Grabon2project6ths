"""
Crawler intelligence package.

    intelligence/
        hub.py                 IntelligenceHub (orchestrator)
        consensus/             cross-source validation
        anomaly/               historical baseline anomalies
        change_detection/        DOM, cashback, screenshot diffs
        lifecycle/               offer lifecycle stages
        intent/                  competitive strategy inference
        sweep/                   market-wide escalation
        streaming/               realtime event stream
"""
from agents.crawler.intelligence.hub import IntelligenceHub
from agents.crawler.intelligence.consensus import ConsensusValidator
from agents.crawler.intelligence.anomaly import AnomalyEngine
from agents.crawler.intelligence.change_detection import (
    ChangeDetectionEngine,
    detect_events,
    pick_best_cashback,
    parse_rate_pct,
    fingerprint_dom,
    normalize_offer,
    offer_key,
)
from agents.crawler.intelligence.lifecycle import OfferLifecycleTracker
from agents.crawler.intelligence.intent import IntentDetector
from agents.crawler.intelligence.sweep import MarketSweep
from agents.crawler.intelligence.streaming import (
    IntelligenceStream,
    get_intelligence_stream,
)

__all__ = [
    "IntelligenceHub",
    "ConsensusValidator",
    "AnomalyEngine",
    "ChangeDetectionEngine",
    "OfferLifecycleTracker",
    "IntentDetector",
    "MarketSweep",
    "IntelligenceStream",
    "get_intelligence_stream",
    "detect_events",
    "pick_best_cashback",
    "parse_rate_pct",
    "fingerprint_dom",
    "normalize_offer",
    "offer_key",
]
