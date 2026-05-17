"""Market-wide sweep — escalate to all competitors on spike signals."""
from __future__ import annotations

from typing import Any, Dict, List

from agents.crawler.crawl.profiles import DEFAULT_COMPETITORS, competitor_count_for_run
from agents.crawler.platform.store import IntelligenceStore

SWEEP_TRIGGER_EVENTS = {
    "cashback_spike_detected",
    "competitor_push_detected",
    "rate_anomaly_detected",
    "visual_sale_detected",
}


class MarketSweep:
    def __init__(self, store: IntelligenceStore):
        self.store = store

    def should_sweep(self, events: List[Dict[str, Any]]) -> bool:
        return any(e.get("type") in SWEEP_TRIGGER_EVENTS for e in events)

    def plan_sweep(
        self,
        merchant_slug: str,
        events: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        trigger = next(
            (e for e in events if e.get("type") in SWEEP_TRIGGER_EVENTS),
            None,
        )
        task_ids: List[int] = []
        for _ in DEFAULT_COMPETITORS:
            tid = self.store.enqueue_crawl(merchant_slug, priority=0)
            task_ids.append(tid)

        sweep_event = {
            "type": "market_sweep_initiated",
            "merchant": merchant_slug,
            "trigger_event": trigger.get("type") if trigger else "unknown",
            "competitors_queued": len(DEFAULT_COMPETITORS),
            "task_ids": task_ids[:5],
        }
        self.store.insert_events(merchant_slug, [sweep_event])
        return {
            "sweep": True,
            "competitors": len(DEFAULT_COMPETITORS),
            "max_competitors": competitor_count_for_run(full_scrape=True),
            "trigger": trigger,
            "event": sweep_event,
        }
