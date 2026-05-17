"""Adaptive monitoring: delegates interval and due-merchant logic to policy."""
from __future__ import annotations

from typing import Dict, List, Optional

from agents.crawler.scheduling.policy import (
    HOT_EVENT_TYPES,
    INTERVAL_CRITICAL,
    INTERVAL_HOT,
    INTERVAL_NORMAL,
    INTERVAL_PRIORITY,
    INTERVAL_QUIET,
    AdaptiveCrawlPolicy,
)
from agents.crawler.crawl.profiles import normalize_query
from agents.crawler.platform.store import IntelligenceStore

# Re-export for backward compatibility
class MarketScheduler:
    def __init__(self, store: Optional[IntelligenceStore] = None):
        self.store = store or IntelligenceStore()
        self.policy = AdaptiveCrawlPolicy(self.store)

    def slug_for_display_name(self, name: str) -> str:
        return normalize_query(name)

    def interval_for_merchant(self, merchant_slug: str) -> int:
        return self.policy.decide_interval(merchant_slug).interval_sec

    def mark_crawled(self, merchant_slug: str, events: List[Dict]) -> Dict[str, object]:
        return self.policy.after_crawl(merchant_slug, events)

    def due_merchants(self, display_names: List[str], *, force_all: bool = False) -> List[str]:
        if force_all:
            return display_names
        return self.policy.due_merchants(display_names)

    def sleep_between_iterations(self, merchants_crawled: int) -> int:
        return self.policy.sleep_between_iterations(merchants_crawled)
