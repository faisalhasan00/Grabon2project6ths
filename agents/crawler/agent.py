"""
Autonomous Market Intelligence Collection Agent.

Continuously collects evidence-backed competitive intelligence: crawl, extract,
validate, detect changes, emit market events, and self-schedule monitoring
(see agents/crawler/SPEC.md for full responsibility spec).
"""
from __future__ import annotations

import asyncio
import os
import sys
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv

from agents.base_agent import BaseAgent
from agents.crawler.crawl.collector import PlaywrightCollector
from agents.crawler.extraction.confidence import aggregate_confidence
from agents.crawler.intelligence import IntelligenceHub, get_intelligence_stream
from agents.crawler.extraction.validation import validate_intelligence_payload
from agents.crawler.crawl.profiles import (
    competitor_count_for_run,
    merchant_display,
    normalize_query,
)
from agents.crawler.crawl.proxy import ProxyPool
from agents.crawler.crawl.reliability import build_targets_ranked
from agents.crawler.scheduling.scheduler import MarketScheduler
from agents.crawler.platform.store import IntelligenceStore
from agents.crawler.crawl.workers import CrawlWorkerPool
from messaging.schemas import AgentMessage, AgentRole, MessageType

if sys.platform.startswith("win"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

load_dotenv()

CYAN = "\033[96m"
RESET = "\033[0m"


class CrawlerAgent(BaseAgent):
    """Autonomous market intelligence collector and surveillance engine."""

    def __init__(self, store: IntelligenceStore | None = None):
        super().__init__(role=AgentRole.CRAWLER, model="gemini-flash-latest", provider="google")
        self.store = store or IntelligenceStore()
        self.scheduler = MarketScheduler(self.store)
        self.policy = self.scheduler.policy
        self.proxy_pool = ProxyPool()
        self.worker_pool = CrawlWorkerPool()
        self.collector_proxy_enabled = self.proxy_pool.enabled
        self._radar_worker_id = f"crawler-{uuid.uuid4().hex[:8]}"
        self.stream = get_intelligence_stream()
        self.intelligence = IntelligenceHub(self.store, self.stream)

    async def handle_request(self, message: AgentMessage) -> AgentMessage:
        payload = message.payload.data
        raw_input = payload.get("input", "") if isinstance(payload, dict) else payload
        if isinstance(raw_input, dict):
            raw_query = raw_input.get("query", raw_input.get("merchant", "myntra"))
        else:
            raw_query = str(raw_input)

        full_scrape = bool(payload.get("full_scrape", False)) if isinstance(payload, dict) else False
        data = await self.collect_intelligence(str(raw_query), full_scrape=full_scrape)
        cost = float(data.pop("_cost_usd", 0.0))
        data.pop("phase", None)
        return self.create_response(message, data, cost=cost)

    async def collect_intelligence(
        self,
        raw_query: str,
        *,
        full_scrape: bool = False,
    ) -> Dict[str, Any]:
        """Core collection API — used by orchestrator and autonomous radar."""
        merchant_slug = normalize_query(raw_query)
        merchant_name = merchant_display(merchant_slug)
        max_competitors = competitor_count_for_run(full_scrape=full_scrape)
        started_at = datetime.now(timezone.utc).isoformat()

        print(f"\n   [Crawler] Intelligence run: {merchant_name} (slug={merchant_slug})")
        targets = build_targets_ranked(
            merchant_slug, self.store, full_scrape=full_scrape, max_competitors=max_competitors
        )
        print(f"   [Crawler] {len(targets)} competitors | workers={self.worker_pool.concurrency}")
        for t in targets:
            print(f"      → {t.source_name}: {t.url}")

        sources: List[Dict[str, Any]] = []
        total_cost = 0.0

        try:
            async with PlaywrightCollector(self.proxy_pool) as collector:
                sources, total_cost = await self.worker_pool.crawl_all(
                    self,
                    self.store,
                    collector,
                    targets,
                    merchant_name,
                    merchant_slug,
                )
        except ImportError as e:
            print(f"   [Crawler] Playwright unavailable: {e}")
            return self._empty_payload(merchant_name, merchant_slug, error=str(e))

        raw_texts = {s.get("source_name", ""): s.pop("_raw_text", "") for s in sources}
        parser_events: List[Dict[str, Any]] = []
        for s in sources:
            parser_events.extend(s.pop("_parser_events", []) or [])
        sources, best_cashback, rate_meta = validate_intelligence_payload(sources, raw_texts)

        all_offers: List[str] = []
        for s in sources:
            all_offers.extend(s.get("offers") or [])

        if rate_meta.get("method") == "no_valid_rate":
            print(f"   [Crawler] No validated cashback rate (noise filtered from page chrome)")

        intel_bundle = await self.intelligence.process(
            merchant_slug,
            sources,
            raw_texts=raw_texts,
            parser_events=parser_events,
        )
        events = intel_bundle["events"]
        schedule_meta = self.scheduler.mark_crawled(merchant_slug, events)
        timeline = self.store.get_merchant_timeline(merchant_slug, limit=5)
        recent_events = self.store.get_recent_events(merchant_slug, limit=10)
        agg_conf = aggregate_confidence(sources)
        consensus = intel_bundle.get("consensus") or {}
        if consensus.get("confidence"):
            agg_conf = min(0.98, (agg_conf + consensus["confidence"]) / 2)
        best_cashback = consensus.get("consensus_rate_label") or best_cashback

        self.store.record_crawl_run(
            merchant_slug,
            offer_count=len(all_offers),
            event_count=len(events),
            confidence=agg_conf,
            cost_usd=total_cost,
            started_at=started_at,
        )

        if events:
            print(f"   [Crawler] {CYAN}Events:{RESET} {[e['type'] for e in events]}")

        return {
            "merchant": merchant_name,
            "merchant_slug": merchant_slug,
            "cashback_rate": best_cashback,
            "offers": list(dict.fromkeys(all_offers))[:30],
            "client_rate": os.getenv("CLIENT_BASE_RATE", "5%"),
            "collected_at": datetime.now(timezone.utc).isoformat(),
            "aggregate_confidence": agg_conf,
            "sources": sources,
            "events": events,
            "recent_market_events": recent_events,
            "offer_lifecycle": self.store.get_offer_lifecycle(merchant_slug, limit=20),
            "crawl_timeline": timeline,
            "monitoring": schedule_meta,
            "source_reliability": dict(self.store.get_source_rankings()),
            "rate_validation": rate_meta,
            "consensus": intel_bundle.get("consensus"),
            "anomaly": intel_bundle.get("anomaly"),
            "category": intel_bundle.get("category"),
            "merchant_memory": intel_bundle.get("merchant_memory"),
            "competitive_intents": [
                e for e in events if e.get("type") == "competitive_intent_detected"
            ],
            "sweep": intel_bundle.get("sweep"),
            "intelligence_stream": intel_bundle.get("stream_recent"),
            "data_source": "live_web",
            "summary": (
                f"Intelligence from {len(sources)} sources; "
                f"{len(events)} events; workers={self.worker_pool.concurrency}."
            ),
            "query": merchant_slug,
            "results": sources,
            "_cost_usd": total_cost,
            "phase": "C",
        }

    def get_due_merchants(self, display_names: List[str] | None = None) -> List[str]:
        return self.policy.due_merchants(display_names)

    def monitoring_summary(self) -> List[Dict[str, Any]]:
        """Per-merchant schedule snapshot for operators."""
        rows = []
        for slug, row in self.store.list_schedules().items():
            decision = self.policy.decide_interval(slug)
            rows.append(
                {
                    "merchant": merchant_display(slug),
                    "slug": slug,
                    "due": self.store.is_merchant_due(slug),
                    "mode": row.get("crawl_mode") or decision.mode,
                    "interval_sec": row.get("crawl_interval_sec") or decision.interval_sec,
                    "next_reason": row.get("monitor_reason") or decision.reason,
                    "hot_until": row.get("hot_until"),
                }
            )
        return sorted(rows, key=lambda r: (r["due"] is False, r.get("interval_sec", 9999)))

    async def run_autonomous_forever(
        self,
        merchants: Optional[List[str]] = None,
    ) -> None:
        """
        Autonomous market radar — no orchestrator required.

        Self-decides which merchants are due, crawl priority, and sleep cadence
        from AdaptiveCrawlPolicy + SQLite history.
        """
        watchlist = merchants or self.policy.active_watchlist()
        print("=" * 55)
        print("CRAWLER AGENT — AUTONOMOUS SURVEILLANCE")
        print("=" * 55)
        print(f"Watchlist: {watchlist}")
        print(f"Workers: {self.worker_pool.concurrency} | Proxies: {'on' if self.collector_proxy_enabled else 'off'}")
        print("Press Ctrl+C to stop.\n")

        iteration = 0
        while True:
            iteration += 1
            watchlist = self.policy.active_watchlist()
            due = self.get_due_merchants(watchlist)
            waiting = [m for m in watchlist if m not in due]
            print(f"\n--- [CRAWLER {iteration}] Due: {due or '(none)'} ---")
            if waiting:
                print(f"   On interval: {waiting}")

            for name in due:
                slug = normalize_query(name)
                decision = self.policy.decide_interval(slug)
                self.store.enqueue_crawl(slug, priority=decision.priority)

            tasks = self.store.claim_tasks(self._radar_worker_id, limit=max(len(due), 1))
            for task in tasks:
                slug = task["merchant_slug"]
                display = self._display_for_slug(slug, watchlist)
                try:
                    payload = await self.collect_intelligence(f"Analyze {display} coupons")
                    self.store.complete_task(
                        task["id"],
                        {
                            "merchant": slug,
                            "events": len(payload.get("events", [])),
                            "mode": (payload.get("monitoring") or {}).get("crawl_mode"),
                        },
                    )
                    self._log_surveillance_result(payload)
                except Exception as e:
                    print(f"   [Crawler] Task {task['id']} failed: {e}")
                    self.store.complete_task(task["id"], {"error": str(e)}, failed=True)

            sleep_sec = self.policy.sleep_between_iterations(len(tasks))
            print(f"--- [CRAWLER {iteration}] {len(tasks)} run(s). Sleep {sleep_sec}s ---")
            await asyncio.sleep(sleep_sec)

    def _display_for_slug(self, slug: str, merchants: List[str]) -> str:
        for name in merchants:
            if normalize_query(name) == slug:
                return name
        return merchant_display(slug)

    def _log_surveillance_result(self, payload: Dict[str, Any]) -> None:
        monitoring = payload.get("monitoring") or {}
        events = payload.get("events") or []
        if events:
            print(f"   [Crawler] Events: {[e.get('type') for e in events]}")
        if monitoring:
            print(
                f"   [Crawler] Next: {monitoring.get('crawl_mode')} "
                f"every {monitoring.get('crawl_interval_sec')}s — {monitoring.get('monitor_reason', '')}"
            )

    def _empty_payload(self, merchant_name: str, slug: str, error: str) -> Dict[str, Any]:
        return {
            "merchant": merchant_name,
            "merchant_slug": slug,
            "cashback_rate": None,
            "offers": [],
            "sources": [],
            "events": [{"type": "collection_failed", "error": error}],
            "aggregate_confidence": 0.0,
            "collected_at": datetime.now(timezone.utc).isoformat(),
            "summary": error,
            "_cost_usd": 0.0,
        }


