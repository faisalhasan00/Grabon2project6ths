"""Concurrent crawl workers with bounded parallelism."""
from __future__ import annotations

import asyncio
import os
from typing import Any, Dict, List, Tuple

from agents.crawler.crawl.collector import PlaywrightCollector
from agents.crawler.crawl.pipeline import process_target
from agents.crawler.crawl.profiles import CrawlTarget
from agents.crawler.platform.store import IntelligenceStore


class CrawlWorkerPool:
    def __init__(self, concurrency: int | None = None):
        self.concurrency = concurrency or int(os.getenv("CRAWLER_WORKERS", "3"))

    async def crawl_all(
        self,
        agent,
        store: IntelligenceStore,
        collector: PlaywrightCollector,
        targets: List[CrawlTarget],
        merchant_name: str,
        merchant_slug: str,
    ) -> Tuple[List[Dict[str, Any]], float]:
        sem = asyncio.Semaphore(self.concurrency)
        results: List[Dict[str, Any]] = []
        total_cost = 0.0

        async def _run(target: CrawlTarget) -> None:
            nonlocal total_cost
            async with sem:
                record, cost = await process_target(
                    agent, store, collector, target, merchant_name, merchant_slug
                )
                results.append(record)
                total_cost += cost

        await asyncio.gather(*[_run(t) for t in targets])
        return results, total_cost
