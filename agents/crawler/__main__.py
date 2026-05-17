"""
Crawler CLI.

  python -m agents.crawler                  # one-shot Flipkart
  python -m agents.crawler Myntra           # one-shot merchant
  python -m agents.crawler --autonomous     # 24/7 surveillance loop
  python -m agents.crawler --status         # monitoring schedule
"""
from __future__ import annotations

import asyncio
import json
import sys

from agents.crawler.agent import CrawlerAgent


async def _one_shot(query: str) -> None:
    data = await CrawlerAgent().collect_intelligence(query)
    print(f"\nMerchant: {data.get('merchant')}")
    print(f"Validated cashback: {data.get('cashback_rate')}")
    print(f"Offers: {len(data.get('offers', []))}")
    print(f"Events: {[e.get('type') for e in data.get('events', [])]}")
    mon = data.get("monitoring") or {}
    if mon:
        print(f"Next crawl: {mon.get('crawl_mode')} in {mon.get('crawl_interval_sec')}s")


async def _status() -> None:
    print(json.dumps(CrawlerAgent().monitoring_summary(), indent=2))


async def _autonomous() -> None:
    await CrawlerAgent().run_autonomous_forever()


def main() -> None:
    args = sys.argv[1:]
    if not args:
        asyncio.run(_one_shot("Flipkart"))
        return
    if args[0] in ("--autonomous", "-a", "--radar"):
        asyncio.run(_autonomous())
        return
    if args[0] in ("--status", "-s"):
        asyncio.run(_status())
        return
    query = " ".join(args).strip()
    if not query.lower().startswith("analyze"):
        query = f"Analyze {query} coupons"
    asyncio.run(_one_shot(query))


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nStopped.")
