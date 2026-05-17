"""Entry point for autonomous crawler surveillance (delegates to CrawlerAgent)."""
from __future__ import annotations

import asyncio

from dotenv import load_dotenv

from agents.crawler.agent import CrawlerAgent

load_dotenv()


async def main() -> None:
    await CrawlerAgent().run_autonomous_forever()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nCrawler surveillance stopped.")
