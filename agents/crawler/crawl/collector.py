"""Playwright acquisition: one browser per batch, proxy rotation, human-like navigation."""
from __future__ import annotations

import asyncio
import random
from dataclasses import dataclass
from typing import Optional

from agents.crawler.extraction.confidence import is_blocked_text
from agents.crawler.crawl.evidence import EvidenceRecord, save_screenshot
from agents.crawler.crawl.profiles import USER_AGENTS
from agents.crawler.crawl.proxy import ProxyPool


@dataclass
class PageSnapshot:
    source_name: str
    url: str
    raw_text: str
    status_code: int
    blocked: bool
    evidence: Optional[EvidenceRecord]
    html: str = ""
    error: Optional[str] = None
    proxy_used: Optional[str] = None


class PlaywrightCollector:
    """Reuses one Chromium instance; rotates proxies per context."""

    def __init__(self, proxy_pool: Optional[ProxyPool] = None):
        self._playwright = None
        self._browser = None
        self.proxy_pool = proxy_pool or ProxyPool()

    async def __aenter__(self):
        from playwright.async_api import async_playwright
        from playwright_stealth import Stealth

        self._stealth_cls = Stealth
        self._playwright = await async_playwright().start()
        self._browser = await self._playwright.chromium.launch(
            headless=True,
            args=["--disable-blink-features=AutomationControlled"],
        )
        if self.proxy_pool.enabled:
            print(f"      [Collector] Proxy pool: {len(self.proxy_pool._proxies)} endpoints")
        return self

    async def __aexit__(self, *args):
        if self._browser:
            await self._browser.close()
        if self._playwright:
            await self._playwright.stop()

    async def fetch(self, source_name: str, url: str) -> PageSnapshot:
        if not self._browser:
            return PageSnapshot(
                source_name=source_name,
                url=url,
                raw_text="",
                status_code=0,
                blocked=True,
                evidence=None,
                error="Browser not initialized",
            )

        proxy_cfg = self.proxy_pool.next() if self.proxy_pool.enabled else None
        proxy_url = proxy_cfg.get("server") if proxy_cfg else None

        context = await self._browser.new_context(
            viewport={"width": 1280, "height": 800},
            user_agent=random.choice(USER_AGENTS),
            proxy=proxy_cfg,
        )
        page = await context.new_page()
        try:
            await self._stealth_cls().apply_stealth_async(page)
            print(f"      [Collector] {url}")
            response = await page.goto(url, wait_until="domcontentloaded", timeout=25000)
            status = response.status if response else 200

            await asyncio.sleep(2.5)
            await page.keyboard.press("Escape")
            await asyncio.sleep(0.3)

            for _ in range(3):
                await page.evaluate(f"window.scrollBy(0, {random.randint(350, 750)})")
                await asyncio.sleep(random.uniform(0.4, 1.2))

            raw_text = await page.evaluate("document.body.innerText") or ""
            html = ""
            try:
                html = (await page.content())[:80000]
            except Exception:
                pass
            blocked = is_blocked_text(raw_text) or status >= 400

            if blocked and proxy_url:
                self.proxy_pool.mark_dead(proxy_url)

            evidence = await save_screenshot(page, source_name, url)

            return PageSnapshot(
                source_name=source_name,
                url=url,
                raw_text=raw_text[:8000],
                status_code=200 if not blocked else status,
                blocked=blocked,
                evidence=evidence,
                html=html,
                proxy_used=proxy_url,
            )
        except Exception as e:
            if proxy_url:
                self.proxy_pool.mark_dead(proxy_url)
            print(f"      [Collector] Failed {source_name}: {e}")
            return PageSnapshot(
                source_name=source_name,
                url=url,
                raw_text="",
                status_code=0,
                blocked=True,
                evidence=None,
                error=str(e),
                proxy_used=proxy_url,
            )
        finally:
            await context.close()
            await asyncio.sleep(random.uniform(1.0, 2.0))
