"""Rotating proxy pool for anti-bot resilience."""
from __future__ import annotations

import os
import random
from typing import Dict, List, Optional


class ProxyPool:
    """Loads proxies from CRAWLER_PROXIES (comma-separated URLs)."""

    def __init__(self, proxies: Optional[List[str]] = None):
        raw = proxies
        if raw is None:
            env = os.getenv("CRAWLER_PROXIES", "")
            raw = [p.strip() for p in env.replace("\n", ",").split(",") if p.strip()]
        self._proxies = raw or []
        self._dead: set[str] = set()

    @property
    def enabled(self) -> bool:
        return len(self._proxies) > 0

    def mark_dead(self, proxy_url: Optional[str]) -> None:
        if proxy_url:
            self._dead.add(proxy_url)

    def next(self) -> Optional[Dict[str, str]]:
        alive = [p for p in self._proxies if p not in self._dead]
        if not alive:
            return None
        url = random.choice(alive)
        return {"server": url}
