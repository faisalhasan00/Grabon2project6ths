"""Confidence scoring for extractions."""
from __future__ import annotations

from typing import Any, Dict, List


BLOCK_SIGNALS = (
    "access denied",
    "403 forbidden",
    "captcha",
    "are you a human",
    "cloudflare",
    "verify you are human",
    "bot detection",
    "please enable javascript",
)


def is_blocked_text(text: str) -> bool:
    lower = (text or "").lower()
    return any(sig in lower for sig in BLOCK_SIGNALS)


def score_source(
    *,
    method: str,
    offers: List[Any],
    blocked: bool,
    raw_text_len: int,
) -> float:
    if blocked:
        return 0.12
    if raw_text_len < 80:
        return 0.25
    if method == "deterministic" and offers:
        return min(0.95, 0.78 + 0.04 * min(len(offers), 5))
    if method == "llm" and offers:
        return min(0.88, 0.65 + 0.05 * min(len(offers), 4))
    if offers:
        return 0.55
    return 0.35


def aggregate_confidence(sources: List[Dict[str, Any]]) -> float:
    if not sources:
        return 0.0
    scores = [s.get("extraction", {}).get("confidence", 0.0) for s in sources]
    return round(sum(scores) / len(scores), 3)
