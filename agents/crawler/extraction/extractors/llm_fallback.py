"""Semantic extraction when deterministic parsing is insufficient."""
from __future__ import annotations

import json
from typing import Any, Dict, Tuple

from agents.crawler.extraction.confidence import score_source


LLM_MODEL = "gemini-flash-latest"
MIN_OFFERS_FOR_SKIP = 2


async def maybe_llm_extract(
    agent,
    *,
    raw_text: str,
    url: str,
    merchant: str,
    source_name: str,
    deterministic_result: Dict[str, Any],
) -> Tuple[Dict[str, Any], float]:
    """
    Call LLM only when deterministic extraction is weak.
    Returns (merged_result, llm_cost).
    """
    offers = deterministic_result.get("offers") or []
    blocked = deterministic_result.get("blocked", False)
    det_conf = deterministic_result.get("extraction", {}).get("confidence", 0)

    if blocked or (len(offers) >= MIN_OFFERS_FOR_SKIP and det_conf >= 0.75):
        return deterministic_result, 0.0

    snippet = raw_text[:6000] if raw_text else ""
    prompt = f"""
Extract deals from this scraped page text for merchant "{merchant}" on source "{source_name}".
URL: {url}
DO NOT invent data. If blocked or empty, return empty offers.

Text:
{snippet}

Return ONLY valid JSON:
{{
  "merchant": "{merchant}",
  "cashback_rate": "string or null",
  "offers": ["[Type] description strings"]
}}
"""
    content, cost = await agent._call_llm(prompt, LLM_MODEL)
    data = agent._clean_json_response(content)
    if not data or "offers" not in data:
        return deterministic_result, cost

    llm_offers = data.get("offers") or []
    merged_offers = list(dict.fromkeys(offers + llm_offers))[:25]
    cashback = data.get("cashback_rate") or deterministic_result.get("cashback_rate")

    merged = {
        **deterministic_result,
        "merchant": merchant,
        "source_name": source_name,
        "cashback_rate": cashback,
        "offers": merged_offers,
        "extraction": {
            "method": "llm",
            "confidence": score_source(
                method="llm",
                offers=merged_offers,
                blocked=False,
                raw_text_len=len(snippet),
            ),
        },
    }
    return merged, cost
