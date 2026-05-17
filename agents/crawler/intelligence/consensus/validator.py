"""Cross-source consensus validation — weighted trust across competitors."""
from __future__ import annotations

from statistics import median
from typing import Any, Dict, List

from agents.crawler.intelligence.change_detection import parse_rate_pct


class ConsensusValidator:
    def validate(self, sources: List[Dict[str, Any]]) -> Dict[str, Any]:
        weighted_rates: List[tuple] = []
        contradictions: List[Dict[str, Any]] = []

        for s in sources:
            if s.get("blocked"):
                continue
            rate = parse_rate_pct(s.get("cashback_rate"))
            if rate is None:
                continue
            weight = float(s.get("reliability_score") or 0.5)
            conf = float((s.get("extraction") or {}).get("confidence", 0.5))
            weight *= conf
            if s.get("requires_revalidation"):
                weight *= 0.3
            weighted_rates.append((rate, weight, s.get("source_name")))

        if not weighted_rates:
            return {
                "consensus_rate": None,
                "consensus_rate_label": None,
                "confidence": 0.0,
                "agreement_ratio": 0.0,
                "contradictions": [],
                "source_count": 0,
            }

        total_w = sum(w for _, w, _ in weighted_rates) or 1.0
        consensus = sum(r * w for r, w, _ in weighted_rates) / total_w
        rates_only = [r for r, _, _ in weighted_rates]
        med = median(rates_only)

        for r, w, name in weighted_rates:
            if abs(r - med) > max(3.0, med * 0.4):
                contradictions.append({
                    "source": name,
                    "rate_pct": r,
                    "median_pct": round(med, 2),
                    "deviation": round(abs(r - med), 2),
                })

        within_band = sum(1 for r in rates_only if abs(r - consensus) <= 2.0)
        agreement = within_band / len(rates_only)

        trust = min(0.95, 0.4 + 0.15 * len(weighted_rates) + 0.3 * agreement)
        if contradictions:
            trust *= max(0.5, 1.0 - 0.1 * len(contradictions))

        return {
            "consensus_rate": round(consensus, 2),
            "consensus_rate_label": f"{consensus:g}%",
            "confidence": round(trust, 3),
            "agreement_ratio": round(agreement, 3),
            "contradictions": contradictions,
            "source_count": len(weighted_rates),
            "median_rate_pct": round(med, 2),
        }
