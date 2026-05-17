"""Advanced anomaly detection — historical baselines vs current observations."""
from __future__ import annotations

import os
from statistics import mean, median
from typing import Any, Dict, List, Optional

from agents.crawler.platform.categories import category_profile
from agents.crawler.intelligence.change_detection import parse_rate_pct
from agents.crawler.platform.store import IntelligenceStore

ANOMALY_Z_THRESHOLD = float(os.getenv("CRAWLER_ANOMALY_Z_THRESHOLD", "2.5"))
ANOMALY_HARD_CAP_RATIO = float(os.getenv("CRAWLER_ANOMALY_HARD_RATIO", "3.0"))


class AnomalyEngine:
    def __init__(self, store: IntelligenceStore):
        self.store = store

    def analyze(
        self,
        merchant_slug: str,
        sources: List[Dict[str, Any]],
        *,
        consensus_rate: Optional[float] = None,
    ) -> Dict[str, Any]:
        history = self.store.get_rate_history(merchant_slug, limit=40)
        rates_hist = [h["rate_pct"] for h in history if h.get("rate_pct") is not None]
        profile = category_profile(merchant_slug)

        current_rates: List[float] = []
        for s in sources:
            r = parse_rate_pct(s.get("cashback_rate"))
            if r is not None:
                current_rates.append(r)
        if consensus_rate is not None:
            current_rates.append(consensus_rate)

        result: Dict[str, Any] = {
            "anomalies": [],
            "confidence_multiplier": 1.0,
            "requires_revalidation": False,
            "escalated": False,
        }
        if not current_rates:
            return result

        current = max(current_rates)
        self.store.record_rate_sample(merchant_slug, None, current)

        if len(rates_hist) < 3:
            if current > profile.max_cashback_typical * 2:
                result["anomalies"].append(self._build_anomaly(
                    merchant_slug, current, mean([profile.max_cashback_typical]),
                    reason="exceeds_category_typical_x2", severity="high",
                ))
            return self._finalize(result, sources)

        hist_avg = mean(rates_hist)
        hist_med = median(rates_hist)
        baseline = hist_med if hist_med else hist_avg
        std = (sum((x - hist_avg) ** 2 for x in rates_hist) / len(rates_hist)) ** 0.5
        z = (current - hist_avg) / std if std > 0.01 else 0.0

        if current >= baseline * ANOMALY_HARD_CAP_RATIO or current >= 40:
            result["anomalies"].append(self._build_anomaly(
                merchant_slug, current, baseline,
                reason="hard_cap_suspect_noise", severity="critical", z_score=z,
            ))
            result["confidence_multiplier"] = 0.25
            result["requires_revalidation"] = True
            result["escalated"] = True
        elif z >= ANOMALY_Z_THRESHOLD or current >= baseline * 2:
            result["anomalies"].append(self._build_anomaly(
                merchant_slug, current, baseline,
                reason="statistical_spike", severity="high", z_score=z,
            ))
            result["confidence_multiplier"] = 0.5
            result["requires_revalidation"] = True
            result["escalated"] = current >= baseline * 2.5

        return self._finalize(result, sources)

    def _build_anomaly(
        self,
        merchant_slug: str,
        current: float,
        baseline: float,
        *,
        reason: str,
        severity: str,
        z_score: float = 0.0,
    ) -> Dict[str, Any]:
        return {
            "type": "rate_anomaly_detected",
            "merchant": merchant_slug,
            "current_pct": current,
            "historical_baseline_pct": round(baseline, 2),
            "delta_pct": round(current - baseline, 2),
            "z_score": round(z_score, 2),
            "reason": reason,
            "severity": severity,
            "action": "downgrade_confidence_and_revalidate",
        }

    def _finalize(self, result: Dict[str, Any], sources: List[Dict[str, Any]]) -> Dict[str, Any]:
        mult = result["confidence_multiplier"]
        if mult < 1.0:
            for s in sources:
                ext = s.get("extraction") or {}
                conf = ext.get("confidence", 0.5) * mult
                ext["confidence"] = round(conf, 3)
                ext["anomaly_adjusted"] = True
                s["extraction"] = ext
                if result["requires_revalidation"]:
                    s["requires_revalidation"] = True
        return result
