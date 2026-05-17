import json
import os
from typing import Dict, Any, List, Optional
from agents.base_agent import BaseAgent
from messaging.schemas import AgentMessage, AgentRole, MessageType
from groq import Groq
from dotenv import load_dotenv

load_dotenv()

HIGH_EVENTS = {
    "cashback_spike_detected",
    "high_cashback_observed",
    "campaign_started",
}
MEDIUM_EVENTS = {
    "new_offers_detected",
    "exclusive_offer_detected",
    "cashback_drop_detected",
}


class AnalystAgent(BaseAgent):
    """
    Analyst Agent: Identifies market gaps and computes defection risk.
    Consumes crawler intelligence events (Phase B).
    """
    def __init__(self, model: str = "llama-3.3-70b-versatile"):
        super().__init__(role=AgentRole.ANALYST, model=model, provider="groq")
        api_key = os.getenv("GROQ_API_KEY")
        if api_key:
            self.client = Groq(api_key=api_key)
        else:
            self.client = None

    async def handle_request(self, message: AgentMessage) -> AgentMessage:
        crawler_data = message.payload.data.get("input", {})
        events = crawler_data.get("events") or []
        print(f"\n   [Analyst] Analyzing {crawler_data.get('merchant')} | {len(events)} market events")

        event_hint = self._risk_hint_from_events(events, crawler_data)
        if event_hint:
            print(f"   [Analyst] Event-driven risk floor: {event_hint}")

        prompt = self._build_prompt(crawler_data, events, event_hint)

        content, cost = await self._call_llm(prompt, self.model_name)
        primary_data = self._clean_json_response(content)

        shadow_content, _ = await self._call_llm(prompt, "llama-3.1-8b-instant")
        shadow_data = self._clean_json_response(shadow_content)

        if primary_data.get("risk_level") != shadow_data.get("risk_level"):
            print(f"[Analyst] SHADOW_DELTA: Primary ({primary_data.get('risk_level')}) vs Shadow ({shadow_data.get('risk_level')})")
            primary_data["shadow_test"] = {
                "shadow_model": "llama-3.1-8b-instant",
                "shadow_risk": shadow_data.get("risk_level"),
                "match": False,
            }

        primary_data = self._apply_event_floor(primary_data, event_hint)
        primary_data["intelligence_events"] = events
        primary_data["aggregate_confidence"] = crawler_data.get("aggregate_confidence")
        primary_data["evidence_summary"] = self._evidence_summary(crawler_data)

        return self.create_response(message, primary_data, cost=cost)

    def _risk_hint_from_events(self, events: List[Dict], data: Dict[str, Any]) -> Optional[str]:
        types = {e.get("type") for e in events}
        if types & HIGH_EVENTS:
            return "HIGH"
        if types & MEDIUM_EVENTS:
            return "MEDIUM"
        conf = data.get("aggregate_confidence") or 0
        if conf < 0.4:
            return "LOW"
        rate = data.get("cashback_rate") or ""
        try:
            import re
            m = re.search(r"(\d+(?:\.\d+)?)", str(rate))
            if m:
                pct = float(m.group(1))
                if pct > 30:
                    return None
                if pct >= 10:
                    return "HIGH"
                if pct >= 5:
                    return "MEDIUM"
        except ValueError:
            pass
        return None

    def _apply_event_floor(self, analysis: Dict[str, Any], hint: Optional[str]) -> Dict[str, Any]:
        if not hint:
            return analysis
        order = {"LOW": 0, "MEDIUM": 1, "HIGH": 2}
        current = analysis.get("risk_level", "LOW").upper()
        if order.get(hint, 0) > order.get(current, 0):
            analysis["risk_level"] = hint
            analysis["reasoning"] = (
                (analysis.get("reasoning") or "")
                + f" [Elevated to {hint} due to market intelligence events.]"
            ).strip()
        analysis["event_risk_floor"] = hint
        return analysis

    def _evidence_summary(self, data: Dict[str, Any]) -> Dict[str, Any]:
        sources = data.get("sources") or []
        with_evidence = sum(1 for s in sources if s.get("evidence"))
        return {
            "sources_count": len(sources),
            "sources_with_screenshot": with_evidence,
            "best_cashback": data.get("cashback_rate"),
            "monitoring": data.get("monitoring"),
        }

    def _build_prompt(self, data: Dict[str, Any], events: List[Dict], event_hint: Optional[str]) -> str:
        events_blob = json.dumps(events[:15], indent=2) if events else "[]"
        hint_line = f"Minimum risk consideration from events: {event_hint}" if event_hint else ""
        return f"""
        Analyze competitor intelligence for the client platform (coupon/cashback).

        Merchant payload (live validated crawl, not demo): {json.dumps({k: data.get(k) for k in (
            "merchant", "cashback_rate", "client_rate", "offers", "aggregate_confidence",
            "summary", "rate_validation", "data_source"
        ) if data.get(k) is not None})}

        Market events from crawler (treat as ground signals): {events_blob}

        {hint_line}

        Rules:
        - If cashback_spike_detected or high_cashback_observed → risk at least HIGH.
        - If campaign_started or many new offers → risk at least MEDIUM.
        - competitor_rate must come from validated cashback in payload (ignore rates above 30%).
        - If cashback_rate is null, say insufficient validated data — do not guess 80%+.
        - Do not invent offers not present in payload.

        Return ONLY valid JSON:
        {{
            "risk_level": "HIGH/MEDIUM/LOW",
            "gap_found": true,
            "competitor_rate": "string",
            "client_rate": "string (client baseline rate from payload)",
            "reasoning": "string referencing events if relevant"
        }}
        """
