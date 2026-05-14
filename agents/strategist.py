import json
import os
from typing import Dict, Any
from agents.base_agent import BaseAgent
from messaging.schemas import AgentMessage, AgentRole, MessageType
import google.generativeai as genai
from dotenv import load_dotenv

load_dotenv()

class StrategistAgent(BaseAgent):
    """
    Strategist Agent: Generates re-negotiation briefs and threat reports.
    Uses Groq Llama3 for high-quality creative synthesis.
    """
    def __init__(self, model: str = "llama-3.3-70b-versatile"):
        # Switched to Groq (Llama 3.3 70b) because Gemini Free Tier has very low rate limits (20 req/day)
        super().__init__(role=AgentRole.STRATEGIST, model=model, provider="groq")

    async def handle_request(self, message: AgentMessage) -> AgentMessage:
        analysis_data = message.payload.data.get("input", {})
        print(f"\n   [Strategist] Generating strategy for risk level: {analysis_data.get('risk_level')}")
        
        # 1. PLAN: Strategy Synthesis
        prompt = self._build_prompt(analysis_data)
        
        # 2. ACT: Call Claude-3 (3rd Provider Requirement)
        content, cost = await self._call_llm(prompt, self.model_name)
        
        # 3. OBSERVE: Parse and validate
        strategy = self._clean_json_response(content)
        
        # 4. DECIDE: Return typed response
        return self.create_response(message, strategy, cost=cost)

    def _build_prompt(self, data: Dict[str, Any]) -> str:
        return f"""
        Generate a strategic business recommendation based on this analysis: {json.dumps(data)}
        Return ONLY valid JSON:
        {{
            "recommendation": "string",
            "priority": "HIGH/MEDIUM/LOW",
            "negotiation_brief": "string",
            "threat_level": "string"
        }}
        """

    # Removed manual _generate_strategy in favor of base agent _call_llm

if __name__ == "__main__":
    import asyncio
    from messaging.schemas import Payload, MessageType

    async def test_run():
        print("\n--- [INDEPENDENT STRATEGIST DEMO] ---")
        agent = StrategistAgent()
        
        # Simulated Analyst Data
        test_payload = Payload(data={
            "input": {
                "risk_level": "HIGH",
                "gap_found": True,
                "competitor_rate": "12%",
                "grabon_rate": "5%",
                "reasoning": "Competitor is significantly beating us on Myntra coupons."
            }
        })
        
        test_msg = AgentMessage(
            message_id="strat_test_001",
            sender=AgentRole.ORCHESTRATOR,
            receiver=AgentRole.STRATEGIST,
            message_type=MessageType.REQUEST,
            payload=test_payload
        )
        
        response = await agent.handle_request(test_msg)
        print(f"\n[DEMO RESULT]\nRecommendation: {response.payload.data.get('recommendation')}")
        print(f"Negotiation Brief: {response.payload.data.get('negotiation_brief')}")

    asyncio.run(test_run())
