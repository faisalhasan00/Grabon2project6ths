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
    Uses Gemini 1.5 Pro for high-quality creative synthesis.
    """
    def __init__(self):
        super().__init__(role=AgentRole.STRATEGIST)
        api_key = os.getenv("GOOGLE_API_KEY")
        if api_key:
            genai.configure(api_key=api_key)
            self.model = genai.GenerativeModel("gemini-flash-latest")
        else:
            self.model = None

    async def handle_request(self, message: AgentMessage) -> AgentMessage:
        analysis_data = message.payload.data.get("input", {})
        print(f"[Strategist] Generating strategy for risk level: {analysis_data.get('risk_level')}")
        
        # 1. PLAN: Strategy Synthesis
        prompt = self._build_prompt(analysis_data)
        
        # 2. ACT: Call Claude-3 (3rd Provider Requirement)
        content, cost = await self._call_llm(prompt, "claude-3-haiku-20240307")
        
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
