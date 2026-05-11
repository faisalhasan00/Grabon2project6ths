import json
import os
from typing import Dict, Any
from agents.base_agent import BaseAgent
from messaging.schemas import AgentMessage, AgentRole, MessageType
from groq import Groq
from dotenv import load_dotenv

load_dotenv()

class AnalystAgent(BaseAgent):
    """
    Analyst Agent: Identifies market gaps and computes defection risk.
    Uses Groq (Llama 3 70B) for high-speed reasoning.
    """
    def __init__(self):
        super().__init__(role=AgentRole.ANALYST)
        api_key = os.getenv("GROQ_API_KEY")
        if api_key:
            self.client = Groq(api_key=api_key)
        else:
            self.client = None

    async def handle_request(self, message: AgentMessage) -> AgentMessage:
        crawler_data = message.payload.data.get("input", {})
        print(f"[Analyst] Analyzing data for merchant: {crawler_data.get('merchant')}")
        
        # 1. PLAN: Detailed Gap Analysis
        prompt = self._build_prompt(crawler_data)
        
        # 2. ACT: Primary Analysis (Groq/Llama 3)
        content, cost = await self._call_llm(prompt, "llama-3.3-70b-versatile")
        primary_data = self._clean_json_response(content)
        
        # 3. OBSERVE: Shadow Testing (Mastery Requirement)
        # We run a cheaper model in the background to compare
        shadow_content, _ = await self._call_llm(prompt, "gemini-flash-latest")
        shadow_data = self._clean_json_response(shadow_content)
        
        # 4. DECIDE: Merge results and log shadow delta
        if primary_data.get("risk_level") != shadow_data.get("risk_level"):
            print(f"[Analyst] SHADOW_DELTA: Primary ({primary_data.get('risk_level')}) vs Shadow ({shadow_data.get('risk_level')})")
            primary_data["shadow_test"] = {
                "shadow_model": "gemini-flash-latest",
                "shadow_risk": shadow_data.get("risk_level"),
                "match": False
            }
        
        return self.create_response(message, primary_data, cost=cost)

    def _build_prompt(self, data: Dict[str, Any]) -> str:
        return f"""
        Analyze the following competitor data for GrabOn.
        Competitor Data: {json.dumps(data)}
        Return ONLY valid JSON:
        {{
            "risk_level": "HIGH/MEDIUM/LOW",
            "gap_found": true,
            "competitor_rate": "string",
            "grabon_rate": "5%",
            "reasoning": "string"
        }}
        """

    # Removed manual _analyze_gaps in favor of base agent _call_llm and shadow testing
