import json
import os
import aiohttp
from typing import Dict, Any
from agents.base_agent import BaseAgent
from messaging.schemas import AgentMessage, AgentRole, MessageType
from dotenv import load_dotenv

load_dotenv()

class AlertAgent(BaseAgent):
    """
    Alert Agent: Formats notifications and sends alerts.
    Simulates using a local Ollama model (Mistral) or a lightweight backup.
    Satisfies Requirement: '4th provider (Local/Ollama)'.
    """
    def __init__(self):
        super().__init__(role=AgentRole.ALERTER)
        self.ollama_url = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")

    async def handle_request(self, message: AgentMessage) -> AgentMessage:
        strategy_data = message.payload.data.get("input", {})
        print(f"[Alert] Preparing notification for strategy...")
        
        # 1. PLAN: Notification Formatting
        prompt = f"Create a short Slack notification for this strategy: {json.dumps(strategy_data)}. Use emojis."
        
        # 2. ACT: Call Local Mistral (4th Provider Requirement)
        content, cost = await self._call_llm(prompt, "mistral")
        
        # 3. OBSERVE & DECIDE
        if "Error" in content or not content:
            # Consistent Fallback if local Ollama isn't running
            notification = {
                "alert_content": f"ACTION REQUIRED: {strategy_data.get('recommendation')}. Priority: {strategy_data.get('priority')}",
                "channel": "SLACK/MOCK"
            }
        else:
            notification = {"alert_content": content, "channel": "SLACK"}
            
        return self.create_response(message, notification, message_type=MessageType.APPROVAL, cost=cost)

    async def _format_with_local_llm(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Attempts to use local Ollama to format the Slack/Email message."""
        payload = {
            "model": "mistral",
            "prompt": f"Create a short Slack notification for this strategy: {json.dumps(data)}. Use emojis.",
            "stream": False
        }
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(f"{self.ollama_url}/api/generate", json=payload) as resp:
                    if resp.status == 200:
                        result = await resp.json()
                        return {"alert_content": result.get("response"), "channel": "SLACK"}
        except Exception as e:
            print(f"[Alert] Local LLM Error: {e}")
        
        # Consistent Fallback
        return {
            "alert_content": f"ACTION REQUIRED: {data.get('recommendation')}. Priority: {data.get('priority')}",
            "channel": "SLACK/MOCK"
        }
