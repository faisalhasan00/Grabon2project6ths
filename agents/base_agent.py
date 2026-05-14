from abc import ABC, abstractmethod
from messaging.schemas import AgentMessage, AgentRole, MessageType, Payload
import uuid
import json
import re

class BaseAgent(ABC):
    """
    Abstract Base Class for all Swarm Agents.
    Ensures strict adherence to the messaging protocol.
    """
    def __init__(self, role: AgentRole, model: str = None, provider: str = None):
        self.role = role
        self.model_name = model
        self.provider = provider

    @abstractmethod
    async def handle_request(self, message: AgentMessage) -> AgentMessage:
        """Process an incoming request and return a typed response."""
        pass

    def create_response(self, request: AgentMessage, data: dict, message_type: MessageType = MessageType.RESPONSE, cost: float = 0.0) -> AgentMessage:
        """Helper to create a valid AgentMessage response."""
        return AgentMessage(
            message_id=str(uuid.uuid4()),
            sender=self.role,
            receiver=request.sender,
            message_type=message_type,
            payload=Payload(data=data),
            version=request.version,
            cost=cost
        )

    def calculate_cost(self, input_tokens: int, output_tokens: int, model_name: str) -> float:
        """
        Calculates cost for budget tracking.
        Satisfies Requirement: 'Track per-agent cost.'
        """
        # Pricing per 1M tokens (Approximate for free tier context)
        prices = {
            "gemini-flash-latest": {"in": 0.075, "out": 0.30},
            "gemini-1.5-flash": {"in": 0.07, "out": 0.21},
            "llama-3.3-70b-versatile": {"in": 0.59, "out": 0.79},
            "llama-3.1-8b-instant": {"in": 0.05, "out": 0.08},
            "gemini-1.5-pro": {"in": 3.50, "out": 10.50},
            "claude-3-haiku-20240307": {"in": 0.25, "out": 1.25},
            "claude-3-5-sonnet-20240620": {"in": 3.00, "out": 15.00},
            "mistral": {"in": 0.0, "out": 0.0} # Local Ollama
        }
        
        rates = prices.get(model_name, {"in": 0.0, "out": 0.0})
        cost = ((input_tokens / 1_000_000) * rates["in"]) + ((output_tokens / 1_000_000) * rates["out"])
        return round(cost, 6)

    async def _call_llm(self, prompt: str, model_name: str) -> str:
        """
        Standardized LLM call method with integrated cost tracking.
        Supports: Google, Groq, Anthropic, and Ollama.
        """
        import os
        import asyncio
        
        try:
            if "gemini" in model_name:
                import google.generativeai as genai
                genai.configure(api_key=os.getenv("GOOGLE_API_KEY"))
                model = genai.GenerativeModel(model_name)
                response = await asyncio.to_thread(model.generate_content, prompt)
                
                
                # Retry logic for rate limits
                for attempt in range(3):
                    try:
                        response = await asyncio.to_thread(model.generate_content, prompt)
                        in_tokens = len(prompt) // 4
                        out_tokens = len(response.text) // 4
                        cost = self.calculate_cost(in_tokens, out_tokens, model_name)
                        return response.text, cost
                    except Exception as e:
                        if "429" in str(e) or "quota" in str(e).lower():
                            await asyncio.sleep(2 ** (attempt + 1))
                            continue
                        raise e

            elif "llama" in model_name:
                from groq import Groq
                client = Groq(api_key=os.getenv("GROQ_API_KEY"))
                
                for attempt in range(3):
                    try:
                        response = await asyncio.to_thread(
                            client.chat.completions.create,
                            messages=[{"role": "user", "content": prompt}],
                            model=model_name
                        )
                        cost = self.calculate_cost(
                            response.usage.prompt_tokens, 
                            response.usage.completion_tokens, 
                            model_name
                        )
                        return response.choices[0].message.content, cost
                    except Exception as e:
                        if "429" in str(e) or "quota" in str(e).lower():
                            await asyncio.sleep(2 ** (attempt + 1))
                            continue
                        raise e

            elif "claude" in model_name:
                # If key is missing, return a mocked response as per rules
                api_key = os.getenv("ANTHROPIC_API_KEY")
                if not api_key:
                    return json.dumps({"status": "MOCKED", "content": "Simulated Claude-3 reasoning for strategy."}), 0.0
                
                import anthropic
                client = anthropic.Anthropic(api_key=api_key)
                response = await asyncio.to_thread(
                    client.messages.create,
                    model=model_name,
                    max_tokens=1024,
                    messages=[{"role": "user", "content": prompt}]
                )
                cost = self.calculate_cost(response.usage.input_tokens, response.usage.output_tokens, model_name)
                return response.content[0].text, cost

            elif "mistral" in model_name:
                import requests
                base_url = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
                response = await asyncio.to_thread(
                    requests.post,
                    f"{base_url}/api/generate",
                    json={"model": model_name, "prompt": prompt, "stream": False}
                )
                # Local is free
                return response.json().get("response", ""), 0.0

            return "Error: Unsupported model", 0.0
        except Exception as e:
            print(f"[{self.role}] LLM Error: {e}")
            return f"Error: {str(e)}", 0.0

    def _clean_json_response(self, text: str) -> dict:
        """
        Robustly extracts JSON from LLM responses even if surrounded by text or markdown.
        """
        try:
            # 1. Try to find JSON inside markdown code blocks
            json_match = re.search(r'```json\s*(.*?)\s*```', text, re.DOTALL)
            if json_match:
                return json.loads(json_match.group(1))
            
            # 2. Try to find anything between curly braces
            curly_match = re.search(r'(\{.*\})', text, re.DOTALL)
            if curly_match:
                return json.loads(curly_match.group(1))
            
            # 3. Last resort: Direct parse
            return json.loads(text)
        except Exception:
            print(f"   [AgentRole.{self.role}] Failed to parse JSON. Raw text: {text[:100]}...")
            return {}
