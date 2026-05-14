import asyncio
import json
import requests
from bs4 import BeautifulSoup
from typing import Dict, Any
from agents.base_agent import BaseAgent
from messaging.schemas import AgentMessage, AgentRole, MessageType
import google.generativeai as genai
import os
from dotenv import load_dotenv

load_dotenv()

class CrawlerAgent(BaseAgent):
    """
    Crawler Agent: Scrapes competitor data and cleans it using Groq.
    Satisfies Requirement: 'At least one step that does NOT use an LLM.'
    """
    def __init__(self, model: str = "llama-3.1-8b-instant"):
        # Switched to Groq (Llama 3.1 8b) for extremely fast and cheap data cleaning
        super().__init__(role=AgentRole.CRAWLER, model=model, provider="groq")
        api_key = os.getenv("GOOGLE_API_KEY")
        if api_key:
            genai.configure(api_key=api_key)
            self.model = genai.GenerativeModel("gemini-flash-latest")
        else:
            self.model = None

    async def handle_request(self, message: AgentMessage) -> AgentMessage:
        query = message.payload.data.get("input", "")
        print(f"\n   [Crawler] Scraping data for: {query}")
        
        # 1. PLAN: Deterministic Scraper (Non-LLM)
        raw_data = self._deterministic_scrape(query)
        
        # 2. ACT: LLM Data Cleaning (Gemini Flash)
        structured_data, cost = await self._act_with_retry(raw_data)
        
        # 3. OBSERVE & DECIDE
        return self.create_response(message, structured_data, cost=cost)

    async def _act_with_retry(self, raw_data: str):
        """ACT phase with Re-planning logic."""
        prompt = f"Clean this data into structured JSON: {raw_data}"
        content, cost = await self._call_llm(prompt, self.model_name)
        data = self._clean_json_response(content)
        
        if not data or "merchant" not in data:
            # RE-PLAN: Try a different approach/prompt
            print("[Crawler] RE-PLANNING: Data missing merchant. Retrying with explicit schema instruction...")
            prompt += "\nREQUIRED: JSON must contain 'merchant' and 'cashback_rate'."
            content2, cost2 = await self._call_llm(prompt, self.model_name)
            data = self._clean_json_response(content2)
            cost += cost2
            
        return data, cost

    def _deterministic_scrape(self, query: str) -> str:
        """
        Pure Python logic to fetch and parse HTML.
        Satisfies Requirement: 'Use 1-2 second delays, rotate User-Agents, handle blocks gracefully.'
        """
        import time
        import random
        
        # 1. Rotate User-Agents
        user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
        ]
        
        # 2. Implement required delay
        time.sleep(random.uniform(1.0, 2.0))
        
        # 3. Attempt Live Scrape
        headers = {"User-Agent": random.choice(user_agents)}
        target_url = f"https://www.google.com/search?q={query}+coupons+grabon" # Search as a proxy to avoid direct blocks
        
        try:
            print(f"[Crawler] Attempting live fetch: {target_url}")
            response = requests.get(target_url, headers=headers, timeout=10)
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')
                # Extract snippet text as 'raw noisy data'
                results = soup.find_all('div')[:5] 
                raw_text = " ".join([r.text for r in results])
                return json.dumps({"merchant": query, "raw_html_snippet": raw_text[:1000], "source": "Live Search"})
        except Exception as e:
            print(f"[Crawler] Live fetch blocked or failed: {e}. Falling back to Mock.")

        # 4. Graceful Fallback to Mock Data (Requirement: 'Handle blocks gracefully')
        mock_html = f"""
        <div class='merchant'>{query}</div>
        <div class='offers'>
            <p>10% Cashback on All Orders</p>
            <p>Flat 500 off on 2000+</p>
        </div>
        """
        soup = BeautifulSoup(mock_html, 'html.parser')
        merchant = soup.find('div', class_='merchant').text
        offers = [p.text for p in soup.find_all('p')]
        
        return json.dumps({"merchant": merchant, "offers": offers, "source": "Internal Fallback (Mock)"})

    # Removed manual _clean_with_llm in favor of base agent _call_llm

if __name__ == "__main__":
    import asyncio
    from messaging.schemas import Payload, MessageType

    async def test_run():
        print("\n--- [INDEPENDENT CRAWLER DEMO] ---")
        agent = CrawlerAgent()
        
        # Test a live crawl
        test_payload = Payload(data={"input": "Analyze Myntra coupons"})
        
        test_msg = AgentMessage(
            message_id="crawl_test_001",
            sender=AgentRole.ORCHESTRATOR,
            receiver=AgentRole.CRAWLER,
            message_type=MessageType.REQUEST,
            payload=test_payload
        )
        
        response = await agent.handle_request(test_msg)
        print(f"\n[DEMO RESULT]\nMerchant: {response.payload.data.get('merchant')}")
        print(f"Status: {response.payload.data.get('status')}")
        print(f"Extracted Source: {response.payload.data.get('source')}")

    asyncio.run(test_run())
