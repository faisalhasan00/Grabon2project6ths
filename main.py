import asyncio
import os
import time
from dotenv import load_dotenv
from state.state_manager import SharedState
from orchestrator.orchestrator import SwarmOrchestrator
from agents.crawler import CrawlerAgent
from agents.analyst import AnalystAgent
from agents.strategist import StrategistAgent
from agents.alerter import AlertAgent
from messaging.schemas import AgentRole

async def main():
    load_dotenv()
    
    # Initialize State & Orchestrator
    state = SharedState()
    orchestrator = SwarmOrchestrator(state, budget_limit=float(os.getenv("MAX_BUDGET_USD", 10.0)))
    
    # Register Agents
    orchestrator.register_agent(AgentRole.CRAWLER, CrawlerAgent())
    orchestrator.register_agent(AgentRole.ANALYST, AnalystAgent())
    orchestrator.register_agent(AgentRole.STRATEGIST, StrategistAgent())
    orchestrator.register_agent(AgentRole.ALERTER, AlertAgent())

    merchants = ["Myntra", "Ajio", "Amazon", "Nykaa", "Flipkart"]
    
    print("="*50)
    print("SWARM: 24/7 COMPETITIVE INTELLIGENCE SERVICE")
    print("="*50)
    print(f"Monitoring {len(merchants)} merchants. Press Ctrl+C to stop.\n")

    iteration = 1
    while True:
        print(f"--- [ITERATION {iteration}] Start Time: {time.strftime('%H:%M:%S')} ---")
        
        for merchant in merchants:
            try:
                # Run the pipeline for each merchant
                await orchestrator.run_pipeline(f"Analyze {merchant} coupons")
                
                # Check budget between runs
                if orchestrator.total_cost >= orchestrator.budget_limit:
                    print("🛑 [CRITICAL] Budget limit reached. Stopping service.")
                    return
                    
            except Exception as e:
                print(f"⚠️ [ERROR] Pipeline failed for {merchant}: {e}")
                continue
        
        print(f"--- [ITERATION {iteration}] Complete. Sleeping for 60s... ---\n")
        iteration += 1
        await asyncio.sleep(60)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 Swarm service stopped by user.")
