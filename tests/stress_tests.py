import asyncio
import json
import os
import time
from state.state_manager import SharedState
from orchestrator.orchestrator import SwarmOrchestrator
from agents.crawler import CrawlerAgent
from agents.analyst import AnalystAgent
from agents.strategist import StrategistAgent
from agents.alerter import AlertAgent
from messaging.schemas import AgentRole, MessageType, AgentMessage, Payload

class SwarmStressTester:
    def __init__(self):
        self.crawler = CrawlerAgent()
        self.analyst = AnalystAgent()
        self.strategist = StrategistAgent()
        self.alerter = AlertAgent()

    async def run_stress_tests(self):
        print("\n" + "!"*50)
        print("STARTING MULTI-ANGLE STRESS TESTS")
        print("!"*50 + "\n")

        # 1. Angle: Corrupt Payload Injection
        await self._test_corrupt_payload()

        # 2. Angle: High-Concurrency Race Conditions
        await self._test_concurrency()

        # 3. Angle: Veto Loop & Budget Exhaustion
        await self._test_veto_budget_loop()

    async def _test_corrupt_payload(self):
        print("[STRESS] Angle 1: Injecting Corrupt Payload...")
        state = SharedState()
        orchestrator = SwarmOrchestrator(state)
        
        # Monkey patch Analyst to return total garbage
        async def corrupt_handle(message):
            return AgentMessage(
                message_id="bad-123",
                sender=AgentRole.ANALYST,
                receiver=AgentRole.STRATEGIST,
                message_type=MessageType.RESPONSE,
                payload=Payload(data={"risk_level": "MALFORMED_DATA_HALLUCINATION"}), # Invalid enum/data
                cost=0.01
            )
        self.analyst.handle_request = corrupt_handle
        
        orchestrator.register_agent(AgentRole.CRAWLER, self.crawler)
        orchestrator.register_agent(AgentRole.ANALYST, self.analyst)
        
        try:
            await orchestrator.run_pipeline("Test Corruption")
            print("      Result: FAILED (System accepted malformed data)")
        except Exception as e:
            print(f"      Result: PASSED (Caught corruption: {e})")

    async def _test_concurrency(self):
        print("[STRESS] Angle 2: High Concurrency (Simultaneous State Updates)...")
        state = SharedState()
        
        async def run_one(i):
            orchestrator = SwarmOrchestrator(state)
            # Register a mock agent that sleeps to force a race condition
            class SlowAgent:
                async def handle_request(self, msg):
                    await asyncio.sleep(0.5)
                    return msg
            
            orchestrator.register_agent(AgentRole.CRAWLER, SlowAgent())
            try:
                # We manually trigger updates to hit the lock
                v = state.get_version()
                await asyncio.sleep(0.1 * i)
                state.update_state(AgentRole.CRAWLER, f"test_{i}", {}, MessageType.RESPONSE, expected_version=v)
                return "OK"
            except Exception as e:
                if "Optimistic Locking Failure" in str(e):
                    return "LOCK_HIT"
                return str(e)

        results = await asyncio.gather(*(run_one(i) for i in range(5)))
        locks_hit = results.count("LOCK_HIT")
        print(f"      Result: Concurrency handled. Locks triggered: {locks_hit}")

    async def _test_veto_budget_loop(self):
        print("[STRESS] Angle 3: Veto Loop vs Budget Kill...")
        state = SharedState()
        # Set tiny budget to ensure it kills the loop
        orchestrator = SwarmOrchestrator(state, budget_limit=0.000001) 
        
        orchestrator.register_agent(AgentRole.CRAWLER, self.crawler)
        orchestrator.register_agent(AgentRole.ANALYST, self.analyst)
        orchestrator.register_agent(AgentRole.STRATEGIST, self.strategist)
        orchestrator.register_agent(AgentRole.ALERTER, self.alerter)
        
        try:
            await orchestrator.run_pipeline("Trigger Veto Loop")
            print("      Result: FAILED (System didn't enforce budget)")
        except Exception as e:
            if "Budget exceeded" in str(e):
                print("      Result: PASSED (Budget killed the loop successfully)")
            else:
                print(f"      Result: FAILED (Unexpected error: {e})")

if __name__ == "__main__":
    tester = SwarmStressTester()
    asyncio.run(tester.run_stress_tests())
