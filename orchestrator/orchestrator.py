import os
import asyncio
import time
import uuid
import json
from typing import Dict, Any, List, Optional
from loguru import logger
from messaging.schemas import AgentMessage, AgentRole, MessageType, Payload
from state.state_manager import SharedState

# Configure Loguru for JSON timeline observability
logger.add("logs/swarm_timeline.json", format="{message}", level="INFO", serialize=True)

class SwarmOrchestrator:
    """
    The control plane for the Swarm.
    Enforces execution order, handles timeouts, budget, and conflict resolution.
    """
    def __init__(self, state: SharedState, budget_limit: Optional[float] = None):
        self.state = state
        # Read from env or use default
        env_budget = os.getenv("MAX_BUDGET_USD")
        self.budget_limit = budget_limit or (float(env_budget) if env_budget else 0.50)
        self.total_cost = 0.0
        self.agents = {}
        self.critical_path = []

    def register_agent(self, role: AgentRole, agent_instance):
        self.agents[role] = agent_instance

    async def run_pipeline(self, initial_query: str):
        # ANSI Color Codes
        CYAN = "\033[96m"
        YELLOW = "\033[93m"
        GREEN = "\033[92m"
        MAGENTA = "\033[95m"
        BLUE = "\033[94m"
        BOLD = "\033[1m"
        RESET = "\033[0m"

        try:
            print(f"\n{BLUE}{BOLD}=================================================={RESET}")
            print(f"{BLUE}{BOLD}[PIPELINE START] Target: {initial_query}{RESET}")
            print(f"{BLUE}{BOLD}=================================================={RESET}\n")
            
            # 1. Crawler Stage
            print(f"{CYAN}{BOLD}[PHASE: CRAWLER]{RESET} Searching for live merchant deals...")
            crawler_data = await self._execute_agent(AgentRole.CRAWLER, initial_query)
            print("") # Vertical spacing

            events = crawler_data.get("events") or []
            if events:
                self._log_event("MARKET_EVENTS", f"{len(events)} intelligence events", {"events": events})
            
            # 1.5 Validation Check
            if not crawler_data or "merchant" not in crawler_data:
                print(f"{YELLOW}[VETO] Analyst rejected noisy output. Retrying...{RESET}")
                self._log_event("DATA_VETO", "Analyst rejected noisy crawler output.", level="WARNING")
                crawler_data = await self._execute_agent(AgentRole.CRAWLER, f"REVISE: Last extraction was noisy. Focus on {initial_query}")
                print("")
            
            # 2. Analyst Stage
            print(f"{YELLOW}{BOLD}[PHASE: ANALYST]{RESET} Computing market gaps and defection risk...")
            analysis_data = await self._execute_agent(AgentRole.ANALYST, crawler_data)
            print("")
            
            # 3. Strategist Stage
            print(f"{GREEN}{BOLD}[PHASE: STRATEGIST]{RESET} Synthesizing negotiation briefs...")
            strategy_data = await self._execute_agent(AgentRole.STRATEGIST, analysis_data)
            print("")
            
            # 4. Conflict Resolution
            print(f"{BLUE}{BOLD}[PHASE: RESOLVE]{RESET} Checking for strategy conflicts...")
            resolved_strategy = await self._handle_conflicts(analysis_data, strategy_data)
            print("")
            self._apply_crawl_feedback(crawler_data, analysis_data, resolved_strategy)
            
            # 5. Agreement Check & Alert Stage
            if analysis_data.get("risk_level") == resolved_strategy.get("priority"):
                print(f"{MAGENTA}{BOLD}[PHASE: ALERTER]{RESET} Dispatching notification to Slack...")
                await self._execute_agent(AgentRole.ALERTER, resolved_strategy)
            else:
                print(f"{MAGENTA}! [ALERT SKIPPED] Severity Mismatch: Risk ({analysis_data.get('risk_level')}) vs Priority ({resolved_strategy.get('priority')}){RESET}")
                self._log_event("ALERT_SKIPPED", "Analyst and Strategist disagreed on severity. Alert withheld.", 
                               {"risk": analysis_data.get("risk_level"), "priority": resolved_strategy.get("priority")})
            
            print(f"\n{BLUE}{BOLD}[PIPELINE COMPLETE] Cost: ${self.total_cost:.4f} | Ver: {self.state.get_version()}{RESET}\n")
            self._log_event("PIPELINE_COMPLETE", f"Pipeline finished. Total Cost: ${self.total_cost:.4f}")
            
        except Exception as e:
            self._log_event("PIPELINE_ERROR", str(e), level="ERROR")
            print(f"[FATAL ERROR] {e}")
            raise e

    async def _execute_agent(self, role: AgentRole, input_data: Any):
        if role not in self.agents:
            raise Exception(f"Agent {role} not registered.")
        
        # Budget Check
        if self.total_cost >= self.budget_limit:
            self._log_event("BUDGET_EXCEEDED", f"Current cost ${self.total_cost} exceeds limit ${self.budget_limit}")
            raise Exception("Budget exceeded")

        agent = self.agents[role]
        request = AgentMessage(
            message_id=str(uuid.uuid4()),
            sender=AgentRole.ORCHESTRATOR,
            receiver=role,
            message_type=MessageType.REQUEST,
            payload=Payload(data={"input": input_data})
        )
        
        self._log_event("PLAN", f"Planning execution for {role.value}", {"input": input_data})
        self._log_event("AGENT_START", f"Activating {role.value}", {"input": input_data})
        
        # Timeout Enforcement (stall > 60s)
        try:
            start_time = time.time()
            response = await asyncio.wait_for(agent.handle_request(request), timeout=60.0)
            latency = time.time() - start_time
            
            # Update state & cost with optimistic locking
            agent_cost = getattr(response, 'cost', 0.0)
            self.total_cost += agent_cost
            
            # The agent's work was based on the version before it started
            current_version = self.state.get_version()
            self.state.update_state(
                agent=role, 
                key=f"{role.value}_output", 
                value=response.payload.data, 
                message_type=MessageType.RESPONSE,
                expected_version=current_version
            )
            
            self.critical_path.append({"agent": role.value, "latency": latency, "cost": agent_cost})
            self._log_event("OBSERVE", f"Observed output from {role.value}", {"output": response.payload.data})
            self._log_event("DECIDE", f"Committing decision for {role.value} to state", {"version": self.state.get_version()})
            self._log_event("AGENT_SUCCESS", f"{role.value} completed", {"output": response.payload.data, "cost": agent_cost})
            
            return response.payload.data
        except asyncio.TimeoutError:
            self._log_event("AGENT_TIMEOUT", f"{role.value} stalled for >60s", level="WARNING")
            raise Exception(f"Agent {role.value} timed out.")

    async def _handle_conflicts(self, analysis: Dict, strategy: Dict) -> Dict:
        """
        Implements Priority Hierarchy and Evidence-Based Resolution.
        Requirement: 'Last write wins' is not resolution.
        """
        risk_level = analysis.get("risk_level", "LOW").upper()
        priority = strategy.get("priority", "LOW").upper()
        
        # Scenario: Analyst says HIGH risk, but Strategist says LOW priority
        if risk_level == "HIGH" and priority == "LOW":
            self._log_event("CONFLICT_DETECTED", "Analyst (HIGH risk) vs Strategist (LOW priority)")
            
            # RESOLUTION: Risk-First Priority
            strategy["priority"] = "HIGH"
            strategy["recommendation"] += " (REVISED: Priority elevated due to Analyst High Risk flag)"
            
            resolution_data = {
                "conflict": "Risk vs Priority Mismatch",
                "resolution": "Analyst risk assessment overruled Strategist priority.",
                "reasoning": f"Evidence: Competitor has {analysis.get('competitor_rate')} vs client {analysis.get('client_rate')}. Risk Tier: {risk_level}.",
                "version": self.state.get_version()
            }
            self.state.update_state(
                agent=AgentRole.ORCHESTRATOR, 
                key="conflicts", 
                value=resolution_data, 
                message_type=MessageType.ESCALATION,
                expected_version=self.state.get_version()
            )
            self._log_event("CONFLICT_RESOLVED", "Risk-First Priority Applied", resolution_data)
            
        return strategy

    def _apply_crawl_feedback(
        self,
        crawler_data: Dict[str, Any],
        analysis_data: Dict[str, Any],
        strategy_data: Optional[Dict[str, Any]],
    ) -> None:
        """Feed analyst/strategist severity back into autonomous crawl scheduling."""
        crawler = self.agents.get(AgentRole.CRAWLER)
        if not crawler or not hasattr(crawler, "apply_pipeline_feedback"):
            return
        slug = crawler_data.get("merchant_slug") or crawler_data.get("query")
        if not slug and crawler_data.get("merchant"):
            from agents.crawler.crawl.profiles import normalize_query
            slug = normalize_query(str(crawler_data["merchant"]))
        if not slug:
            return
        priority = strategy_data.get("priority") if strategy_data else None
        decision = crawler.apply_pipeline_feedback(
            str(slug),
            analyst_risk=analysis_data.get("risk_level"),
            strategist_priority=priority,
        )
        if decision:
            print(
                f"   [Scheduler] Autonomous boost → {slug}: "
                f"{decision.mode} every {decision.interval_sec}s ({decision.reason})"
            )

    def _log_event(self, event_type: str, message: str, data: Optional[Dict] = None, level: str = "INFO"):
        log_entry = {
            "timestamp": time.time(),
            "event": event_type,
            "message": message,
            "metadata": data or {},
            "cumulative_cost": self.total_cost,
            "active_version": self.state.get_version()
        }
        if level == "INFO":
            logger.info(json.dumps(log_entry))
        elif level == "WARNING":
            logger.warning(json.dumps(log_entry))
        else:
            logger.error(json.dumps(log_entry))
