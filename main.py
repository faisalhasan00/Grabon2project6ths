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


async def run_analyst_strategist_only(orchestrator: SwarmOrchestrator, crawler_data: dict) -> None:
    """Analyst → strategist → alert without re-crawling (crawl already done)."""
    analysis = await orchestrator._execute_agent(AgentRole.ANALYST, crawler_data)
    strategy = await orchestrator._execute_agent(AgentRole.STRATEGIST, analysis)
    resolved = await orchestrator._handle_conflicts(analysis, strategy)
    orchestrator._apply_crawl_feedback(crawler_data, analysis, resolved)
    if analysis.get("risk_level") == resolved.get("priority"):
        await orchestrator._execute_agent(AgentRole.ALERTER, resolved)


async def main():
    load_dotenv()

    state = SharedState()
    orchestrator = SwarmOrchestrator(state, budget_limit=float(os.getenv("MAX_BUDGET_USD", 10.0)))

    crawler = CrawlerAgent()
    orchestrator.register_agent(AgentRole.CRAWLER, crawler)
    orchestrator.register_agent(AgentRole.ANALYST, AnalystAgent())
    orchestrator.register_agent(AgentRole.STRATEGIST, StrategistAgent())
    orchestrator.register_agent(AgentRole.ALERTER, AlertAgent())

    watchlist = crawler.policy.active_watchlist()

    print("=" * 50)
    print("SWARM: ADAPTIVE AUTONOMOUS MARKET INTELLIGENCE")
    print("=" * 50)
    print(f"Watchlist: {watchlist}")
    print("Crawler self-decides due merchants, cadence, and pipeline depth.")
    print("Press Ctrl+C to stop.\n")

    iteration = 1
    BLUE = "\033[94m"
    BOLD = "\033[1m"
    RESET = "\033[0m"

    while True:
        print(f"{BLUE}{BOLD}--- [ITERATION {iteration}] {time.strftime('%H:%M:%S')} ---{RESET}")

        due = crawler.get_due_merchants(watchlist)
        if not due:
            print("   [Scheduler] No merchants due — sleeping.")
        else:
            skipped = [m for m in watchlist if m not in due]
            if skipped:
                print(f"   [Scheduler] Due (priority order): {due}")
                print(f"   [Scheduler] Waiting on interval: {skipped}")

        crawled = 0
        for merchant in due:
            slug = crawler.scheduler.slug_for_display_name(merchant)
            schedule = crawler.store.get_merchant_schedule(slug) or {}
            mode = schedule.get("crawl_mode", "normal")
            try:
                intel = await crawler.collect_intelligence(f"Analyze {merchant} coupons")
                crawled += 1
                events = intel.get("events") or []
                monitoring = intel.get("monitoring") or {}
                print(
                    f"   [Scheduler] {merchant}: mode={monitoring.get('crawl_mode', mode)} "
                    f"next in {monitoring.get('crawl_interval_sec')}s — {monitoring.get('monitor_reason', '')}"
                )

                if crawler.policy.should_run_full_pipeline(
                    events=events,
                    crawl_mode=monitoring.get("crawl_mode"),
                ):
                    print(f"   [Scheduler] Hot market → full pipeline for {merchant}")
                    await run_analyst_strategist_only(orchestrator, intel)
                else:
                    print(f"   [Scheduler] Stable → crawl-only for {merchant} (saving LLM budget)")

                if orchestrator.total_cost >= orchestrator.budget_limit:
                    print("🛑 [CRITICAL] Budget limit reached. Stopping service.")
                    return

            except Exception as e:
                print(f"⚠️ [ERROR] Failed for {merchant}: {e}")
                continue

        sleep_sec = crawler.policy.sleep_between_iterations(crawled)
        print(f"--- [ITERATION {iteration}] Done ({crawled} crawls). Sleep {sleep_sec}s ---\n")
        iteration += 1
        await asyncio.sleep(sleep_sec)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 Swarm service stopped by user.")
