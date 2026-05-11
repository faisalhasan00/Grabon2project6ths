# Swarm: Multi-Agent Competitive Intelligence System

## Overview
Swarm is a production-style multi-agent orchestration system built for the GrabOn AI Labs Engineering Challenge. It automates the competitive intelligence pipeline by scraping deal data, analyzing market gaps, and synthesizing strategic business recommendations.

### Why I Chose This Assignment
I chose the **Competitive Intelligence Swarm** because it represents the "Hard" tier of the challenge, requiring sophisticated state management (Optimistic Locking) and a robust Orchestrator (Control Plane). It allows me to demonstrate my ability to build systems that are not just "agentic" but also **enterprise-ready, budget-aware, and deterministic**.

### Key Features
- **Mastery Architecture**: Explicit **Plan/Act/Observe/Decide** loop phases.
- **Versioned Shared State**: Centralized state manager with full audit trails and true **Optimistic Locking**.
- **Multi-LLM & Shadow Testing**: Orchestrates **4 providers** (Gemini, Llama 3/Groq, Claude, Mistral). Includes **Shadow Testing** (background model comparison).
- **Production Guardrails**: Real-time **USD Budget Enforcement**, 60s Stall Protection, and **Re-planning** on agent failure.
- **Observability**: Structured JSON timeline logs with explicit loop phase attribution.

## 🔌 Live vs Mocked Status
| Module | Provider | Status | Reason |
| :--- | :--- | :--- | :--- |
| **Crawler** | Google (Gemini Flash) | **LIVE** | Live parsing of merchant data. |
| **Analyst** | Groq (Llama 3) | **LIVE** | High-speed gap analysis. |
| **Strategist** | Google (Gemini Flash) | **LIVE** | Strategy synthesis. |
| **Alerter** | Python (Mock) | **MOCKED** | Real architecture implemented, but outputs to logs to avoid requiring Slack API tokens. |
| **Web Scraping**| BeautifulSoup4 | **LIVE** | Real fetch with **Rotating User-Agents** and **1-2s Delays**. Graceful fallback implemented for anti-bot blocks. |

## Architecture Diagram
![Architecture Diagram](docs/images/architecture.png)

### 🎯 What is happening here?
1.  **The Orchestrator**: This is the "Manager." It makes sure every agent stays on budget and finishes on time.
2.  **The Crawler**: Like a researcher, it visits competitor sites to see what deals they have right now.
3.  **The Analyst**: Like a data scientist, it looks for "Gaps" where our competitors are beating us.
4.  **The Strategist**: Like a CEO, it writes a strategy to win back those customers.
5.  **The Alerter**: Like a messenger, it delivers the final report directly to your team.

## Per-Module Design Decisions & Tradeoffs
1.  **Orchestrator (Control Plane)**:
    *   *Decision*: Chose a centralized hub-and-spoke instead of a fully decentralized swarm.
    *   *Tradeoff*: Simpler to enforce budget and timeouts, but creates a single point of failure.
2.  **State Manager (Optimistic Locking)**:
    *   *Decision*: Implemented version vectors for every key update.
    *   *Tradeoff*: Prevents race conditions but requires agents/orchestrator to fetch the latest version before writing.
3.  **Crawler Agent (Hybrid Scraping)**:
    *   *Decision*: Use BeautifulSoup for raw HTML extraction followed by LLM-based structured JSON parsing.
    *   *Tradeoff*: Much cheaper and more reliable than passing raw HTML to expensive LLMs.
4.  **Analyst/Strategist (Messaging)**:
    *   *Decision*: Enforced strict Pydantic `Payload` schemas.
    *   *Tradeoff*: High reliability and deterministic parsing, but makes the system less flexible for unstructured data without schema changes.

## What Broke First
The biggest challenge was handling **Pydantic Model Strictness** during budget tracking. Initially, I attempted to dynamically inject cost data into validated messages, which triggered validation errors. I resolved this by refactoring the `AgentMessage` schema to include a native `cost` field, ensuring the budget logic was first-class and typed.

## How to Run
1. **Install Dependencies**:
   ```bash
   pip install -r requirements.txt
   ```
2. **Configure Environment**:
   Create a `.env` file with the following:
   ```env
   GOOGLE_API_KEY=your_gemini_key
   GROQ_API_KEY=your_groq_key
   MAX_BUDGET_USD=0.50
   ```
3. **Run Scenarios**:
   ```bash
   $env:PYTHONPATH = ".;$env:PYTHONPATH"; python tests/run_scenarios.py
   ```
4. **Start 24/7 Service**:
   ```bash
   python main.py
   ```
5. **View Logs**:
   Check `logs/swarm_timeline.json` for the full execution trace.

## (e) Evaluation Results
Based on the raw report in `reports/eval_report.json`:

| Metric | Result | Note |
| :--- | :--- | :--- |
| **Pass Rate** | 100% | System recovered from all failures and noisy data. |
| **Avg Accuracy** | 94% | Gap detection correctly identified risks in test scenarios. |
| **Avg Latency** | 10.1s | End-to-end orchestration across 4 agents. |
| **Avg Cost** | $0.0004 | Highly optimized using Groq Llama 3.1/3.3 models. |

## (f) 'What Broke First' (Hardest Bug)
The most significant challenge was **Free-Tier Rate Limiting (429 Errors)** and **Model Decommissioning**. 
*   **The Issue**: The 24/7 monitoring loop quickly exhausted the Gemini Flash free quota (20 req/day). Simultaneously, Groq decommissioned the specific Llama 3 models used during initial development, causing a `400 Invalid Request` error across the entire swarm.
*   **The Fix**: I implemented a **Multi-Provider Fallback** system and upgraded the `BaseAgent` with **Exponential Backoff**. I also transitioned high-frequency tasks to the newer **Llama 3.3 70B** and **Llama 3.1 8B** models on Groq. This ensures the service remains stable even if one provider or model becomes unavailable.

## (g) What I would change with 2 more weeks
1.  **Distributed State Manager**: Replace the in-memory Python `SharedState` with **Redis** to allow horizontal scaling of agents across multiple servers.
2.  **Multimodal Vision Agents**: Integrate **GPT-4o or Gemini Pro Vision** to capture screenshots of competitor coupon pages, ensuring accuracy even when HTML is obfuscated.
3.  **Human-in-the-Loop Dashboard**: Create a Next.js dashboard where GrabOn category managers can "Approve" or "Veto" the Strategist's ROI recommendations before they are executed.

## Cost Data
*   **Total Development Cost**: ~$0.15 USD (Estimated across 500+ test runs).
*   **One Full Agent Run**: **$0.0004** (Extremely cost-efficient for production).
*   **One Full Eval Suite Run (3 Scenarios)**: **$0.0012**.
*   **ROI Analysis**: Compared to a human analyst costing ~$25/hour, this system provides a **99.9% cost reduction** while operating 24/7.
