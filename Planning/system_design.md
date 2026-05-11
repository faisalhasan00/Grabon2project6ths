# System Design: The Swarm Architecture

## High-Level Overview
The system follows an **Orchestrated Swarm** pattern. Unlike autonomous agents that loop indefinitely, this system uses a central **Orchestrator** to enforce budgets, timeouts, and structured state transitions.

## Architecture Diagram
```mermaid
graph TD
    User((User Input)) --> Orch[Orchestrator]
    Orch --> State[(Shared State Store<br/>Versioned)]
    
    subgraph Agents
        C[Crawler Agent<br/>Gemini Flash + BS4]
        A[Analyst Agent<br/>Groq/Llama3]
        S[Strategist Agent<br/>Gemini Pro]
    end
    
    Orch --> C
    C --> State
    Orch --> A
    A --> State
    
    A -- "Conflict Check" --> CR{Conflict Resolver}
    S -- "Conflict Check" --> CR
    
    CR -- "Resolution" --> Orch
    Orch --> S
    S --> State
    
    Orch --> Alert[Alert Agent<br/>Ollama/Local]
    
    subgraph Observability
        Log[JSON Timeline Logs]
        Budget[Budget Tracker]
    end
    
    Orch -.-> Log
    Orch -.-> Budget
```

## Key Components

### 1. Messaging (messaging/schemas.py)
Uses Pydantic V2 for strict type enforcement.
- **Message Types**: REQUEST, RESPONSE, ESCALATION, VETO, APPROVAL, REVISION_NEEDED.
- **Roles**: ORCHESTRATOR, CRAWLER, ANALYST, STRATEGIST, ALERTER.

### 2. State Management (state/state_manager.py)
Centralized "Source of Truth" with versioning.
- Every state mutation is attributed to an agent.
- Audit trail for every change.

### 3. Orchestration (orchestrator/orchestrator.py)
The control plane that manages:
- **Budgeting**: Stop execution if cost > $0.50.
- **Timeouts**: Intervene if an agent stalls for > 60s.
- **Conflict Resolution**: Logical arbitration when agents disagree.
