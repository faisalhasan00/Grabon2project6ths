# Architecture Decision Records (ADR)

This document captures the key technical decisions made during the development of the Swarm system.

## ADR 001: Use of Pydantic for Messaging
- **Context**: Agents often hallucinate or change response formats.
- **Decision**: All communication must pass through Pydantic validators.
- **Consequence**: Invalid messages are rejected early, preventing downstream failures.

## ADR 002: Centralized State vs. Passing Context
- **Context**: Large contexts lead to token bloat and "lost in the middle" issues.
- **Decision**: Use a shared state manager with versioning.
- **Consequence**: Agents only see relevant state slices; the Orchestrator maintains full history.

## ADR 003: Multi-Provider Strategy
- **Context**: Avoiding vendor lock-in and managing rate limits.
- **Decision**: Use Gemini (Google), Groq (Meta/Llama), and Ollama (Local).
- **Consequence**: Satisfies the 4+ provider requirement and provides fallback options.

## ADR 004: Programmatic Conflict Resolution
- **Context**: LLMs can "hallucinate" agreement to avoid conflict.
- **Decision**: Use deterministic logic (Risk vs. Priority) to flag conflicts for the Orchestrator to resolve.
- **Consequence**: Reliable handling of edge cases where business logic must take priority.
