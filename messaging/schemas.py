from enum import Enum
from typing import Any, Dict, List, Optional, Union
from pydantic import BaseModel, Field, field_validator
from datetime import datetime

class MessageType(str, Enum):
    REQUEST = "request"
    RESPONSE = "response"
    ESCALATION = "escalation"
    VETO = "veto"
    APPROVAL = "approval"
    REVISION_NEEDED = "revision_needed"

class AgentRole(str, Enum):
    ORCHESTRATOR = "orchestrator"
    CRAWLER = "crawler"
    ANALYST = "analyst"
    STRATEGIST = "strategist"
    ALERTER = "alerter"

class Payload(BaseModel):
    """Base payload for all agent communications."""
    data: Dict[str, Any]
    metadata: Optional[Dict[str, Any]] = Field(default_factory=dict)

class AgentMessage(BaseModel):
    """The standard message envelope for the Swarm."""
    message_id: str
    sender: AgentRole
    receiver: AgentRole
    message_type: MessageType
    payload: Payload
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    version: int = 1
    cost: float = 0.0

    @field_validator('message_type')
    @classmethod
    def validate_message_type(cls, v):
        if v not in MessageType:
            raise ValueError(f"Invalid message type: {v}")
        return v
