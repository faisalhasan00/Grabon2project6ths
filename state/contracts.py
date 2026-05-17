from pydantic import BaseModel, Field, field_validator
from typing import List, Optional
from enum import Enum

# --- ONTOLOGY ENUMS ---
class OfferType(str, Enum):
    CASHBACK = "cashback"
    COUPON = "coupon"
    BANK_OFFER = "bank_offer"
    SALE = "sale"
    UNKNOWN = "unknown"

class PlacementType(str, Enum):
    BANNER = "banner"
    CONTENT = "content"
    POPUP = "popup"
    UNKNOWN = "unknown"

# --- DATA GOVERNANCE CONTRACTS ---
class OfferContract(BaseModel):
    """Ontological definition of a validated offer."""
    offer_type: OfferType = OfferType.UNKNOWN
    description: str = Field(..., min_length=5)
    code: Optional[str] = None
    expiry: Optional[str] = None
    placement: PlacementType = PlacementType.UNKNOWN
    
    @field_validator("description")
    def validate_description(cls, v: str) -> str:
        if len(v) > 500:
            raise ValueError("Data Contract Violation: Description exceeds 500 chars (possible hallucination or raw HTML leakage).")
        return v

class CrawlerPayloadContract(BaseModel):
    """Strict data contract for incoming Crawler JSON payloads."""
    target: str
    url_visited: str
    status_code: int
    timestamp: Optional[int] = None
    screenshot: Optional[str] = None
    raw_text: str = Field(..., description="Raw extracted text")
    
    @field_validator("status_code")
    def validate_status(cls, v: int) -> int:
        if v not in [200, 201, 301, 302]:
            raise ValueError(f"Data Contract Violation: Invalid or blocked Status Code {v}.")
        return v
