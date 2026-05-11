"""
Defines the core data models for market data ingestion and processing.
"""
from datetime import datetime, timezone
from typing import Any
from pydantic import BaseModel, Field, model_validator



class MarketEnvelope(BaseModel):
    """
    Represents a standardized envelope for market data events.

    This model encapsulates market data with metadata about its source,
    timing, and quality, providing a consistent structure for data flowing
    through the system.
    """

    # Schema version for backwards compatibility (must be >= 1)
    schema_version: int = Field(default=1, ge=1)

    # Data source identifier 
    source: str = Field(min_length=1)

    # Unique identifier for the financial instrument (e.g., ticker, ISIN)
    instrument_id: str = Field(min_length=1)

    # Timestamp when the market event occurred (UTC)
    event_time_utc: datetime

    # Timestamp when the event was ingested into the system (UTC)
    ingest_time_utc: datetime

    # Data quality indicator (0=clean, 1=stale, 3=estimated
    quality_flag: int = Field(default=0, ge=0, le=3)

    # Dynamic payload containing the actual market data content
    payload: dict[str, Any]

    @model_validator(mode="after") 
    def event_before_ingest(self) -> "MarketEnvelope":
        """
        Validates that the event time occurred before the ingest time.
        
        This ensures logical consistency: an event must happen before it can
        be ingested into the system. Raises ValueError if this constraint is violated.
        """
        if self.event_time_utc > self.ingest_time_utc:
            raise ValueError("event_time_utc cannot be after ingest_time_utc")
        return self