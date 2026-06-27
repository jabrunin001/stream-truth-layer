from enum import Enum
from pydantic import BaseModel


class EventType(str, Enum):
    BID = "bid"
    SOLD = "sold"
    VIEW = "view"


class Event(BaseModel):
    show_id: int
    type: EventType
    bidder_id: str | None = None
    amount_cents: int = 0
    event_time: float
    ingest_time: float
