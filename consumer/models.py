from datetime import datetime
from pydantic import BaseModel
from enum import Enum

class Event(BaseModel, frozen=True):
    event_id: int
    event_type: str
    user_id: int
    amount: float
    currency: str
    timestamp: datetime
    source_service: str

class EventType(Enum):
    CREATED = 'transaction_created'

class Currency(Enum):
    USD = 'USD'

class Transaction(BaseModel):
    event_id: int
    event_type: EventType
    user_id: int
    amount: float
    currency: Currency
    event_time: datetime
    processed_time: datetime
    source_service: str

    def model_post_init(context: Any) -> None:
        print("Post init")

    @classmethod
    def from_event(cls, event: Event):
        event_type = EventType[event.event_type]
        currency = Currency[event.currency]
        processed_time = datetime.now()
        return cls(
            event_id=event.event_id,
            event_type=event_type,
            user_id=event.user_id,
            amount=event.amount,
            currency=currency,
            event_time=event.timestamp,
            processed_time=processed_time,
            source_service=event.source_service
        )