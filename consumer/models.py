from datetime import datetime
from pydantic import BaseModel

class Event(BaseModel):
    event_id: int
    event_type: str
    user_id: int
    amount: float
    currency: str
    timestamp: datetime
    source_service: str