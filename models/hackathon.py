from pydantic import BaseModel
from typing import Optional


class Hackathon(BaseModel):
    """
    Represents the data structure of a Hackathon.
    """
    name: str
    start_date: str
    end_date: str
    mode: str  # "Online", "Offline", or "Hybrid"
    location: Optional[str] = None
    start_time: Optional[str] = None
    prize_pool: Optional[str] = None
    organization: Optional[str] = None
    application_deadline: Optional[str] = None
    url: Optional[str] = None
    description: Optional[str] = None 