from dataclasses import dataclass
from datetime import date
from typing import Optional

@dataclass
class StandardResult:
    """
    A standardized observation of a track/xc result.
    This decouples the parser from the database schema.
    """
    # Meet Metadata
    meet_name: str
    meet_url: str
    date_start: Optional[date]
    date_end: Optional[date]
    venue_name: Optional[str]
    venue_city: Optional[str]
    venue_state: Optional[str]
    venue_altitude: Optional[str] = 0 # if unknown, default to 0
    season: str # 'indoor', 'outdoor', 'xc'
    facility_type: Optional[str] # '200 Banked', ' 200 Flat', '400', etc.

    # Result Data
    event_name: str
    heat_number: int
    heat_place: Optional[int] 
    overall_place: Optional[int]
    athlete_name: str
    athlete_id: str # custom ID we generate for the athlete (method TBD) (So we can have linkage between hs + college)
    source_id: Optional[str] = None # Generic ID from the source (e.g., Athletic.net meet ID)
    athlete_url_slug: Optional[str]
    team_name: Optional[str]
    team_slug: Optional[str]
    gender: str # 'M' or 'F'

    
    # Mark Data
    mark_raw: str
    mark_seconds: Optional[float]
    mark_metric: Optional[float]
    wind: float = 0.0