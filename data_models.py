from dataclasses import dataclass
from datetime import date
from typing import Optional

@dataclass
class StandardResult:
    """
    A standardized observation of a track/xc result.
    This decouples the parser from the database schema.
    """
    # --- 1. REQUIRED FIELDS (Must correspond to arguments passed without defaults) ---
    meet_name: str
    meet_url: str
    season: str # 'indoor', 'outdoor', 'xc'
    event_name: str
    heat_number: int
    athlete_name: str
    gender: str # 'M' or 'F'
    mark_raw: str
    source_id: str  # Generic ID (e.g. Athletic.net ID)


    # --- 2. OPTIONAL FIELDS (Must have defaults, e.g. = None) ---
    
    # Meet Metadata
    date_start: Optional[date] = None
    date_end: Optional[date] = None
    venue_name: Optional[str] = None
    venue_city: Optional[str] = None
    venue_state: Optional[str] = None
    venue_altitude: float = 0.0  # Changed to float (was str)
    facility_type: Optional[str] = None # '200 Banked', '200 Flat', etc.

    # Result Data
    heat_place: Optional[int] = None
    overall_place: Optional[int] = None
    
    # IDs
    # Note: athlete_id set to Optional because Parser yields None initially
    athlete_id: Optional[str] = None 
    
    # Links
    athlete_url_slug: Optional[str] = None
    team_name: Optional[str] = None
    team_slug: Optional[str] = None

    # Mark Data
    mark_seconds: Optional[float] = None
    mark_metric: Optional[float] = None
    wind: float = 0.0