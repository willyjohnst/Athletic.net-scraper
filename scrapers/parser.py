import logging
import asyncio
import httpx
import re
from datetime import datetime
from data_models import StandardResult

BASE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Origin": "https://www.athletic.net",
    "Referer": "https://www.athletic.net/", # Important for Cloudflare
    "Sec-Ch-Ua": '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": '"Windows"',
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
}

# --- CONSTANTS ---
API_GET_MEET = "https://www.athletic.net/api/v1/Meet/GetMeetData?meetId={meet_id}&sport={sport}"
API_GET_RESULTS = "https://www.athletic.net/api/v1/Meet/GetAllResultsData?rawResults=false&showTips=false"

BASE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Origin": "https://www.athletic.net",
}

# The dictionary you discovered!
FINALIZED_REASONS = {
    30: "Official",
    20: "Complete",
    10: "In Progress",
    0: "In Review",
    -10: "Time Trial",
    -20: "Polar Bear",
    -30: "Partial Results"
}

class AthleticNetParser:
    def __init__(self):
        self.logger = logging.getLogger("AthleticNetParser")

    async def fetch_meet_results(self, meet_id: str, season_hint: str):
        self.logger.info(f"Fetching Meet {meet_id}...")
        sport_code = "xc" if season_hint == "xc" else "tf"
        
        async with httpx.AsyncClient(headers=BASE_HEADERS, timeout=20.0) as client:
            # --- STEP 1: Get Meet Metadata & JWT Token ---
            try:
                meta_url = API_GET_MEET.format(meet_id=meet_id, sport=sport_code)
                resp = await client.get(meta_url)
                if resp.status_code != 200:
                    self.logger.error(f"Failed to fetch metadata. HTTP {resp.status_code}")
                    return []
                
                meet_data = resp.json()
            except Exception as e:
                self.logger.error(f"API Connection Error: {e}")
                return []

            jwt_token = meet_data.get('jwtMeet')
            if not jwt_token:
                self.logger.error("No JWT Token found in Meet Data!")
                return []

            # Parse Metadata
            m_info = meet_data.get('meet', {})
            date_start = self._parse_api_date(m_info.get('StartDate'))
            date_end = self._parse_api_date(m_info.get('EndDate'))
            
            loc_info = m_info.get('Location', {})
            venue_name = loc_info.get('Name', 'Unknown Venue')
            venue_city = loc_info.get('City')
            venue_state = loc_info.get('State')
            
            # Extract your cool finalized reason
            fin_code = m_info.get('FinalizedReason', 0)
            data_quality = FINALIZED_REASONS.get(fin_code, "Unknown")
            self.logger.info(f"Meet Status: {data_quality}")

            # --- STEP 2: Fetch ALL Results ---
            auth_headers = BASE_HEADERS.copy()
            auth_headers['anettokens'] = jwt_token
            auth_headers['Referer'] = f"https://www.athletic.net/TrackAndField/meet/{meet_id}/results"
            
            try:
                res_resp = await client.get(API_GET_RESULTS, headers=auth_headers)
                if res_resp.status_code == 403:
                    self.logger.error("HTTP 403 Forbidden: Cloudflare blocked the request or the JWT Header name is wrong.")
                    return []
                elif res_resp.status_code != 200:
                    self.logger.error(f"Failed to fetch results. HTTP {res_resp.status_code}")
                    return []
                results_data = res_resp.json()
            except Exception as e:
                self.logger.error(f"Results API Error: {e}")
                return []

            # --- STEP 3: Parse the API: athletic.net/api/v1/Meet/GetAllResultsData?rawResults=false&showTips=false ---
            final_results = []
            flat_events = results_data.get('flatEvents', [])
            
            for event_group in flat_events:
                # Group Metadata
                div_name = event_group.get('Division', '')
                raw_event_name = event_group.get('Event', '')
                gender_code = event_group.get('Gender', 'U')
                
                # Combine to make e.g., "Varsity Mens 100 Meters"
                full_event_name = f"{div_name} {'Mens' if gender_code=='M' else 'Womens'} {raw_event_name}".strip()
                
                for res in event_group.get('results', []):
                    mark_raw = str(res.get('Result', '')).strip()
                    if not mark_raw: continue # Skip empty rows
                    
                    parsed_mark = self._parse_mark(mark_raw)
                    
                    sr = StandardResult(
                        # Metadata
                        meet_name=m_info.get('Name', 'Unknown Meet'),
                        meet_url=f"https://www.athletic.net/TrackAndField/meet/{meet_id}/results",
                        season='outdoor' if sport_code == 'tf' else 'xc', # Basic fallback
                        date_start=date_start,
                        date_end=date_end,
                        venue_name=venue_name,
                        venue_city=venue_city,
                        venue_state=venue_state,
                        venue_lat = loc_info.get('Lat'),
                        venue_lon = loc_info.get('Long'),
                        venue_altitude = await self._fetch_altitude(client, loc_info.get('Lat'), loc_info.get('Long')),
                        facility_type = self._parse_facility_type(sport_code, loc_info, m_info.get('Name', 'Unknown Meet')),
                        
                        # Event
                        event_name=full_event_name,
                        heat_number=0, # API doesn't clearly split heats in flatEvents, default to 0
                        heat_place=None, 
                        overall_place=self._parse_place(res.get('Place')),
                        gender=gender_code,
                        
                        # Athlete
                        athlete_name=f"{res.get('FirstName', '')} {res.get('LastName', '')}".strip(),
                        athlete_id=None, # Processor handles this
                        source_id=str(res.get('AthleteID')) if res.get('AthleteID') else None,
                        athlete_url_slug=None,
                        
                        # Team
                        team_name=res.get('SchoolName'),
                        team_slug=str(res.get('TeamID')), # Use their ID as slug for perfect matching
                        
                        # Mark
                        mark_raw=mark_raw,
                        mark_seconds=parsed_mark['seconds'],
                        mark_metric=parsed_mark['metric'],
                        wind=float(res.get('Wind')) if res.get('Wind') else 0.0
                    )
                    final_results.append(sr)

            self.logger.info(f"Finished Meet {meet_id}. Parsed {len(final_results)} results.")
            return final_results


    def _parse_api_date(self, date_str):
        if not date_str: return None
        try:
            # Handle "2015-05-15T00:00:00"
            return datetime.fromisoformat(date_str.split('T')[0]).date()
        except: return None

    def _parse_place(self, place_val):
        if not place_val: return None
        try:
            # Handle "1", "1.", "3"
            clean = str(place_val).replace('.', '').strip()
            return int(clean)
        except: return None

    def _parse_mark(self, mark_raw):
        """Parse mark string into seconds and/or metric values."""
        data = {'seconds': None, 'metric': None}
        if not mark_raw: return data
        
        clean = re.sub(r'\s*\([^\)]*\)', '', mark_raw)
        clean = re.sub(r'[a-zA-Z\']', '', clean).strip() # Removed ' for feet parsing
        
        # Very simple metric/imperial check based on your data ("39'9" or "10.6")
        if "'" in mark_raw:
            # Handle 39'9
            try:
                parts = mark_raw.split("'")
                feet = float(parts[0])
                inches = float(parts[1]) if len(parts)>1 and parts[1] else 0.0
                data['metric'] = round((feet * 0.3048) + (inches * 0.0254), 2)
            except: pass
        elif ':' in clean or '.' in clean:
            # Handle Time
            val = self._convert_to_seconds(clean)
            if val: data['seconds'] = round(val, 2)
            
        return data

    async def _fetch_altitude(self, client, lat, lon):
        """
        Fetches altitude in meters using Open-Meteo's free elevation API.
        """
        if not lat or not lon:
            return 0.0
            
        url = f"https://api.open-meteo.com/v1/elevation?latitude={lat}&longitude={lon}"
        
        try:
            # We use the existing client but don't need the Athletic.net specific headers
            resp = await client.get(url, timeout=5.0)
            if resp.status_code == 200:
                data = resp.json()
                elevation_list = data.get('elevation')
                if elevation_list and len(elevation_list) > 0:
                    return float(elevation_list[0])
        except Exception as e:
            pass
            
        return 0.0 # Default to sea level if the API fails

    def _convert_to_seconds(self, mark_str):
        if not mark_str: return None
        try:
            parts = mark_str.split(':')
            if len(parts) == 1: return float(parts[0])
            elif len(parts) == 2: return float(parts[0]) * 60 + float(parts[1])
        except: return None
        return None

    def _parse_facility_type(self, sport_code, loc_info, meet_name):
        """
        Hybrid facility parser. Tries API first, falls back to text analysis.
        """
        if sport_code == 'xc':
            return "XC Course"
            
        # --- 1. Try Explicit API Fields ---
        track_length = loc_info.get('TrackLength')
        track_type = loc_info.get('TrackType') 
        is_indoor = loc_info.get('Indoor')

        # If API gives us pristine data, use it immediately
        if is_indoor and track_length and track_type:
            return f"{track_length}m {track_type.capitalize()}"

        # --- 2. Fallback: Text Analysis ---
        venue_name = loc_info.get('Name', '')
        search_text = f"{venue_name} {meet_name}".lower()

        # Flag indoors based on keywords or API flag
        indoor_flag = is_indoor or any(w in search_text for w in ['indoor', 'dome', 'fieldhouse', 'complex'])

        # Regex to find numbers like 200m, 300 meters, 180m
        size_match = re.search(r'\b(1[5-9]\d|2\d\d|3\d\d|400)\s*(?:m|meter|meters)\b', search_text)
        size = int(size_match.group(1)) if size_match else track_length

        if indoor_flag or (size and size < 400):
            parts = []
            if size:
                parts.append(f"{size}m")

            # Apply your specific logic
            if 'banked' in search_text:
                parts.append("Banked")
            elif 'flat' in search_text:
                parts.append("Flat")
            elif 'oversized' in search_text or (size and 200 < size < 400):
                parts.append("Oversized")
            # If size is exactly 200 and no words found, it remains just "200m" (Banked/Flat unknown)

            if parts:
                return " ".join(parts)
            return "Indoor Track" # Generic fallback

        return "Outdoor Track"