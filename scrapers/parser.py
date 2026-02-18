import logging
import asyncio
import httpx
import re
from datetime import datetime
from core.data_models import StandardResult

# --- CONSTANTS ---
API_GET_MEET = "https://www.athletic.net/api/v1/Meet/GetMeet/{meet_id}"
API_GET_RESULTS = "https://www.athletic.net/api/v1/Meet/GetEventResults"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://www.athletic.net/",
}

class AthleticNetParser:
    def __init__(self):
        self.logger = logging.getLogger("AthleticNetParser")

    async def fetch_meet_results(self, meet_id: str, meet_metadata: dict = None):
        self.logger.info(f"⚡ Fetching Meet {meet_id}...")
        
        async with httpx.AsyncClient(headers=HEADERS, timeout=20.0) as client:
            # 1. GET METADATA
            try:
                resp = await client.get(API_GET_MEET.format(meet_id=meet_id))
                if resp.status_code != 200: return []
                meet_data = resp.json()
            except Exception as e:
                self.logger.error(f"API Error: {e}")
                return []

            m_info = meet_data.get('meet', {})
            
            # Metadata Extraction
            date_start = self._parse_api_date(m_info.get('Start'))
            date_end = self._parse_api_date(m_info.get('End'))
            site_info = m_info.get('Site', {})
            venue_name = site_info.get('Name', m_info.get('LocationName', ''))
            
            # 2. COLLECT EVENTS
            tasks = []
            divisions = meet_data.get('divisions', [])
            for div in divisions:
                div_name = div.get('Name', '')
                for event in div.get('events', []):
                    event_id = event.get('IDEvent')
                    if not event_id: continue
                    
                    full_event_name = f"{div_name} {event.get('Gender', '')} {event.get('Name', '')}".strip()
                    gender = 'M' if event.get('Gender') == 'M' else 'F'
                    
                    tasks.append(
                        self._fetch_single_event(client, meet_id, event_id, full_event_name, gender)
                    )

            # 3. FETCH RESULTS
            all_event_results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # 4. PARSE TO STANDARD MODEL
            final_results = []
            
            for event_group in all_event_results:
                if not event_group or isinstance(event_group, Exception): continue
                
                # --- CALC HEAT PLACES ---
                # Group by heat first to calculate heat_place
                heats = {}
                for res in event_group:
                    h = res.get('HeatID', 1) # Default to Heat 1 if missing
                    if h not in heats: heats[h] = []
                    heats[h].append(res)
                
                # Sort and rank within heats
                for h_num, heat_results in heats.items():
                    # Sort by Place (if exists) or Mark
                    # Note: API usually returns them sorted, but we ensure it.
                    # We rely on the order returned by API for heat rank if place is missing.
                    for rank, res in enumerate(heat_results, 1):
                        
                        mark_raw = res.get('Mark', '')
                        parsed_mark = self._parse_mark(mark_raw)
                        
                        sr = StandardResult(
                            # Metadata
                            meet_name=m_info.get('Name', 'Unknown'),
                            meet_url=f"https://www.athletic.net/TrackAndField/meet/{meet_id}/results",
                            date_start=date_start,
                            date_end=date_end,
                            venue_name=venue_name,
                            venue_city=site_info.get('City'),
                            venue_state=site_info.get('State'),
                            veune_altitude=0, # API rarely provides this explicitely
                            season='outdoor', # TODO: Detect season
                            facility_type=None, 

                            # Result
                            event_name=res['meta_event_name'],
                            heat_data=h_num, # The Heat Number
                            heat_place=rank, # Calculated Rank in Heat
                            overall_place=self._parse_place(res.get('Place')),
                            athlete_name=f"{res.get('FirstName','')} {res.get('LastName','')}".strip(),
                            
                            # IDs
                            athlete_id=None, # To be filled by Processor
                            site_id="athletic.net",
                            # CRITICAL: This is the Linkage ID
                            source_id=str(res.get('AthleteID')), 
                            
                            athlete_url_slug=None,
                            team_name=res.get('Team', {}).get('Name') if isinstance(res.get('Team'), dict) else None,
                            team_slug=str(res.get('Team', {}).get('IDTeam')) if isinstance(res.get('Team'), dict) else None,

                            # Mark
                            mark_raw=mark_raw,
                            mark_seconds=parsed_mark['seconds'],
                            mark_metric=parsed_mark['metric'],
                            wind=float(res.get('Wind', 0.0) or 0.0),
                            gender=res['meta_gender']
                        )
                        final_results.append(sr)

            return final_results

    async def _fetch_single_event(self, client, meet_id, event_id, event_name, gender):
        try:
            params = {"meetId": meet_id, "eventId": event_id}
            resp = await client.get(API_GET_RESULTS, params=params)
            if resp.status_code == 200:
                data = resp.json()
                results = data if isinstance(data, list) else data.get('results', [])
                for r in results:
                    r['meta_event_name'] = event_name
                    r['meta_gender'] = gender
                return results
            return []
        except: return []

    def _parse_api_date(self, date_str):
        if not date_str: return None
        try: return datetime.fromisoformat(date_str).date()
        except: return None

    def _parse_place(self, val):
        try: return int(val)
        except: return None

    def _parse_mark(self, mark_raw):
        data = {'seconds': None, 'metric': None}
        if not mark_raw: return data
        clean = re.sub(r'[a-zA-Z]', '', mark_raw).strip()
        
        # Simple detection
        if ':' in clean or '.' in clean:
            try:
                parts = clean.split(':')
                if len(parts) == 1: sec = float(parts[0])
                elif len(parts) == 2: sec = float(parts[0])*60 + float(parts[1])
                else: sec = 0.0
                data['seconds'] = sec
            except: pass
        return data