import logging
import asyncio
import random
from curl_cffi.requests import AsyncSession

class AthleteParser:
    def __init__(self):
        self.logger = logging.getLogger("AthleteParser")
        
        self.BASE_HEADERS = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://www.athletic.net/",
            "anet-appinfo": "web:web:0:240",
            "anet-site-roles-token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VySWQiOjAsInVzZXJSb2xlcyI6W10sIm5iZiI6MTc3MzA2MjkzNywiZXhwIjoxNzczNjY3Nzk3LCJpYXQiOjE3NzMwNjI5OTcsImlzcyI6ImF0aGxldGljLm5ldCIsImF1ZCI6Imp3dFVzZXJSb2xlc1NpdGVXaWRlIn0.c4_Hgy4ROLGPmcE8VY7nMogD_BYS6ZKbJkqVW7pzqzc"
        }

    async def fetch_athlete_bio(self, athlete_id):
        # Initial human delay
        await asyncio.sleep(random.uniform(1.0, 5.0)) 
        
        clean_id = str(athlete_id).strip()
        
        # 1. DYNAMIC HEADERS: Mimic a human navigating to this specific profile
        request_headers = self.BASE_HEADERS.copy()
        request_headers["Referer"] = f"https://www.athletic.net/athlete/{clean_id}/track-and-field/"
        request_headers["anet-site-roles-token"] = request_headers["anet-site-roles-token"].strip()
        
        tf_url = f"https://www.athletic.net/api/v1/AthleteBio/GetAthleteBioData?athleteId={clean_id}&sport=tf&level=0"
        xc_url = f"https://www.athletic.net/api/v1/AthleteBio/GetAthleteBioData?athleteId={clean_id}&sport=xc&level=0"
        
        payload = {"tf": {}, "xc": {}}
        
        async with AsyncSession(impersonate="chrome120", headers=request_headers, timeout=15.0) as client:
            try:
                # 2. Fetch TF
                tf_resp = await client.get(tf_url)
                if tf_resp.status_code == 200:
                    payload["tf"] = tf_resp.json()
                elif tf_resp.status_code == 403:
                    self.logger.error(f"TF 403 Forbidden for {clean_id}")
                
                # 3. MICRO-DELAY: Prevent firing two requests in the exact same millisecond
                await asyncio.sleep(random.uniform(1, 5))
                
                # 4. Fetch XC
                xc_resp = await client.get(xc_url)
                if xc_resp.status_code == 200:
                    payload["xc"] = xc_resp.json()
                elif xc_resp.status_code == 403:
                    self.logger.error(f"XC 403 Forbidden for {clean_id}")
                    
                return payload
            except Exception as e:
                self.logger.error(f"Network error fetching {athlete_id}: {e}")
                return None