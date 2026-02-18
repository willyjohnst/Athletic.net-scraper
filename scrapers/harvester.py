import httpx
import asyncio
import logging
import calendar
from datetime import date, datetime

# Constants for Athletic.net API
MAIN_SITE_BASE = "https://www.athletic.net"
API_REGIONS = f"{MAIN_SITE_BASE}/api/v1/public/GetStatesCountries2"
API_EVENTS = f"{MAIN_SITE_BASE}/api/v1/Event/Events"


HEADERS = {
    'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    'Accept': 'application/json, text/plain, */*',
    'Content-Type': 'application/json',
    'Origin': 'https://www.athletic.net',
}

async def fetch_season_meets(year):
    """
    Entry point expected by main.py.
    Orchestrates the fetching of meets for a given year and sport mode.
    """

    # 2. Get Region List (We must query by state/region on Athletic.net)
    print(f"Fetching regions for Athletic.net...")
    regions = await _get_regions()
    if not regions:
        logging.error("Failed to retrieve regions. Aborting.")
        return []

    # 3. Create a client and scan all months
    found_urls = set()
    
    async with httpx.AsyncClient(headers=HEADERS, timeout=30.0) as client:
        for month in range(1, 13):
            print(f"  > Scanning Athletic.net for {year}-{month:02d} ...", end="")
            
            # Create a task for every region for this specific month
            tasks = []
            for region in regions:
                # Filter: Only scan US states and maybe Canada to save time? 
                if region.get('CountryCode') == 'US': 
                    tasks.append(
                        _fetch_region_month(client, region, year, month)
                    )
            
            # Run all states for this month in parallel
            # We use return_exceptions=True to prevent one failed state from crashing the batch
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Harvest URLs from results
            for res in results:
                if isinstance(res, list):
                    found_urls.update(res)
            
            # Small buffer between months
            await asyncio.sleep(3)

    print(f"Total meets found: {len(found_urls)}")
    return list(found_urls)

async def _get_regions():
    """Fetches list of valid states/countries from API."""
    async with httpx.AsyncClient(headers=HEADERS) as client:
        try:
            resp = await client.get(API_REGIONS)
            if resp.status_code == 200:
                data = resp.json()
                states = data.get('states')
                
                countries_data = data.get('countries', {})
                countries_list = []
                
                for c_code, c_name in countries_data.items():
                    countries_list.append({
                        'CountryCode': c_code,  
                        'Code': '',
                        'Name': c_name
                    })
                
                return states + countries_list
            return []
        except Exception as e:
            logging.error(f"Region fetch failed: {e}")
            return []

async def _fetch_region_month(client, region, year, month):
    """
    Fetches events for a specific region and month.
    Returns a list of Meet URLs.
    """
    # Calculate dates
    start_date = date(year, month, 1)
    last_day = calendar.monthrange(year, month)[1]
    end_date = date(year, month, last_day)
    
    payload = {
        "start": start_date.strftime("%Y-%m-%d"),
        "end": end_date.strftime("%Y-%m-%d"),
        "levelMask": 0,       
        "sportMask": 0,
        "country": region.get('CountryCode'),
        "state": region.get('Code'),
        "location": "",
        "distanceKM": 0,
        "filterTerm": ""
    }

    # We need a referer to look legitimate, though API often accepts without
    headers_local = client.headers.copy()
    headers_local['Referer'] = f"https://www.athletic.net/events/us/{region.get('Code')}/{year}-{month}-1"

    try:
        response = await client.post(API_EVENTS, json=payload, headers=headers_local)
        
        if response.status_code == 200:
            data = response.json()
            events = []
            
            # API can return a dict with 'events' key or just a list
            if isinstance(data, dict):
                events = data.get('events', [])
            elif isinstance(data, list):
                events = data
            
            # Extract URLs
            urls = []
            for event in events:
                # We construct the URL standard for Athletic.net
                # IDMeet is the crucial key
                meet_id = event.get('IDMeet')
                if meet_id:
                    urls.append(f"https://www.athletic.net/TrackAndField/meet/{meet_id}")
            return urls
            
    except Exception as e:
        # Lower log level to avoid spamming console on minor timeouts
        logging.debug(f"Error fetching {region.get('Code')} {month}/{year}: {e}")
    
    return []