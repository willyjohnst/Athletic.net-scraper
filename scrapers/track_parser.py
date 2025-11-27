from lxml import html
import httpx
import asyncio
import logging
import traceback
import re
from datetime import datetime
import random

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:109.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15"
]

BASE_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Referer": "https://www.tfrrs.org/results_search.html",
    "Origin": "https://www.tfrrs.org/"
}

async def fetch_track_meet(url):
    """
    Robust fetcher with retries, 403 handling, and stealth mechanisms.
    """
    headers = BASE_HEADERS.copy()
    headers["User-Agent"] = random.choice(USER_AGENTS)

    # JITTER
    await asyncio.sleep(random.uniform(1.5, 4.0))

    async with httpx.AsyncClient(headers=headers, follow_redirects=True) as client:
        consecutive_failures = 0

        while True:
            try:
                response = await client.get(url, timeout=15.0)
                    
                if response.status_code == 403:
                    logging.error(f"403 Forbidden for {url}. Switching User-Agent and retrying...")
                    consecutive_failures += 1
                    headers["User-Agent"] = random.choice(USER_AGENTS)
                    
                    if consecutive_failures > 4:
                        logging.error(f"Giving up on {url} after multiple 403 blocks.")
                        return None
                    await asyncio.sleep(random.uniform(10.0, 20.0))
                    continue

                if response.status_code != 200:
                    logging.warning(f"Failed to get {url}: Status {response.status_code}")
                    return None

                return parse_track_meet(response.text, url)

            except httpx.RequestError as e:
                logging.error(f"Network Error for {url}: {type(e).__name__} - {e}")
                consecutive_failures += 1
                if consecutive_failures > 3:
                    return None
                await asyncio.sleep(random.uniform(2.0, 5.0))
                continue
                    
            except Exception as e:
                logging.error(f"Unexpected Error fetching {url}: {e}")
                logging.error(traceback.format_exc())
                return None

def clean_text(text, limit=100):
    """
    Normalizes whitespace and truncates to DB limits.
    Replaces newlines with space, removes double spaces.
    """
    if not text:
        return None
    
    # Replace formatting chars with space
    text = text.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ')
    
    # Collapse multiple spaces "   " -> " "
    text = re.sub(r'\s+', ' ', text).strip()
    
    # Hard truncate to prevent DB errors
    if len(text) > limit:
        return text[:limit]
        
    return text

def parse_track_meet(html_content, url):
    tree = html.fromstring(html_content)
    
    try:
        raw_title = tree.xpath('//h3[@class="panel-title"]/text()')[0]
        meet_title = clean_text(raw_title, 255)
    except IndexError:
        meet_title = "Unknown Meet"

    # DATE
    try:
        raw_date = tree.xpath('//div[@class="col-lg-8"]/div/div/div[1]')[0].text_content()
        # Note: clean_meet_date handles its own cleaning/parsing
        meet_date = clean_meet_date(raw_date.strip())
    except Exception:
        meet_date = None 

    # LOCATION
    loc_data = parse_location(tree)

    results = []
    rows = tree.xpath('//tr')
    
    current_event = "Unknown"
    
    for row in rows:
        header = row.xpath('.//div[@class="custom-table-title"]/h3/text()')
        if header:
            current_event = clean_text(header[0], 255)
            continue
        
        header_alt = row.xpath('.//th[@class="custom-table-title"]/text()')
        if header_alt:
            current_event = clean_text(header_alt[0], 255)
            continue

        cols = row.xpath('.//td')
        if not cols: 
            continue 
            
        try:
            place = clean_text(cols[0].text_content(), 10) # Place is short
            athlete_node = cols[1].xpath('.//a')
            
            if not athlete_node:
                continue 
                
            athlete_name = clean_text(athlete_node[0].text_content(), 100)
            athlete_url = athlete_node[0].get('href')
            
            tfrrs_id_match = re.search(r'/athletes/(\d+)', athlete_url)
            if not tfrrs_id_match:
                continue
            tfrrs_id = tfrrs_id_match.group(1)
            
            team_name = clean_text(cols[2].text_content(), 255)
            mark_raw = clean_text(cols[3].text_content(), 50)
            
            results.append({
                "event_name": current_event,
                "place": place,
                "athlete_name": athlete_name,
                "tfrrs_id": tfrrs_id,
                "team_name": team_name,
                "team_slug": team_name.replace(" ", "_"),
                "mark_raw": mark_raw,
                "mark_seconds": convert_to_seconds(mark_raw)
            })
            
        except Exception as e:
            continue

    meet_info = {
        "name": meet_title, 
        "url": url, 
        "date": meet_date, 
        "venue_name": loc_data['venue'],
        "venue_city": loc_data['city'],
        "venue_state": loc_data['state']
    }

    return {
        "meet_info": meet_info,
        "results": results
    }

def parse_location(tree):
    data = {"venue": None, "city": None, "state": None}
    try:
        full_loc_nodes = tree.xpath('//div[contains(@class, "panel-heading-normal-text")]')
        if len(full_loc_nodes) < 2: 
            return data
        
        # CLEAN THE RAW HTML TEXT FIRST
        # This fixes issues where newlines break Regex
        full_text = clean_text(full_loc_nodes[1].text_content(), 500)
        
        host_nodes = tree.xpath('//span[contains(@class, "panel-heading-text") and contains(text(), "HOST")]')
        host_name = ""
        if host_nodes:
            raw_host = host_nodes[0].text_content().replace("HOST:", "")
            host_name = clean_text(raw_host, 255)
            data["venue"] = host_name

        location_part = full_text
        if host_name and host_name in full_text:
            location_part = full_text.replace(host_name, "").strip()
        
        # Regex is safer now that 'full_text' is a single line
        match = re.search(r'([a-zA-Z\s\.]+),\s*([A-Z]{2})', location_part)
        if match:
            data["city"] = clean_text(match.group(1), 100)
            data["state"] = clean_text(match.group(2), 50)
        else:
            if not data["venue"]:
                data["venue"] = clean_text(full_text, 255)
    except Exception as e:
        pass
        
    return data

def clean_meet_date(date_str):
    if not date_str: return None
    try:
        # Pre-clean the date string too
        date_str = re.sub(r'\s+', ' ', date_str).strip()
        
        parts = date_str.split(',')
        if len(parts) != 2: return None
        year = parts[1].strip()
        month_day = parts[0].strip()
        if "-" in month_day:
            month_day = month_day.split("-")[0]
        clean_str = f"{month_day}, {year}"
        dt = datetime.strptime(clean_str, "%B %d, %Y")
        return dt.date() 
    except:
        return None

def convert_to_seconds(mark_str):
    if not mark_str: return None
    clean_mark = re.sub(r'\(.*?\)', '', mark_str).strip()
    clean_mark = re.sub(r'[a-zA-Z]', '', clean_mark).strip()
    if not clean_mark: return None
    try:
        parts = clean_mark.split(':')
        if len(parts) == 1: return float(parts[0])
        elif len(parts) == 2: return float(parts[0]) * 60 + float(parts[1])
        elif len(parts) == 3: return float(parts[0]) * 3600 + float(parts[1]) * 60 + float(parts[2])
    except: return None
    return None