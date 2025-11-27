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

async def fetch_xc_meet(url):
    headers = BASE_HEADERS.copy()
    headers["User-Agent"] = random.choice(USER_AGENTS)
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
                    if consecutive_failures > 4: return None
                    await asyncio.sleep(random.uniform(10.0, 20.0))
                    continue

                if response.status_code != 200:
                    logging.warning(f"Failed to get {url}: Status {response.status_code}")
                    return None

                return parse_xc_meet(response.text, url)
            except httpx.RequestError as e:
                logging.error(f"Network Error for {url}: {e}")
                consecutive_failures += 1
                if consecutive_failures > 3: return None
                await asyncio.sleep(random.uniform(2.0, 5.0))
                continue
            except Exception as e:
                logging.error(f"Unexpected Error fetching {url}: {e}")
                return None

def clean_text(text, limit=255):
    if not text: return None
    text = text.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ')
    text = re.sub(r'\s+', ' ', text).strip()
    if len(text) > limit:
        return text[:limit]
    return text

def parse_xc_meet(html_content, url):
    tree = html.fromstring(html_content)
    try:
        raw_title = tree.xpath('//h3[@class="panel-title"]/text()')[0]
        meet_title = clean_text(raw_title, 255)
    except:
        meet_title = "Unknown XC Meet"

    try:
        raw_date = tree.xpath('//div[@class="col-lg-8"]/div/div/div[1]')[0].text_content()
        meet_date = clean_meet_date(raw_date.strip())
    except Exception:
        meet_date = None

    loc_data = parse_location(tree)

    results = []
    athlete_rows = tree.xpath('//tr[.//a[contains(@href, "/athletes/")]]')
    for row in athlete_rows:
        cols = row.xpath('.//td')
        if len(cols) < 4: continue
        try:
            place = clean_text(cols[0].text_content(), 10)
            
            athlete_node = cols[1].xpath('.//a')[0]
            athlete_name = clean_text(athlete_node.text_content(), 100)
            tfrrs_id = re.search(r'/athletes/(\d+)', athlete_node.get('href')).group(1)
            
            team_node = cols[3].xpath('.//a')
            if team_node: 
                team_name = clean_text(team_node[0].text_content(), 255)
            else: 
                team_name = clean_text(cols[3].text_content(), 255)
                
            mark_raw = clean_text(cols[5].text_content(), 50) 
            mark_seconds = convert_to_seconds(mark_raw)

            results.append({
                "event_name": "Cross Country", 
                "place": place,
                "athlete_name": athlete_name,
                "tfrrs_id": tfrrs_id,
                "team_name": team_name,
                "team_slug": team_name.replace(" ", "_"),
                "mark_raw": mark_raw,
                "mark_seconds": mark_seconds
            })
        except: continue

    meet_info = {
        "name": meet_title, 
        "url": url, 
        "date": meet_date,
        "venue_name": loc_data['venue'],
        "venue_city": loc_data['city'],
        "venue_state": loc_data['state']
    }
            
    return {"meet_info": meet_info, "results": results}

def parse_location(tree):
    data = {"venue": None, "city": None, "state": None}
    try:
        full_loc_nodes = tree.xpath('//div[contains(@class, "panel-heading-normal-text")]')
        if len(full_loc_nodes) < 2: return data
        
        # Clean text BEFORE Regex to handle newlines
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
        
        match = re.search(r'([a-zA-Z\s\.]+),\s*([A-Z]{2})', location_part)
        if match:
            data["city"] = clean_text(match.group(1), 100)
            data["state"] = clean_text(match.group(2), 50)
        else:
            if not data["venue"]: 
                data["venue"] = clean_text(full_text, 255)
    except Exception: pass
    return data

def clean_meet_date(date_str):
    if not date_str: return None
    try:
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
    clean_mark = re.sub(r'[a-zA-Z]', '', mark_str).strip()
    try:
        parts = clean_mark.split(':')
        if len(parts) == 1: return float(parts[0])
        elif len(parts) == 2: return float(parts[0]) * 60 + float(parts[1])
        elif len(parts) == 3: return float(parts[0]) * 3600 + float(parts[1]) * 60 + float(parts[2])
    except: return None
    return None