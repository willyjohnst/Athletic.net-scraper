from lxml import html
import httpx
import asyncio
import logging
import re
from datetime import datetime
import random

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
]
BASE_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}

async def fetch_track_meet(url):
    headers = BASE_HEADERS.copy()
    headers["User-Agent"] = random.choice(USER_AGENTS)
    await asyncio.sleep(random.uniform(1.0, 2.0))

    async with httpx.AsyncClient(headers=headers, follow_redirects=True, timeout=20.0) as client:
        try:
            response = await client.get(url)
            if response.status_code != 200: return None
            
            tree = html.fromstring(response.text)
            parent_data = parse_track_meet_content(response.text, url)
            
            # Recursive check for Compiled pages
            compiled_links = tree.xpath("//a[contains(text(), 'Compiled')]")
            if compiled_links:
                all_results = []
                for link in compiled_links:
                    href = link.get('href')
                    if href:
                        if href.startswith("/"): href = f"https://www.tfrrs.org{href}"
                        sub_data = await fetch_sub_page(client, href)
                        if sub_data:
                            all_results.extend(sub_data['results'])
                            if not parent_data['meet_info']['season'] and sub_data['meet_info']['season']:
                                parent_data['meet_info']['season'] = sub_data['meet_info']['season']
                parent_data['results'] = all_results
            return parent_data
        except Exception as e:
            logging.error(f"Error fetching {url}: {e}")
            return None

async def fetch_sub_page(client, url):
    try:
        await asyncio.sleep(0.5)
        resp = await client.get(url)
        return parse_track_meet_content(resp.text, url)
    except: return None

def get_column_indices(headers):
    indices = {}
    for i, th in enumerate(headers):
        text = th.text_content().strip().upper()
        if 'PL' in text: indices['place'] = i
        elif 'NAME' in text or 'ATHLETE' in text: indices['athlete'] = i
        elif 'TEAM' in text: indices['team'] = i
        elif 'TIME' in text or 'MARK' in text or 'RESULT' in text: indices['mark'] = i
        elif 'WIND' in text: indices['wind'] = i
        elif 'HEAT' in text or 'SECTION' in text: indices['heat'] = i
    return indices

def parse_track_meet_content(html_content, url):
    tree = html.fromstring(html_content)
    
    # Metadata
    try:
        raw_title = tree.xpath('//h3[@class="panel-title"]/text()')[0]
        meet_title = clean_text(raw_title)
    except: meet_title = "Unknown Meet"

    date_start, date_end = None, None
    try:
        date_node = tree.xpath('//div[contains(@class, "panel-heading-normal-text")]')[0]
        date_start, date_end = clean_meet_date(date_node.text_content().strip())
    except: pass

    loc_data = parse_location(tree)
    facility_type = parse_facility_type(tree)

    results = []
    tables = tree.xpath('//table')
    events_found = set()

    for table in tables:
        # --- IMPROVED HEADER PARSING ---
        # 1. Look for the immediate preceding header (Could be "Heat 1" OR "Men's 100m")
        header_nodes = table.xpath('./preceding::h3')
        if not header_nodes:
            continue
            
        primary_header = clean_text(header_nodes[-1].text_content())
        secondary_header = ""
        
        # Check if primary_header looks like a Section/Heat title
        # e.g. "Heat 1", "Flight 2", "Section 3"
        is_section_header = re.search(r'^(?:Heat|Section|Flight)\s+\d+$', primary_header, re.IGNORECASE)
        
        if is_section_header:
            # If the closest header is just "Heat 1", we need to go back one more to find the Event Name
            if len(header_nodes) >= 2:
                # Combine them: "Men's 100m Heat 1"
                event_name_part = clean_text(header_nodes[-2].text_content())
                current_event = f"{event_name_part} {primary_header}"
            else:
                # Fallback if structure is weird
                current_event = primary_header
        else:
            # It's likely the main event name (e.g. "Men's 100m Finals")
            current_event = primary_header

        headers = table.xpath('.//thead//th')
        if not headers: headers = table.xpath('.//tr[1]//td')
        if not headers: continue

        col_map = get_column_indices(headers)
        if 'mark' not in col_map and 'athlete' not in col_map: continue
            
        events_found.add(current_event)

        rows = table.xpath('.//tbody/tr')
        if not rows: rows = table.xpath('.//tr[position()>1]')
        
        for row in rows:
            cols = row.xpath('.//td')
            if not cols: continue
            
            try:
                # Place
                place = None
                if 'place' in col_map and len(cols) > col_map['place']:
                    place = clean_place(cols[col_map['place']].text_content())

                # Athlete
                if 'athlete' not in col_map or len(cols) <= col_map['athlete']: continue
                athlete_node = cols[col_map['athlete']].xpath('.//a')
                if not athlete_node: continue
                athlete_name = clean_text(athlete_node[0].text_content())
                athlete_url = athlete_node[0].get('href')
                tfrrs_id, url_slug = None, None
                if athlete_url:
                    id_match = re.search(r'/athletes/(\d+)(?:/([^/\?]+))?', athlete_url)
                    if id_match: tfrrs_id, url_slug = id_match.groups()
                if not tfrrs_id: continue

                # Team
                team_name = None
                team_slug = None
                if 'team' in col_map and len(cols) > col_map['team']:
                    team_col = cols[col_map['team']]
                    team_name = clean_text(team_col.text_content())
                    
                    # Extract Slug
                    team_link = team_col.xpath('.//a/@href')
                    if team_link:
                        slug_match = re.search(r'/teams/(?:[^/]+/)?([^/]+)\.html', team_link[0])
                        if slug_match: team_slug = slug_match.group(1)
                    if not team_slug and team_name:
                         team_slug = team_name.replace(" ", "_")

                # Mark / Time
                mark_raw = None
                if 'mark' in col_map and len(cols) > col_map['mark']:
                    mark_raw = clean_text(cols[col_map['mark']].text_content(), limit=50)
                
                if not mark_raw: continue 

                # Metrics
                parsed_mark = parse_mark_data(mark_raw)
                mark_seconds = parsed_mark['seconds']
                mark_metric = parsed_mark['metric']

                wind = 0.0
                if 'wind' in col_map and len(cols) > col_map['wind']:
                    w_txt = cols[col_map['wind']].text_content()
                    try: wind = float(re.findall(r"[-+]?\d*\.\d+|\d+", w_txt)[0])
                    except: pass
                
                gender = 'M' if "Men" in current_event else 'F' if "Women" in current_event else None

                results.append({
                    "event_name": current_event,
                    "place": place,
                    "athlete_name": athlete_name,
                    "tfrrs_id": tfrrs_id,
                    "url_slug": url_slug,
                    "team_name": team_name,
                    "team_slug": team_slug,
                    "mark_raw": mark_raw,
                    "mark_seconds": mark_seconds,
                    "mark_metric": mark_metric,
                    "wind": wind,
                    "gender": gender
                })
            except Exception: continue

    detected_season = detect_season(meet_title, events_found, facility_type)
    
    meet_info = {
        "name": meet_title, "url": url, "date_start": date_start,
        "date_end": date_end, "venue_name": loc_data['venue'],
        "venue_city": loc_data['city'], "venue_state": loc_data['state'],
        "season": detected_season, "facility_type": facility_type
    }
    return {"meet_info": meet_info, "results": results}

# Helpers
def clean_text(text, limit=255): 
    if not text: return None
    text = re.sub(r'\s+', ' ', text).strip()
    return text[:limit]

def clean_place(text): 
    match = re.search(r'(\d+)', text)
    return int(match.group(1)) if match else None

def parse_mark_data(mark_raw):
    data = {'seconds': None, 'metric': None}
    if not mark_raw: return data
    
    clean = re.sub(r'\s*\([^\)]*\)', '', mark_raw) 
    clean = re.sub(r'[a-zA-Z]', '', clean).strip()
    
    if 'm' in mark_raw:
        try:
            match = re.search(r'(\d+\.\d+)m', mark_raw)
            if match: 
                data['metric'] = round(float(match.group(1)), 2)
        except: pass
    
    if ':' in clean or ('.' in clean and len(clean) < 10):
        val = convert_to_seconds(clean)
        if val:
            data['seconds'] = round(val, 2)
        
    return data

def convert_to_seconds(mark_str):
    if not mark_str: return None
    try:
        parts = mark_str.split(':')
        if len(parts) == 1: return float(parts[0])
        elif len(parts) == 2: return float(parts[0]) * 60 + float(parts[1])
        elif len(parts) == 3: return float(parts[0]) * 3600 + float(parts[1]) * 60 + float(parts[2])
    except: return None
    return None

def parse_facility_type(tree):
    txt = tree.text_content()
    if "Banked" in txt: return "Banked"
    if "Flat" in txt: return "Flat"
    if "Oversized" in txt: return "Oversized"
    return None

def detect_season(title, events, facility):
    if facility: return "indoor"
    if "Indoor" in title: return "indoor"
    if "Outdoor" in title: return "outdoor"
    return "outdoor"

def parse_location(tree):
    data = {"venue": None, "city": None, "state": None}
    try:
        nodes = tree.xpath('//div[contains(@class, "panel-heading-normal-text")]')
        if len(nodes) >= 2:
            full = clean_text(nodes[1].text_content())
            m = re.search(r'([a-zA-Z\s\.]+),\s*([A-Z]{2})', full)
            if m:
                data['city'] = m.group(1).strip()
                data['state'] = m.group(2).strip()
                raw_venue = full[:m.start()].strip()
                data['venue'] = raw_venue.rstrip(" -").strip()
            else: 
                data['venue'] = full.rstrip(" -").strip()
    except: pass
    return data

def clean_meet_date(date_str):
    if not date_str: return None, None
    try:
        parts = date_str.split(',')
        if len(parts) < 2: return None, None
        year = parts[-1].strip()
        main = parts[0].strip()
        m = re.match(r'([a-zA-Z]+)\s+(.+)', main)
        if not m: return None, None
        month, days = m.groups()
        if "-" in days:
            start_d, end_d = days.split("-")
            start = datetime.strptime(f"{month} {start_d}, {year}", "%B %d, %Y").date()
            try: end = datetime.strptime(f"{month} {end_d}, {year}", "%B %d, %Y").date()
            except: end = start 
            return start, end
        else:
            start = datetime.strptime(f"{month} {days}, {year}", "%B %d, %Y").date()
            return start, None
    except: return None, None