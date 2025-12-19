from lxml import html
import httpx
import asyncio
import logging
import re
from datetime import datetime
import random

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
]
BASE_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}

async def fetch_xc_meet(url):
    headers = BASE_HEADERS.copy()
    headers["User-Agent"] = random.choice(USER_AGENTS)
    await asyncio.sleep(random.uniform(1.5, 3.0))

    async with httpx.AsyncClient(headers=headers, follow_redirects=True) as client:
        try:
            response = await client.get(url, timeout=15.0)
            if response.status_code != 200: return None
            return parse_xc_meet(response.text, url)
        except Exception as e:
            logging.error(f"Error fetching {url}: {e}")
            return None

def clean_text(text, limit=255):
    if not text: return None
    text = re.sub(r'\s+', ' ', text).strip()
    return text[:limit]

def get_column_indices(header_row):
    indices = {}
    headers = header_row.xpath('.//th')
    for i, th in enumerate(headers):
        text = th.text_content().strip().upper()
        if 'PL' in text: indices['place'] = i
        elif 'NAME' in text or 'ATHLETE' in text: indices['athlete'] = i
        elif 'TEAM' in text: indices['team'] = i
        elif 'TIME' in text or 'MARK' in text: 
            if 'mark' not in indices or 'FINISH' in text or 'TOTAL' in text:
                indices['mark'] = i
    return indices

def parse_xc_meet(html_content, url):
    tree = html.fromstring(html_content)
    try:
        raw_title = tree.xpath('//h3[@class="panel-title"]/text()')[0]
        meet_title = clean_text(raw_title, 255)
    except: meet_title = "Unknown XC Meet"

    try:
        raw_date = tree.xpath('//div[@class="col-lg-8"]/div/div/div[1]')[0].text_content()
        date_start, date_end = clean_meet_date(raw_date.strip())
    except: date_start, date_end = None, None

    loc_data = parse_location(tree)
    results = []
    
    event_headers = tree.xpath('//div[contains(@class, "custom-table-title-xc")]')
    if event_headers:
        for header_node in event_headers:
            try:
                event_name = clean_text(header_node.xpath('.//h3/text()')[0], 255)
                current_gender = 'M' if "Men" in event_name and "Women" not in event_name else 'F' if "Women" in event_name else None
                table = header_node.xpath('./following::table[1]')
                if table:
                    rows = table[0].xpath('.//tr')
                    col_map = {'place': 0, 'athlete': 1, 'team': 3, 'mark': 5}
                    for row in rows:
                        if row.xpath('.//th'):
                            col_map = get_column_indices(row)
                            continue
                        results.extend(parse_xc_row(row, event_name, current_gender, col_map))
            except: continue
    else:
        # Legacy Fallback
        rows = tree.xpath('//tr')
        col_map = {'place': 0, 'athlete': 1, 'team': 3, 'mark': 5}
        current_event = "Cross Country"
        current_gender = None
        for row in rows:
            if row.xpath('.//th'):
                col_map = get_column_indices(row)
                continue
            header = row.xpath('.//div[@class="custom-table-title"]/h3/text()')
            if header:
                current_event = clean_text(header[0], 255)
                if "Men" in current_event: current_gender = 'M'
                elif "Women" in current_event: current_gender = 'F'
                continue
            results.extend(parse_xc_row(row, current_event, current_gender, col_map))

    meet_info = {
        "name": meet_title, "url": url, "date_start": date_start,
        "date_end": date_end, "venue_name": loc_data['venue'],
        "venue_city": loc_data['city'], "venue_state": loc_data['state'],
        "season": "xc", "facility_type": "XC Course"
    }
    return {"meet_info": meet_info, "results": results}

def parse_xc_row(row, event_name, gender, col_map):
    data = []
    cols = row.xpath('.//td')
    if not cols: return data
    
    athlete_idx = col_map.get('athlete', 1)
    if len(cols) <= athlete_idx: return data

    athlete_node_list = cols[athlete_idx].xpath('.//a')
    if not athlete_node_list: return data
        
    try:
        place_idx = col_map.get('place', 0)
        place = clean_text(cols[place_idx].text_content(), 10) if len(cols) > place_idx else None
        
        athlete_node = athlete_node_list[0]
        athlete_name = clean_text(athlete_node.text_content(), 100)
        athlete_url = athlete_node.get('href')
        
        tfrrs_id, url_slug = None, None
        if athlete_url:
            id_match = re.search(r'/athletes/(\d+)(?:/([^/\?]+))?', athlete_url)
            if id_match: tfrrs_id, url_slug = id_match.groups()
        if not tfrrs_id: return data

        # --- TEAM SLUG FIX ---
        team_idx = col_map.get('team', 3)
        team_name = None
        team_slug = None
        if len(cols) > team_idx:
            team_col = cols[team_idx]
            team_name = clean_text(team_col.text_content(), 255)
            # Extract Link
            team_link = team_col.xpath('.//a/@href')
            if team_link:
                # FIXED REGEX: Handles 'tf/', 'xc/', 'track/' or generic folders
                slug_match = re.search(r'/teams/(?:[^/]+/)?([^/]+)\.html', team_link[0])
                if slug_match: team_slug = slug_match.group(1)
            # Fallback
            if not team_slug and team_name:
                team_slug = team_name.replace(" ", "_")

        mark_idx = col_map.get('mark', 5)
        if len(cols) <= mark_idx and len(cols) > 4: mark_idx = 4
        mark_raw = clean_text(cols[mark_idx].text_content(), 50) if len(cols) > mark_idx else None

        # SKIP IF NO MARK
        if not mark_raw or mark_raw in ["DNS", "DNF", "NT"]: return data

        data.append({
            "event_name": event_name, 
            "place": place,
            "athlete_name": athlete_name,
            "tfrrs_id": tfrrs_id,
            "url_slug": url_slug,
            "gender": gender,
            "team_name": team_name,
            "team_slug": team_slug,
            "mark_raw": mark_raw,
            "mark_seconds": convert_to_seconds(mark_raw)
        })
    except: pass
    return data

def parse_location(tree):
    """
    Improved Location Parser for XC.
    Splits by <br> or HOST: to separate Venue from City/State.
    """
    data = {"venue": None, "city": None, "state": None}
    try:
        nodes = tree.xpath('//div[contains(@class, "panel-heading-normal-text")]')
        if len(nodes) < 2: return data
        
        loc_node = nodes[1]
        
        # 1. Transform <br> and "HOST:" into newlines for splitting
        from lxml.etree import tostring
        html_str = tostring(loc_node, encoding='unicode')
        html_str = re.sub(r'<br\s*/?>', '\n', html_str, flags=re.IGNORECASE)
        html_str = re.sub(r'HOST:', '\n', html_str, flags=re.IGNORECASE)
        
        # 2. Get clean lines
        clean_str = html.fromstring(html_str).text_content()
        lines = [line.strip() for line in clean_str.split('\n') if line.strip()]
        
        if not lines: return data
        
        # 3. Parse Bottom-Up: Look for State or City/State at the end
        # Heuristic: Last line is usually location
        last_line = lines[-1]
        
        # Check if last line is just State (and maybe zip)
        m_state_only = re.search(r'^([A-Z]{2})(?:\s+[\d\-]+)?$', last_line)
        
        if m_state_only:
            data['state'] = m_state_only.group(1)
            lines.pop() # Remove state line
            if lines:
                # Next last line is likely City (or Address + City)
                # If no comma, we assume the whole line is city for now, 
                # or split by common street suffixes if needed.
                data['city'] = lines.pop().rstrip(',')
        else:
            # Check if last line is "City, State" or "City State"
            if parse_city_state_blob(last_line, data):
                lines.pop()
        
        # 4. Remaining lines are Venue + Address
        # We want to filter out purely address lines to get a clean Venue Name
        venue_parts = []
        for line in lines:
            # Heuristic: If line starts with a number or PO Box, it's an address
            if re.match(r'^(\d+|P\.? ?O\.? ?Box)', line, re.IGNORECASE):
                continue
            venue_parts.append(line)
            
        if venue_parts:
            data['venue'] = ", ".join(venue_parts).strip().rstrip(',')
            
    except Exception: pass
    return data

def parse_city_state_blob(text, data_dict):
    """Parses 'City, State' or 'City State Zip'."""
    text = text.strip()
    
    # 1. "City, State" (Comma is safe)
    m = re.search(r'^(.*?),\s*([A-Z]{2})(?:\s+[\d\-]+)?$', text)
    if m:
        data_dict['city'] = m.group(1).strip()
        data_dict['state'] = m.group(2).strip()
        return True
    
    # 2. "City State" (No comma)
    # Heuristic: State is 2 uppercase letters at end
    m = re.search(r'^(.*?)\s+([A-Z]{2})(?:\s+[\d\-]+)?$', text)
    if m:
        city_part = m.group(1).strip()
        # Safety: City shouldn't start with a number (address) or be too long
        if len(city_part) < 35 and not re.match(r'^\d', city_part):
            # Advanced: If city_part ends in "Rd" or "St", it's likely part of address.
            # Try to split: "Grapevine Rd Wenham" -> "Wenham"
            addr_split = re.split(r'\b(?:Rd|St|Ave|Blvd|Dr|Ln|Way|Pk|Hwy)\.?\s+', city_part, flags=re.IGNORECASE)
            if len(addr_split) > 1:
                data_dict['city'] = addr_split[-1].strip()
            else:
                data_dict['city'] = city_part
            
            data_dict['state'] = m.group(2).strip()
            return True
            
    return False

def clean_meet_date(date_str):
    if not date_str: return None, None
    try:
        parts = date_str.split(',')
        if len(parts) < 2: return None, None
        year = parts[-1].strip()
        main = parts[0].strip()
        if "-" in main:
            start_d, end_d = main.split("-")
            start = datetime.strptime(f"{start_d}, {year}", "%B %d, %Y").date()
            try:
                prefix = start_d.split()[0]
                end = datetime.strptime(f"{prefix} {end_d}, {year}", "%B %d, %Y").date()
            except: end = datetime.strptime(f"{end_d}, {year}", "%B %d, %Y").date()
            return start, end
        else:
            return datetime.strptime(f"{main}, {year}", "%B %d, %Y").date(), None
    except: return None, None

def convert_to_seconds(mark_str):
    if not mark_str: return None
    clean = re.sub(r'[a-zA-Z]', '', mark_str).strip()
    try:
        parts = clean.split(':')
        if len(parts) == 1: return float(parts[0])
        elif len(parts) == 2: return float(parts[0]) * 60 + float(parts[1])
        elif len(parts) == 3: return float(parts[0]) * 3600 + float(parts[1]) * 60 + float(parts[2])
    except: return None
    return None