from scrapers.track_parser import fetch_track_meet
from scrapers.xc_parser import fetch_xc_meet
import logging
from datetime import date
import re

async def process_single_meet(db, url, season_arg):
    # 1. Fetch Data
    if "/xc/" in url or season_arg == "xc":
        data = await fetch_xc_meet(url)
        season_to_save = "xc"
    else:
        data = await fetch_track_meet(url)
        detected = data.get('meet_info', {}).get('season')
        season_to_save = detected if detected else season_arg

    if not data:
        logging.error(f"Skipping {url} due to download failure.")
        return

    # 2. Save Meet
    meet_info = data['meet_info']
    if not meet_info.get('date_start'): meet_info['date_start'] = date(2025, 1, 1)
    if 'date_end' not in meet_info: meet_info['date_end'] = None

    meet_id = await db.save_meet(meet_info, season_to_save)

    # 3. Process Results
    clean_performances = []
    
    for row in data['results']:
        # SAFETY CHECK: Skip if mark_raw is somehow null
        if not row.get('mark_raw'):
            continue

        # Handle Team ID safely
        team_id = None
        if row.get('team_name'):
            team_id = await db.get_or_create_team(row['team_name'], row['team_slug'])
            
            # DEBUG: Diagnose why team_id is always 1
            if team_id == 1:
                print(f"[DEBUG] Team ID 1 assigned to: {row['team_name']} (Slug: {row.get('team_slug')})")

        athlete_id = await db.get_or_create_athlete(
            row['athlete_name'], 
            row['tfrrs_id'],
            team_id,
            row.get('gender'),
            row.get('url_slug')
        )

        event_meta = normalize_event(row['event_name'])
        
        final_gender = row.get('gender')
        if not final_gender and event_meta['gender']:
            final_gender = event_meta['gender']
        
        # Get/Create Abstract Event
        event_id = await db.get_or_create_event(
            row['event_name'],
            event_meta['std_name'],
            event_meta['distance'],
            final_gender,
            event_meta['is_field'],
            event_meta['is_relay']
        )
        
        # Get/Create Specific Race
        race_id = await db.get_or_create_race(
            meet_id, 
            event_id, 
            event_meta['round'], 
            event_meta['section'], 
            final_gender
        )

        clean_performances.append((
            athlete_id,
            meet_id,
            team_id,
            race_id,
            row['mark_raw'],
            row.get('mark_seconds'),
            row.get('mark_metric'),
            row.get('wind', 0.0),
            clean_place(row['place'])
        ))

    await db.save_performance_batch(clean_performances)
    logging.info(f"Saved {len(clean_performances)} rows from {url}")

def normalize_event(raw_name):
    name = raw_name.lower().strip()
    meta = {
        'std_name': raw_name, 'distance': None,
        'is_relay': False, 'is_field': False,
        'gender': None, 'round': 'Finals', 'section': None
    }
    if "men" in name and "women" not in name: meta['gender'] = 'M'
    elif "women" in name: meta['gender'] = 'F'
    
    # Improved Relay Detection
    if any(x in name for x in ['relay', '4x', 'dmr', 'smr', 'distance medley', 'shuttle']): 
        meta['is_relay'] = True
    
    # Improved Field Detection
    if any(x in name for x in ['jump', 'vault', 'throw', 'shot', 'discus', 'hammer', 'javelin', 'heptathlon', 'decathlon', 'pentathlon', 'pole', 'weight']): 
        meta['is_field'] = True
    
    if "prelim" in name: meta['round'] = "Prelims"
    elif "semi" in name: meta['round'] = "Semis"
    elif "qual" in name: meta['round'] = "Qualifying"
    
    # IMPROVED SECTION REGEX
    sec_match = re.search(r'(?:heat|section|flight|sec|h|f)\s*(\d+)', name)
    if sec_match: meta['section'] = int(sec_match.group(1))

    # Clean the name to find distance
    clean = re.sub(r"(men's|women's|prelims|finals|semifinals|qualifying|heat|section|flight|invitational|open|seeded|unseeded|dash|run|hurdles|championship|of america)", "", name).strip()
    clean = re.sub(r"\s+", " ", clean)
    
    # --- MILE FIX ---
    if "mile" in clean and not meta['is_relay']:
        meta['distance'] = 1609
        if "women" in name: meta['std_name'] = "Women's Mile" 
        else: meta['std_name'] = "Mile" 
    else:
        # Distance Regex
        dist_match = re.search(r'(\d+)(?:m|k|km|meters)?', clean)
        if dist_match and not meta['is_relay'] and not meta['is_field']:
            val = int(dist_match.group(1))
            if 'k' in name or (val < 40 and val > 0): 
                meta['distance'] = val * 1000
            else: 
                meta['distance'] = val
             
    if meta['is_relay']:
        if "4x400" in name: meta['std_name'] = "4x400"
        elif "4x100" in name: meta['std_name'] = "4x100"
        elif "4x200" in name: meta['std_name'] = "4x200"
        elif "4x800" in name: meta['std_name'] = "4x800"
        elif "4x1500" in name: meta['std_name'] = "4x1500"
        elif "dmr" in name or "distance medley" in name: meta['std_name'] = "DMR"
        elif "sprint medley" in name or "smr" in name: meta['std_name'] = "SMR"
        elif "shuttle" in name: meta['std_name'] = "Shuttle Hurdle"
        # --- NEW 4xMile Fix ---
        elif "4xmile" in name or "4 x 1 mile" in name: meta['std_name'] = "4xMile"
    elif meta['is_field']: 
        if "high" in name: meta['std_name'] = "High Jump"
        elif "pole" in name: meta['std_name'] = "Pole Vault"
        elif "long" in name: meta['std_name'] = "Long Jump"
        elif "triple" in name: meta['std_name'] = "Triple Jump"
        elif "shot" in name: meta['std_name'] = "Shot Put"
        elif "discus" in name: meta['std_name'] = "Discus"
        elif "hammer" in name: meta['std_name'] = "Hammer"
        elif "javelin" in name: meta['std_name'] = "Javelin"
        elif "weight" in name: meta['std_name'] = "Weight Throw"
        else: meta['std_name'] = clean.title()
    elif meta['distance']:
        meta['std_name'] = f"{int(meta['distance'])}m"
        if "hurdle" in name: meta['std_name'] += "H"
        if "steep" in name: meta['std_name'] += " SC"
    
    if meta['std_name'] and len(meta['std_name']) > 50:
         meta['std_name'] = meta['std_name'][:50]

    return meta

def clean_place(place_str):
    if not place_str: return None
    if isinstance(place_str, int): return place_str
    if place_str.isdigit(): return int(place_str)
    match = re.match(r'^(\d+)', place_str)
    if match: return int(match.group(1))
    return None