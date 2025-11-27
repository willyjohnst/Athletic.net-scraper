from .track_parser import fetch_track_meet
from .xc_parser import fetch_xc_meet
import logging
from datetime import date
import re

async def process_single_meet(db, url, season):
    """
    Decides which fetcher to use and saves data.
    """
    # 1. Fetch Data
    if "/xc/" in url or season == "xc":
        data = await fetch_xc_meet(url)
    else:
        data = await fetch_track_meet(url)

    if not data:
        logging.error(f"Skipping {url} due to download/parse failure.")
        return

    # 2. Save Meet Metadata
    if not data['meet_info'].get('date'):
        data['meet_info']['date'] = date(2025, 1, 1)

    meet_id = await db.save_meet(data['meet_info'], season)

    # 3. Save Athletes and Performances
    clean_performances = []
    
    for row in data['results']:
        team_id = await db.get_or_create_team(row['team_name'], row['team_slug'])
        
        athlete_id = await db.get_or_create_athlete(
            row['athlete_name'], 
            row['tfrrs_id'],
            team_id
        )

        # FIX: Resolve Event ID from the string name
        event_id = await db.get_or_create_event(row['event_name'])

        clean_performances.append((
            athlete_id,
            meet_id,
            team_id,
            event_id,    # Now passing the real integer ID, not None
            row['mark_raw'],
            row.get('mark_seconds'),
            row.get('mark_metric'),
            row.get('wind', 0.0),
            clean_place(row['place'])
        ))

    await db.save_performance_batch(clean_performances)
    print(f"Saved {len(clean_performances)} rows from {url}")

def clean_place(place_str):
    """
    Safely converts place strings to integers.
    """
    if not place_str: return None
    
    if place_str.isdigit():
        return int(place_str)
        
    match = re.match(r'^(\d+)', place_str)
    if match:
        return int(match.group(1))
        
    return None