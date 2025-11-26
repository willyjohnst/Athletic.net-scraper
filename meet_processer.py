import httpx
from .track_parser import parse_track_meet
from .xc_parser import parse_xc_meet

async def process_single_meet(db, url, season):
    """
    Decides which parser to use and saves data.
    """
    async with httpx.AsyncClient() as client:
        response = await client.get(url, timeout=20.0)
        
    if response.status_code != 200:
        raise Exception(f"HTTP {response.status_code}")

    html_content = response.text

    # Logic Fork: Polymorphic parsing based on URL or content
    if "/xc/" in url or season == "xc":
        data = parse_xc_meet(html_content, url)
    else:
        data = parse_track_meet(html_content, url)

    # Data structure returned from parsers:
    # {
    #   "meet_info": {...},
    #   "results": [ {...performance objects...} ]
    # }

    # Save Meet Metadata first
    meet_id = await db.save_meet(data['meet_info'])

    # Save Athletes and Performances
    # We must resolve athlete_ids and team_ids before inserting performances
    clean_performances = []
    
    for row in data['results']:
        # Check/Create Team
        team_id = await db.get_or_create_team(row['team_name'], row['team_slug'])
        
        # Check/Create Athlete
        athlete_id = await db.get_or_create_athlete(
            row['athlete_name'], 
            row['tfrrs_id'], # The crucial immutable ID
            team_id
        )

        clean_performances.append((
            athlete_id,
            meet_id,
            team_id,
            row['event_id'], # You'd need an event lookup here
            row['mark_raw'],
            row['mark_seconds'],
            row['place'],
            data['meet_info']['date']
        ))

    await db.save_performance_batch(clean_performances)
    print(f"Saved {len(clean_performances)} rows from {url}")