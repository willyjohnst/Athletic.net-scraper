import logging
import datetime 
from datetime import date
from scrapers.parser import AthleticNetParser

async def process_single_meet(db, url_or_id, season_arg=None):
    # 1. Initialize Parser
    parser = AthleticNetParser()

    meet_id = url_or_id
    if "athletic.net" in str(url_or_id):
        # Extract ID logic here if needed, assuming generic ID passed for now
        meet_id = str(url_or_id).split("/")[-1] 

    # 2. Fetch Data (Returns List[StandardResult])
    # Pass season_arg to help parser with sport context (xc vs tf)
    results = await parser.fetch_meet_results(meet_id, season_hint=season_arg)
    
    if not results:
        logging.error(f"No results found for {meet_id}")
        return

    # Grab metadata from the first result row
    first = results[0]

    # --- 3. VENUE RESOLUTION & RECONCILIATION ---
    v_lat = getattr(first, 'venue_lat', None)
    v_lon = getattr(first, 'venue_lon', None)
    
    # 3a. Resolve the Parent Facility (Campus)
    db_facility = await db.get_or_create_facility(
        name=first.venue_name,
        city=first.venue_city,
        state=first.venue_state,
        lat=v_lat,
        lon=v_lon
    )

    # 3b. Resolve the Child Track (Specific Surface)
    db_track = await db.get_or_create_track(
        facility_id=db_facility['id'],
        facility_type=first.facility_type,
        altitude=first.venue_altitude
    )

    final_facility = first.facility_type
    final_altitude = first.venue_altitude

    db_altitude = db_track.get('altitude_meters')
        
    if db_altitude is not None:
        db_altitude = float(db_altitude)

    # Fill missing Scraped Data with established DB Data
    if not final_facility and db_track['facility_type']:
        final_facility = db_track['facility_type']
            
    if (not final_altitude or final_altitude == 0.0) and db_altitude:
        final_altitude = db_altitude
            
    # Check for Discrepancies and Log them
    if first.facility_type and  db_track['facility_type']:
        if first.facility_type.lower() != db_track['facility_type'].lower():
            logging.warning(
                f"[DISCREPANCY] Venue '{first.venue_name}': "
                f"Scraped facility '{first.facility_type}' differs from DB '{ db_track['facility_type']}'. "
                f"Keeping Scraped data."
            )
                
    if final_altitude and final_altitude > 0 and db_altitude:
        # If the scrape found an altitude that differs by more than 10 meters from the DB
        if abs(final_altitude - db_altitude) > 10:
                logging.warning(
                f"[DISCREPANCY] Altitude for '{first.venue_name}': "
                f"Scraped {final_altitude}m differs from DB {db_altitude}m. "
                f"Keeping Scraped data."
            )

    # 4. Save Meet (Using Reconciled Venue Data)
    meet_info = {
        "name": first.meet_name,
        "url": first.meet_url,
        "date_start": first.date_start or date(2025, 1, 1),
        "date_end": first.date_end,
        "venue_id": db_track['id'],
        "facility_type": db_track['facility_type'],
        "altitude": float(db_track['altitude_meters']) if db_track['altitude_meters'] else 0.0
    }
    
    season_to_save = first.season if first.season else season_arg
    db_meet_id = await db.save_meet(meet_info, season_to_save)

    # 5. Process Results
    clean_performances = []
    
    for row in results:
        if not getattr(row, 'mark_raw', None):
            continue

        # Handle Team safely
        team_id = None
        if row.team_name:
            team_id = await db.get_or_create_team(row.team_name, getattr(row, 'team_slug', None))

        # Handle Athlete
        athlete_id = await db.get_or_create_athlete(
            display_name=row.athlete_name, 
            external_id=getattr(row, 'source_id', None), 
            source_name="athletic_net",  
            team_id=team_id,
            gender=row.gender
        )

        if not athlete_id:
            continue

        # Handle Event / Race
        h_num = getattr(row, 'heat_number', getattr(row, 'heat_data', 1))
        
        race_id = await db.get_or_create_race_context(
            meet_id=db_meet_id,
            event_name=row.event_name,
            round_name="Final", 
            heat=h_num,
            gender=row.gender,
            wind=row.wind
        )

        clean_performances.append((
            athlete_id,
            db_meet_id,
            team_id,
            race_id,
            row.mark_raw,
            row.mark_seconds,
            row.mark_metric,
            row.wind,
            row.overall_place,
            row.heat_place 
        ))

    if clean_performances:
        await db.save_performance_batch(clean_performances)
        print(f"{datetime.datetime.now().time()} Saved {len(clean_performances)} results from {meet_id}")
        logging.info(f"Saved {len(clean_performances)} results from {meet_id}")