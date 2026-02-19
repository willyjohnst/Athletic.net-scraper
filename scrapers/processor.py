import logging
from scrapers.parser import AthleticNetParser

async def process_single_meet(db, url_or_id):
    parser = AthleticNetParser()
    
    meet_id = url_or_id
    if "athletic.net" in str(url_or_id):
        # Extract ID logic here if needed, assuming generic ID passed for now
        meet_id = url_or_id.split("/")[-1] 

    # 2. Fetch Data (Returns List[StandardResult])
    results = await parser.fetch_meet_results(meet_id)
    
    if not results:
        logging.error(f"No results found for {meet_id}")
        return

    # 3. Save Meet (Using the first result to get metadata)
    # Ideally, fetch_meet_results returns (meet_metadata, results), but for now we extract from first row
    first = results[0]
    meet_info = {
        "name": first.meet_name,
        "url": first.meet_url,
        "date_start": first.date_start,
        "date_end": first.date_end,
        "venue": first.venue_name,
        "city": first.venue_city,
        "state": first.venue_state
    }
    
    # Save Meet to DB
    db_meet_id = await db.save_meet(meet_info, first.season)

    clean_performances = []
    
    for row in results:
        # 4. Handle Team
        team_id = await db.get_or_create_team(row.team_name, row.team_slug)

        # 5. Handle Athlete (LINKAGE LOGIC)
        # We pass the 'source_id' (Athletic.net ID) to the DB
        # The DB should check the aliases table for this ID.
        athlete_id = await db.get_or_create_athlete(
            name=row.athlete_name, 
            external_id=row.source_id, # Crucial: Pass the source ID
            source_name=row.site_id,   # Crucial: "athletic.net"
            team_id=team_id,
            gender=row.gender
        )

        # 6. Handle Event / Race
        # (Assuming you have logic to map "Varsity Boys 100m" -> "100m")
        # For now, simplistic usage:
        race_id = await db.get_or_create_race_context(
            meet_id=db_meet_id,
            event_name=row.event_name,
            round="Final", # Parser needs to improve round detection
            heat=row.heat_data,
            gender=row.gender
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
            row.heat_place # Added Heat Place
        ))

    await db.save_performance_batch(clean_performances)
    logging.info(f"Saved {len(clean_performances)} results from {meet_id}")