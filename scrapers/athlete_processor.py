import logging
import asyncio
from datetime import datetime
from datetime import date
from scrapers.athlete_parser import AthleteParser
from scrapers.parser import AthleticNetParser  # Need this for the mark parser

logger = logging.getLogger("AthleteProcessor")

async def process_single_athlete(db, internal_id, athletic_net_id):
    api_parser = AthleteParser()
    mark_parser = AthleticNetParser() # We just need this for _parse_mark()
    
    full_payload = await api_parser.fetch_athlete_bio(athletic_net_id)
    if not full_payload: return

    tf_data = full_payload.get('tf', {})
    xc_data = full_payload.get('xc', {})

    # --- 1. RUN YOUR EXISTING TIMELINE LOGIC ---
    grades_tf = tf_data.get('grades', {})
    grades_xc = xc_data.get('grades', {})

    # FIX: Use colons to create a proper dictionary
    grades_dict = {'xc': grades_xc, 'tf': grades_tf}
        
    # --- 1. NORMALIZE SEASONS INTO ACADEMIC YEARS ---
    academic_years = {}

    def init_ay(ay):
        if ay not in academic_years:
            academic_years[ay] = {'grade': 0, 'xc': False, 'indoor': False, 'outdoor': False, 'teams': set()}

    # Parse XC (Fall) -> Shift year + 1 to align with Spring Academic Year
    for key, grade_val in grades_dict.get('xc', {}).items():
        if grade_val >= 400 or grade_val in (0, 99): continue
        parts = key.split('_')
        if len(parts) != 2: continue
        
        ay = int(parts[1]) + 1
        init_ay(ay)
        academic_years[ay]['grade'] = max(academic_years[ay]['grade'], grade_val)
        academic_years[ay]['xc'] = True
        if int(parts[0]) != 0: academic_years[ay]['teams'].add(int(parts[0]))

    # Parse TF (Winter/Spring)
    for key, grade_val in grades_dict.get('tf', {}).items():
        if grade_val >= 400 or grade_val in (0, 99): continue
        parts = key.split('_')
        if len(parts) != 2: continue
        
        season_code = int(parts[1])
        if season_code > 10000:
            ay = season_code % 10000
            season_str = 'indoor'
        else:
            ay = season_code
            season_str = 'outdoor'
            
        init_ay(ay)
        academic_years[ay]['grade'] = max(academic_years[ay]['grade'], grade_val)
        academic_years[ay][season_str] = True
        if int(parts[0]) != 0: academic_years[ay]['teams'].add(int(parts[0]))

    # --- 2. INITIALIZE TRACKING VARIABLES ---
    timeline = {
        "hs_start": None, "hs_end": None, "hs_grad": False,
        "col_start": None, "col_end": None, "col_grad": False,
        "is_transfer": False, "has_gap_year": False, "covid_years": 0,
        "rs_xc": False, "rs_indoor": False, "rs_outdoor": False,
        "current_level": "None"
    }
    
    sorted_years = sorted(academic_years.keys())
    previous_ay = None
    previous_grade = None
    all_college_teams = set()
    
    # --- 3. THE ELIGIBILITY ENGINE ---
    for ay in sorted_years:
        data = academic_years[ay]
        g = data['grade']
        
        # High School (9-12)
        if 9 <= g <= 12:
            if not timeline["hs_start"] or g == 9: timeline["hs_start"] = ay
            if not timeline["hs_end"] or ay > timeline["hs_end"]: timeline['hs_end'] = ay
            if g == 12: timeline["hs_grad"] = True
            timeline["current_level"] = "HS"
            
        # College (21-25)
        elif 21 <= g <= 25:
            if not timeline["col_start"] or g == 21: timeline["col_start"] = ay
            if not timeline["col_end"] or g < timeline["col_end"]: timeline["col_end"] = ay
            if g >= 24: timeline["col_grad"] = True
            timeline["current_level"] = "College"
            all_college_teams.update(data['teams'])
            
            # Specific Redshirt Detection
            if not data['xc']: timeline["rs_xc"] = True
            if not data['indoor']: timeline["rs_indoor"] = True
            if not data['outdoor']: timeline["rs_outdoor"] = True
            
            # Outlier Detection
            if previous_ay:
                if (ay - previous_ay > 1):
                    timeline["has_gap_year"] = True
                    timeline["rs_xc"] = True
                    timeline["rs_indoor"] = True
                    timeline["rs_outdoor"] = True
                    
                if (g == previous_grade) and (ay - previous_ay == 1):
                    if ay == 2021: 
                        timeline["covid_years"] += 1
                        
        previous_ay = ay
        previous_grade = g

    if len(all_college_teams) > 1:
        timeline["is_transfer"] = True
        
    # --- 3b. SPRINTER / THROWER CORRECTION ---
    college_xc_seasons = sum(
        1 for ay, data in academic_years.items() 
        if 21 <= data['grade'] <= 25 and data['xc']
    )
    
    if college_xc_seasons == 0:
        timeline["rs_xc"] = False
                
    # --- 4. BACK-CALCULATE "UNKNOWN" YEARS ---
    if timeline["col_start"] and not timeline["hs_grad"]:
        timeline["hs_end"] = timeline["col_start"] - 1
        timeline["hs_grad"] = True
        timeline["hs_start"] = timeline["hs_end"] - 3 



    # --- 5. EXTRACT MEETS & EVENTS ---
    meets_dict = tf_data.get("meets", {})
    meets_dict.update(xc_data.get("meets", {}))

    events_dict = {}
    for e in tf_data.get("eventsTF", []) or []:
        events_dict[str(e["IDEvent"])] = e["Event"]
    for e in xc_data.get("eventsXC", []) or []:
        events_dict[str(e["IDEvent"])] = e["Event"]

    # --- 6. COMBINE ALL RESULTS ---
    all_results = []
    if tf_data.get("resultsTF"): all_results.extend(tf_data["resultsTF"])
    if xc_data.get("resultsXC"): all_results.extend(xc_data["resultsXC"])

    clean_performances = []

    # --- 7. THE RACE PARSING ENGINE ---
    for res in all_results:
        meet_id_raw = str(res.get("MeetID"))
        event_id_raw = str(res.get("EventID"))
        
        meet_info = meets_dict.get(meet_id_raw, {})
        event_name = events_dict.get(event_id_raw, "Unknown Event")
        
        mark_raw = str(res.get("Result", "")).strip()
        parsed_mark = mark_parser._parse_mark(mark_raw)
        
        meet_date_str = meet_info.get("EndDate", "2025-01-01T00:00:00").split('T')[0]
        meet_date = datetime.strptime(meet_date_str, "%Y-%m-%d").date()
        
        meet_payload = {
            "name": meet_info.get("MeetName", "Unknown Meet"),
            "url": f"https://www.athletic.net/TrackAndField/meet/{meet_id_raw}",
            "date_start": meet_date,
            "date_end": meet_date,
            "venue_id": None, 
            "facility_type": "Outdoor Track" if tf_data else "XC Course",
            "altitude": 0.0
        }
        
        db_meet_id = await db.save_meet(meet_payload, "outdoor")
        
        race_id = await db.get_or_create_race_context(
            meet_id=db_meet_id,
            event_name=event_name,
            round_name=res.get("Round", "F"),
            heat=1, 
            gender=res.get("Gender", "M"), 
            wind=res.get("Wind")
        )
        
        # 4c. Append Performance (Must be exactly 10 elements!)
        clean_performances.append((
            internal_id,                            # p[0]: athlete_id
            db_meet_id,                             # p[1]: meet_id (skipped by db loop)
            None,                                   # p[2]: team_id (we can map SchoolID later)
            race_id,                                # p[3]: race_id
            mark_raw,                               # p[4]: mark_raw
            parsed_mark['seconds'],                 # p[5]: mark_seconds
            parsed_mark['metric'],                  # p[6]: mark_metric
            float(res.get("Wind")) if res.get("Wind") else 0.0, # p[7]: wind (skipped by db loop)
            mark_parser._parse_place(res.get("Place")),         # p[8]: overall_place
            None                                    # p[9]: heat_place
        ))

    college = False
    for key, grade_val in grades_dict.get('xc', {}).items():
          if grade_val > 12 and grade_val < 30:
              college = True
    for key, grade_val in grades_dict.get('tf', {}).items():
        if grade_val > 12 and grade_val < 30:
            college = True

    # --- 8. BATCH SAVE PERFORMANCES ---
    if clean_performances:
        await db.save_athlete_timeline(internal_id, timeline)
        await db.save_performance_batch(clean_performances)
        print(f"Saved Timeline: {timeline} for {athletic_net_id}")
        print(f"{datetime.now().time()}. Saved {len(clean_performances)} races for {athletic_net_id}. College status: {college}")
        logger.info(f"Saved {len(clean_performances)} races and timeline: {timeline} for {athletic_net_id}")


