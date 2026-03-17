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

    # --- 1. RUN YOUR EXISTING TIMELINE LOGIC ---
    grades_tf = tf_data.get('grades', {})

    # FIX: Use colons to create a proper dictionary
    grades_dict = {'tf': grades_tf}
        
    # --- 1. NORMALIZE SEASONS INTO ACADEMIC YEARS ---
    academic_years = {}

    def init_ay(ay):
        if ay not in academic_years:
            academic_years[ay] = {'grade': 0, 'xc': False, 'indoor': False, 'outdoor': False, 'teams': set()}

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
    all_college_teams = set()
    
    inferred_hs_starts = []
    inferred_hs_ends = []
    inferred_col_starts = []
    max_col_year = None
    max_col_grade = 0
    
    previous_ay = None
    previous_grade = None

    # --- 3. THE ELIGIBILITY ENGINE (Base Pass) ---
    for ay in sorted_years:
        data = academic_years[ay]
        g = data['grade']
        
        # 3a. Gather High School Inferences
        if 9 <= g <= 12:
            inferred_hs_starts.append(ay - (g - 9))
            inferred_hs_ends.append(ay + (12 - g))
            
        # 3b. Gather College Inferences
        elif 21 <= g <= 25:
            inferred_col_starts.append(ay - (g - 21))
            if max_col_year is None or ay > max_col_year: 
                max_col_year = ay
            max_col_grade = max(max_col_grade, g)
            all_college_teams.update(data['teams'])
            
            if not data['indoor']: timeline["rs_indoor"] = True
            if not data['outdoor']: timeline["rs_outdoor"] = True
            
        # 3c. Internal Gap Year / Covid Detection
        if previous_ay:
            if ay - previous_ay > 1:
                timeline["has_gap_year"] = True
            if g == previous_grade and (ay - previous_ay == 1):
                # Grade didn't change but the year advanced
                if ay in (2021, 2022): 
                    timeline["covid_years"] += 1

        previous_ay = ay
        previous_grade = g

    # --- 4. LOGICAL INFERENCES & BACK-CALCULATIONS ---
    
    # Apply HS timelines (taking the earliest start / latest end if data fluctuates)
    if inferred_hs_starts:
        timeline["hs_start"] = min(inferred_hs_starts)
        timeline["hs_end"] = max(inferred_hs_ends)

    # Apply College timelines
    if inferred_col_starts:
        timeline["col_start"] = min(inferred_col_starts)
        # Expected graduation is typically 4 years (start + 3), but adjust if they stayed longer
        timeline["col_end"] = max(timeline["col_start"] + 3, max_col_year)

    # Back-calculate missing HS info if they are in college
    if timeline["col_start"]:
        timeline["hs_grad"] = True # Must have graduated HS to be in college
        if not timeline["hs_start"] or not timeline["hs_end"]:
            # Standard assumption: HS ends the spring before college starts
            timeline["hs_end"] = timeline["col_start"] - 1
            timeline["hs_start"] = timeline["hs_end"] - 3
            
    # Explicit Gap Year Check (Between HS and College)
    if timeline["hs_end"] and timeline["col_start"]:
        # Standard track: HS end 2020 -> Col Start 2021 (Difference of 1)
        if (timeline["col_start"] - timeline["hs_end"]) > 1:
            timeline["has_gap_year"] = True

    # Check Transfers
    if len(all_college_teams) > 1:
        timeline["is_transfer"] = True

    # --- 4c. CURRENT LEVEL & GRADUATION STATUS (Anchor: 2026) ---
    current_year = 2026 
    
    if timeline["hs_end"] and current_year > timeline["hs_end"]:
        timeline["hs_grad"] = True
        
    if (timeline["col_end"] and current_year > timeline["col_end"]) or max_col_grade >= 24:
        timeline["col_grad"] = True

    # Assign Current Level hierarchically
    if timeline["col_start"] and not timeline["col_grad"]:
        timeline["current_level"] = "College"
    elif timeline["hs_start"] and not timeline["hs_grad"]:
        timeline["current_level"] = "HS"
    elif timeline["col_grad"]:
        timeline["current_level"] = "Post-Collegiate"
    elif timeline["hs_grad"] and not timeline["col_start"]:
        timeline["current_level"] = "Post-HS"


    # --- 5. EXTRACT MEETS & EVENTS ---
    meets_dict = tf_data.get("meets", {})

    events_dict = {}
    for e in tf_data.get("eventsTF", []) or []:
        events_dict[str(e["IDEvent"])] = e["Event"]

    # --- 6. COMBINE ALL RESULTS ---
    all_results = []
    if tf_data.get("resultsTF"): all_results.extend(tf_data["resultsTF"])

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


