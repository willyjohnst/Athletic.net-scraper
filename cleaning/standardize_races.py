from db_connection import Database
import asyncpg
import asyncio
import re
 
# MAPPING TABLE
# Maps cleaned event name fragments -> standard_events.name
# This is the single source of truth. Add new mappings here.
EVENT_MAP = {
    #  Sprints 
    "50 meters":        "50m",
    "50 meter":         "50m",
    "50m":              "50m",
    "55 meters":        "55m",
    "55 meter":         "55m",
    "55m":              "55m",
    "60 meters":        "60m",
    "60 meter":         "60m",
    "60m":              "60m",
    "70 meters":        "70m",
    "70 meter":         "70m",
    "70m":              "70m",
    "100 meters":       "100m",
    "100 meter":        "100m",
    "100m":             "100m",
    "200 meters":       "200m",
    "200 meter":        "200m",
    "200m":             "200m",
    "300 meters":       "300m",
    "300 meter":        "300m",
    "300m":             "300m",
    "400 meters":       "400m",
    "400 meter":        "400m",
    "400m":             "400m",

    #  Mid-distance 
    "500 meters":       "500m",
    "500 meter":        "500m",
    "500m":             "500m",
    "600 meters":       "600m",
    "600 meter":        "600m",
    "600m":             "600m",
    "800 meters":       "800m",
    "800 meter":        "800m",
    "800m":             "800m",
    "1000 meters":      "1000m",
    "1000 meter":       "1000m",
    "1000m":            "1000m",
    "1200 meters":      "1200m",
    "1200 meter":       "1200m",
    "1200m":            "1200m",
    "1500 meters":      "1500m",
    "1500 meter":       "1500m",
    "1500m":            "1500m",
    "1600 meters":      "1600m",
    "1600 meter":       "1600m",
    "1600m":            "1600m",
    "mile":             "1 Mile",
    "1 mile":           "1 Mile",
    "one mile":         "1 Mile",
    "mile run":         "1 Mile",

    #  Distance 
    "2400 meters":      "2400m",
    "2400 meter":       "2400m",
    "2400m":            "2400m",
    "3000 meters":      "3000m",
    "3000 meter":       "3000m",
    "3000m":            "3000m",
    "3200 meters":      "3200m",
    "3200 meter":       "3200m",
    "3200m":            "3200m",
    "2 mile":           "2 Mile",
    "2 miles":          "2 Mile",
    "two mile":         "2 Mile",
    "two miles":        "2 Mile",
    "2 mile run":       "2 Mile",
    "5000 meters":      "5000m",
    "5000 meter":       "5000m",
    "5000m":            "5000m",
    "5k":               "5000m",
    "10000 meters":     "10000m",
    "10000 meter":      "10000m",
    "10000m":           "10000m",
    "10k":              "10000m",

    #  Steeplechase 
    "2000 steeplechase":    "2000m Steeplechase",
    "2000m steeplechase":   "2000m Steeplechase",
    "2k steeplechase":      "2000m Steeplechase",
    "3000 steeplechase":    "3000m Steeplechase",
    "3000m steeplechase":   "3000m Steeplechase",
    "3k steeplechase":      "3000m Steeplechase",
    "steeplechase":         "3000m Steeplechase",

    #  Hurdles 
    "55m hurdles":      "55m Hurdles",
    "55 hurdles":       "55m Hurdles",
    "60m hurdles":      "60m Hurdles",
    "60 hurdles":       "60m Hurdles",
    "65m hurdles":      "65m Hurdles",
    "65 hurdles":       "65m Hurdles",
    "75m hurdles":      "75m Hurdles",
    "75 hurdles":       "75m Hurdles",
    "80m hurdles":      "80m Hurdles",
    "80 hurdles":       "80m Hurdles",
    "100m hurdles":     "100m Hurdles",
    "100 hurdles":      "100m Hurdles",
    "110m hurdles":     "110m Hurdles",
    "110 hurdles":      "110m Hurdles",
    "200m hurdles":     "200m Hurdles",
    "200 hurdles":      "200m Hurdles",
    "300m hurdles":     "300m Hurdles",
    "300 hurdles":      "300m Hurdles",
    "400m hurdles":     "400m Hurdles",
    "400 hurdles":      "400m Hurdles",

    # Shuttle hurdles
    "4x55 shuttle hurdles":     "4x55 Shuttle Hurdles",
    "4x55m shuttle hurdles":    "4x55 Shuttle Hurdles",
    "4x100 shuttle hurdles":    "4x100 Shuttle Hurdles",
    "4x100m shuttle hurdles":   "4x100 Shuttle Hurdles",
    "4x102.5 shuttle hurdles":  "4x102.5 Shuttle Hurdles",
    "4x102.5m shuttle hurdles": "4x102.5 Shuttle Hurdles",
    "4x110 shuttle hurdles":    "4x110 Shuttle Hurdles",
    "4x110m shuttle hurdles":   "4x110 Shuttle Hurdles",
    "shuttle hurdle relay":     "4x110 Shuttle Hurdles",
    "shuttle hurdles":          "4x110 Shuttle Hurdles",

    #  Relays 
    "4x100 relay":      "4x100m Relay",
    "4x100m relay":     "4x100m Relay",
    "4x100 throwers relay": "4x100 Throwers Relay",
    "4x100m throwers relay": "4x100 Throwers Relay",
    "4x160 relay":      "4x160m Relay",
    "4x160m relay":     "4x160m Relay",
    "4x200 relay":      "4x200m Relay",
    "4x200m relay":     "4x200m Relay",
    "4x400 relay":      "4x400m Relay",
    "4x400m relay":     "4x400m Relay",
    "4x800 relay":      "4x800m Relay",
    "4x800m relay":     "4x800m Relay",
    "4x1500 relay":     "4x1500m Relay",
    "4x1500m relay":    "4x1500m Relay",
    "4x1600 relay":     "4x1600m Relay",
    "4x1600m relay":    "4x1600m Relay",
    "4xmile relay":     "4xMile Relay",
    "4x mile relay":    "4xMile Relay",
    "dmr":              "DMR",
    "distance medley relay":    "DMR",
    "distance medley":          "DMR",
    "smr":              "SMR",
    "sprint medley relay":      "SMR",
    "sprint medley":            "SMR",
    "smr 1600m":        "SMR",
    "smr 1600":         "SMR",
    "smr 800m":         "SMR 800m",
    "smr 800":          "SMR 800m",
    "dmr 4000m":        "DMR",
    "dmr 4000":         "DMR",
    "dmr 4800m":        "DMR",
    "dmr 4800":         "DMR",

    #  Yard distances (older meets) 
    "100 yard dash":    "100 Yards",
    "100 yards":        "100 Yards",
    "220 yards":        "220 Yards",
    "440 yards":        "440 Yards",
    "880 yards":        "880 Yards",
    "120y hurdles":     "120y Hurdles",
    "120 yard hurdles": "120y Hurdles",
    "4x440 yard relay": "4x440 Yard Relay",
    "4x440 relay":      "4x440 Yard Relay",
    "4x220 yard relay": "4x220 Yard Relay",
    "4x220 relay":      "4x220 Yard Relay",

    #  Field events 
    "high jump":        "High Jump",
    "pole vault":       "Pole Vault",
    "long jump":        "Long Jump",
    "triple jump":      "Triple Jump",
    "shot put":         "Shot Put",
    "discus":           "Discus",
    "discus throw":     "Discus",
    "hammer":           "Hammer Throw",
    "hammer throw":     "Hammer Throw",
    "javelin":          "Javelin",
    "javelin throw":    "Javelin",
    "weight throw":     "Weight Throw",
    "weight":           "Weight Throw",
}

PREFIX_PHRASES = [
    "junior varsity", "junior high",
    "high school", "middle school",
    "frosh/soph", "frosh-soph", "fr/so", "freshman/sophomore",
    "sub-bantam", "sub bantam",
    "new division",
]

PREFIX_WORDS = {
    # Gender
    "mens", "men", "womens", "women", "boys", "boy", "girls", "girl",
    "male", "female",
    # Competition level
    "varsity", "junior", "jv", "frosh", "soph", "freshman", "sophomore",
    "collegiate", "college", "university",
    "school", "middle", "elementary", "jr",
    # Ordinals and grade
    "1st", "2nd", "3rd", "4th", "5th", "6th", "7th", "8th", "9th",
    "10th", "11th", "12th", "grade",
    # USATF age divisions
    "youth", "senior", "intermediate", "midget", "bantam",
    # Division/classification
    "new", "division", "div", "class", "group",
    # Round/result labels
    "results", "finals", "prelim", "prelims", "preliminaries",
    "qualifying", "trials", "open", "invitational", "championship",
    "elite", "seeded", "unseeded", "section", "heat", "flight",
    # Misc
    "throwers", "thrower", "indoor", "under", "unified"
}

def strip_prefix(name_raw: str) -> str:
    """
    Strips all non-event prefix words from the front of an event name.
    Handles: "Frosh/Soph Mens 100 Meters" -> "100 Meters"
             "8 & Under Womens 200 Meters" -> "200 Meters"
             "Division 1 Finals Mens Shot Put" -> "Shot Put"
             "High School Mens High Jump" -> "High Jump"
    """
    # Normalize separators: "Frosh/Soph" -> "Frosh Soph", "8 & Under" -> "8 Under"
    s = name_raw.replace("/", " ").replace("&", " ")
    # Collapse whitespace
    s = re.sub(r'\s+', ' ', s).strip()
    
    # Phase 1: Strip multi-word phrases from the front first
    # This prevents "high" from being stripped out of "High Jump"
    # by removing "High School" as a unit before single-word processing
    changed = True
    while changed:
        changed = False
        s_lower = s.lower()
        for phrase in PREFIX_PHRASES:
            if s_lower.startswith(phrase + " ") or s_lower.startswith(phrase):
                s = s[len(phrase):].strip()
                s_lower = s.lower()
                changed = True
    
    # Phase 2: Strip single prefix words from the front
    words = s.split(' ')
    i = 0
    while i < len(words):
        w = words[i].lower().rstrip(".,;:")
        if w in PREFIX_WORDS:
            i += 1
        elif re.match(r'^\d+$', w):
            # Bare number — STOP if the next word looks like part of 
            # an event (meters, relay, hurdles, jump, put, vault, etc.)
            if i + 1 < len(words):
                next_w = words[i + 1].lower()
                if next_w in ('meters', 'meter', 'miles', 'mile', 'yards', 'yard',
                              'steeplechase', 'hurdles', 'hurdle',
                              'jump', 'put', 'vault', 'throw') or next_w.startswith('x'):
                    break
            i += 1
        elif re.match(r'^\d+-\d+$', w):
            # Age range like "9-10"
            i += 1
        else:
            break
    
    return ' '.join(words[i:]).strip()

# Events to skip entirely
# Multi-event umbrella entries and racewalks — sub-events of multi-events
# are handled by the prefix stripper above (e.g. "Decathlon Mens 100 Meters"
# strips "Decathlon Mens" and matches "100 Meters" -> "100m")
SKIP_PATTERNS = re.compile(
    r'^(?:.*\s)?'  # optional prefix
    r'(?:'
    r'(?:decathlon|heptathlon|pentathlon|indoor pentathlon|pentathlon \(indoor\)|pentathlon \(outdoor\))\s*$'
    r'|'
    r'(?:.*(?:race\s*walk|racewalk))'
    r')',
    re.IGNORECASE
)


def classify_event(name_raw: str) -> str | None:
    """
    Takes a raw event name from the races table and returns 
    the matching standard_events.name, or None if no match.
    
    Returns None for multi-event umbrella entries, racewalks, 
    and unrecognized events.
    """
    if not name_raw:
        return None

    raw = name_raw.strip()

    # Skip explicit unknowns
    if raw.lower() in ('unknown event', 'unknown', 'tba', 'tbd'):
        return None

    # Skip multi-event umbrellas (bare "Decathlon", "Heptathlon", etc.) and racewalks
    if SKIP_PATTERNS.match(raw):
        return None

    # Strip the level/gender prefix to get the core event name
    core = strip_prefix(raw)

    # Normalize: lowercase, collapse whitespace, strip commas from numbers
    core_lower = re.sub(r'\s+', ' ', core.lower()).strip()
    core_lower = re.sub(r'(\d),(\d)', r'\1\2', core_lower)  # "10,000" -> "10000"

    # Direct lookup
    if core_lower in EVENT_MAP:
        return EVENT_MAP[core_lower]

    # Try without trailing " run" / " dash" / " race"
    for suffix in (' run', ' dash', ' race'):
        if core_lower.endswith(suffix):
            without = core_lower[:-len(suffix)].strip()
            if without in EVENT_MAP:
                return EVENT_MAP[without]

    # Try singular "meter" -> "meters" (e.g. "800 Meter" -> "800 meters")
    core_meters = re.sub(r'(\d)\s*meter\b', r'\1 meters', core_lower)
    if core_meters in EVENT_MAP:
        return EVENT_MAP[core_meters]

    # Handle "meter(s)" variants with "run"/"dash" suffix
    for suffix in (' run', ' dash', ' race'):
        if core_meters.endswith(suffix):
            without = core_meters[:-len(suffix)].strip()
            if without in EVENT_MAP:
                return EVENT_MAP[without]

    # Handle "k" abbreviation: "3k" -> "3000m", "5k" already mapped
    k_match = re.match(r'^(\d+)k$', core_lower)
    if k_match:
        distance = int(k_match.group(1)) * 1000
        candidate = f"{distance} meters"
        if candidate in EVENT_MAP:
            return EVENT_MAP[candidate]

    return None


async def standardize_event_names(dry_run: bool = True, batch_size: int = 5000):
    """
    Reads all races with NULL standard_event_id, classifies them,
    and updates the database in batches.
    """
    db = Database()
    await db.connect()

    unstandardized_races = await db.get_unstandardized_events()
    print(f"Found {len(unstandardized_races)} unstandardized races")

    # Load standard_events lookup: name -> id
    standard_events = await db.get_audit_query(
        "SELECT id, name FROM standard_events"
    )
    event_name_to_id = {row['name']: row['id'] for row in standard_events}
    print(f"Loaded {len(event_name_to_id)} standard events: {list(event_name_to_id.keys())}")

    matched = 0
    unmatched = 0
    skipped = 0
    mapped_but_missing = {}  # mapped to a name that's not in standard_events DB
    truly_unmatched = {}     # no mapping found at all
    updates = []

    for race in unstandardized_races:
        race_id = race['id']
        name_raw = race['name_raw']

        standard_name = classify_event(name_raw)

        if standard_name is None:
            skipped += 1
            key = name_raw.strip()
            truly_unmatched[key] = truly_unmatched.get(key, 0) + 1
        elif standard_name in event_name_to_id:
            standard_event_id = event_name_to_id[standard_name]
            updates.append((standard_event_id, race_id))
            matched += 1
        else:
            unmatched += 1
            mapped_but_missing[standard_name] = mapped_but_missing.get(standard_name, 0) + 1

    # Report
    total = matched + unmatched + skipped
    print(f"\n{'='*60}")
    print(f"RESULTS: {matched}/{total} matched ({100*matched/total:.1f}%)")
    print(f"         {skipped}/{total} skipped ({100*skipped/total:.1f}%)")
    print(f"         {unmatched}/{total} mapped but missing from DB ({100*unmatched/total:.1f}%)")

    if mapped_but_missing:
        print(f"\n--- Events mapped but NOT in standard_events table (add these): ---")
        for name, count in sorted(mapped_but_missing.items(), key=lambda x: -x[1]):
            print(f"  {count:>8}  {name}")

    if truly_unmatched:
        print(f"\n--- Top 50 truly unmatched (no mapping found): ---")
        for name, count in sorted(truly_unmatched.items(), key=lambda x: -x[1])[:50]:
            print(f"  {count:>8}  {name}")

    # Write to DB
    if not dry_run and updates:
        print(f"\nWriting {len(updates)} updates in batches of {batch_size}...")
        for i in range(0, len(updates), batch_size):
            batch = updates[i:i+batch_size]
            await db.pool.executemany(
                "UPDATE races SET standard_event_id = $1 WHERE id = $2",
                batch
            )
            print(f"  Batch {i//batch_size + 1}: updated {len(batch)} races")
        print("Done.")
    elif dry_run:
        print("\n[DRY RUN] No changes written. Run with dry_run=False to apply.")

    await db.close()


