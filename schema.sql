-- TFRRS Analytics Database Schema (Normalized)

CREATE TYPE season_type AS ENUM ('xc', 'indoor', 'outdoor');
CREATE TYPE gender_type AS ENUM ('M', 'F', 'X');
CREATE TYPE performance_status AS ENUM ('OK', 'DNS', 'DNF', 'DQ', 'FS', 'FOUL', 'NM');

-- 1. TEAMS
CREATE TABLE IF NOT EXISTS teams (
    team_id SERIAL PRIMARY KEY,
    tfrrs_slug VARCHAR(255) UNIQUE NOT NULL,
    name VARCHAR(255) NOT NULL,
    current_division VARCHAR(50), 
    state VARCHAR(10),
    conference VARCHAR(100), -- NEW: Conference tracking
    scraped BOOLEAN DEFAULT FALSE, -- NEW: Track if we have visited the team page
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- 2. ATHLETES
CREATE TABLE IF NOT EXISTS athletes (
    athlete_id SERIAL PRIMARY KEY,
    tfrrs_id VARCHAR(50) UNIQUE NOT NULL,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    gender gender_type,
    current_team_id INTEGER REFERENCES teams(team_id),
    url_slug VARCHAR(255), 
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- 3. MEETS
CREATE TABLE IF NOT EXISTS meets (
    meet_id SERIAL PRIMARY KEY,
    tfrrs_id VARCHAR(50) UNIQUE NOT NULL,
    name VARCHAR(255) NOT NULL,
    date_start DATE NOT NULL,
    date_end DATE, 
    season season_type,
    year INTEGER NOT NULL,
    venue_name VARCHAR(255),
    venue_city VARCHAR(100),
    venue_state VARCHAR(50),
    facility_type VARCHAR(100),
    created_at TIMESTAMP DEFAULT NOW()
);

-- 4. EVENTS 
CREATE TABLE IF NOT EXISTS events (
    event_id SERIAL PRIMARY KEY,
    name_raw VARCHAR(255) UNIQUE NOT NULL, 
    std_name VARCHAR(50),                  
    distance_meters FLOAT,
    gender gender_type,
    is_field BOOLEAN DEFAULT FALSE,
    is_relay BOOLEAN DEFAULT FALSE
);

-- 5. RACES
CREATE TABLE IF NOT EXISTS races (
    race_id BIGSERIAL PRIMARY KEY,
    meet_id INTEGER REFERENCES meets(meet_id),
    event_id INTEGER REFERENCES events(event_id),
    round_name VARCHAR(50), 
    section_number INTEGER, 
    gender gender_type,
    created_at TIMESTAMP DEFAULT NOW(),
    
    CONSTRAINT uniq_race UNIQUE (meet_id, event_id, round_name, section_number)
);

-- 6. PERFORMANCES
CREATE TABLE IF NOT EXISTS performances (
    perf_id BIGSERIAL PRIMARY KEY,
    meet_id INTEGER REFERENCES meets(meet_id),
    athlete_id INTEGER REFERENCES athletes(athlete_id),
    race_id BIGINT REFERENCES races(race_id), 
    team_id INTEGER REFERENCES teams(team_id),
    
    mark_raw VARCHAR(50) NOT NULL,
    mark_seconds FLOAT,
    mark_metric FLOAT,
    wind_reading FLOAT,
    
    place INTEGER,
    status performance_status DEFAULT 'OK',
    created_at TIMESTAMP DEFAULT NOW(),
    
    CONSTRAINT uniq_perf UNIQUE (athlete_id, race_id)
);

-- 7. RELAY SPLITS
CREATE TABLE IF NOT EXISTS relay_splits (
    split_id BIGSERIAL PRIMARY KEY,
    perf_id BIGINT REFERENCES performances(perf_id) ON DELETE CASCADE,
    athlete_id INTEGER REFERENCES athletes(athlete_id),
    leg_number INTEGER,
    split_seconds FLOAT
);

-- INDEXES
CREATE INDEX idx_perf_athlete ON performances(athlete_id);
CREATE INDEX idx_perf_race ON performances(race_id);
CREATE INDEX idx_races_meet ON races(meet_id);
CREATE INDEX idx_meet_season ON meets(season, year);
CREATE INDEX idx_events_name ON events(name_raw);
CREATE INDEX idx_teams_scraped ON teams(scraped);