import asyncpg
import os
import logging

class Database:
    def __init__(self):
        self.pool = None
        self.user = os.getenv("DB_USER", "postgres")
        self.password = os.getenv("DB_PASS", "postgres")
        self.host = os.getenv("DB_HOST", "localhost")
        self.port = os.getenv("DB_PORT", "5432")
        self.target_db = os.getenv("DB_NAME", "athletic.net")
        
        self.dsn = f"postgres://{self.user}:{self.password}@{self.host}:{self.port}/{self.target_db}"
        self.admin_dsn = f"postgres://{self.user}:{self.password}@{self.host}:{self.port}/postgres"

    async def connect(self):
        await self._ensure_database_exists()
        try:
            self.pool = await asyncpg.create_pool(self.dsn, min_size=10, max_size=85)
            # If you have an initialize_schema method, it runs here. 
            # Otherwise, we assume the schema is managed manually via SQL script.
        except Exception as e:
            logging.error(f"Failed to connect to {self.target_db}: {e}")
            raise

    async def _ensure_database_exists(self):
        try:
            conn = await asyncpg.connect(self.admin_dsn)
            exists = await conn.fetchval("SELECT 1 FROM pg_database WHERE datname = $1", self.target_db)
            if not exists:
                await conn.execute(f'CREATE DATABASE "{self.target_db}"')
            await conn.close()
        except Exception as e:
            logging.error(f"Error ensuring database exists: {e}")
            raise

    async def close(self):
        if self.pool:
            await self.pool.close()

    async def get_source_id(self, source_name):
        """Helper to get the ID of a source, creating it if it doesn't exist."""
        sql = "SELECT id FROM sources WHERE name = $1"
        source_id = await self.pool.fetchval(sql, source_name)
        if not source_id:
            insert_sql = "INSERT INTO sources (name) VALUES ($1) RETURNING id"
            source_id = await self.pool.fetchval(insert_sql, source_name)
        return source_id

    async def get_or_create_facility(self, name, city, state, lat, lon):
        """Finds or creates the parent geographic campus."""
        # 1. Try Name + State exact/fuzzy match
        if name and state:
            query = "SELECT * FROM facilities WHERE name ILIKE $1 AND state = $2 LIMIT 1"
            record = await self.pool.fetchrow(query, f"%{name}%", state)
            if record: return dict(record)


        # 2. Try spatial match (~1km bounding box)
        if lat is not None and lon is not None:
            query = "SELECT * FROM facilities WHERE latitude BETWEEN $1 AND $2 AND longitude BETWEEN $3 AND $4 LIMIT 1"
            record = await self.pool.fetchrow(query, lat - 0.01, lat + 0.01, lon - 0.01, lon + 0.01)
            if record: return dict(record)


        # 3. Create new Facility
        insert_query = """
            INSERT INTO facilities (name, city, state, latitude, longitude)
            VALUES ($1, $2, $3, $4, $5) RETURNING *
        """
        new_record = await self.pool.fetchrow(insert_query, name, city, state, lat, lon)
        return dict(new_record)

    async def get_or_create_track(self, facility_id, facility_type, altitude):
        """Finds or creates a specific running surface at a facility."""
        if not facility_type:
            facility_type = "Unknown Surface"
            
        check_sql = "SELECT * FROM tracks WHERE facility_id = $1 AND facility_type = $2 LIMIT 1"
        record = await self.pool.fetchrow(check_sql, facility_id, facility_type)
        if record: return dict(record)

        insert_sql = """
            INSERT INTO tracks (facility_id, facility_type, altitude_meters)
            VALUES ($1, $2, $3) RETURNING *
        """
        new_record = await self.pool.fetchrow(insert_sql, facility_id, facility_type, float(altitude) if altitude else 0.0)
        return dict(new_record)

    async def save_meet(self, meet_info, season):
        """Upserts a meet into the meets table based on its URL."""
        sql = """
            INSERT INTO meets (name, url, date_start, date_end, season, venue_id, facility_type, altitude)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            ON CONFLICT (url) DO UPDATE 
                SET name = EXCLUDED.name,
                    date_start = EXCLUDED.date_start,
                    date_end = EXCLUDED.date_end,
                    season = EXCLUDED.season,
                    venue_id = EXCLUDED.venue_id,
                    facility_type = EXCLUDED.facility_type,
                    altitude = EXCLUDED.altitude
            RETURNING id;
        """
        return await self.pool.fetchval(
            sql, 
            meet_info['name'], 
            meet_info.get('url'), 
            meet_info.get('date_start'), 
            meet_info.get('date_end'), 
            season, 
            meet_info.get('venue_id'), 
            meet_info.get('facility_type'), 
            meet_info.get('altitude')
        )

    async def get_or_create_team(self, name, external_id, source_name="athletic_net"):
        if not name: return None
        # Fallback if external_id is missing: use a cleaned name as a slug
        if not external_id: external_id = name.replace(" ", "_").lower()
        
        source_id = await self.get_source_id(source_name)
        
        # 1. Check if the alias exists (The Spoke)
        check_sql = "SELECT team_id FROM team_aliases WHERE source_id = $1 AND external_id = $2 LIMIT 1"
        team_id = await self.pool.fetchval(check_sql, source_id, str(external_id))
        
        if team_id: return team_id
            
        # 2. If no alias, create the Team record (The Hub)
        insert_team_sql = "INSERT INTO teams (name) VALUES ($1) RETURNING id"
        team_id = await self.pool.fetchval(insert_team_sql, name)
        
        # 3. Create the Alias record
        insert_alias_sql = """
            INSERT INTO team_aliases (team_id, source_id, external_id, external_name) 
            VALUES ($1, $2, $3, $4)
        """
        await self.pool.execute(insert_alias_sql, team_id, source_id, str(external_id), name)
        
        return team_id

    async def get_or_create_athlete(self, display_name, external_id, source_name="athletic_net", team_id=None, gender=None):
        if not display_name or not external_id: return None
        
        source_id = await self.get_source_id(source_name)
        
        # 1. Check if the alias exists
        check_sql = "SELECT athlete_id FROM athlete_aliases WHERE source_id = $1 AND external_id = $2 LIMIT 1"
        athlete_id = await self.pool.fetchval(check_sql, source_id, str(external_id))
        
        if athlete_id: return athlete_id
            
        # 2. If no alias, create the Athlete record (The Hub)
        insert_athlete_sql = "INSERT INTO athletes (display_name, gender) VALUES ($1, $2) RETURNING id"
        athlete_id = await self.pool.fetchval(insert_athlete_sql, display_name, gender)
        
        # 3. Create the Alias record
        insert_alias_sql = """
            INSERT INTO athlete_aliases (athlete_id, source_id, external_id, external_name) 
            VALUES ($1, $2, $3, $4)
        """
        await self.pool.execute(insert_alias_sql, athlete_id, source_id, str(external_id), display_name)
        
        return athlete_id

    async def get_athlete_subset(self):
        """
        Uses an Aggressive Whitelist CTE to guarantee collegiate athletes.
        """
        sql = """
            WITH TargetAthletes AS (
                SELECT 
                    a.id as internal_id, 
                    al.external_id as athletic_net_id, 
                    COUNT(p.id) as race_count
                FROM athletes a
                JOIN athlete_aliases al ON a.id = al.athlete_id
                JOIN sources s ON al.source_id = s.id
                JOIN performances p ON a.id = p.athlete_id
                JOIN teams t ON p.team_id = t.id -- INNER JOIN ensures they are attached to a team
                WHERE s.name = 'athletic_net'
                  AND al.external_id IS NOT NULL
                  
                GROUP BY a.id, al.external_id
                HAVING COUNT(p.id) >= 4
            )
            SELECT internal_id, athletic_net_id
            FROM TargetAthletes
        """
        records = await self.pool.fetch(sql)
        return [dict(r) for r in records]

    async def save_athlete_timeline(self, athlete_id, timeline):
        """
        Inserts or updates the computed eligibility and timeline for an athlete.
        """
        sql = """
            INSERT INTO athlete_timelines (
                athlete_id, hs_start_year, hs_end_year, hs_graduated,
                college_start_year, college_end_year, college_graduated,
                is_transfer, has_gap_year, covid_years,
                rs_xc, rs_indoor, rs_outdoor, current_level
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14
            )
            ON CONFLICT (athlete_id) DO UPDATE SET
                hs_start_year = EXCLUDED.hs_start_year,
                hs_end_year = EXCLUDED.hs_end_year,
                hs_graduated = EXCLUDED.hs_graduated,
                college_start_year = EXCLUDED.college_start_year,
                college_end_year = EXCLUDED.college_end_year,
                college_graduated = EXCLUDED.college_graduated,
                is_transfer = EXCLUDED.is_transfer,
                has_gap_year = EXCLUDED.has_gap_year,
                covid_years = EXCLUDED.covid_years,
                rs_xc = EXCLUDED.rs_xc,
                rs_indoor = EXCLUDED.rs_indoor,
                rs_outdoor = EXCLUDED.rs_outdoor,
                current_level = EXCLUDED.current_level;
        """
        
        try:
            await self.pool.execute(
                sql,
                athlete_id,
                timeline.get("hs_start"),
                timeline.get("hs_end"),
                timeline.get("hs_grad", False),
                timeline.get("col_start"),
                timeline.get("col_end"),
                timeline.get("col_grad", False),
                timeline.get("is_transfer", False),
                timeline.get("has_gap_year", False),
                timeline.get("covid_years", 0),
                timeline.get("rs_xc", False),
                timeline.get("rs_indoor", False),
                timeline.get("rs_outdoor", False),
                timeline.get("current_level", "None")
            )
        except Exception as e:
            self.logger.error(f"Failed to save timeline for {athlete_id}: {e}")

    async def get_or_create_race_context(self, meet_id, event_name, round_name, heat, gender, wind):
        # Look for an existing race context
        check_sql = """
            SELECT id FROM races 
            WHERE meet_id = $1 AND name_raw = $2 AND heat_number = $3 
            LIMIT 1
        """
        heat_num = int(heat) if heat else 1
        race_id = await self.pool.fetchval(check_sql, meet_id, event_name, heat_num)
        
        if race_id: return race_id
            
        # Create the race context
        insert_sql = """
            INSERT INTO races (meet_id, name_raw, gender, round, heat_number, wind)
            VALUES ($1, $2, $3, $4, $5, $6) RETURNING id
        """
        return await self.pool.fetchval(
            insert_sql, meet_id, event_name, gender, round_name, heat_num, float(wind) if wind else 0.0
        )

    async def save_performance_batch(self, performances):
        """
        Inserts a batch of performances into the normalized database.
        """
        # Note: No meet_id or wind in this INSERT statement!
        sql = """
            INSERT INTO performances 
            (athlete_id, team_id, race_id, mark_raw, mark_seconds, mark_metric, overall_place, heat_place)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            ON CONFLICT (athlete_id, race_id) DO UPDATE 
            SET mark_raw = EXCLUDED.mark_raw,
                mark_seconds = EXCLUDED.mark_seconds,
                mark_metric = EXCLUDED.mark_metric,
                overall_place = EXCLUDED.overall_place,
                heat_place = EXCLUDED.heat_place
        """
        
        records_to_insert = []
        for p in performances:
            # The tuple 'p' coming from processor.py contains:
            # 0=athlete_id, 1=meet_id(skip), 2=team_id, 3=race_id, 4=mark_raw, 
            # 5=mark_seconds, 6=mark_metric, 7=wind(skip), 8=overall_place, 9=heat_place
            
            records_to_insert.append((
                p[0], # $1: athlete_id
                p[2], # $2: team_id
                p[3], # $3: race_id
                p[4], # $4: mark_raw
                p[5], # $5: mark_seconds
                p[6], # $6: mark_metric
                p[8], # $7: overall_place
                p[9]  # $8: heat_place
            ))
            
        if records_to_insert:
            await self.pool.executemany(sql, records_to_insert)

    async def get_scraped_meet_urls(self):
            """
            Returns a fast-lookup set of all meet URLs currently in the database.
            """
            sql = "SELECT url FROM meets WHERE url IS NOT NULL"
            # Fetch all URLs in one quick query
            records = await self.pool.fetch(sql)
            # Return as a set comprehension for instant 'in' checks
            return {record['url'] for record in records}


    # Data cleaning functions
    async def get_performance_times(self, lower_bound=0.00, upper_bound=3600.00, limit=100):
            """
            Returns a limited number of performances.
            """
            sql = """SELECT mark_raw, mark_seconds 
            FROM performances 
            WHERE mark_seconds >= $1 
            AND mark_seconds <= $2
            LIMIT $3"""

            return await self.pool.fetch(sql, lower_bound, upper_bound, limit)

