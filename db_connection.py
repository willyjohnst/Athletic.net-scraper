import asyncpg
import os
import logging
import re

class Database:
    def __init__(self):
        self.pool = None
        # Default to the new database name 'tfrrs_analytics_db'
        self.user = os.getenv("DB_USER", "postgres")
        self.password = os.getenv("DB_PASS", "postgres")
        self.host = os.getenv("DB_HOST", "localhost")
        self.port = os.getenv("DB_PORT", "5432")
        self.dbname = os.getenv("DB_NAME", "tfrrs_analytics_db")
        
        self.dsn = f"postgres://{self.user}:{self.password}@{self.host}:{self.port}/{self.dbname}"

    async def connect(self):
        try:
            self.pool = await asyncpg.create_pool(self.dsn)
            await self.initialize_schema()
        except asyncpg.InvalidCatalogNameError:
            logging.error(f"Database '{self.dbname}' does not exist.")
            logging.error(f"Please run: createdb {self.dbname}")
            raise

    async def close(self):
        if self.pool:
            await self.pool.close()

    async def initialize_schema(self):
        schema_path = os.path.join(os.path.dirname(__file__), 'schema.sql')
        if not os.path.exists(schema_path):
            schema_path = 'database/schema.sql'

        if not os.path.exists(schema_path):
             logging.warning("Schema file not found. Skipping auto-initialization.")
             return

        with open(schema_path, 'r') as f:
            schema_sql = f.read()

        async with self.pool.acquire() as conn:
            table_exists = await conn.fetchval("SELECT to_regclass('public.athletes');")
            if not table_exists:
                print("--- Initializing Database Schema ---")
                await conn.execute(schema_sql)
                print("--- Schema Applied Successfully ---")

    async def get_or_create_team(self, name, url_slug):
        if not url_slug:
            url_slug = name.replace(" ", "_").lower()

        sql = """
            INSERT INTO teams (name, tfrrs_slug)
            VALUES ($1, $2)
            ON CONFLICT (tfrrs_slug) DO UPDATE 
                SET name = EXCLUDED.name,
                    updated_at = NOW()
            RETURNING team_id;
        """
        async with self.pool.acquire() as conn:
            return await conn.fetchval(sql, name, url_slug)
            
    async def get_or_create_athlete(self, name, tfrrs_id, team_id):
        parts = name.split(" ", 1)
        first = parts[0]
        last = parts[1] if len(parts) > 1 else ""

        sql = """
            INSERT INTO athletes (tfrrs_id, first_name, last_name, current_team_id)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (tfrrs_id) DO UPDATE 
                SET current_team_id = EXCLUDED.current_team_id
            RETURNING athlete_id;
        """
        async with self.pool.acquire() as conn:
            return await conn.fetchval(sql, tfrrs_id, first, last, team_id)

    async def get_or_create_event(self, event_name_raw):
        """
        Ensures the event exists in the events table.
        """
        if not event_name_raw:
            return None

        # Clean whitespace
        event_name_raw = event_name_raw.strip()
        
        # We only insert 'name_raw' initially. 
        # 'std_name' and 'distance_meters' can be populated by a separate normalization script later.
        sql = """
            INSERT INTO events (name_raw)
            VALUES ($1)
            ON CONFLICT (name_raw) DO UPDATE 
                SET name_raw = EXCLUDED.name_raw -- No-op to return ID
            RETURNING event_id;
        """
        async with self.pool.acquire() as conn:
            return await conn.fetchval(sql, event_name_raw)

    async def save_meet(self, meet_info, season='outdoor'):
        tfrrs_id = "0"
        match = re.search(r'/results/(xc/)?(\d+)', meet_info['url'])
        if match:
            tfrrs_id = match.group(2)
            
        sql = """
            INSERT INTO meets (
                tfrrs_id, name, date_start, year, season,
                venue_name, venue_city, venue_state
            )
            VALUES (
                $1, $2, $3::date, EXTRACT(YEAR FROM $3::date)::int, $4::season_type,
                $5, $6, $7
            )
            ON CONFLICT (tfrrs_id) DO UPDATE 
                SET name = EXCLUDED.name,
                    date_start = EXCLUDED.date_start,
                    year = EXCLUDED.year,
                    season = EXCLUDED.season,
                    venue_name = EXCLUDED.venue_name,
                    venue_city = EXCLUDED.venue_city,
                    venue_state = EXCLUDED.venue_state
            RETURNING meet_id;
        """
        async with self.pool.acquire() as conn:
            return await conn.fetchval(
                sql, 
                tfrrs_id, 
                meet_info['name'], 
                meet_info['date'], 
                season,
                meet_info.get('venue_name'),
                meet_info.get('venue_city'),
                meet_info.get('venue_state')
            )

    async def save_performance_batch(self, performances):
        # Insert raw performance data
        sql = """
            INSERT INTO performances 
            (athlete_id, meet_id, team_id, event_id, mark_raw, mark_seconds, mark_metric, wind_reading, place, created_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NOW())
            ON CONFLICT (athlete_id, meet_id, event_id, round_name, mark_raw) 
            DO UPDATE SET 
                mark_seconds = EXCLUDED.mark_seconds,
                mark_metric = EXCLUDED.mark_metric,
                wind_reading = EXCLUDED.wind_reading,
                place = EXCLUDED.place
        """
        async with self.pool.acquire() as conn:
             if performances:
                await conn.executemany(sql, performances)