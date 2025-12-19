import asyncpg
import os
import logging
import re

class Database:
    def __init__(self):
        self.pool = None
        self.user = os.getenv("DB_USER", "postgres")
        self.password = os.getenv("DB_PASS", "postgres")
        self.host = os.getenv("DB_HOST", "localhost")
        self.port = os.getenv("DB_PORT", "5432")
        self.target_db = os.getenv("DB_NAME", "tfrrs_analytics_db")
        
        self.dsn = f"postgres://{self.user}:{self.password}@{self.host}:{self.port}/{self.target_db}"
        self.admin_dsn = f"postgres://{self.user}:{self.password}@{self.host}:{self.port}/postgres"

    async def connect(self):
        await self._ensure_database_exists()
        try:
            self.pool = await asyncpg.create_pool(self.dsn)
            await self.initialize_schema()
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
        except Exception: pass

    async def close(self):
        if self.pool: await self.pool.close()

    async def initialize_schema(self):
        if os.path.exists('schema.sql'):
            with open('schema.sql', 'r') as f:
                sql = f.read()
                async with self.pool.acquire() as conn:
                    exists = await conn.fetchval("SELECT to_regclass('public.teams')")
                    if not exists:
                        await conn.execute(sql)
                    else:
                        try:
                            await conn.execute("ALTER TABLE teams ADD COLUMN IF NOT EXISTS conference VARCHAR(100);")
                            await conn.execute("ALTER TABLE teams ADD COLUMN IF NOT EXISTS scraped BOOLEAN DEFAULT FALSE;")
                        except: pass

    # --- TEAM SCRAPER METHODS ---
    
    async def get_unscraped_teams(self):
        sql = "SELECT tfrrs_slug FROM teams WHERE scraped = FALSE AND tfrrs_slug IS NOT NULL"
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(sql)
            return [r['tfrrs_slug'] for r in rows]

    async def update_team_details(self, slug, division, conference, state):
        sql = """
            UPDATE teams 
            SET current_division = $2, 
                conference = $3, 
                state = COALESCE($4, state), 
                scraped = TRUE, 
                updated_at = NOW()
            WHERE tfrrs_slug = $1
        """
        async with self.pool.acquire() as conn:
            await conn.execute(sql, slug, division, conference, state)

    async def mark_team_scraped(self, slug):
        async with self.pool.acquire() as conn:
            await conn.execute("UPDATE teams SET scraped = TRUE WHERE tfrrs_slug = $1", slug)

    # --- STANDARD METHODS ---

    async def get_or_create_team(self, name, url_slug):
        if not name: return None
        if not url_slug: url_slug = name.replace(" ", "_").lower()
        
        sql = """
            INSERT INTO teams (name, tfrrs_slug) VALUES ($1, $2)
            ON CONFLICT (tfrrs_slug) DO UPDATE SET name = EXCLUDED.name
            RETURNING team_id;
        """
        async with self.pool.acquire() as conn:
            return await conn.fetchval(sql, name, url_slug)
            
    async def get_or_create_athlete(self, name, tfrrs_id, team_id, gender, url_slug):
        parts = name.split(" ", 1)
        first = parts[0]
        last = parts[1] if len(parts) > 1 else ""
        
        sql = """
            INSERT INTO athletes (tfrrs_id, first_name, last_name, current_team_id, gender, url_slug)
            VALUES ($1, $2, $3, $4, $5::gender_type, $6)
            ON CONFLICT (tfrrs_id) DO UPDATE SET 
                current_team_id = EXCLUDED.current_team_id,
                url_slug = EXCLUDED.url_slug
            RETURNING athlete_id;
        """
        async with self.pool.acquire() as conn:
            return await conn.fetchval(sql, tfrrs_id, first, last, team_id, gender, url_slug)

    async def get_or_create_event(self, name_raw, std_name, distance, gender, is_field, is_relay):
        sql = """
            INSERT INTO events (name_raw, std_name, distance_meters, gender, is_field, is_relay) 
            VALUES ($1, $2, $3, $4::gender_type, $5, $6)
            ON CONFLICT (name_raw) DO UPDATE SET 
                std_name = EXCLUDED.std_name,
                distance_meters = EXCLUDED.distance_meters,
                is_field = EXCLUDED.is_field,
                is_relay = EXCLUDED.is_relay,
                gender = EXCLUDED.gender
            RETURNING event_id;
        """
        async with self.pool.acquire() as conn:
            return await conn.fetchval(sql, name_raw, std_name, distance, gender, is_field, is_relay)

    async def get_or_create_race(self, meet_id, event_id, round_name, section_num, gender):
        sql = """
            INSERT INTO races (meet_id, event_id, round_name, section_number, gender)
            VALUES ($1, $2, $3, $4, $5::gender_type)
            ON CONFLICT (meet_id, event_id, round_name, section_number) 
            DO UPDATE SET gender = EXCLUDED.gender
            RETURNING race_id;
        """
        async with self.pool.acquire() as conn:
            return await conn.fetchval(sql, meet_id, event_id, round_name, section_num, gender)

    async def save_meet(self, meet_info, season):
        tfrrs_id = "0"
        match = re.search(r'/results/(xc/)?(\d+)', meet_info['url'])
        if match: tfrrs_id = match.group(2)
            
        sql = """
            INSERT INTO meets (
                tfrrs_id, name, date_start, date_end, year, season,
                venue_name, venue_city, venue_state, facility_type
            )
            VALUES (
                $1, $2, $3::date, $4::date, EXTRACT(YEAR FROM $3::date)::int, $5::season_type,
                $6, $7, $8, $9
            )
            ON CONFLICT (tfrrs_id) DO UPDATE 
                SET name = EXCLUDED.name,
                    date_end = EXCLUDED.date_end,
                    facility_type = EXCLUDED.facility_type,
                    venue_name = EXCLUDED.venue_name,
                    venue_city = EXCLUDED.venue_city,
                    venue_state = EXCLUDED.venue_state,
                    season = EXCLUDED.season 
            RETURNING meet_id;
        """
        async with self.pool.acquire() as conn:
            return await conn.fetchval(sql, tfrrs_id, meet_info['name'], meet_info['date_start'], meet_info['date_end'], season, meet_info.get('venue_name'), meet_info.get('venue_city'), meet_info.get('venue_state'), meet_info.get('facility_type'))

    async def save_performance_batch(self, performances):
        sql = """
            INSERT INTO performances 
            (athlete_id, meet_id, team_id, race_id, mark_raw, mark_seconds, mark_metric, wind_reading, place, created_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NOW())
            ON CONFLICT (athlete_id, race_id) 
            DO UPDATE SET 
                mark_seconds = EXCLUDED.mark_seconds,
                mark_metric = EXCLUDED.mark_metric,
                mark_raw = EXCLUDED.mark_raw,
                place = EXCLUDED.place
        """
        async with self.pool.acquire() as conn:
             if performances: await conn.executemany(sql, performances)