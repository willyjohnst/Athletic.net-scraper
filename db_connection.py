import asyncpg
import os

class Database:
    def __init__(self):
        self.pool = None
        # In prod, fetch from env variables
        self.dsn = "postgres://user:password@localhost:5432/tfrrs_db"

    async def connect(self):
        self.pool = await asyncpg.create_pool(self.dsn)

    async def close(self):
        await self.pool.close()

    async def get_or_create_team(self, name, url_slug):
        """
        Ensures team exists. Returns team_id.
        """
        sql = """
            INSERT INTO teams (name, url_slug)
            VALUES ($1, $2)
            ON CONFLICT (url_slug) DO UPDATE SET name = EXCLUDED.name
            RETURNING team_id;
        """
        async with self.pool.acquire() as conn:
            return await conn.fetchval(sql, name, url_slug)

    async def save_performance_batch(self, performances):
        """
        Bulk insert performances.
        Expecting list of tuples: 
        (athlete_id, meet_id, team_id, event_id, mark_raw, mark_sec, place, date)
        """
        sql = """
            INSERT INTO performances 
            (athlete_id, meet_id, team_id, event_id, mark_raw, mark_seconds, place, date)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            ON CONFLICT (perf_id) DO NOTHING;
        """
        async with self.pool.acquire() as conn:
            # executemany is much faster for bulk data
            await conn.executemany(sql, performances)