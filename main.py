import asyncio
import logging
import argparse
from scrapers.harvester import fetch_season_meets
from scrapers.processor import process_single_meet
from scrapers.team_scraper import TeamScraper
from db_connection import Database

logging.basicConfig(
    filename='scraper.log', 
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s'
)

logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

async def run_scraper(start_year, end_year):
    db = Database()
    await db.connect()
    
    for year in range(start_year, end_year + 1):
            print(f"--- Starting Scrape: {year} ---")
            
            # 1. Harvest
            meet_urls = await fetch_season_meets(year)
            print(f"Found {len(meet_urls)} meets.")
            
            # 2. Process
            sem = asyncio.Semaphore(2)
            tasks = []
            for url in meet_urls:
                task = asyncio.create_task(bounded_process(sem, db, url))
                tasks.append(task)
            
            await asyncio.gather(*tasks)
            print(f"Completed {year}")

    await db.close()

async def run_team_backfill():
    """Runs the specialized Team Scraper."""
    print("--- Starting Team Info Backfill ---")
    db = Database()
    await db.connect()
    
    scraper = TeamScraper(db)
    await scraper.backfill_teams()
    
    await db.close()
    print("--- Team Backfill Complete ---")

async def bounded_process(sem, db, url):
    async with sem:
        try:
            await process_single_meet(db, url)
        except Exception as e:
            logging.error(f"Failed to process {url}: {e}")
            print(f"X Failed: {url}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="TFRRS Scraper")
    parser.add_argument("--start", type=int, default=2024, help="Start Year")
    parser.add_argument("--end", type=int, default=2024, help="End Year")
    parser.add_argument("--teams", action="store_true", help="Run Team Info Backfill ONLY")
    
    args = parser.parse_args()
    
    if args.teams:
        asyncio.run(run_team_backfill())

    else:
        asyncio.run(run_scraper(args.start, args.end))