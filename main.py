import asyncio
import logging
from scrapers.harvester import fetch_season_meets
from db_connection import Database

# Configure logging to catch missed scrapes
logging.basicConfig(filename='scraper.log', level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

async def run_historical_backfill(start_year, end_year):
    """
    Orchestrates the yearly scrape.
    This is the "Hybrid Archival-Meet Traversal Strategy".
    """
    db = Database()
    await db.connect()

    seasons = ["xc", "indoor", "outdoor"]
    
    for year in range(start_year, end_year + 1):
        print(f"--- Starting Archive Scrape for {year} ---")
        
        for season in seasons:
            # 1. Discovery Phase: Find all Meet URLs for this season
            # We await this because we need the list before we can process them
            print(f"Fetching {season} meet list for {year}...")
            meet_urls = await fetch_season_meets(year, season)
            
            # 2. Processing Phase: Async parsing of meets
            sem = asyncio.Semaphore(2) 
            
            tasks = []
            for url in meet_urls:
                task = asyncio.create_task(process_meet_safe(sem, db, url, season))
                tasks.append(task)
            
            # Wait for all meets in this season to finish before moving to next
            await asyncio.gather(*tasks)
            print(f"Completed {season} {year}")

    await db.close()




async def process_meet_safe(sem, db, url, season):
    """
    Wrapper to handle errors gracefully without crashing the main loop.
    """
    async with sem:
        try:
            from scrapers.processor import process_single_meet
            await process_single_meet(db, url, season)
        except Exception as e:
            logging.error(f"FAILED TO PROCESS MEET {url}: {str(e)}")
            print(f"X Failed: {url}")

if __name__ == "__main__":
    # Example: Scrape the 2023 season
    asyncio.run(run_historical_backfill(2023, 2023))