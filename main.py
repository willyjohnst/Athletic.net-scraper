import asyncio
import logging
import argparse
import re
from cleaning.audit_parser import *
from scrapers.harvester import fetch_season_meets
from scrapers.processor import process_single_meet
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
            total_meets_found = await fetch_season_meets(year)
            
            already_scraped_urls = await db.get_scraped_meet_urls()

            scraped_ids = set()
            for url in already_scraped_urls:
                match = re.search(r'/meet/(\d+)', str(url))
                if match:
                    scraped_ids.add(match.group(1))

            # 3. Filter the incoming meets
            meets_to_scrape = []
            for meet_url in total_meets_found:
                match = re.search(r'/meet/(\d+)', str(meet_url))
                if match:
                    meet_id = match.group(1)
                    if meet_id not in scraped_ids:
                        meets_to_scrape.append(meet_url)
                else:
                    meets_to_scrape.append(meet_url)
            
            print(f"Total meets found: {len(total_meets_found)}")
            print(f"Already scraped: {len(scraped_ids)}")
            print(f"Remaining to scrape: {len(meets_to_scrape)}")
            
            logging.info(f"Total meets found: {len(total_meets_found)}")
            logging.info(f"Already scraped: {len(scraped_ids)}")
            logging.info(f"Remaining to scrape: {len(meets_to_scrape)}")
            
            # 2. Process
            sem = asyncio.Semaphore(75)
            tasks = []
            for url in meets_to_scrape:
                task = asyncio.create_task(bounded_process(sem, db, url))
                tasks.append(task)
            
            await asyncio.gather(*tasks)
            print(f"Completed {year}")
            logging.info(f"Completed {year}")
    await db.close()

async def run_athlete_backfill():
    print("--- Starting Athlete Info Backfill ---")
    db = Database()
    await db.connect()
    from scrapers.athlete_processor import process_single_athlete
    athletes = await db.get_athlete_subset() 
    
    sem = asyncio.Semaphore(5)

    async def safe_process(record):
        async with sem: 
            a_internal_id = record['internal_id']
            anet_id = record['athletic_net_id']        
            await process_single_athlete(db, a_internal_id, anet_id)

    tasks = []
    for athlete_record in athletes:
        
        task = asyncio.create_task(safe_process(athlete_record))
        tasks.append(task)
    await asyncio.gather(*tasks)
    
    await db.close()
    print("--- Athlete Backfill Complete ---")
    logging.info(f"Athlete Backfill Complete")

async def bounded_process(sem, db, url):
    async with sem:
        try:
            await process_single_meet(db, url)
        except Exception as e:
            logging.error(f"Failed to process {url}: {e}")
            print(f"X Failed: {url}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="TFRRS Scraper")
    parser.add_argument("--start", type=int, default=2021, help="Start Year")
    parser.add_argument("--end", type=int, default=2026, help="End Year")
    parser.add_argument("--athletes", type=bool, default=False, help="Run Athlete Profile Scraper") 
    parser.add_argument("--parser_audit", type=int, default=1, help="Call with an integer value for each parser audit function")
    
    args = parser.parse_args()
    
    if args.athletes:
        asyncio.run(run_athlete_backfill())
    elif args.parser_audit:
        if args.parser_audit == 1:
            asyncio.run(performance_time_audit())
    else:
        asyncio.run(run_scraper(args.start, args.end))