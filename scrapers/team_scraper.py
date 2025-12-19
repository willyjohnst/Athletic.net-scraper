import httpx
from lxml import html
import asyncio
import logging
import random
import re

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
]

class TeamScraper:
    def __init__(self, db):
        self.db = db
        self.base_url = "https://www.tfrrs.org/teams/xc" # XC path works for most teams
        self.headers = {
            "User-Agent": random.choice(USER_AGENTS),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        }

    async def scrape_team(self, client, slug):
        """
        Fetches a team page and parses Division, Conference, and State.
        """
        url = f"{self.base_url}/{slug}.html"
        
        try:
            # Polite delay
            await asyncio.sleep(random.uniform(0.5, 1.5))
            
            response = await client.get(url, headers=self.headers, follow_redirects=True)
            if response.status_code != 200:
                logging.warning(f"Failed to fetch team {slug}: Status {response.status_code}")
                # Mark scraped=True anyway to prevent infinite loops on dead links
                await self.db.mark_team_scraped(slug) 
                return

            tree = html.fromstring(response.text)
            
            # --- Parsing Logic ---
            # 1. State from Slug (e.g., NC_college_m_Wingate -> NC)
            state = slug.split('_')[0].upper() if '_' in slug else None
            if state and len(state) > 2: state = None # Safety check

            # 2. Parse Header for Conf/Div
            # XPath from your doc: //div[@class="panel-second-title"]/div[1]/div/span/a
            links = tree.xpath('//div[@class="panel-second-title"]//span[contains(@class, "panel-heading-normal-text")]//a')
            
            division = None
            conference = None
            
            for link in links:
                text = link.text_content().strip()
                href = link.get('href', '')
                
                # Division / Region Logic
                if "Region" in text or "District" in text:
                    # Often contains division too: "DII Southeast Region"
                    if "D1" in text or "DI" in text: division = "DI"
                    elif "D2" in text or "DII" in text: division = "DII"
                    elif "D3" in text or "DIII" in text: division = "DIII"
                    elif "NAIA" in text: division = "NAIA"
                    elif "NJCAA" in text: division = "NJCAA"
                    continue # Skip setting conference if it's a region link

                # Explicit Division Links (rare but possible)
                if text in ["DI", "DII", "DIII", "NAIA", "NJCAA"]:
                    division = text
                    continue

                # If not a region/division link, it's likely the Conference
                # Heuristic: Conferences don't usually have "Region" in name
                if not conference:
                    conference = text
            
            # 3. Update DB
            await self.db.update_team_details(slug, division, conference, state)
            logging.info(f"Updated Team {slug}: Div={division}, Conf={conference}, State={state}")

        except Exception as e:
            logging.error(f"Error scraping team {slug}: {e}")

    async def backfill_teams(self):
        """
        Main loop to process all unscraped teams.
        """
        # 1. Get Unscraped Teams
        slugs = await self.db.get_unscraped_teams()
        logging.info(f"Found {len(slugs)} unscraped teams.")
        
        async with httpx.AsyncClient(timeout=15.0) as client:
            for i, slug in enumerate(slugs):
                await self.scrape_team(client, slug)
                
                # Progress Log
                if i % 10 == 0:
                    print(f"Processed {i}/{len(slugs)} teams...")
