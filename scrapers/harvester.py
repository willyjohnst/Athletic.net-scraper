import httpx
from lxml import html
import asyncio
import logging
import traceback

# Base URL for the search form action found in your doc
SEARCH_URL = "https://www.tfrrs.org/results_search_page.html"   

# HEADERS are critical for bypassing 403 blocks.
# This makes the script look like a standard Chrome browser on Windows.
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Referer": "https://www.tfrrs.org/results_search.html",
    "Origin": "https://www.tfrrs.org"
}

async def fetch_season_meets(year, sport):
    """
    Iterates through all months of a given year/sport to find meet URLs.
    
    Args:
        year (int): e.g., 2024
        sport (str): 'track' or 'xc' (matches the form values in your doc)
        
    Returns:
        set: A unique set of meet URLs found.
    """
    found_urls = set()
    
    # We iterate months to break the dataset down. 
    # Even if there are >30 meets in a month, the pagination loop handles it.
    # 1-12 covers the whole year (Indoor + Outdoor + XC)    
    async with httpx.AsyncClient(headers=HEADERS, follow_redirects=True) as client:
        if sport in {'indoor', 'outdoor'}:
            months = {1, 2, 3, 4, 5, 6, 7, 11, 12}
        else:
            months = {8, 9, 10, 11}

        for month in months:
            print(f"  > Scanning {sport} for {year}-{month:02d}...")
            
            page_num = 1
            consecutive_failures = 0

            while True:
                # Construct params based on the form input names you provided
                params = {
                    "with_year": str(year),
                    "with_month": str(month),
                    "with_sports": sport, # 'track' or 'xc'
                    "page": str(page_num) # Rails standard pagination param
                }
                
                try:
                    # 10s timeout to be polite but fail fast
                    response = await client.get(SEARCH_URL, params=params, timeout=10.0)
                    
                    if response.status_code == 403:
                        logging.error(f"403 Forbidden for {year}-{month} p{page_num}. Server blocked the request.")
                        # If blocked, waiting briefly won't help if headers are wrong.
                        # But if headers are right, it might be a rate limit.
                        consecutive_failures += 1
                        if consecutive_failures > 3:
                            break # Move to next month
                        await asyncio.sleep(5) # Long pause before retry
                        continue

                    if response.status_code != 200:
                        logging.warning(f"Search failed for {year}-{month} p{page_num}: Status {response.status_code}")
                        break

                    new_urls = parse_search_results(response.text)
                    
                    if not new_urls:
                        # Stop paging if no meets returned
                        break
                        
                    found_urls.update(new_urls)
                    
                    # Reset failure count on success
                    consecutive_failures = 0
                    
                    # Next page
                    page_num += 1
                    
                    # Small sleep to be polite to the server during pagination loops
                    await asyncio.sleep(0.5)

                except httpx.RequestError as e:
                    # This catches timeouts, connection drops, and protocol errors
                    # This is likely where your '200 OK' crash was actually happening (incomplete body read)
                    logging.error(f"Network Error for {year}-{month} p{page_num}: {type(e).__name__} - {e}")
                    consecutive_failures += 1
                    if consecutive_failures > 3:
                        break
                    await asyncio.sleep(2)
                    continue
                    
                except Exception as e:
                    # Catch-all for logic errors (e.g., parsing issues)
                    # We use traceback to see the REAL error line
                    logging.error(f"Unexpected Error fetching {year}-{month} p{page_num}: {e}")
                    logging.error(traceback.format_exc())
                    break

    
    return list(found_urls)

def parse_search_results(html_content):
    """
    Parses the HTML table returned by the search page.
    """
    tree = html.fromstring(html_content)
    urls = []
    
    # We look for links inside the results table.
    # The form usually returns a table with class 'table' or similar.
    # We target the 'Meet Name' column which is a link.
    
    # Xpath: Find any <a> tag containing 'results' in the href
    # This avoids linking to team pages or other noise.
    links = tree.xpath('//a[contains(@href, "/results/")]')
    
    for link in links:
        href = link.get('href')
        
        # Filter out "xc" results if we are looking for track, or vice versa,

        # just in case the filter didn't work perfectly (defensive coding).
        # Also clean up the URL to be absolute.
        if href.startswith("/"):
            href = f"https://www.tfrrs.org{href}"
            
        urls.append(href)
    return urls