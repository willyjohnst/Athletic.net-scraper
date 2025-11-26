from lxml import html
import re

def parse_xc_meet(html_content, url):
    """
    Specialized logic for XC.
    Must handle 'Team Scoring' vs 'Individual Results'.
    """
    tree = html.fromstring(html_content)
    results = []
    
    # XC pages often have two main tables. We need the one with detailed individual results.
    # Look for the table that contains individual athlete links.
    
    # Strategy: Find all rows that contain an athlete link
    athlete_rows = tree.xpath('//tr[.//a[contains(@href, "/athletes/")]]')
    
    for row in athlete_rows:
        cols = row.xpath('.//td')
        if len(cols) < 4: continue
        
        # XC Columns are often: Place, Name, Year, Team, Avg Mile, Time, Score
        try:
            place = cols[0].text_content().strip()
            
            athlete_node = cols[1].xpath('.//a')[0]
            athlete_name = athlete_node.text_content().strip()
            tfrrs_id = re.search(r'/athletes/(\d+)', athlete_node.get('href')).group(1)
            
            # Team is usually in col 3
            team_node = cols[3].xpath('.//a')
            if team_node:
                team_name = team_node[0].text_content().strip()
            else:
                team_name = cols[3].text_content().strip()
                
            mark_raw = cols[5].text_content().strip() # Total Time
            
            results.append({
                "event_name": "Cross Country 8k", # Logic needed to determine distance
                "place": place,
                "athlete_name": athlete_name,
                "tfrrs_id": tfrrs_id,
                "team_name": team_name,
                "team_slug": team_name,
                "mark_raw": mark_raw,
                "mark_seconds": 0.0 # Convert 25:00.00 -> 1500.00
            })
        except:
            continue
            
    return {
        "meet_info": {"url": url}, # Populate full info
        "results": results
    }