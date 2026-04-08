from db_connection import Database
import pandas as pd

async def audit_seconds_conversions():
    db = Database()
    await db.connect()

    performances = await db.get_performance_times(600, 3600, 100)

    for performance in performances:
        mark_raw = performance.get("mark_raw")
        mark_seconds = performance.get("mark_seconds")
        print(f"mark_raw = {mark_raw}. mark_seconds = {mark_seconds}")

    await db.close()

async def audit_race_times():
    db = Database()
    await db.connect()

    hs_bests = pd.read_csv("C:\Users\wjohnst1\source\repos\willyjohnst\athletic.net scraper\documents\event_bests\hs_records_by_grade.csv")
    college_bests = pd.read_csv("C:\Users\wjohnst1\source\repos\willyjohnst\athletic.net scraper\documents\event_bests\ncaa_records.csv")


    # Print out graphs and percentages that are faster than the gender-age-group wr
    for record in hs_bests:
        
        # get fastest times from database where year = year_in_record and event = event

        sql = """SELECT 

        """
        get_audit_query

    # also print out an athletic.net link to the meet and performance