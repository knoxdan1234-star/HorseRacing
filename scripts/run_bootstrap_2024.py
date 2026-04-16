"""Bootstrap the 2024/25 HKJC season into the database."""
import sys
import logging
import time
from datetime import date

sys.path.insert(0, ".")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
)

from db.database import get_session, init_database
from agents.collector.hkjc.scraper_results import ResultsScraper
from agents.collector.data_cleaner import DataCleaner

init_database()

MEETINGS = [
    (date(2024,9,8),"ST"),(date(2024,9,11),"HV"),(date(2024,9,15),"ST"),(date(2024,9,18),"HV"),
    (date(2024,9,22),"ST"),(date(2024,9,25),"HV"),(date(2024,9,28),"ST"),(date(2024,10,6),"ST"),
    (date(2024,10,9),"HV"),(date(2024,10,13),"ST"),(date(2024,10,16),"HV"),(date(2024,10,20),"ST"),
    (date(2024,10,23),"ST"),(date(2024,10,27),"HV"),(date(2024,10,30),"HV"),(date(2024,11,3),"ST"),
    (date(2024,11,6),"HV"),(date(2024,11,9),"ST"),(date(2024,11,13),"HV"),(date(2024,11,17),"ST"),
    (date(2024,11,20),"HV"),(date(2024,11,24),"ST"),(date(2024,11,27),"HV"),(date(2024,12,1),"ST"),
    (date(2024,12,4),"HV"),(date(2024,12,8),"ST"),(date(2024,12,11),"HV"),(date(2024,12,15),"ST"),
    (date(2024,12,18),"ST"),(date(2024,12,22),"ST"),(date(2024,12,29),"ST"),(date(2025,1,1),"ST"),
    (date(2025,1,5),"ST"),(date(2025,1,8),"HV"),(date(2025,1,12),"ST"),(date(2025,1,15),"HV"),
    (date(2025,1,19),"ST"),(date(2025,1,22),"HV"),(date(2025,1,26),"ST"),(date(2025,2,5),"HV"),
    (date(2025,2,9),"ST"),(date(2025,2,12),"ST"),(date(2025,2,16),"ST"),(date(2025,2,19),"HV"),
    (date(2025,2,23),"ST"),(date(2025,2,26),"HV"),(date(2025,3,2),"ST"),(date(2025,3,5),"HV"),
    (date(2025,3,9),"ST"),(date(2025,3,12),"HV"),(date(2025,3,15),"ST"),(date(2025,3,19),"HV"),
    (date(2025,3,23),"ST"),(date(2025,3,26),"ST"),(date(2025,3,30),"ST"),(date(2025,4,2),"HV"),
    (date(2025,4,6),"ST"),(date(2025,4,9),"HV"),(date(2025,4,13),"ST"),(date(2025,4,16),"HV"),
    (date(2025,4,20),"ST"),(date(2025,4,23),"HV"),(date(2025,4,27),"ST"),(date(2025,4,30),"HV"),
    (date(2025,5,4),"ST"),(date(2025,5,7),"HV"),(date(2025,5,10),"ST"),(date(2025,5,14),"HV"),
    (date(2025,5,18),"ST"),(date(2025,5,21),"HV"),(date(2025,5,25),"ST"),(date(2025,5,28),"HV"),
    (date(2025,5,31),"ST"),(date(2025,6,4),"HV"),(date(2025,6,8),"ST"),(date(2025,6,11),"HV"),
    (date(2025,6,14),"ST"),(date(2025,6,22),"ST"),(date(2025,6,25),"HV"),(date(2025,6,28),"ST"),
    (date(2025,7,5),"ST"),(date(2025,7,9),"HV"),(date(2025,7,13),"ST"),(date(2025,7,16),"HV"),
]

scraper = ResultsScraper()
total_races = 0
total_runners = 0
total_dividends = 0

with get_session() as session:
    cleaner = DataCleaner(session)

    for i, (d, rc) in enumerate(MEETINGS):
        print(f"[{i+1}/{len(MEETINGS)}] Scraping {d} {rc}...")
        try:
            results = scraper.scrape_meeting(d, rc)
            meeting_races = 0
            for result in results:
                stored = cleaner.store_race_result(result)
                if stored:
                    meeting_races += 1
                    total_runners += len(result.runners)
                    total_dividends += len(result.dividends)
            total_races += meeting_races
            print(f"  -> {meeting_races} races stored (total: {total_races})")
        except Exception as e:
            print(f"  -> ERROR: {e}")

        time.sleep(1)

print(f"\n=== BOOTSTRAP 2024/25 COMPLETE ===")
print(f"Total meetings: {len(MEETINGS)}")
print(f"Total races: {total_races}")
print(f"Total runners: {total_runners}")
print(f"Total dividends: {total_dividends}")
