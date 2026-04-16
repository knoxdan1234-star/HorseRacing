"""Bootstrap the current 2025/26 HKJC season into the database."""
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

# All 58 meetings found for 2025/26 season (probed from HKJC)
MEETINGS = [
    (date(2025,9,7),"ST"),(date(2025,9,10),"HV"),(date(2025,9,14),"ST"),(date(2025,9,17),"HV"),
    (date(2025,9,21),"ST"),(date(2025,9,28),"ST"),(date(2025,10,1),"ST"),(date(2025,10,4),"ST"),
    (date(2025,10,8),"HV"),(date(2025,10,12),"ST"),(date(2025,10,15),"HV"),(date(2025,10,19),"ST"),
    (date(2025,10,22),"HV"),(date(2025,10,26),"ST"),(date(2025,11,2),"HV"),(date(2025,11,5),"HV"),
    (date(2025,11,9),"ST"),(date(2025,11,12),"HV"),(date(2025,11,15),"ST"),(date(2025,11,19),"HV"),
    (date(2025,11,23),"ST"),(date(2025,11,26),"HV"),(date(2025,11,30),"ST"),(date(2025,12,3),"HV"),
    (date(2025,12,7),"ST"),(date(2025,12,10),"HV"),(date(2025,12,14),"ST"),(date(2025,12,17),"HV"),
    (date(2025,12,20),"ST"),(date(2025,12,27),"ST"),(date(2026,1,4),"ST"),(date(2026,1,7),"HV"),
    (date(2026,1,11),"ST"),(date(2026,1,14),"HV"),(date(2026,1,18),"ST"),(date(2026,1,21),"ST"),
    (date(2026,1,25),"ST"),(date(2026,1,28),"HV"),(date(2026,2,1),"ST"),(date(2026,2,4),"HV"),
    (date(2026,2,8),"ST"),(date(2026,2,11),"HV"),(date(2026,2,14),"ST"),(date(2026,2,22),"ST"),
    (date(2026,2,25),"HV"),(date(2026,3,1),"ST"),(date(2026,3,4),"HV"),(date(2026,3,8),"ST"),
    (date(2026,3,11),"HV"),(date(2026,3,15),"ST"),(date(2026,3,18),"HV"),(date(2026,3,22),"ST"),
    (date(2026,3,25),"HV"),(date(2026,3,29),"ST"),(date(2026,4,1),"ST"),(date(2026,4,8),"HV"),
    (date(2026,4,12),"ST"),(date(2026,4,15),"HV"),
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

print(f"\n=== BOOTSTRAP COMPLETE ===")
print(f"Total meetings: {len(MEETINGS)}")
print(f"Total races: {total_races}")
print(f"Total runners: {total_runners}")
print(f"Total dividends: {total_dividends}")
