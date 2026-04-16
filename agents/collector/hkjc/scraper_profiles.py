"""
HKJC Horse/Jockey/Trainer Profile Scraper

Scrapes detailed profiles and statistics for horses, jockeys, and trainers.
"""

import logging
import re
import time
from dataclasses import dataclass, field
from datetime import date

import requests
from bs4 import BeautifulSoup

from config import settings

logger = logging.getLogger(__name__)


@dataclass
class HorseProfile:
    code: str
    name: str
    name_tc: str = ""
    age: int | None = None
    sex: str = ""
    color: str = ""
    import_type: str = ""
    current_rating: int | None = None
    season_stakes: float = 0.0
    total_starts: int = 0
    total_wins: int = 0
    total_places: int = 0  # 2nd + 3rd
    sire: str = ""
    dam: str = ""
    dam_sire: str = ""
    country_of_origin: str = ""
    owner: str = ""
    trainer: str = ""


@dataclass
class PastPerformance:
    race_date: date
    racecourse: str
    race_no: int
    distance: int | None = None
    track_type: str = ""
    going: str = ""
    race_class: str = ""
    draw: int | None = None
    rating: int | None = None
    weight: int | None = None
    jockey: str = ""
    finish_position: int | None = None
    lbw: str = ""
    finish_time: str = ""
    win_odds: float | None = None


@dataclass
class JockeyStats:
    code: str
    name: str
    name_tc: str = ""
    season_wins: int = 0
    season_seconds: int = 0
    season_thirds: int = 0
    season_rides: int = 0
    season_stakes: float = 0.0


@dataclass
class TrainerStats:
    code: str
    name: str
    name_tc: str = ""
    season_wins: int = 0
    season_seconds: int = 0
    season_thirds: int = 0
    season_runners: int = 0
    season_stakes: float = 0.0


class ProfileScraper:
    """Scrapes horse profiles, jockey stats, and trainer stats from HKJC."""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
        })
        self.delay = settings.SCRAPE_DELAY

    def scrape_horse(self, horse_code: str) -> HorseProfile | None:
        """Scrape a horse profile page."""
        url = f"{settings.HKJC_HORSE_URL}?HorseId={horse_code}"
        logger.debug("Scraping horse: %s", url)

        try:
            resp = self.session.get(url, timeout=settings.REQUEST_TIMEOUT)
            resp.raise_for_status()
        except requests.RequestException as e:
            logger.error("Failed to scrape horse %s: %s", horse_code, e)
            return None

        soup = BeautifulSoup(resp.text, "lxml")
        return self._parse_horse_profile(soup, horse_code)

    def _parse_horse_profile(
        self, soup: BeautifulSoup, horse_code: str
    ) -> HorseProfile | None:
        """Parse the horse profile page."""
        profile = HorseProfile(code=horse_code, name="")

        # Horse name (English and Chinese)
        name_el = soup.find("span", class_="horse_name") or soup.find("h1")
        if name_el:
            profile.name = name_el.get_text(strip=True)

        # Look for Chinese name
        for el in soup.find_all(["span", "td"]):
            text = el.get_text(strip=True)
            # Chinese characters in horse names
            if re.match(r"^[\u4e00-\u9fff]{2,6}$", text):
                profile.name_tc = text
                break

        # Parse info table (key-value pairs)
        for table in soup.find_all("table"):
            rows = table.find_all("tr")
            for row in rows:
                cells = row.find_all("td")
                if len(cells) < 2:
                    continue

                label = cells[0].get_text(strip=True).lower()
                value = cells[1].get_text(strip=True)

                if "age" in label or "sex" in label:
                    # Usually "5 / Gelding" or similar
                    parts = value.split("/")
                    if len(parts) >= 1:
                        age_match = re.search(r"\d+", parts[0])
                        if age_match:
                            profile.age = int(age_match.group())
                    if len(parts) >= 2:
                        sex_text = parts[1].strip().lower()
                        if "gelding" in sex_text:
                            profile.sex = "G"
                        elif "mare" in sex_text or "filly" in sex_text:
                            profile.sex = "M"
                        else:
                            profile.sex = "H"

                elif "colour" in label or "color" in label:
                    profile.color = value

                elif "import" in label:
                    profile.import_type = value

                elif "rating" in label and "current" in label:
                    rating_match = re.search(r"\d+", value)
                    if rating_match:
                        profile.current_rating = int(rating_match.group())

                elif "sire" in label:
                    profile.sire = value

                elif "dam" in label and "sire" not in label:
                    profile.dam = value

                elif "dam" in label and "sire" in label:
                    profile.dam_sire = value

                elif "owner" in label:
                    profile.owner = value

                elif "trainer" in label:
                    profile.trainer = value

                elif "origin" in label or "country" in label:
                    profile.country_of_origin = value

        # Parse race record (wins/places)
        record_text = soup.get_text()
        record_match = re.search(
            r"(\d+)-(\d+)-(\d+)-(\d+)",  # starts-wins-2nds-3rds
            record_text,
        )
        if record_match:
            profile.total_starts = int(record_match.group(1))
            profile.total_wins = int(record_match.group(2))
            profile.total_places = int(record_match.group(3)) + int(record_match.group(4))

        # Season stakes
        stakes_match = re.search(r"Season Stakes\s*:?\s*\$?([\d,]+)", record_text, re.IGNORECASE)
        if stakes_match:
            try:
                profile.season_stakes = float(stakes_match.group(1).replace(",", ""))
            except ValueError:
                pass

        if not profile.name:
            return None

        return profile

    def scrape_horse_form(self, horse_code: str) -> list[PastPerformance]:
        """Scrape past performance records for a horse."""
        url = f"{settings.HKJC_HORSE_URL}?HorseId={horse_code}"

        try:
            resp = self.session.get(url, timeout=settings.REQUEST_TIMEOUT)
            resp.raise_for_status()
        except requests.RequestException as e:
            logger.error("Failed to scrape form for %s: %s", horse_code, e)
            return []

        soup = BeautifulSoup(resp.text, "lxml")
        performances = []

        # Look for the past performance table
        for table in soup.find_all("table"):
            header_text = table.get_text(strip=True).lower()
            if "date" in header_text and ("dist" in header_text or "going" in header_text):
                rows = table.find_all("tr")[1:]  # Skip header
                for row in rows:
                    cells = row.find_all("td")
                    if len(cells) >= 8:
                        perf = self._parse_performance_row(cells)
                        if perf:
                            performances.append(perf)
                break

        return performances

    def _parse_performance_row(self, cells: list) -> PastPerformance | None:
        """Parse a single past performance row."""
        try:
            # Typical: Date, Track, Dist, Going, Class, Draw, Rtg, Wt, Jockey, Plc, LBW, Time, Odds
            date_text = cells[0].get_text(strip=True)
            try:
                race_date = date.fromisoformat(
                    date_text.replace("/", "-") if "/" in date_text else date_text
                )
            except ValueError:
                return None

            perf = PastPerformance(
                race_date=race_date,
                racecourse=cells[1].get_text(strip=True) if len(cells) > 1 else "",
                race_no=0,
            )

            if len(cells) > 2:
                dist_text = cells[2].get_text(strip=True)
                dist_match = re.search(r"\d+", dist_text)
                if dist_match:
                    perf.distance = int(dist_match.group())

            if len(cells) > 3:
                perf.going = cells[3].get_text(strip=True)

            if len(cells) > 4:
                perf.race_class = cells[4].get_text(strip=True)

            if len(cells) > 5:
                draw_text = cells[5].get_text(strip=True)
                if draw_text.isdigit():
                    perf.draw = int(draw_text)

            if len(cells) > 6:
                rating_text = cells[6].get_text(strip=True)
                if rating_text.isdigit():
                    perf.rating = int(rating_text)

            if len(cells) > 7:
                wt_text = cells[7].get_text(strip=True)
                if wt_text.isdigit():
                    perf.weight = int(wt_text)

            if len(cells) > 8:
                perf.jockey = cells[8].get_text(strip=True)

            if len(cells) > 9:
                plc_text = cells[9].get_text(strip=True)
                if plc_text.isdigit():
                    perf.finish_position = int(plc_text)

            if len(cells) > 10:
                perf.lbw = cells[10].get_text(strip=True)

            if len(cells) > 11:
                perf.finish_time = cells[11].get_text(strip=True)

            if len(cells) > 12:
                odds_text = cells[12].get_text(strip=True)
                try:
                    perf.win_odds = float(odds_text)
                except (ValueError, TypeError):
                    pass

            return perf

        except Exception as e:
            logger.debug("Failed to parse performance row: %s", e)
            return None

    def scrape_jockey_rankings(self) -> list[JockeyStats]:
        """Scrape current season jockey rankings."""
        url = settings.HKJC_JOCKEY_RANKING_URL
        logger.debug("Scraping jockey rankings: %s", url)

        try:
            resp = self.session.get(url, timeout=settings.REQUEST_TIMEOUT)
            resp.raise_for_status()
        except requests.RequestException as e:
            logger.error("Failed to scrape jockey rankings: %s", e)
            return []

        soup = BeautifulSoup(resp.text, "lxml")
        jockeys = []

        table = soup.find("table", class_="table_bd") or soup.find("table", class_="race_table")
        if not table:
            return jockeys

        for row in table.find_all("tr"):
            cells = row.find_all("td")
            if len(cells) < 5:
                continue

            name_cell = cells[0]
            name = name_cell.get_text(strip=True)
            if not name or name.lower() in ("jockey", "rank"):
                continue

            # Extract jockey code from link
            code = ""
            link = name_cell.find("a")
            if link and link.get("href"):
                code_match = re.search(r"JockeyId=(\w+)", link["href"])
                if code_match:
                    code = code_match.group(1)

            stats = JockeyStats(code=code or name[:3].upper(), name=name)

            # Parse W-2nd-3rd-Rides
            for i, attr in enumerate(["season_wins", "season_seconds", "season_thirds", "season_rides"], 1):
                if i < len(cells):
                    val = cells[i].get_text(strip=True)
                    if val.isdigit():
                        setattr(stats, attr, int(val))

            # Stakes
            if len(cells) > 5:
                stakes_text = cells[5].get_text(strip=True).replace(",", "").replace("$", "")
                try:
                    stats.season_stakes = float(stakes_text)
                except (ValueError, TypeError):
                    pass

            jockeys.append(stats)

        return jockeys

    def scrape_trainer_rankings(self) -> list[TrainerStats]:
        """Scrape current season trainer rankings."""
        url = settings.HKJC_TRAINER_RANKING_URL
        logger.debug("Scraping trainer rankings: %s", url)

        try:
            resp = self.session.get(url, timeout=settings.REQUEST_TIMEOUT)
            resp.raise_for_status()
        except requests.RequestException as e:
            logger.error("Failed to scrape trainer rankings: %s", e)
            return []

        soup = BeautifulSoup(resp.text, "lxml")
        trainers = []

        table = soup.find("table", class_="table_bd") or soup.find("table", class_="race_table")
        if not table:
            return trainers

        for row in table.find_all("tr"):
            cells = row.find_all("td")
            if len(cells) < 5:
                continue

            name_cell = cells[0]
            name = name_cell.get_text(strip=True)
            if not name or name.lower() in ("trainer", "rank"):
                continue

            code = ""
            link = name_cell.find("a")
            if link and link.get("href"):
                code_match = re.search(r"TrainerId=(\w+)", link["href"])
                if code_match:
                    code = code_match.group(1)

            stats = TrainerStats(code=code or name[:3].upper(), name=name)

            for i, attr in enumerate(["season_wins", "season_seconds", "season_thirds", "season_runners"], 1):
                if i < len(cells):
                    val = cells[i].get_text(strip=True)
                    if val.isdigit():
                        setattr(stats, attr, int(val))

            if len(cells) > 5:
                stakes_text = cells[5].get_text(strip=True).replace(",", "").replace("$", "")
                try:
                    stats.season_stakes = float(stakes_text)
                except (ValueError, TypeError):
                    pass

            trainers.append(stats)

        return trainers
