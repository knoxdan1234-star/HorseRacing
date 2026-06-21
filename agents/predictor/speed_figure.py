"""
Speed figures.

Converts a runner's raw finish time into a cross-race comparable rating of how
fast it actually ran, normalized for distance and going (track condition). A
horse that ran a fast time in a slow race gets credit the finishing position
hides — the most predictive signal professional handicappers (and Benter) use.

Figure = (runner's speed - par speed for that distance, going-adjusted) * 100.
~0 is an average performance; higher is faster. Stored on Runner.speed_figure
by scripts/backfill_speed_figures.py and read as per-horse form features.

Note: v1 normalizes by distance + going only. A per-day track variant (how fast
the surface ran that day) is a known refinement left for v2.
"""
import logging
import re
import statistics
from collections import defaultdict

logger = logging.getLogger(__name__)

MIN_SAMPLES = 20  # minimum times needed to trust a par/going baseline


def parse_time(t: str | None) -> float | None:
    """'1:35.88' -> 95.88 seconds; '58.42' -> 58.42. None if unparseable/insane."""
    if not t:
        return None
    m = re.match(r"^\s*(?:(\d+):)?(\d{1,2}(?:\.\d+)?)\s*$", str(t))
    if not m:
        return None
    mins = float(m.group(1)) if m.group(1) else 0.0
    total = mins * 60 + float(m.group(2))
    return total if 30.0 < total < 400.0 else None  # HK races run ~55s–230s


class SpeedFigureCalculator:
    """Builds par baselines once from all historical times, then rates any time."""

    def __init__(self, session):
        self.session = session
        self._par_speed: dict[int, float] = {}   # distance(m) -> median speed (m/s)
        self._going_adj: dict[str, float] = {}    # going -> median (speed - par)
        self._build_baselines()

    def _build_baselines(self):
        from db.models import Race, Runner

        rows = (
            self.session.query(Race.distance, Race.going, Runner.finish_time)
            .join(Runner, Runner.race_id == Race.id)
            .filter(
                Runner.finish_position.isnot(None),
                Runner.finish_position > 0,
                Runner.finish_time.isnot(None),
                Race.distance.isnot(None),
            )
            .all()
        )

        by_dist: dict[int, list] = defaultdict(list)
        samples = []  # (going, dist, speed)
        for dist, going, ft in rows:
            secs = parse_time(ft)
            if not secs or not dist:
                continue
            speed = dist / secs
            by_dist[dist].append(speed)
            samples.append((going, dist, speed))

        self._par_speed = {d: statistics.median(v) for d, v in by_dist.items()
                           if len(v) >= MIN_SAMPLES}

        going_resid: dict[str, list] = defaultdict(list)
        for going, dist, speed in samples:
            par = self._par_speed.get(dist)
            if par is not None:
                going_resid[going].append(speed - par)
        self._going_adj = {g: statistics.median(v) for g, v in going_resid.items()
                           if len(v) >= MIN_SAMPLES}

        logger.info("Speed baselines: %d distances, %d going categories",
                    len(self._par_speed), len(self._going_adj))

    def _par_for(self, distance: int) -> float | None:
        par = self._par_speed.get(distance)
        if par is not None:
            return par
        if not self._par_speed:
            return None
        nearest = min(self._par_speed, key=lambda d: abs(d - distance))
        return self._par_speed[nearest] if abs(nearest - distance) <= 100 else None

    def figure(self, finish_time, distance, going) -> float | None:
        secs = parse_time(finish_time)
        if not secs or not distance:
            return None
        par = self._par_for(distance)
        if par is None:
            return None
        speed = distance / secs
        adj = self._going_adj.get(going, 0.0)
        fig = round((speed - par - adj) * 100.0, 2)
        # A same-distance figure beyond ~±120 means a mistimed / pulled-up run
        # (bad data), not a real performance — reject so it can't skew features.
        return fig if -120.0 <= fig <= 120.0 else None
