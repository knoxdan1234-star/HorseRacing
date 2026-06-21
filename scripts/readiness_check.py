"""
Race-day readiness check, on demand.

Runs the pipeline readiness checks for a date (default: today HK), prints a
green/red summary, and sends a Discord report unless --no-discord is given.

Usage:
  python scripts/readiness_check.py                 # today, + Discord report
  python scripts/readiness_check.py --date 2026-06-24
  python scripts/readiness_check.py --no-discord    # print only, no Discord

Exit code is non-zero if any CRITICAL check fails (handy for cron/monitoring).
"""
import argparse
import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from config.logging_config import setup_logging
from db.database import get_session
from agents.monitor.readiness import RaceDayReadiness
from discord_bot.webhook import DiscordWebhook


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--date", help="YYYY-MM-DD (default: today in HK time)")
    ap.add_argument("--no-discord", action="store_true", help="print only, don't send to Discord")
    args = ap.parse_args()

    setup_logging()
    session = get_session()
    target = date.fromisoformat(args.date) if args.date else None
    discord = None if args.no_discord else DiscordWebhook()

    checks = RaceDayReadiness(session).report(target, discord=discord)

    print()
    for c in checks:
        tag = "OK" if c.ok else c.severity
        print(f"[{tag:8}] {c.name}: {c.detail}")
    fails = [c for c in checks if not c.ok]
    print("\n" + ("ALL CLEAR ✅" if not fails else f"{len(fails)} issue(s) ⚠️"))

    sys.exit(1 if any(c.severity == "CRITICAL" for c in checks) else 0)


if __name__ == "__main__":
    main()
