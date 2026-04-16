"""
Health Checker

Monitors system health: CPU, memory, disk, database, scraper freshness.
"""

import logging
import os
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

import psutil
from sqlalchemy import func
from sqlalchemy.orm import Session

from config import settings
from db.models import ModelMetadata, Race

logger = logging.getLogger(__name__)


@dataclass
class HealthStatus:
    component: str
    status: str  # OK, WARNING, ERROR, CRITICAL
    message: str
    value: str = ""


class HealthChecker:
    """Runs system health checks."""

    def __init__(self, session: Session):
        self.session = session

    def run_all_checks(self) -> list[HealthStatus]:
        """Run all health checks and return results."""
        checks = [
            self.check_cpu_usage(),
            self.check_memory_usage(),
            self.check_disk_space(),
            self.check_database_integrity(),
            self.check_database_size(),
            self.check_scraper_freshness(),
            self.check_model_staleness(),
        ]
        return checks

    def check_cpu_usage(self) -> HealthStatus:
        """Check CPU usage."""
        cpu_pct = psutil.cpu_percent(interval=1)
        status = "OK"
        if cpu_pct > 90:
            status = "CRITICAL"
        elif cpu_pct > 75:
            status = "WARNING"

        return HealthStatus(
            component="CPU",
            status=status,
            message=f"CPU usage: {cpu_pct:.1f}%",
            value=f"{cpu_pct:.1f}%",
        )

    def check_memory_usage(self) -> HealthStatus:
        """Check RAM usage."""
        mem = psutil.virtual_memory()
        status = "OK"
        if mem.percent > 90:
            status = "CRITICAL"
        elif mem.percent > 80:
            status = "WARNING"

        return HealthStatus(
            component="Memory",
            status=status,
            message=f"Memory: {mem.percent:.1f}% ({mem.used // (1024**2)}MB / {mem.total // (1024**2)}MB)",
            value=f"{mem.percent:.1f}%",
        )

    def check_disk_space(self) -> HealthStatus:
        """Check disk usage."""
        disk = psutil.disk_usage(str(settings.DATA_DIR))
        used_pct = disk.percent
        status = "OK"
        if used_pct > 90:
            status = "CRITICAL"
        elif used_pct > 80:
            status = "WARNING"

        free_gb = disk.free / (1024 ** 3)
        return HealthStatus(
            component="Disk",
            status=status,
            message=f"Disk: {used_pct:.1f}% used ({free_gb:.1f}GB free)",
            value=f"{used_pct:.1f}%",
        )

    def check_database_integrity(self) -> HealthStatus:
        """Run SQLite integrity check."""
        try:
            result = self.session.execute(
                __import__("sqlalchemy").text("PRAGMA integrity_check")
            )
            check = result.scalar()
            if check == "ok":
                return HealthStatus("Database", "OK", "Integrity check passed")
            else:
                return HealthStatus("Database", "CRITICAL", f"Integrity check failed: {check}")
        except Exception as e:
            return HealthStatus("Database", "ERROR", f"Integrity check error: {e}")

    def check_database_size(self) -> HealthStatus:
        """Check SQLite file size."""
        db_path = settings.DATA_DIR / "horseracing.db"
        if not db_path.exists():
            return HealthStatus("DB Size", "WARNING", "Database file not found")

        size_mb = db_path.stat().st_size / (1024 ** 2)
        status = "OK"
        if size_mb > 5000:
            status = "WARNING"

        return HealthStatus(
            component="DB Size",
            status=status,
            message=f"Database size: {size_mb:.1f}MB",
            value=f"{size_mb:.1f}MB",
        )

    def check_scraper_freshness(self) -> HealthStatus:
        """Check when the last race was scraped."""
        latest_race = (
            self.session.query(func.max(Race.created_at)).scalar()
        )

        if not latest_race:
            return HealthStatus("Scraper", "WARNING", "No race data in database")

        age = datetime.utcnow() - latest_race
        status = "OK"
        if age > timedelta(days=14):
            status = "WARNING"
        if age > timedelta(days=30):
            status = "ERROR"

        return HealthStatus(
            component="Scraper",
            status=status,
            message=f"Last scrape: {latest_race.strftime('%Y-%m-%d %H:%M')} ({age.days}d ago)",
            value=f"{age.days}d ago",
        )

    def check_model_staleness(self) -> HealthStatus:
        """Check when the model was last trained."""
        latest_model = (
            self.session.query(ModelMetadata)
            .order_by(ModelMetadata.trained_at.desc())
            .first()
        )

        if not latest_model:
            return HealthStatus("Model", "WARNING", "No model trained yet")

        age = datetime.utcnow() - latest_model.trained_at
        status = "OK"
        if age > timedelta(days=45):
            status = "WARNING"
        if age > timedelta(days=90):
            status = "ERROR"

        return HealthStatus(
            component="Model",
            status=status,
            message=f"Model '{latest_model.version}' trained {age.days}d ago (AUC: {latest_model.validation_metric or 0:.4f})",
            value=f"{age.days}d ago",
        )
