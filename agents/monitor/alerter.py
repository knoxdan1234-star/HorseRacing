"""
Alert Dispatcher

Sends alerts to Discord with rate limiting to prevent spam.
"""

import logging
import time
from collections import defaultdict
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

# Rate limit: max 1 alert per category per hour
RATE_LIMIT_SECONDS = 3600


class Alerter:
    """Sends alerts to Discord with rate limiting."""

    def __init__(self, discord_webhook=None):
        self._last_alert_times: dict[str, datetime] = defaultdict(lambda: datetime.min)
        self._discord = discord_webhook  # Injected DiscordWebhook instance

    def set_discord(self, discord_webhook):
        """Set the Discord webhook sender (lazy injection to avoid circular imports)."""
        self._discord = discord_webhook

    def send_alert(
        self,
        severity: str,
        category: str,
        message: str,
        details: str = "",
    ) -> bool:
        """
        Send an alert if not rate-limited.
        Returns True if the alert was sent, False if rate-limited.
        """
        # Rate limiting
        key = f"{severity}:{category}"
        now = datetime.utcnow()
        last = self._last_alert_times[key]

        if (now - last).total_seconds() < RATE_LIMIT_SECONDS:
            logger.debug("Alert rate-limited: %s", key)
            return False

        self._last_alert_times[key] = now

        # Log it
        log_level = {
            "INFO": logging.INFO,
            "WARNING": logging.WARNING,
            "ERROR": logging.ERROR,
            "CRITICAL": logging.CRITICAL,
        }.get(severity, logging.INFO)

        logger.log(log_level, "[%s] %s: %s", severity, category, message)

        # Send to Discord
        if self._discord:
            color_map = {
                "INFO": 3447003,      # Blue
                "WARNING": 16776960,   # Yellow
                "ERROR": 16744448,     # Orange
                "CRITICAL": 16711680,  # Red
            }
            color = color_map.get(severity, 3447003)

            try:
                self._discord.send_embed(
                    title=f"[{severity}] {category}",
                    description=message,
                    fields=[{"name": "Details", "value": details}] if details else [],
                    color=color,
                )
            except Exception as e:
                logger.error("Failed to send Discord alert: %s", e)

        return True

    def send_health_report(self, health_statuses: list) -> None:
        """Send a combined health report if any issues exist."""
        issues = [s for s in health_statuses if s.status != "OK"]

        if not issues:
            return

        # Only send if there are warnings or worse
        worst = max(issues, key=lambda s: {"WARNING": 1, "ERROR": 2, "CRITICAL": 3}.get(s.status, 0))
        severity = worst.status

        message_lines = []
        for s in health_statuses:
            icon = {"OK": "OK", "WARNING": "WARN", "ERROR": "ERR", "CRITICAL": "CRIT"}.get(s.status, "?")
            message_lines.append(f"[{icon}] {s.component}: {s.message}")

        self.send_alert(
            severity=severity,
            category="health",
            message="\n".join(message_lines),
        )
