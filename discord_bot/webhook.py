"""
Discord Webhook Integration

Sends prediction embeds and P&L reports to Discord channels.
"""

import logging
from datetime import datetime

import requests

from config import settings

logger = logging.getLogger(__name__)


class DiscordWebhook:
    """Sends messages to Discord via webhooks."""

    def __init__(
        self,
        webhook_url: str | None = None,
        pnl_webhook_url: str | None = None,
    ):
        self.webhook_url = webhook_url or settings.DISCORD_WEBHOOK_URL
        self.pnl_webhook_url = pnl_webhook_url or settings.DISCORD_PNL_WEBHOOK_URL or self.webhook_url

    def _post(self, url: str, payload: dict) -> bool:
        """Send a POST request to Discord webhook."""
        if not url:
            logger.warning("Discord webhook URL not configured")
            return False

        try:
            resp = requests.post(url, json=payload, timeout=10)
            if resp.status_code == 204:
                return True
            elif resp.status_code == 429:
                # Rate limited
                retry_after = resp.json().get("retry_after", 5)
                logger.warning("Discord rate limited, retry after %ss", retry_after)
                import time
                time.sleep(retry_after)
                resp = requests.post(url, json=payload, timeout=10)
                return resp.status_code == 204
            else:
                logger.error("Discord webhook failed: %d %s", resp.status_code, resp.text)
                return False
        except requests.RequestException as e:
            logger.error("Discord request failed: %s", e)
            return False

    def send_embed(
        self,
        title: str,
        description: str = "",
        fields: list[dict] | None = None,
        color: int = 3447003,
        url: str | None = None,
    ) -> bool:
        """Send a Discord embed message."""
        embed = {
            "title": title,
            "description": description,
            "color": color,
            "timestamp": datetime.utcnow().isoformat(),
        }
        if fields:
            embed["fields"] = [
                {"name": f["name"], "value": f["value"], "inline": f.get("inline", False)}
                for f in fields
            ]

        target_url = url or self.webhook_url
        return self._post(target_url, {"embeds": [embed]})

    def send_prediction(
        self,
        race_info: dict,
        predictions: list[dict],
        value_bets: list[dict],
    ) -> bool:
        """
        Send prediction embed for a race.

        race_info: {race_no, racecourse, class, distance, track_type, going, field_size}
        predictions: [{horse_no, horse_name, jockey, trainer, win_prob, odds, win_rank}]
        value_bets: [{horse_no, bet_type, bet_amount, edge, odds}]
        """
        course_name = {"ST": "Sha Tin", "HV": "Happy Valley", "MJC": "Macau"}.get(
            race_info.get("racecourse", ""), race_info.get("racecourse", "")
        )

        title = f"Race {race_info.get('race_no', '?')} | {race_info.get('class', '')} - {race_info.get('distance', '')}m {race_info.get('track_type', '')} - {course_name}"

        # Description with race details
        desc = f"Going: {race_info.get('going', 'N/A')} | Field: {race_info.get('field_size', '?')} runners"

        # Top picks
        fields = []
        top_preds = sorted(predictions, key=lambda p: p.get("win_rank", 99))[:5]

        for i, pred in enumerate(top_preds, 1):
            horse_no = pred.get("horse_no", "?")
            name = pred.get("horse_name", "Unknown")
            jockey = pred.get("jockey", "")
            trainer = pred.get("trainer", "")
            prob = pred.get("win_prob", 0)
            odds = pred.get("odds", 0)

            # Check if value bet
            vb = next((b for b in value_bets if b.get("horse_no") == horse_no), None)
            value_tag = " | VALUE BET" if vb else ""
            bet_info = f"\nKelly Bet: ${vb['bet_amount']:.0f}" if vb and vb.get("bet_amount", 0) > 0 else ""

            fields.append({
                "name": f"{i}. #{horse_no} {name}",
                "value": (
                    f"J: {jockey} | T: {trainer}\n"
                    f"Win Prob: {prob:.1%} | Odds: {odds}{value_tag}{bet_info}"
                ),
                "inline": False,
            })

        return self.send_embed(
            title=title,
            description=desc,
            fields=fields,
            color=3066993,  # Green
        )

    def send_race_result(
        self,
        race_info: dict,
        bet_results: list[dict],
        meeting_pnl: float,
    ) -> bool:
        """Send post-race result summary."""
        title = f"Results: R{race_info.get('race_no', '?')} {race_info.get('racecourse', '')}"

        fields = []
        for result in bet_results:
            status = "WON" if result.get("pnl", 0) > 0 else "LOST"
            pnl = result.get("pnl", 0)
            fields.append({
                "name": f"#{result.get('horse_no', '?')} ({result.get('bet_type', 'WIN')})",
                "value": f"{status} | P&L: ${pnl:+.2f} | Pos: {result.get('position', '?')}",
                "inline": True,
            })

        color = 3066993 if meeting_pnl >= 0 else 15158332  # Green if profit, red if loss

        return self.send_embed(
            title=title,
            description=f"Meeting P&L so far: ${meeting_pnl:+.2f}",
            fields=fields,
            color=color,
        )

    def send_weekly_pnl(self, pnl_data) -> bool:
        """Send weekly P&L summary (Monday report)."""
        color = 3066993 if pnl_data.net_pnl >= 0 else 15158332

        fields = [
            {"name": "Period", "value": f"{pnl_data.week_start} to {pnl_data.week_end}", "inline": False},
            {"name": "Meetings", "value": str(pnl_data.num_meetings), "inline": True},
            {"name": "Total Bets", "value": str(pnl_data.num_bets), "inline": True},
            {"name": "Won", "value": str(pnl_data.num_wins), "inline": True},
            {"name": "Gross Profit", "value": f"${pnl_data.gross_profit:+,.2f}", "inline": True},
            {"name": "Gross Loss", "value": f"${pnl_data.gross_loss:,.2f}", "inline": True},
            {"name": "Net P&L", "value": f"${pnl_data.net_pnl:+,.2f} ({pnl_data.roi_pct:+.1f}%)", "inline": True},
            {"name": "Best Bet", "value": pnl_data.best_bet, "inline": False},
            {"name": "Worst Bet", "value": pnl_data.worst_bet, "inline": False},
            {"name": "Season Total P&L", "value": f"${pnl_data.season_total_pnl:+,.2f}", "inline": True},
            {"name": "Current Bankroll", "value": f"${pnl_data.current_bankroll:,.2f}", "inline": True},
        ]

        return self.send_embed(
            title="Weekly P&L Report",
            description="Monday Summary",
            fields=fields,
            color=color,
            url=self.pnl_webhook_url,
        )

    def send_alert(self, severity: str, message: str) -> bool:
        """Send a system alert."""
        color_map = {
            "INFO": 3447003,
            "WARNING": 16776960,
            "ERROR": 16744448,
            "CRITICAL": 16711680,
        }
        return self.send_embed(
            title=f"System Alert [{severity}]",
            description=message,
            color=color_map.get(severity, 3447003),
        )
