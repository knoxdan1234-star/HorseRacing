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
        Send prediction embed for a race (Traditional Chinese).

        race_info: {race_no, racecourse, class, distance, track_type, going, field_size}
        predictions: [{horse_no, horse_name, jockey, trainer, win_prob, odds, win_rank}]
        value_bets: [{horse_no, bet_type, bet_amount, edge, odds}]
        """
        course_name = {"ST": "沙田", "HV": "跑馬地", "MJC": "澳門"}.get(
            race_info.get("racecourse", ""), race_info.get("racecourse", "")
        )
        track_name = {"Turf": "草地", "AWT": "全天候跑道", "Dirt": "泥地"}.get(
            race_info.get("track_type", ""), race_info.get("track_type", "")
        )
        going_map = {
            "Good": "好地", "Good to Firm": "好至快",
            "Good to Yielding": "好至黏", "Yielding": "黏地",
            "Soft": "軟地", "Wet Fast": "濕快", "Wet Slow": "濕慢",
            "Heavy": "大爛",
        }
        going = going_map.get(race_info.get("going", ""), race_info.get("going", "未知"))

        title = (
            f"第{race_info.get('race_no', '?')}場 | "
            f"{race_info.get('class', '')} - "
            f"{race_info.get('distance', '')}米 {track_name} - {course_name}"
        )

        desc = f"場地狀況: {going} | 出賽馬匹: {race_info.get('field_size', '?')} 匹"

        fields = []
        top_preds = sorted(predictions, key=lambda p: p.get("win_rank", 99))[:5]

        for i, pred in enumerate(top_preds, 1):
            horse_no = pred.get("horse_no", "?")
            name = pred.get("horse_name", "未知")
            jockey = pred.get("jockey", "")
            trainer = pred.get("trainer", "")
            prob = pred.get("win_prob", 0)
            odds = pred.get("odds", 0)

            vb = next((b for b in value_bets if b.get("horse_no") == horse_no), None)
            value_tag = " | 💎 有價值投注" if vb else ""
            bet_info = f"\n建議投注: HK${vb['bet_amount']:.0f}" if vb and vb.get("bet_amount", 0) > 0 else ""

            fields.append({
                "name": f"{i}. #{horse_no} {name}",
                "value": (
                    f"騎師: {jockey} | 練馬師: {trainer}\n"
                    f"頭馬機率: {prob:.1%} | 賠率: {odds}{value_tag}{bet_info}"
                ),
                "inline": False,
            })

        return self.send_embed(
            title=title,
            description=desc,
            fields=fields,
            color=3066993,
        )

    def send_race_result(
        self,
        race_info: dict,
        bet_results: list[dict],
        meeting_pnl: float,
    ) -> bool:
        """Send post-race result summary (Traditional Chinese)."""
        course_name = {"ST": "沙田", "HV": "跑馬地", "MJC": "澳門"}.get(
            race_info.get("racecourse", ""), race_info.get("racecourse", "")
        )
        title = f"賽果: 第{race_info.get('race_no', '?')}場 {course_name}"

        bet_type_map = {"WIN": "獨贏", "PLA": "位置", "QIN": "連贏",
                        "QPL": "位置Q", "FCT": "單T", "TCE": "三T"}

        fields = []
        for result in bet_results:
            pnl = result.get("pnl", 0)
            status = "✅ 中" if pnl > 0 else "❌ 失"
            bet_type_tc = bet_type_map.get(result.get("bet_type", "WIN"), result.get("bet_type", ""))
            fields.append({
                "name": f"#{result.get('horse_no', '?')} ({bet_type_tc})",
                "value": f"{status} | 盈虧: HK${pnl:+.2f} | 名次: {result.get('position', '?')}",
                "inline": True,
            })

        color = 3066993 if meeting_pnl >= 0 else 15158332

        return self.send_embed(
            title=title,
            description=f"本場日累積盈虧: HK${meeting_pnl:+.2f}",
            fields=fields,
            color=color,
        )

    def send_weekly_pnl(self, pnl_data) -> bool:
        """Send weekly P&L summary (Monday report, Traditional Chinese)."""
        color = 3066993 if pnl_data.net_pnl >= 0 else 15158332

        fields = [
            {"name": "報告期間", "value": f"{pnl_data.week_start} 至 {pnl_data.week_end}", "inline": False},
            {"name": "賽馬日數", "value": str(pnl_data.num_meetings), "inline": True},
            {"name": "投注次數", "value": str(pnl_data.num_bets), "inline": True},
            {"name": "中注次數", "value": str(pnl_data.num_wins), "inline": True},
            {"name": "毛利", "value": f"HK${pnl_data.gross_profit:+,.2f}", "inline": True},
            {"name": "毛損", "value": f"HK${pnl_data.gross_loss:,.2f}", "inline": True},
            {"name": "淨盈虧", "value": f"HK${pnl_data.net_pnl:+,.2f} ({pnl_data.roi_pct:+.1f}%)", "inline": True},
            {"name": "🏆 最佳一注", "value": pnl_data.best_bet, "inline": False},
            {"name": "💔 最差一注", "value": pnl_data.worst_bet, "inline": False},
            {"name": "季度總盈虧", "value": f"HK${pnl_data.season_total_pnl:+,.2f}", "inline": True},
            {"name": "目前本金", "value": f"HK${pnl_data.current_bankroll:,.2f}", "inline": True},
        ]

        return self.send_embed(
            title="每週盈虧報告",
            description="星期一總結",
            fields=fields,
            color=color,
            url=self.pnl_webhook_url,
        )

    def send_alert(self, severity: str, message: str) -> bool:
        """Send a system alert (Traditional Chinese)."""
        color_map = {
            "INFO": 3447003,
            "WARNING": 16776960,
            "ERROR": 16744448,
            "CRITICAL": 16711680,
        }
        severity_tc = {
            "INFO": "資訊", "WARNING": "警告",
            "ERROR": "錯誤", "CRITICAL": "嚴重",
        }.get(severity, severity)
        return self.send_embed(
            title=f"系統警示 [{severity_tc}]",
            description=message,
            color=color_map.get(severity, 3447003),
        )
