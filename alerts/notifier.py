"""
Notifier — sends combined Telegram digest.
One message per run: expansion alerts + website changes + new signal count.
"""

import logging
import os
from datetime import datetime, timezone

import requests

logger = logging.getLogger(__name__)

TELEGRAM_API = "https://api.telegram.org/bot{token}/sendMessage"

SIGNAL_TYPE_EMOJI = {
    "lateral_hire":    "🔀",
    "ranking":         "🏆",
    "bar_leadership":  "🏅",
    "court_record":    "📋",
    "job_posting":     "💼",
    "recruit_posting": "🎓",
    "bar_speaking":    "🎙",
    "bar_sponsorship": "🤝",
    "press_release":   "📰",
    "publication":     "📄",
    "practice_page":   "🖥",
}


class Notifier:
    def __init__(self, config):
        self.token   = config.TELEGRAM_BOT_TOKEN
        self.chat_id = config.TELEGRAM_CHAT_ID
        self.dash_url = os.getenv("DASHBOARD_URL", "")
        self.run_url  = (
            f"https://github.com/{os.getenv('GITHUB_REPOSITORY', '')}"
            f"/actions/runs/{os.getenv('GITHUB_RUN_ID', '')}"
        )

    def send_combined_digest(
        self,
        alerts: list[dict],
        website_changes: list[dict],
        new_signals: list[dict] = None,
    ):
        if not self.token or not self.chat_id:
            logger.warning("Telegram not configured — skipping notification")
            return

        msg = self._build_message(alerts, website_changes, new_signals or [])
        self._send(msg)

    def _build_message(
        self,
        alerts: list[dict],
        changes: list[dict],
        new_signals: list[dict],
    ) -> str:
        now  = datetime.now(timezone.utc)
        week = now.strftime("Week of %B %d, %Y")
        lines = [
            "📊 *Law Firm Expansion Tracker*",
            f"_{week}_",
        ]
        if self.dash_url:
            lines.append(f"[🖥 Open Live Dashboard]({self.dash_url})")
        lines.append("─" * 34)

        # Signal summary
        sig_count = len(new_signals)
        firm_count = len({s["firm_id"] for s in new_signals})
        lines.append(f"*{sig_count} new signal(s)* across *{firm_count} firm(s)*\n")

        # Website changes
        if changes:
            lines.append(f"🔄 *{len(changes)} website change(s) detected*")
            for ch in changes[:3]:
                lines.append(f"  • {ch['firm_name']}: [{ch['url']}]({ch['url']})")
            lines.append("")

        # Ranked expansion alerts
        if not alerts:
            lines.append("_No new expansion alerts this period._")
        else:
            for i, a in enumerate(alerts[:8], 1):
                spike_tag = " 🔥" if a.get("is_spike") else ""
                mult_str  = f"↑ {a['baseline_mult']}× baseline" if a["baseline_mult"] > 1.2 else ""
                lines.append(
                    f"*{i}. {a['firm_name']}*\n"
                    f"  {a['dept_emoji']} {a['department']}{spike_tag}\n"
                    f"  Score: *{a['expansion_score']}* {mult_str}\n"
                    f"  Signals: {a['signal_count']}"
                )

                # Breakdown
                bd = a.get("signal_breakdown", {})
                bd_parts = []
                for stype, count in sorted(bd.items(), key=lambda x: -x[1]):
                    emoji = SIGNAL_TYPE_EMOJI.get(stype, "•")
                    bd_parts.append(f"{emoji} {count} {stype.replace('_', ' ')}")
                if bd_parts:
                    lines.append("  " + " · ".join(bd_parts[:4]))

                # Top 3 signal excerpts
                for s in a.get("signals", [])[:3]:
                    emoji = SIGNAL_TYPE_EMOJI.get(s["signal_type"], "•")
                    url   = s.get("url", "")
                    title = s["title"][:90]
                    line  = f"  {emoji} [{title}]({url})" if url else f"  {emoji} {title}"
                    lines.append(line)

                lines.append("")

        if self.run_url and os.getenv("GITHUB_RUN_ID"):
            lines.append(f"[📋 View Actions Log]({self.run_url})")

        return "\n".join(lines)

    def _send(self, text: str):
        url = TELEGRAM_API.format(token=self.token)
        # Telegram has 4096 char limit per message — split if needed
        chunks = _split_message(text, 4000)
        for chunk in chunks:
            try:
                resp = requests.post(url, json={
                    "chat_id":    self.chat_id,
                    "text":       chunk,
                    "parse_mode": "Markdown",
                    "disable_web_page_preview": True,
                }, timeout=15)
                if not resp.ok:
                    logger.error(f"Telegram error {resp.status_code}: {resp.text[:200]}")
            except Exception as e:
                logger.error(f"Telegram send failed: {e}")


def _split_message(text: str, max_len: int) -> list[str]:
    if len(text) <= max_len:
        return [text]
    parts = []
    while text:
        if len(text) <= max_len:
            parts.append(text)
            break
        # Split at last newline before limit
        cut = text.rfind("\n", 0, max_len)
        if cut == -1:
            cut = max_len
        parts.append(text[:cut])
        text = text[cut:].lstrip("\n")
    return parts
