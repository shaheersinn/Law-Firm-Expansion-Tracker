"""
Telegram notifier for law firm expansion alerts.

Sends a weekly digest with:
  - Top expanding firms ranked by signal strength
  - Per-firm department breakdown
  - Key evidence (top 2 signals per department)
  - Website change alerts
"""

import logging
import requests
from config import Config

logger = logging.getLogger("notifier")

DEPT_EMOJI = {
    "Corporate / M&A":              "🤝",
    "Private Equity":               "💰",
    "Capital Markets":              "📈",
    "Litigation & Disputes":        "⚖️",
    "Restructuring & Insolvency":   "🔄",
    "Real Estate":                  "🏢",
    "Tax":                          "🧾",
    "Employment & Labour":          "👷",
    "Intellectual Property":        "💡",
    "Data Privacy & Cybersecurity": "🔒",
    "ESG & Regulatory":             "🌿",
    "Energy & Natural Resources":   "⚡",
    "Financial Services & Regulatory": "🏦",
    "Competition & Antitrust":      "🔍",
    "Healthcare & Life Sciences":   "🏥",
    "Immigration":                  "🛂",
    "Infrastructure & Projects":    "🏗️",
}

SIGNAL_LABEL = {
    "lateral_hire":     "👤 Lateral hire",
    "job_posting":      "📋 Job posting",
    "press_release":    "📰 Press release",
    "publication":      "✍️ Publication",
    "practice_page":    "🌐 New practice page",
    "attorney_profile": "👔 Attorney profile",
    # Enhanced
    "bar_leadership":   "🏅 Bar section leadership",
    "ranking":          "🏆 Chambers/Legal 500 ranking",
    "court_record":     "⚖️ CanLII court record",
    "recruit_posting":  "🎓 Student/articling recruit",
    "bar_speaking":     "🎤 Bar speaking engagement",
    "bar_sponsorship":  "🤝 Bar sponsorship",
    "bar_mention":      "📌 Bar association mention",
}


class Notifier:
    def __init__(self, config: Config):
        self.config = config
        self.base_url = f"https://api.telegram.org/bot{config.TELEGRAM_BOT_TOKEN}"

    def send_weekly_digest(self, alerts: list[dict], website_changes: list[dict]):
        """Send the full weekly expansion digest."""
        if not alerts and not website_changes:
            self._send("📊 <b>Weekly Law Firm Expansion Report</b>\n\nNo significant expansion signals detected this week.")
            return

        # Split into chunks — Telegram has a 4096 char limit
        messages = self._build_digest(alerts, website_changes)
        for msg in messages:
            self._send(msg)

    def send_new_signal_alert(self, signal: dict, department: str):
        """Send an immediate alert when a high-confidence signal is found (lateral hire)."""
        emoji = DEPT_EMOJI.get(department, "⚖️")
        msg = (
            f"🚨 <b>New Expansion Signal</b>\n\n"
            f"🏛 <b>{signal['firm_name']}</b>\n"
            f"{emoji} Department: <b>{department}</b>\n"
            f"📌 Type: {SIGNAL_LABEL.get(signal['signal_type'], signal['signal_type'])}\n"
            f"📝 {signal['title']}\n"
        )
        if signal.get("matched_keywords"):
            kws = ", ".join(signal["matched_keywords"][:5])
            msg += f"🔑 Keywords: <i>{kws}</i>\n"
        if signal.get("url"):
            msg += f"🔗 <a href='{signal['url']}'>Source</a>"
        self._send(msg)

    def _build_digest(self, alerts: list[dict], website_changes: list[dict]) -> list[str]:
        messages = []

        # Header
        from datetime import datetime
        week = datetime.utcnow().strftime("Week of %B %d, %Y")
        header = (
            f"📊 <b>Law Firm Expansion Tracker</b>\n"
            f"<i>{week}</i>\n"
            f"{'─' * 30}\n"
            f"<b>{len(alerts)} expansion signal(s)</b> detected across {len(set(a['firm_id'] for a in alerts))} firm(s)\n\n"
        )

        # Build per-alert entries
        entries = []
        for i, alert in enumerate(alerts[:15], 1):  # cap at 15 to avoid spam
            emoji = DEPT_EMOJI.get(alert["department"], "⚖️")
            spike_note = ""
            if alert.get("is_spike") and alert.get("spike_ratio"):
                spike_note = f" ↑ {alert['spike_ratio']}× vs baseline"

            entry = [
                f"{i}. 🏛 <b>{alert['firm_name']}</b>",
                f"   {emoji} <b>{alert['department']}</b>",
                f"   Score: <b>{alert['expansion_score']}</b>{spike_note}",
                f"   Signals: {alert['signal_count']} ({self._format_breakdown(alert['signal_breakdown'])})",
            ]

            # Show top 2 evidence signals
            for sig in alert.get("top_signals", [])[:2]:
                label = SIGNAL_LABEL.get(sig["signal_type"], sig["signal_type"])
                entry.append(f"   • {label}: <i>{sig['title'][:80]}</i>")

            entries.append("\n".join(entry))

        # Website changes section
        web_section = ""
        if website_changes:
            web_section = "\n\n🌐 <b>Practice Page Changes Detected</b>\n"
            for change in website_changes[:5]:
                web_section += f"• <b>{change['firm_name']}</b> — {change['message']}\n"

        # Assemble — split into 4096-char chunks
        body = "\n\n".join(entries)
        footer = (
            "\n\n─\n"
            "<b>Signal key:</b> 🏅 Bar leadership (3.5) · 🏆 Ranking (3.0) · "
            "👤 Lateral hire (3.0) · ⚖️ Court record (2.5) · 🌐 Practice page (2.5) · "
            "📋 Job (2.0) · 🎓 Recruit (2.0) · 📰 Press (1.5) · ✍️ Publication (1.0)\n"
            "Law Firm Expansion Tracker 🤖"
        )

        full = header + body + web_section + footer

        # Telegram message limit is 4096 chars
        while len(full) > 4000:
            # Remove last entry and try again
            entries = entries[:-1]
            body = "\n\n".join(entries)
            full = header + body + web_section + footer
            if not entries:
                break

        messages.append(full)
        return messages

    def _format_breakdown(self, breakdown: dict) -> str:
        if not breakdown:
            return ""
        parts = []
        labels = {
            "lateral_hire":   "hires",
            "job_posting":    "jobs",
            "press_release":  "press",
            "publication":    "pubs",
            "practice_page":  "page",
            "attorney_profile": "bios",
            "bar_leadership": "bar-lead",
            "ranking":        "rankings",
            "court_record":   "court",
            "recruit_posting": "recruit",
            "bar_speaking":   "speaking",
            "bar_sponsorship":"sponsor",
        }
        for k, v in breakdown.items():
            if v > 0:
                parts.append(f"{v} {labels.get(k, k)}")
        return ", ".join(parts)

    def _send(self, text: str):
        if not self.config.TELEGRAM_BOT_TOKEN or not self.config.TELEGRAM_CHAT_ID:
            logger.warning("Telegram not configured")
            return
        try:
            resp = requests.post(
                f"{self.base_url}/sendMessage",
                json={
                    "chat_id": self.config.TELEGRAM_CHAT_ID,
                    "text": text,
                    "parse_mode": "HTML",
                    "disable_web_page_preview": True,
                },
                timeout=15,
            )
            if resp.ok:
                logger.info("Telegram message sent")
            else:
                logger.error(f"Telegram error: {resp.status_code} {resp.text[:200]}")
        except requests.RequestException as e:
            logger.error(f"Telegram request failed: {e}")
