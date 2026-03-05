"""
Telegram notifier — instant alerts and weekly digest.
Dashboard URL is auto-embedded in every message.
"""

import logging
import os
import requests
from config import Config

logger = logging.getLogger("notifier")

DEPT_EMOJI = {
    "Corporate / M&A":                 "🤝",
    "Private Equity":                  "💰",
    "Capital Markets":                 "📈",
    "Litigation & Disputes":           "⚖️",
    "Restructuring & Insolvency":      "🔄",
    "Real Estate":                     "🏢",
    "Tax":                             "🧾",
    "Employment & Labour":             "👷",
    "Intellectual Property":           "💡",
    "Data Privacy & Cybersecurity":    "🔒",
    "ESG & Regulatory":                "🌿",
    "Energy & Natural Resources":      "⚡",
    "Financial Services & Regulatory": "🏦",
    "Competition & Antitrust":         "🔍",
    "Healthcare & Life Sciences":      "🏥",
    "Immigration":                     "🛂",
    "Infrastructure & Projects":       "🏗️",
}

SIGNAL_LABEL = {
    "lateral_hire":     "👤 Lateral Hire",
    "job_posting":      "📋 Job Posting",
    "press_release":    "📰 Press Release",
    "publication":      "✍️ Publication",
    "practice_page":    "🌐 New Practice Page",
    "attorney_profile": "👔 Attorney Profile",
    "bar_leadership":   "🏅 Bar Leadership",
    "ranking":          "🏆 Ranking",
    "court_record":     "⚖️ Court/Regulatory Record",
    "recruit_posting":  "🎓 Student Recruit",
    "bar_speaking":     "🎤 Conference Speaking",
    "bar_sponsorship":  "🤝 Sponsorship",
    "bar_mention":      "📌 Bar Mention",
}

SIGNAL_BREAKDOWN_LABELS = {
    "lateral_hire":     "hire",
    "job_posting":      "job",
    "bar_leadership":   "bar-lead",
    "ranking":          "ranking",
    "court_record":     "court",
    "recruit_posting":  "recruit",
    "press_release":    "press",
    "publication":      "pub",
    "practice_page":    "page",
    "bar_speaking":     "speaking",
    "bar_sponsorship":  "sponsor",
    "attorney_profile": "bio",
    "bar_mention":      "mention",
}


class Notifier:
    def __init__(self, config: Config):
        self.config = config
        self.base_url = f"https://api.telegram.org/bot{config.TELEGRAM_BOT_TOKEN}"
        # Auto-infer dashboard URL from GitHub environment
        repo = os.environ.get("GITHUB_REPOSITORY", "")
        self.dashboard_url = config.DASHBOARD_URL or (
            f"https://{repo.split('/')[0]}.github.io/{repo.split('/')[-1]}/"
            if "/" in repo else ""
        )

    # ── Instant alert ──────────────────────────────────────────────────
    def send_new_signal_alert(self, signal: dict, department: str):
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
            msg += f"🔗 <a href='{signal['url']}'>Source</a>\n"
        if self.dashboard_url:
            msg += f"\n🖥 <a href='{self.dashboard_url}'>View Full Dashboard →</a>"
        self._send(msg)

    # ── Weekly digest ──────────────────────────────────────────────────
    def send_weekly_digest(self, alerts: list, website_changes: list):
        if not alerts and not website_changes:
            self._send(
                "📊 <b>Law Firm Expansion Tracker</b>\n\n"
                "No significant expansion signals detected this week."
                + (f"\n\n🖥 <a href='{self.dashboard_url}'>Dashboard</a>" if self.dashboard_url else "")
            )
            return

        for msg in self._build_digest(alerts, website_changes):
            self._send(msg)

    def _build_digest(self, alerts: list, website_changes: list) -> list[str]:
        from datetime import datetime
        week = datetime.utcnow().strftime("Week of %B %d, %Y")
        n_firms = len(set(a["firm_id"] for a in alerts))

        dash_line = f"\n\n🖥 <a href='{self.dashboard_url}'>Open Live Dashboard →</a>\n" if self.dashboard_url else "\n"

        header = (
            f"📊 <b>Law Firm Expansion Tracker</b>\n"
            f"<i>{week}</i>"
            f"{dash_line}"
            f"{'─' * 30}\n"
            f"<b>{len(alerts)} expansion signal(s)</b> across {n_firms} firm(s)\n\n"
        )

        entries = []
        for i, alert in enumerate(alerts[:15], 1):
            emoji = DEPT_EMOJI.get(alert["department"], "⚖️")
            spike = ""
            if alert.get("spike_ratio") and alert["spike_ratio"] > 1:
                spike = f" ↑ {alert['spike_ratio']}× baseline"
            z_note = ""
            if alert.get("z_score", 0) >= 2.0:
                z_note = " 🔥"

            parts = [
                f"{i}. 🏛 <b>{alert['firm_name']}</b>",
                f"   {emoji} <b>{alert['department']}</b>{z_note}",
                f"   Score: <b>{alert['expansion_score']}</b>{spike}",
                f"   Signals: {alert['signal_count']} ({self._fmt_breakdown(alert['signal_breakdown'])})",
            ]
            for sig in alert.get("top_signals", [])[:2]:
                label = SIGNAL_LABEL.get(sig["signal_type"], sig["signal_type"])
                parts.append(f"   • {label}: <i>{sig['title'][:80]}</i>")

            entries.append("\n".join(parts))

        web_sec = ""
        if website_changes:
            web_sec = "\n\n🌐 <b>Practice Page Changes</b>\n"
            for c in website_changes[:5]:
                web_sec += f"• <b>{c['firm_name']}</b> — {c['message']}\n"

        footer = (
            "\n\n─\n"
            "<b>Weights:</b> 🏅 Bar lead (3.5) · 🏆 Ranking (3.0) · 👤 Lateral (3.0) · "
            "⚖️ Court (2.5) · 🌐 Practice page (2.5) · 📋 Job (2.0) · 🎓 Recruit (2.0) · "
            "📰 Press (1.5) · 🎤 Speaking (1.5) · ✍️ Pub (1.0)\n"
            "Law Firm Expansion Tracker 🤖"
        )

        body = "\n\n".join(entries)
        full = header + body + web_sec + footer

        # Chunk to 4000-char Telegram limit
        messages = []
        while len(full) > 4000 and entries:
            entries = entries[:-1]
            body = "\n\n".join(entries)
            full = header + body + web_sec + footer
        messages.append(full)
        return messages

    def _fmt_breakdown(self, bd: dict) -> str:
        if not bd:
            return ""
        parts = [f"{v} {SIGNAL_BREAKDOWN_LABELS.get(k,k)}" for k, v in bd.items() if v > 0]
        return ", ".join(parts)

    def _send(self, text: str):
        if not self.config.TELEGRAM_BOT_TOKEN or not self.config.TELEGRAM_CHAT_ID:
            logger.warning("Telegram not configured — skipping send")
            return
        try:
            resp = requests.post(
                f"{self.base_url}/sendMessage",
                json={
                    "chat_id":                  self.config.TELEGRAM_CHAT_ID,
                    "text":                     text,
                    "parse_mode":               "HTML",
                    "disable_web_page_preview": False,
                },
                timeout=15,
            )
            if resp.ok:
                logger.info("Telegram message sent")
            else:
                logger.error(f"Telegram error {resp.status_code}: {resp.text[:200]}")
        except requests.RequestException as e:
            logger.error(f"Telegram request failed: {e}")
