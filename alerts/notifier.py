"""
Notifier — sends Telegram digest with rich formatting.

Always links to https://law-firm-tracker.vercel.app/ (hardcoded default,
overridable via DASHBOARD_URL env var).

Includes:
  - Vercel dashboard link at top of every message
  - Strength badges  🟡 🟠 🔴 🚨
  - Signal source breakdown per alert
  - Evidence links (top 3 per alert)
  - Corroboration source list
  - GitHub Actions run link
  - Instant lateral-hire flash alert
"""

import logging
import os
import html
from datetime import datetime, timezone

import requests

logger = logging.getLogger(__name__)

TELEGRAM_API  = "https://api.telegram.org/bot{token}/sendMessage"
DASHBOARD_URL = os.getenv("DASHBOARD_URL", "https://law-firm-tracker.vercel.app/")

SIGNAL_EMOJI = {
    "lateral_hire":    "🔀",
    "ranking":         "🏆",
    "bar_leadership":  "🏅",
    "court_record":    "📋",
    "job_posting":     "💼",
    "recruit_posting": "🎓",
    "press_release":   "📰",
    "publication":     "📄",
    "practice_page":   "🌐",
    "attorney_profile":"👔",
}

STRENGTH = [
    (12.0, "🚨 Very Strong"),
    (8.0,  "🔴 Strong"),
    (5.0,  "🟠 Moderate"),
    (0.0,  "🟡 Emerging"),
]

DEPT_EMOJI = {
    "Corporate/M&A":         "🤝",
    "Capital Markets":       "📈",
    "Private Equity":        "💰",
    "Litigation":            "⚖️",
    "Tax":                   "🧾",
    "Restructuring":         "🔄",
    "Real Estate":           "🏢",
    "Employment":            "👷",
    "IP":                    "💡",
    "Data Privacy":          "🔒",
    "ESG":                   "🌿",
    "Energy":                "⚡",
    "Financial Services":    "🏦",
    "Competition":           "🔍",
    "Healthcare":            "🏥",
    "Immigration":           "🛂",
    "Infrastructure":        "🏗️",
}


def _e(s: str) -> str:
    return html.escape(str(s), quote=False)


def _strength(score: float) -> str:
    for threshold, label in STRENGTH:
        if score >= threshold:
            return label
    return "🟡 Emerging"


def _dept_emoji(dept: str) -> str:
    for key, emoji in DEPT_EMOJI.items():
        if key.lower() in dept.lower():
            return emoji
    return "🏛"


class Notifier:
    def __init__(self, config):
        self.token   = config.TELEGRAM_BOT_TOKEN
        self.chat_id = config.TELEGRAM_CHAT_ID
        self.run_url = (
            f"https://github.com/{os.getenv('GITHUB_REPOSITORY', '')}"
            f"/actions/runs/{os.getenv('GITHUB_RUN_ID', '')}"
        )

    def send_combined_digest(self, alerts, website_changes, new_signals=None):
        if not self.token or not self.chat_id:
            logger.warning("Telegram not configured — skipping")
            return

        new_signals = new_signals or []
        if not alerts and not website_changes and not new_signals:
            logger.info("Nothing to report — no Telegram message sent")
            return

        msg = self._build(alerts, website_changes, new_signals)
        self._send(msg)

    def send_lateral_flash(self, signal: dict):
        """Instant alert for a high-confidence lateral hire."""
        if not self.token or not self.chat_id:
            return
        dept = signal.get("department", "")
        emoji = _dept_emoji(dept)
        msg = (
            f"⚡ <b>LATERAL HIRE DETECTED</b>\n\n"
            f"<b>{_e(signal['firm_name'])}</b>\n"
            f"{emoji} {_e(dept)}\n\n"
            f"📌 {_e(signal['title'][:200])}\n\n"
            f'🖥 <a href="{_e(DASHBOARD_URL)}">View Dashboard</a>'
        )
        self._send(msg)

    def _build(self, alerts, changes, new_signals) -> str:
        now  = datetime.now(timezone.utc)
        date = now.strftime("%a %b %d, %Y · %H:%M UTC")

        lines = [
            "📊 <b>Law Firm Expansion Tracker</b>",
            f"<i>{_e(date)}</i>",
            "",
            f'🖥 <b><a href="{_e(DASHBOARD_URL)}">law-firm-tracker.vercel.app</a></b>',
            "─" * 30,
        ]

        # ── Summary ──────────────────────────────────────────────────────────
        sig_count  = len(new_signals)
        firm_count = len({s["firm_id"] for s in new_signals}) if new_signals else 0
        alert_count = len(alerts)

        lines.append(
            f"<b>{sig_count}</b> new signal(s) · "
            f"<b>{firm_count}</b> firm(s) · "
            f"<b>{alert_count}</b> alert(s)"
        )
        lines.append("")

        # ── Website changes ───────────────────────────────────────────────────
        if changes:
            lines.append(f"🔄 <b>{len(changes)} website change(s)</b>")
            for ch in changes[:3]:
                lines.append(f"  • {_e(ch['firm_name'])}")
            lines.append("")

        # ── Expansion alerts ──────────────────────────────────────────────────
        if not alerts:
            lines.append("<i>No new expansion alerts this period.</i>")
        else:
            lines.append(f"<b>🚨 {len(alerts)} Expansion Alert(s)</b>")
            lines.append("")

            for i, a in enumerate(alerts[:10], 1):
                strength = _strength(a["expansion_score"])
                dept     = a.get("department", "")
                emoji    = _dept_emoji(dept)
                spike    = "  🔥 SPIKE" if a.get("is_spike") else ""
                mult_str = ""
                if a.get("baseline_mult", 1.0) > 1.3:
                    mult_str = f"  ↑{a['baseline_mult']}× baseline"

                lines.append(f"<b>{i}. {_e(a['firm_name'])}</b>")
                lines.append(
                    f"  {emoji} <b>{_e(dept)}</b>  ·  {strength}{spike}"
                )
                lines.append(
                    f"  Score: <b>{a['expansion_score']}</b>{_e(mult_str)}"
                    f"  ·  {a['signal_count']} signal(s)"
                )

                # Source breakdown
                bd = a.get("signal_breakdown", {})
                if bd:
                    parts = [
                        f"{SIGNAL_EMOJI.get(t, '•')} {n} {t.replace('_', ' ')}"
                        for t, n in sorted(bd.items(), key=lambda x: -x[1])[:4]
                    ]
                    lines.append("  " + " · ".join(parts))

                # Evidence links
                for s in a.get("signals", [])[:3]:
                    em    = SIGNAL_EMOJI.get(s["signal_type"], "•")
                    url   = s.get("url", "")
                    title = _e(s["title"][:80])
                    if url:
                        lines.append(f'  {em} <a href="{_e(url)}">{title}</a>')
                    else:
                        lines.append(f"  {em} <i>{title}</i>")

                # Corroboration
                src = a.get("source_types", [])
                if len(src) >= 2:
                    lines.append(f"  <i>Sources: {', '.join(src[:4])}</i>")

                lines.append("")

        # ── Footer ────────────────────────────────────────────────────────────
        lines.append(f'🖥 <a href="{_e(DASHBOARD_URL)}">View full dashboard →</a>')

        if os.getenv("GITHUB_RUN_ID"):
            lines.append(f'📋 <a href="{_e(self.run_url)}">Actions log</a>')

        return "\n".join(lines)

    def _send(self, text: str):
        url = TELEGRAM_API.format(token=self.token)
        for chunk in _chunk(text, 4000):
            try:
                resp = requests.post(url, json={
                    "chat_id":                  self.chat_id,
                    "text":                     chunk,
                    "parse_mode":               "HTML",
                    "disable_web_page_preview": True,
                }, timeout=15)
                if resp.ok:
                    logger.info("Telegram delivered")
                else:
                    logger.error(f"Telegram {resp.status_code}: {resp.text[:300]}")
            except Exception as e:
                logger.error(f"Telegram send failed: {e}")


def _chunk(text: str, max_len: int) -> list:
    if len(text) <= max_len:
        return [text]
    parts = []
    while text:
        if len(text) <= max_len:
            parts.append(text)
            break
        cut = text.rfind("\n", 0, max_len)
        if cut == -1:
            cut = max_len
        parts.append(text[:cut])
        text = text[cut:].lstrip("\n")
    return parts
