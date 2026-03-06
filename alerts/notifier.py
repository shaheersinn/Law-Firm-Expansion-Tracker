"""
Telegram notifier — ONE combined message per run.

Replaces the old per-signal blast (which caused 10+ separate Telegram messages).
Every collect run sends a single message containing:
  - Count of new signals collected this run
  - Top expansion alerts ranked by score with evidence
  - Practice page changes
  - Dashboard link + GitHub Actions run link
"""

import os
import logging
import requests
from datetime import datetime, timezone
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
    "lateral_hire":     "👤 Lateral hire",
    "job_posting":      "📋 Job posting",
    "press_release":    "📰 Press release",
    "publication":      "✍️ Publication",
    "practice_page":    "🌐 Practice page",
    "attorney_profile": "👔 Attorney profile",
    "bar_leadership":   "🏅 Bar leadership",
    "ranking":          "🏆 Ranking",
    "court_record":     "⚖️ Court record",
    "recruit_posting":  "🎓 Articling recruit",
    "bar_speaking":     "🎤 Speaking engagement",
    "bar_sponsorship":  "🤝 Bar sponsorship",
    "bar_mention":      "📌 Bar mention",
}

# Set DASHBOARD_URL in GitHub Secrets (or .env) to point at your GitHub Pages dashboard
# e.g. https://yourname.github.io/law-firm-tracker/
_REPO  = os.environ.get("GITHUB_REPOSITORY", "")
_RUN   = os.environ.get("GITHUB_RUN_ID", "")
_OWNER = _REPO.split("/")[0] if "/" in _REPO else ""
_RNAME = _REPO.split("/")[1] if "/" in _REPO else "law-firm-tracker"

DASHBOARD_URL = os.environ.get(
    "DASHBOARD_URL",
    f"https://{_OWNER}.github.io/{_RNAME}/" if _OWNER else f"https://github.com/{_REPO}",
)
RUN_URL = (
    f"https://github.com/{_REPO}/actions/runs/{_RUN}"
    if (_REPO and _RUN) else ""
)


class Notifier:
    def __init__(self, config: Config):
        self.config = config
        self.base_url = f"https://api.telegram.org/bot{config.TELEGRAM_BOT_TOKEN}"

    # ------------------------------------------------------------------ #
    #  Public API
    # ------------------------------------------------------------------ #

    def send_combined_digest(
        self,
        alerts: list[dict],
        website_changes: list[dict],
        new_signals: list = None,
    ):
        """
        Send exactly ONE Telegram message covering the full collect run.
        Replaces the old per-signal individual blast.
        """
        if not self.config.TELEGRAM_BOT_TOKEN or not self.config.TELEGRAM_CHAT_ID:
            logger.warning("Telegram not configured — skipping")
            return
        msg = self._build_message(alerts, website_changes, new_signals or [])
        self._send(msg)

    # Backward-compat alias so any remaining send_weekly_digest() calls still work
    def send_weekly_digest(self, alerts, website_changes, **kwargs):
        self.send_combined_digest(alerts, website_changes, kwargs.get("new_signals", []))

    # ------------------------------------------------------------------ #
    #  Message builder
    # ------------------------------------------------------------------ #

    def _build_message(
        self,
        alerts: list[dict],
        website_changes: list[dict],
        new_signals: list,
    ) -> str:
        now     = datetime.now(timezone.utc)
        ts      = now.strftime("%b %d %Y · %H:%M UTC")
        n_firms = len(set(a["firm_id"] for a in alerts)) if alerts else 0

        lines = [
            "📊 <b>Law Firm Expansion Tracker</b>",
            f"<i>{ts}</i>",
            "─" * 30,
        ]

        # ── New signals collected this run ───────────────────────────────
        if new_signals:
            by_type: dict[str, int] = {}
            for s in new_signals:
                by_type[s["signal_type"]] = by_type.get(s["signal_type"], 0) + 1
            type_parts = [
                f"{v} {SIGNAL_LABEL.get(k, k).split()[-1].lower()}"
                for k, v in sorted(by_type.items(), key=lambda x: -x[1])[:4]
                if v > 0
            ]
            lines.append(
                f"🆕 <b>{len(new_signals)} new signal(s)</b> · "
                f"{len(set(s['firm_id'] for s in new_signals))} firm(s)"
            )
            if type_parts:
                lines.append(f"   <i>{' · '.join(type_parts)}</i>")
            lines.append("")

        # ── Expansion alerts ─────────────────────────────────────────────
        if alerts:
            lines.append(
                f"🔥 <b>{len(alerts)} expansion alert(s)</b> across <b>{n_firms} firm(s)</b>"
            )
            lines.append("")
            for i, a in enumerate(alerts[:12], 1):
                emoji = DEPT_EMOJI.get(a["department"], "⚖️")
                spike = f" ↑{a['spike_ratio']}×" if a.get("is_spike") and a.get("spike_ratio") else ""
                lines.append(
                    f"{i}. 🏛 <b>{a['firm_name']}</b> · {emoji} {a['department']}"
                )
                lines.append(
                    f"   Score <b>{a['expansion_score']}</b>{spike} · "
                    f"{a['signal_count']} signal(s) · {self._fmt_breakdown(a['signal_breakdown'])}"
                )
                for sig in a.get("top_signals", [])[:2]:
                    label = SIGNAL_LABEL.get(sig["signal_type"], sig["signal_type"])
                    title = sig["title"][:75].rstrip()
                    url   = sig.get("url", "")
                    if url:
                        lines.append(f"   • {label}: <a href='{url}'>{title}</a>")
                    else:
                        lines.append(f"   • {label}: <i>{title}</i>")
                lines.append("")
        else:
            lines.append("ℹ️ No new expansion alerts this run.")
            lines.append("")

        # ── Website changes ──────────────────────────────────────────────
        if website_changes:
            lines.append("🌐 <b>Practice Page Changes</b>")
            for ch in website_changes[:4]:
                lines.append(f"   • <b>{ch['firm_name']}</b> — {ch['message']}")
            lines.append("")

        # ── Footer: dashboard + run links ────────────────────────────────
        lines.append("─" * 30)
        footer_links = [f"<a href='{DASHBOARD_URL}'>📈 Dashboard</a>"]
        if RUN_URL:
            footer_links.append(f"<a href='{RUN_URL}'>📋 Run Logs</a>")
        lines.append("  ·  ".join(footer_links))
        lines.append(
            "<i>🏅3.5 · 🏆👤3.0 · ⚖️🌐2.5 · 📋🎓2.0 · 📰1.5 · ✍️1.0</i>"
        )

        full = "\n".join(lines)

        # Telegram hard limit — trim from bottom if over 4000 chars
        while len(full) > 4000 and len(alerts) > 1:
            alerts = alerts[:-1]
            return self._build_message(alerts, website_changes, new_signals)

        return full

    # ------------------------------------------------------------------ #
    #  Helpers
    # ------------------------------------------------------------------ #

    def _fmt_breakdown(self, breakdown: dict) -> str:
        if not breakdown:
            return ""
        short = {
            "lateral_hire":    "hire",  "job_posting":     "job",
            "press_release":   "press", "publication":     "pub",
            "practice_page":   "pg",    "attorney_profile":"bio",
            "bar_leadership":  "bar",   "ranking":         "rank",
            "court_record":    "court", "recruit_posting":  "rec",
        }
        return ", ".join(
            f"{v} {short.get(k, k)}" for k, v in breakdown.items() if v > 0
        )

    def _send(self, text: str):
        try:
            resp = requests.post(
                f"{self.base_url}/sendMessage",
                json={
                    "chat_id":                  self.config.TELEGRAM_CHAT_ID,
                    "text":                     text,
                    "parse_mode":               "HTML",
                    "disable_web_page_preview": True,
                },
                timeout=15,
            )
            if resp.ok:
                logger.info("Telegram combined digest sent (1 message)")
            else:
                logger.error(f"Telegram error {resp.status_code}: {resp.text[:200]}")
        except requests.RequestException as e:
            logger.error(f"Telegram request failed: {e}")
