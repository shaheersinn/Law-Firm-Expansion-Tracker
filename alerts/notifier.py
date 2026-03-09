"""
Telegram notifier.
Sends a single combined digest message per run.
"""

import logging
import os
import requests

logger = logging.getLogger("alerts.notifier")

DEPT_EMOJI = {
    "Corporate/M&A": "🏢",
    "Private Equity": "💰",
    "Capital Markets": "📈",
    "Litigation": "⚖️",
    "Restructuring": "🔄",
    "Real Estate": "🏗️",
    "Tax": "📋",
    "Employment": "👔",
    "IP": "💡",
    "Data Privacy": "🔒",
    "ESG": "🌿",
    "Energy": "⚡",
    "Financial Services": "🏦",
    "Competition": "🔍",
    "Healthcare": "🏥",
    "Immigration": "✈️",
    "Infrastructure": "🛣️",
}

TYPE_EMOJI = {
    "lateral_hire":       "🚀",
    "bar_leadership":     "🏅",
    "ranking":            "🏆",
    "office_lease":       "🏢",
    "alumni_hire":        "🎓",
    "job_posting":        "💼",
    "deal_record":        "📝",
    "court_record":       "⚖️",
    "press_release":      "📣",
    "thought_leadership": "✍️",
    "diversity_signal":   "🌈",
    "ip_filing":          "💡",
    "bar_speaking":       "🎤",
    "recruit_posting":    "🎓",
    "publication":        "📚",
}

TIER_LABEL = {1: "🔴 Tier 1", 2: "🟡 Tier 2", 3: "🟢 Tier 3"}

MAX_MSG_LEN = 4000


class Notifier:
    def __init__(self, config):
        self.token   = config.TELEGRAM_BOT_TOKEN
        self.chat_id = config.TELEGRAM_CHAT_ID
        self.dash_url = os.getenv("DASHBOARD_URL", "")
        self.run_id   = os.getenv("GITHUB_RUN_ID", "")
        self.repo     = os.getenv("GITHUB_REPOSITORY", "")

    def send_combined_digest(
        self,
        alerts: list[dict],
        website_changes: list[dict],
        new_signals: list[dict] | None = None,
    ):
        if not self.token or not self.chat_id:
            logger.warning("Telegram not configured — skipping notification")
            return

        from datetime import datetime, timezone
        today = datetime.now(timezone.utc).strftime("%B %d, %Y")
        new_signals = new_signals or []

        # Count signal types
        from collections import Counter
        type_counts = Counter(s.get("signal_type", "other") for s in new_signals)

        lines = [
            f"📊 *Law Firm Expansion Tracker*",
            f"_{today}_",
        ]
        if self.dash_url:
            lines.append(f"🖥 [Open Live Dashboard]({self.dash_url})")
        lines.append("─" * 34)

        # Summary
        lines.append(
            f"*{len(new_signals)}* new signal(s) · "
            f"*{len(alerts)}* expansion alert(s)"
        )
        if type_counts:
            top = ", ".join(f"{TYPE_EMOJI.get(t,'•')} {c} {t.replace('_',' ')}"
                            for t, c in type_counts.most_common(4))
            lines.append(f"_{top}_")

        if not alerts and not website_changes:
            lines.append("\n_No new expansion spikes this run._")
            self._send("\n".join(lines))
            return

        # Expansion alerts
        if alerts:
            lines.append(f"\n*🔔 {len(alerts)} Expansion Alert(s)*\n")
            for i, alert in enumerate(alerts[:12], 1):
                dept_e = DEPT_EMOJI.get(alert["department"], "📌")
                lines.append(
                    f"{i}. 🏛 *{alert['firm_name']}*\n"
                    f"   {dept_e} {alert['department']}"
                    + (" 🔥" if alert.get("z_score", 0) >= 2.0 else "")
                    + f"\n   Score: *{alert['expansion_score']}*"
                    + (f" ↑{alert['z_score']}× baseline" if alert.get("z_score") else "")
                    + f"\n   Signals: {alert['signal_count']}"
                )
                # Top 2 signal bullets
                for sig in alert.get("top_signals", [])[:2]:
                    te = TYPE_EMOJI.get(sig.get("signal_type", ""), "•")
                    title = sig.get("title", "")[:90].replace("*", "").replace("[", "").replace("]", "")
                    url   = sig.get("url", "")
                    if url:
                        lines.append(f"   {te} [{title}]({url})")
                    else:
                        lines.append(f"   {te} {title}")
                lines.append("")

        # Website changes
        if website_changes:
            lines.append(f"*🔄 {len(website_changes)} Website Change(s)*")
            for chg in website_changes[:5]:
                lines.append(f"  • *{chg['firm_name']}* — [{chg['title']}]({chg['url']})")

        # Footer
        if self.run_id and self.repo:
            log_url = f"https://github.com/{self.repo}/actions/runs/{self.run_id}"
            lines.append(f"\n[📋 View Run Log]({log_url})")

        msg = "\n".join(lines)
        self._send(msg)

    def _send(self, text: str):
        # Telegram has a 4096-char limit — split if needed
        chunks = [text[i:i + MAX_MSG_LEN] for i in range(0, len(text), MAX_MSG_LEN)]
        for chunk in chunks:
            try:
                resp = requests.post(
                    f"https://api.telegram.org/bot{self.token}/sendMessage",
                    json={
                        "chat_id": self.chat_id,
                        "text": chunk,
                        "parse_mode": "Markdown",
                        "disable_web_page_preview": True,
                    },
                    timeout=15,
                )
                resp.raise_for_status()
                logger.info("Telegram delivered")
            except Exception as e:
                logger.error(f"Telegram failed: {e}")
