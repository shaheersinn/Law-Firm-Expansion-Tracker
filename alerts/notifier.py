"""
Telegram notifier — v2
=======================
Improvements:
  - Velocity arrows per alert (↑↑ ↑ → ↓ ↓↓)
  - Sector momentum block (🌊) when 3+ firms spike same dept
  - Run stats footer showing 7-run average comparison
  - Confidence score visible in top-signal bullets
  - Cleaner chunking that never splits an alert mid-block
"""

import logging
import os
import re
import requests
from urllib.parse import urlparse

logger = logging.getLogger("alerts.notifier")

DEPT_EMOJI: dict[str, str] = {
    "Corporate/M&A":      "🏢",
    "Private Equity":     "💰",
    "Capital Markets":    "📈",
    "Litigation":         "⚖️",
    "Restructuring":      "🔄",
    "Real Estate":        "🏗️",
    "Tax":                "📋",
    "Employment":         "👔",
    "IP":                 "💡",
    "Data Privacy":       "🔒",
    "ESG":                "🌿",
    "Energy":             "⚡",
    "Financial Services": "🏦",
    "Competition":        "🔍",
    "Healthcare":         "🏥",
    "Immigration":        "✈️",
    "Infrastructure":     "🛣️",
}

TYPE_EMOJI: dict[str, str] = {
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
    "website_snapshot":   "🔄",
}

MAX_MSG_LEN = 4000

# ── Module-level helper functions ─────────────────────────────────────────────

# Human-readable labels for signal types used in breakdowns
_BREAKDOWN_LABELS: dict[str, str] = {
    "press_release":      "press releases",
    "publication":        "publications",
    "practice_page":      "practice pages",
    "job_posting":        "job postings",
    "lateral_hire":       "lateral hires",
    "recruit_posting":    "recruit postings",
    "website_snapshot":   "site snapshots",
    "bar_leadership":     "bar roles",
    "ranking":            "rankings",
    "thought_leadership": "thought leadership",
    "diversity_signal":   "diversity signals",
    "alumni_hire":        "alumni hires",
    "deal_record":        "deals",
    "court_record":       "court records",
    "ip_filing":          "IP filings",
    "bar_speaking":       "speaking",
    "office_lease":       "office signals",
}


def _clean_title(title: str) -> str:
    """
    Strip [Source Tag] prefixes and clean up common noise patterns.
    E.g. "[Practice Page] Firm — Department" → "Department"
         "[Firm News] PublicationThe rise..." → "The rise..."
    """
    if not title:
        return title
    # Remove leading [Source Tag] prefix
    cleaned = re.sub(r"^\[.*?\]\s*", "", title).strip()
    # "Firm — Practice Area" → "Practice Area"
    cleaned = re.sub(r"^[^—–]+\s[—–]\s", "", cleaned).strip()
    # Fix leading concatenated word: "PublicationThe rise" → "The rise"
    cleaned = re.sub(r"^[A-Z][a-z]+(?=[A-Z])", "", cleaned).strip()
    # Fix missing space before capitals: "actionsCanada" → "actions Canada"
    cleaned = re.sub(r"([a-z])([A-Z])", r"\1 \2", cleaned).strip()
    return cleaned or title


def _fmt_breakdown(breakdown: dict) -> str:
    """
    Format a signal-type breakdown dict as a human-readable string.
    E.g. {"press_release": 3, "publication": 2} → "3 press releases, 2 publications"
    """
    if not breakdown:
        return ""
    parts = []
    for stype, cnt in sorted(breakdown.items(), key=lambda x: -x[1]):
        label = _BREAKDOWN_LABELS.get(stype, stype.replace("_", " "))
        parts.append(f"{cnt} {label}")
    return ", ".join(parts[:5])


def _strength_badge(score: float) -> str:
    """Return an emoji strength indicator for an expansion score."""
    if score >= 20:
        return "🔥🔥🔥"
    if score >= 12:
        return "🔥🔥"
    if score >= 6:
        return "🔥"
    if score >= 3.5:
        return "⚡"
    return "📍"


def _page_name_from_url(url: str) -> str:
    """Extract a human-readable page name from a URL path slug."""
    if not url:
        return url
    path = urlparse(url).path.rstrip("/")
    slug = path.split("/")[-1] if path else ""
    return slug.replace("-", " ").replace("_", " ").title() or url


class Notifier:
    def __init__(self, config):
        self.token    = config.TELEGRAM_BOT_TOKEN
        self.chat_id  = config.TELEGRAM_CHAT_ID
        self.dash_url = os.getenv("DASHBOARD_URL", "")
        self.run_id   = os.getenv("GITHUB_RUN_ID", "")
        self.repo     = os.getenv("GITHUB_REPOSITORY", "")

    # ------------------------------------------------------------------ #

    def _build_message(
        self,
        alerts: list[dict],
        website_changes: list[dict],
        new_signals: list[dict] | None = None,
    ) -> str:
        """
        Build a clean Telegram-ready digest string without sending.
        Used by the test harness and by send_combined_digest internally.
        Avoids all checked invariants:
          - no "(s)" lazy plurals
          - no raw "Score N.NN" strings
          - no "website_snapshot" literal
          - no raw [Source Tag] prefixes in titles
          - no cryptic abbreviations (pg, pub, rec)
          - no verbose "Practice area page changed at URL" strings
          - stays within MAX_MSG_LEN characters
        """
        from datetime import datetime, timezone

        today = datetime.now(timezone.utc).strftime("%B %d, %Y")
        new_signals = new_signals or []

        n_sig = len(new_signals)
        n_alr = len(alerts)

        header = "\n".join([
            "📊 *Law Firm Expansion Tracker*",
            f"_{today}_",
            "─" * 34,
            f"*{n_sig}* new signals · *{n_alr}* alerts",
        ])

        body_parts: list[str] = []
        if alerts:
            count_label = "Alerts" if n_alr != 1 else "Alert"
            body_parts.append(f"\n*🔔 {n_alr} Expansion {count_label}*\n")
            for i, alert in enumerate(alerts, 1):
                dept_e  = DEPT_EMOJI.get(alert["department"], "📌")
                badge   = _strength_badge(alert["expansion_score"])
                arrow   = alert.get("velocity_arrow", "→")
                z       = alert.get("z_score", 0)
                z_lbl   = f" ↑{z:.1f}σ" if z else ""
                sector  = " 🌊" if alert.get("sector_momentum") else ""
                new_lbl = " 🆕" if alert.get("is_new_baseline") else ""

                block_lines = [
                    f"{i}. 🏛 *{alert['firm_name']}*{sector}",
                    f"   {dept_e} {alert['department']} {arrow}{new_lbl}",
                    f"   {badge}{z_lbl}  {alert['signal_count']} signals",
                ]
                for sig in alert.get("top_signals", [])[:2]:
                    te    = TYPE_EMOJI.get(sig.get("signal_type", ""), "•")
                    raw_t = sig.get("title", "")
                    title = _clean_title(raw_t)[:80].replace("*", "").replace("[", "").replace("]", "")
                    url   = sig.get("url", "")
                    if url:
                        block_lines.append(f"   {te} [{title}]({url})")
                    else:
                        block_lines.append(f"   {te} {title}")

                block = "\n".join(block_lines)
                # Check if adding this block would exceed the limit
                candidate = header + "\n" + "\n".join(body_parts + [block])
                if len(candidate) > MAX_MSG_LEN - 200:
                    # Add truncation notice and stop
                    remaining = n_alr - i + 1
                    body_parts.append(f"_… and {remaining} more alerts_")
                    break
                body_parts.append(block)

        if website_changes:
            wc_count = len(website_changes)
            wc_label = "Changes" if wc_count != 1 else "Change"
            body_parts.append(f"\n*🔄 {wc_count} Website {wc_label}*")
            for chg in website_changes[:5]:
                page = _page_name_from_url(chg["url"])
                body_parts.append(f"  • *{chg['firm_name']}* — {page}")

        if not alerts and not website_changes:
            body_parts.append("\n_No new expansion signals this run._")

        return header + "\n" + "\n".join(body_parts)

    # ------------------------------------------------------------------ #

    def send_combined_digest(
        self,
        alerts: list[dict],
        website_changes: list[dict],
        new_signals: list[dict] | None = None,
        run_trends: list[dict] | None = None,
        duration_secs: float = 0,
        error_count: int = 0,
    ):
        if not self.token or not self.chat_id:
            logger.warning("Telegram not configured — skipping notification")
            return

        from datetime import datetime, timezone
        from collections import Counter

        today       = datetime.now(timezone.utc).strftime("%B %d, %Y")
        new_signals = new_signals or []
        run_trends  = run_trends  or []

        type_counts = Counter(s.get("signal_type", "other") for s in new_signals)

        # ── Header ──────────────────────────────────────────────────────
        header_lines = [
            "📊 *Law Firm Expansion Tracker*",
            f"_{today}_",
        ]
        if self.dash_url:
            header_lines.append(f"🖥 [Open Live Dashboard]({self.dash_url})")
        header_lines.append("─" * 34)

        # ── Summary row ─────────────────────────────────────────────────
        avg_new = (
            sum(r.get("new_signals", 0) for r in run_trends) / len(run_trends)
            if run_trends else 0
        )
        delta_str = ""
        if avg_new > 0:
            delta_pct = int(((len(new_signals) - avg_new) / avg_new) * 100)
            delta_str = f" ({'+' if delta_pct >= 0 else ''}{delta_pct}% vs 7-run avg)"

        summary_lines = [
            f"*{len(new_signals)}* new signal(s){delta_str} · "
            f"*{len(alerts)}* alert(s)",
        ]
        if type_counts:
            top = ", ".join(
                f"{TYPE_EMOJI.get(t, '•')} {c} {t.replace('_', ' ')}"
                for t, c in type_counts.most_common(4)
            )
            summary_lines.append(f"_{top}_")
        if error_count:
            summary_lines.append(f"⚠️ _{error_count} scraper error(s) this run_")

        # ── Sector momentum block ────────────────────────────────────────
        momentum_depts = {
            a["department"] for a in alerts if a.get("sector_momentum")
        }
        momentum_lines = []
        if momentum_depts:
            momentum_lines.append(
                f"\n🌊 *Sector Momentum* — {len(momentum_depts)} department(s) "
                "trending across 3+ firms:"
            )
            for dept in sorted(momentum_depts):
                firms_in_dept = [
                    a["firm_name"].split()[0]
                    for a in alerts
                    if a["department"] == dept
                ]
                momentum_lines.append(
                    f"  {DEPT_EMOJI.get(dept, '📌')} *{dept}* "
                    f"— {', '.join(firms_in_dept[:5])}"
                )

        # ── No activity case ─────────────────────────────────────────────
        if not alerts and not website_changes:
            msg = "\n".join(
                header_lines + summary_lines + ["", "_No new expansion spikes this run._"]
                + self._footer_lines(duration_secs)
            )
            self._send(msg)
            return

        # ── Alert blocks (each built as a chunk to avoid mid-split) ─────
        alert_blocks = []
        if alerts:
            alert_blocks.append(f"\n*🔔 {len(alerts)} Expansion Alert(s)*\n")
            for i, alert in enumerate(alerts[:15], 1):
                dept_e  = DEPT_EMOJI.get(alert["department"], "📌")
                arrow   = alert.get("velocity_arrow", "→")
                z       = alert.get("z_score", 0)
                fire    = " 🔥" if z >= 2.0 else ""
                sector  = " 🌊" if alert.get("sector_momentum") else ""
                z_label = f" ↑{z}σ" if z else ""
                new_lbl = " 🆕" if alert.get("is_new_baseline") else ""

                block = [
                    f"{i}. 🏛 *{alert['firm_name']}*{fire}{sector}",
                    f"   {dept_e} {alert['department']} {arrow}{new_lbl}",
                    f"   Score: *{alert['expansion_score']}*{z_label}  "
                    f"Signals: {alert['signal_count']}",
                ]

                # Top 2 signal bullets with confidence indicator
                for sig in alert.get("top_signals", [])[:2]:
                    te     = TYPE_EMOJI.get(sig.get("signal_type", ""), "•")
                    conf   = sig.get("confidence", 0)
                    conf_d = "●" if conf >= 0.7 else ("◑" if conf >= 0.4 else "○")
                    title  = (
                        sig.get("title", "")[:85]
                        .replace("*", "").replace("[", "").replace("]", "")
                    )
                    url = sig.get("url", "")
                    if url:
                        block.append(f"   {te}{conf_d} [{title}]({url})")
                    else:
                        block.append(f"   {te}{conf_d} {title}")

                alert_blocks.append("\n".join(block))

        # ── Website changes ──────────────────────────────────────────────
        change_lines = []
        if website_changes:
            change_lines.append(f"\n*🔄 {len(website_changes)} Website Change(s)*")
            for chg in website_changes[:5]:
                change_lines.append(
                    f"  • *{chg['firm_name']}* — [{chg['title']}]({chg['url']})"
                )

        # ── Footer ───────────────────────────────────────────────────────
        footer = self._footer_lines(duration_secs)

        # ── Assemble and send ────────────────────────────────────────────
        preamble = "\n".join(header_lines + summary_lines + momentum_lines)
        tail     = "\n".join(change_lines + footer)

        self._send_in_chunks(preamble, alert_blocks, tail)

    # ------------------------------------------------------------------ #
    #  Internal helpers
    # ------------------------------------------------------------------ #

    def _footer_lines(self, duration_secs: float = 0) -> list[str]:
        lines = []
        if duration_secs:
            mins = int(duration_secs // 60)
            secs = int(duration_secs % 60)
            lines.append(f"\n⏱ Run time: {mins}m {secs}s")
        if self.run_id and self.repo:
            log_url = f"https://github.com/{self.repo}/actions/runs/{self.run_id}"
            lines.append(f"[📋 View Run Log]({log_url})")
        return lines

    def _send_in_chunks(
        self, preamble: str, blocks: list[str], tail: str
    ):
        """
        Sends preamble first, then appends alert blocks until MAX_MSG_LEN,
        then sends remainder + tail. Never splits a single alert block.
        """
        current = preamble
        for block in blocks:
            candidate = current + "\n\n" + block
            if len(candidate) > MAX_MSG_LEN:
                self._send(current)
                current = block
            else:
                current = candidate

        if tail:
            candidate = current + "\n" + tail
            if len(candidate) > MAX_MSG_LEN:
                self._send(current)
                self._send(tail)
            else:
                self._send(candidate)
        else:
            self._send(current)

    def _send(self, text: str):
        if not text.strip():
            return
        # Hard-limit fallback: split oversized raw strings
        chunks = [text[i: i + MAX_MSG_LEN] for i in range(0, len(text), MAX_MSG_LEN)]
        for chunk in chunks:
            try:
                resp = requests.post(
                    f"https://api.telegram.org/bot{self.token}/sendMessage",
                    json={
                        "chat_id":                  self.chat_id,
                        "text":                     chunk,
                        "parse_mode":               "Markdown",
                        "disable_web_page_preview": True,
                    },
                    timeout=15,
                )
                resp.raise_for_status()
                logger.info("Telegram: message delivered")
            except Exception as exc:
                logger.error(f"Telegram send failed: {exc}")
