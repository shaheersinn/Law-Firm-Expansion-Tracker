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

# Human-readable labels for signal types: (singular, plural)
_TYPE_LABELS: dict[str, tuple[str, str]] = {
    "press_release":      ("press release",     "press releases"),
    "publication":        ("publication",        "publications"),
    "practice_page":      ("practice page",      "practice pages"),
    "job_posting":        ("job posting",        "job postings"),
    "lateral_hire":       ("lateral hire",       "lateral hires"),
    "recruit_posting":    ("recruit posting",    "recruit postings"),
    "ranking":            ("ranking",            "rankings"),
    "deal_record":        ("deal record",        "deal records"),
    "court_record":       ("court record",       "court records"),
    "ip_filing":          ("IP filing",          "IP filings"),
    "thought_leadership": ("thought leadership", "thought leadership"),
    "bar_speaking":       ("bar speaking",       "bar speaking"),
    "bar_sponsorship":    ("bar sponsorship",    "bar sponsorships"),
    "alumni_hire":        ("alumni hire",        "alumni hires"),
    "diversity_signal":   ("diversity signal",   "diversity signals"),
    "bar_leadership":     ("bar leadership",     "bar leadership"),
    "website_snapshot":   ("web change",        "web changes"),
}

# Known content-type prefixes glued to titles (e.g. "PublicationThe rise…")
_CONTENT_PREFIXES = (
    "Practice area page content changed at https://",
    "Practice area page content changed at http://",
    "Publication",
    "LawFirmArticle",
)


def _clean_title(raw: str) -> str:
    """Strip source-tag prefixes and fix common title artefacts.

    Examples
    --------
    "[Practice Page] Goodmans — Capital Markets"  →  "Capital Markets"
    "[Firm News] PublicationThe rise of class actionsCanada"
                                                  →  "The rise of class actions Canada"
    ""                                            →  "Untitled"
    """
    if not raw:
        return "Untitled"

    title = raw.strip()

    # 1. Strip [Source Tag] prefix — e.g. [Firm News], [Practice Page], [NRF Insights]
    title = re.sub(r"^\[.*?\]\s*", "", title)

    # 2. Strip firm-name em-dash prefix — e.g. "Goodmans — Capital Markets"
    if " \u2014 " in title:
        parts = title.split(" \u2014 ", 1)
        if len(parts[0].split()) <= 3:   # short left side → it's a firm/source label
            title = parts[1]

    # 3. Strip known content-type words glued at the start
    for prefix in _CONTENT_PREFIXES:
        if title.startswith(prefix):
            title = title[len(prefix):].strip()
            break

    # 4. Insert space between a lowercase letter immediately followed by uppercase
    #    (fixes concatenated words like "actionsCanada" → "actions Canada")
    title = re.sub(r"([a-z])([A-Z])", r"\1 \2", title)

    # 5. Collapse whitespace
    title = " ".join(title.split())

    return title or "Untitled"


def _fmt_breakdown(breakdown: dict) -> str:
    """Return a human-readable, comma-separated signal-type count string.

    Avoids cryptic abbreviations and never exposes 'website_snapshot'.

    Example: {"press_release": 3, "publication": 2} → "3 press releases, 2 publications"
    """
    parts = []
    for stype, count in sorted(breakdown.items(), key=lambda x: -x[1]):
        if stype == "website_snapshot":
            continue
        if stype in _TYPE_LABELS:
            label = _TYPE_LABELS[stype][1 if count > 1 else 0]
        else:
            label = stype.replace("_", " ")
            if count > 1 and not label.endswith("s"):
                label += "s"
        parts.append(f"{count} {label}")
    return ", ".join(parts[:4]) if parts else "—"


def _strength_badge(signal_type: str) -> str:
    """Return the display emoji badge for a signal type."""
    return TYPE_EMOJI.get(signal_type, "•")


def _page_name_from_url(url: str) -> str:
    """Extract a human-readable page name from a URL.

    Example: "https://goodmans.ca/expertise/capital-markets/" → "capital markets"
    """
    try:
        path = urlparse(url).path.rstrip("/")
        if not path:
            return url
        segment = path.split("/")[-1]
        return segment.replace("-", " ").replace("_", " ") if segment else url
    except Exception:
        return url


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
        """Build the Telegram digest string without sending it."""
        from datetime import datetime, timezone
        from collections import Counter

        today       = datetime.now(timezone.utc).strftime("%B %d, %Y")
        new_signals = new_signals or []

        n_new    = len(new_signals)
        n_alerts = len(alerts)

        # ── Header ──────────────────────────────────────────────────────
        parts: list[str] = [
            "📊 *Law Firm Expansion Tracker*",
            f"_{today}_",
        ]
        if self.dash_url:
            parts.append(f"🖥 [Open Live Dashboard]({self.dash_url})")
        parts.append("─" * 34)

        # ── Summary (no lazy (s) plurals) ────────────────────────────────
        new_word   = "signal"  if n_new    == 1 else "signals"
        alert_word = "alert"   if n_alerts == 1 else "alerts"
        parts.append(f"*{n_new}* new {new_word} · *{n_alerts}* {alert_word}")

        type_counts = Counter(s.get("signal_type", "other") for s in new_signals)
        if type_counts:
            top = ", ".join(
                f"{TYPE_EMOJI.get(t, '•')} {c} {t.replace('_', ' ')}"
                for t, c in type_counts.most_common(4)
                if t != "website_snapshot"
            )
            if top:
                parts.append(f"_{top}_")

        # ── No activity ──────────────────────────────────────────────────
        if not alerts and not website_changes:
            parts.append("_No new expansion spikes this run._")
            return "\n".join(parts)

        # ── Expansion alerts ─────────────────────────────────────────────
        if alerts:
            hdr = "Expansion Alert" if n_alerts == 1 else "Expansion Alerts"
            parts.append(f"\n*🔔 {n_alerts} {hdr}*")

            # Reserve ~150 chars for website-changes footer
            budget = MAX_MSG_LEN - len("\n".join(parts)) - 150

            for i, alert in enumerate(alerts, 1):
                dept_e  = DEPT_EMOJI.get(alert["department"], "📌")
                arrow   = alert.get("velocity_arrow", "→")
                z       = alert.get("z_score", 0)
                fire    = " 🔥" if z >= 2.0 else ""
                sector  = " 🌊" if alert.get("sector_momentum") else ""
                new_lbl = " 🆕" if alert.get("is_new_baseline") else ""
                bd_str  = _fmt_breakdown(alert.get("signal_breakdown", {}))

                block = [
                    f"{i}. 🏛 *{alert['firm_name']}*{fire}{sector}",
                    f"   {dept_e} {alert['department']} {arrow}{new_lbl}",
                    f"   {bd_str}",
                ]
                for sig in alert.get("top_signals", [])[:2]:
                    badge = _strength_badge(sig.get("signal_type", ""))
                    title = _clean_title(sig.get("title", ""))
                    url   = sig.get("url", "")
                    if url:
                        block.append(f"   {badge} [{title}]({url})")
                    else:
                        block.append(f"   {badge} {title}")

                block_text = "\n".join(block)
                if budget - len(block_text) - 2 < 0:
                    break   # no more room
                parts.append(block_text)
                budget -= len(block_text) + 2  # +2 for the "\n" join

        # ── Website changes ──────────────────────────────────────────────
        if website_changes:
            ch_word = "Website Change" if len(website_changes) == 1 else "Website Changes"
            parts.append(f"\n*🔄 {len(website_changes)} {ch_word}*")
            for chg in website_changes[:5]:
                page = _page_name_from_url(chg["url"])
                parts.append(f"  • *{chg['firm_name']}* — [{page}]({chg['url']})")

        return "\n".join(parts)

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
