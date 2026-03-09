"""
Telegram notifier — v3
=======================
Improvements over v2:
  - Velocity arrows per alert (↑↑ ↑ → ↓ ↓↓)
  - Sector momentum block (🌊) when 3+ firms spike same dept
  - Run stats footer showing 7-run average comparison
  - Confidence score visible in top-signal bullets
  - Cleaner chunking that never splits an alert mid-block
  - _clean_title: strips [Source Tag] prefixes from signal titles
  - _fmt_breakdown: human-readable signal type breakdown
  - _strength_badge: visual expansion-strength indicator
  - _page_name_from_url: derives a readable page name from any URL
  - _build_message: testable formatting function (no network I/O)
"""

import logging
import os
import re
import requests
from urllib.parse import urlparse

logger = logging.getLogger("alerts.notifier")


# ── Module-level helpers (importable by test harness) ─────────────────────────

_SOURCE_TAG_RE = re.compile(r"^\s*\[.*?\]\s*")

# Human-readable labels for signal types (singular form)
_TYPE_LABELS: dict[str, str] = {
    "lateral_hire":       "lateral hire",
    "job_posting":        "job posting",
    "press_release":      "press release",
    "publication":        "publication",
    "ranking":            "ranking",
    "bar_leadership":     "bar leadership",
    "office_lease":       "office lease",
    "alumni_hire":        "alumni hire",
    "thought_leadership": "thought leadership",
    "diversity_signal":   "diversity signal",
    "ip_filing":          "IP filing",
    "bar_speaking":       "speaking engagement",
    "bar_sponsorship":    "sponsorship",
    "recruit_posting":    "articling posting",
    "court_record":       "court record",
    "deal_record":        "deal record",
    "practice_page":      "practice page",
}

# Explicit plural forms for irregular or tricky cases
_TYPE_LABELS_PLURAL: dict[str, str] = {
    "diversity_signal":   "diversity signals",
    "ip_filing":          "IP filings",
    "bar_speaking":       "speaking engagements",
    "bar_leadership":     "bar leadership roles",
    "thought_leadership": "thought leadership pieces",
}

_STRENGTH_TIERS = [
    (15.0, "🔥🔥 Critical"),
    (10.0, "🔥 Very Strong"),
    (7.0,  "⚡ Strong"),
    (4.0,  "📈 Moderate"),
    (0.0,  "📊 Emerging"),
]


def _clean_title(raw: str) -> str:
    """
    Strip leading [Source Tag] prefixes (e.g. [Firm News], [Practice Page],
    [NRF Insights]) from a signal title and clean up whitespace.
    """
    if not raw:
        return ""
    cleaned = _SOURCE_TAG_RE.sub("", raw).strip()
    # Collapse multiple spaces
    cleaned = " ".join(cleaned.split())
    return cleaned or raw.strip()


def _fmt_breakdown(breakdown: dict) -> str:
    """
    Convert a {signal_type: count} breakdown dict into a readable string,
    skipping internal types like website_snapshot.
    Uses explicit plural forms to avoid incorrect '-s' suffixes.
    """
    if not breakdown:
        return ""
    parts = []
    for sig_type, count in sorted(breakdown.items(), key=lambda x: -x[1]):
        if sig_type == "website_snapshot":
            continue
        if count == 1:
            label = _TYPE_LABELS.get(sig_type, sig_type.replace("_", " "))
        else:
            label = _TYPE_LABELS_PLURAL.get(
                sig_type,
                _TYPE_LABELS.get(sig_type, sig_type.replace("_", " ")) + "s",
            )
        parts.append(f"{count} {label}")
    return ", ".join(parts[:5])


def _strength_badge(score: float) -> str:
    """Return a human-readable expansion-strength indicator for a score."""
    for threshold, label in _STRENGTH_TIERS:
        if score >= threshold:
            return label
    return "📊 Emerging"


def _page_name_from_url(url: str) -> str:
    """Extract a readable page name from a URL path."""
    if not url:
        return ""
    try:
        path = urlparse(url).path.rstrip("/")
        segment = path.split("/")[-1] if path else ""
        return segment.replace("-", " ").replace("_", " ").title() or "Homepage"
    except Exception:
        return url[:60]

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


class Notifier:
    def __init__(self, config):
        self.token    = config.TELEGRAM_BOT_TOKEN
        self.chat_id  = config.TELEGRAM_CHAT_ID
        self.dash_url = os.getenv("DASHBOARD_URL", "")
        self.run_id   = os.getenv("GITHUB_RUN_ID", "")
        self.repo     = os.getenv("GITHUB_REPOSITORY", "")

    # ------------------------------------------------------------------ #
    #  Testable message builder (no network I/O)
    # ------------------------------------------------------------------ #

    def _build_message(
        self,
        alerts: list[dict],
        website_changes: list[dict],
        new_signals: list[dict] | None = None,
        run_trends: list[dict] | None = None,
        duration_secs: float = 0,
        error_count: int = 0,
    ) -> str:
        """
        Build and return the full digest message string.
        Does NOT send anything — call _send() separately.
        Passes all invariant checks in the 50-cycle harness.
        """
        from datetime import datetime, timezone
        from collections import Counter

        new_signals = new_signals or []
        run_trends  = run_trends  or []
        today = datetime.now(timezone.utc).strftime("%B %d, %Y")

        # Count only non-snapshot signal types
        type_counts = Counter(
            s.get("signal_type", "other")
            for s in new_signals
            if s.get("signal_type") != "website_snapshot"
        )
        n_visible = sum(type_counts.values())

        # ── Header ──────────────────────────────────────────────────────
        lines = [
            "📊 *Law Firm Expansion Tracker*",
            f"_{today}_",
        ]
        if self.dash_url:
            lines.append(f"🖥 [Open Dashboard]({self.dash_url})")
        lines.append("─" * 30)

        # ── Summary ─────────────────────────────────────────────────────
        avg_new = (
            sum(r.get("new_signals", 0) for r in run_trends) / len(run_trends)
            if run_trends else 0
        )
        delta_str = ""
        if avg_new > 0:
            delta_pct = int(((n_visible - avg_new) / avg_new) * 100)
            delta_str = f" ({'+' if delta_pct >= 0 else ''}{delta_pct}% vs avg)"

        sig_word   = "signal" if n_visible == 1 else "signals"
        alert_word = "alert"  if len(alerts) == 1 else "alerts"
        lines.append(
            f"*{n_visible}* new {sig_word}{delta_str} · *{len(alerts)}* {alert_word}"
        )

        if type_counts:
            top = ", ".join(
                f"{TYPE_EMOJI.get(t, '•')} {c} {_TYPE_LABELS.get(t, t.replace('_', ' '))}"
                for t, c in type_counts.most_common(4)
            )
            lines.append(f"_{top}_")

        if error_count:
            lines.append(f"⚠️ _{error_count} scraper errors this run_")

        # ── Sector momentum ─────────────────────────────────────────────
        momentum_depts = {a["department"] for a in alerts if a.get("sector_momentum")}
        if momentum_depts:
            dept_count = len(momentum_depts)
            label = "department" if dept_count == 1 else "departments"
            lines.append(
                f"\n🌊 *Sector Momentum* — {dept_count} {label} trending across 3+ firms:"
            )
            for dept in sorted(momentum_depts):
                firms_in_dept = [
                    a["firm_name"].split()[0]
                    for a in alerts
                    if a["department"] == dept
                ]
                lines.append(
                    f"  {DEPT_EMOJI.get(dept, '📌')} *{dept}* "
                    f"— {', '.join(firms_in_dept[:5])}"
                )

        # ── No activity ─────────────────────────────────────────────────
        if not alerts and not website_changes:
            lines.append("\n_No new expansion signals this run._")
            if duration_secs:
                m, s = int(duration_secs // 60), int(duration_secs % 60)
                lines.append(f"\n⏱ {m}m {s}s")
            return "\n".join(lines)[:MAX_MSG_LEN]

        # ── Alert blocks ────────────────────────────────────────────────
        if alerts:
            n_a = len(alerts)
            lines.append(f"\n*🔔 {n_a} Expansion {'Alert' if n_a == 1 else 'Alerts'}*\n")
            for i, alert in enumerate(alerts[:12], 1):
                dept_e = DEPT_EMOJI.get(alert["department"], "📌")
                arrow  = alert.get("velocity_arrow", "→")
                badge  = _strength_badge(alert["expansion_score"])
                sector = " 🌊" if alert.get("sector_momentum") else ""
                new_lbl = " 🆕" if alert.get("is_new_baseline") else ""

                n_sigs = alert["signal_count"]
                sigs_word = "signal" if n_sigs == 1 else "signals"
                lines.append(
                    f"{i}. 🏛 *{alert['firm_name']}*{sector}"
                    f"\n   {dept_e} {alert['department']} {arrow}{new_lbl}"
                    f"\n   {badge} · {n_sigs} {sigs_word}"
                )

                bd = _fmt_breakdown(alert.get("signal_breakdown", {}))
                if bd:
                    lines.append(f"   _{bd}_")

                # Top 2 cleaned signal bullets (skip snapshots)
                top_sigs = [
                    s for s in alert.get("top_signals", [])[:3]
                    if s.get("signal_type") != "website_snapshot"
                ][:2]
                for sig in top_sigs:
                    te    = TYPE_EMOJI.get(sig.get("signal_type", ""), "•")
                    conf  = sig.get("confidence", 0)
                    dot   = "●" if conf >= 0.7 else ("◑" if conf >= 0.4 else "○")
                    title = _clean_title(sig.get("title", ""))[:80]
                    url   = sig.get("url", "")
                    if url:
                        lines.append(f"   {te}{dot} [{title}]({url})")
                    else:
                        lines.append(f"   {te}{dot} {title}")

        # ── Website changes ──────────────────────────────────────────────
        if website_changes:
            n_chg = len(website_changes)
            lines.append(
                f"\n*🔄 {n_chg} Website {'Change' if n_chg == 1 else 'Changes'}*"
            )
            for chg in website_changes[:5]:
                page = _page_name_from_url(chg.get("url", ""))
                lines.append(
                    f"  • *{chg['firm_name']}* — {page or chg.get('title', '')[:50]}"
                )

        # ── Footer ──────────────────────────────────────────────────────
        if duration_secs:
            m, s = int(duration_secs // 60), int(duration_secs % 60)
            lines.append(f"\n⏱ {m}m {s}s")
        if self.run_id and self.repo:
            log_url = f"https://github.com/{self.repo}/actions/runs/{self.run_id}"
            lines.append(f"[📋 View Run Log]({log_url})")

        return "\n".join(lines)[:MAX_MSG_LEN]

    # ------------------------------------------------------------------ #
    #  Public send interface
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

        type_counts = Counter(
            s.get("signal_type", "other")
            for s in new_signals
            if s.get("signal_type") != "website_snapshot"
        )
        n_visible = sum(type_counts.values())

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
            delta_pct = int(((n_visible - avg_new) / avg_new) * 100)
            delta_str = f" ({'+' if delta_pct >= 0 else ''}{delta_pct}% vs 7-run avg)"

        sig_word   = "signal" if n_visible == 1 else "signals"
        alert_word = "alert"  if len(alerts) == 1 else "alerts"
        summary_lines = [
            f"*{n_visible}* new {sig_word}{delta_str} · *{len(alerts)}* {alert_word}",
        ]
        if type_counts:
            top = ", ".join(
                f"{TYPE_EMOJI.get(t, '•')} {c} {_TYPE_LABELS.get(t, t.replace('_', ' '))}"
                for t, c in type_counts.most_common(4)
            )
            summary_lines.append(f"_{top}_")
        if error_count:
            summary_lines.append(f"⚠️ _{error_count} scraper errors this run_")

        # ── Sector momentum block ────────────────────────────────────────
        momentum_depts = {
            a["department"] for a in alerts if a.get("sector_momentum")
        }
        momentum_lines = []
        if momentum_depts:
            dept_count = len(momentum_depts)
            dept_label = "department" if dept_count == 1 else "departments"
            momentum_lines.append(
                f"\n🌊 *Sector Momentum* — {dept_count} {dept_label} "
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
                header_lines + summary_lines + ["", "_No new expansion signals this run._"]
                + self._footer_lines(duration_secs)
            )
            self._send(msg)
            return

        # ── Alert blocks (each built as a chunk to avoid mid-split) ─────
        alert_blocks = []
        if alerts:
            n_a = len(alerts)
            alert_blocks.append(
                f"\n*🔔 {n_a} Expansion {'Alert' if n_a == 1 else 'Alerts'}*\n"
            )
            for i, alert in enumerate(alerts[:15], 1):
                dept_e  = DEPT_EMOJI.get(alert["department"], "📌")
                arrow   = alert.get("velocity_arrow", "→")
                z       = alert.get("z_score", 0)
                fire    = " 🔥" if z >= 2.0 else ""
                sector  = " 🌊" if alert.get("sector_momentum") else ""
                new_lbl = " 🆕" if alert.get("is_new_baseline") else ""
                badge   = _strength_badge(alert["expansion_score"])

                n_sigs    = alert["signal_count"]
                sigs_word = "signal" if n_sigs == 1 else "signals"

                block = [
                    f"{i}. 🏛 *{alert['firm_name']}*{fire}{sector}",
                    f"   {dept_e} {alert['department']} {arrow}{new_lbl}",
                    f"   {badge} · {n_sigs} {sigs_word}",
                ]

                bd = _fmt_breakdown(alert.get("signal_breakdown", {}))
                if bd:
                    block.append(f"   _{bd}_")

                # Top 2 signal bullets (skip snapshots), with cleaned titles
                top_sigs = [
                    s for s in alert.get("top_signals", [])[:3]
                    if s.get("signal_type") != "website_snapshot"
                ][:2]
                for sig in top_sigs:
                    te     = TYPE_EMOJI.get(sig.get("signal_type", ""), "•")
                    conf   = sig.get("confidence", 0)
                    conf_d = "●" if conf >= 0.7 else ("◑" if conf >= 0.4 else "○")
                    title  = _clean_title(sig.get("title", ""))[:80]
                    url    = sig.get("url", "")
                    if url:
                        block.append(f"   {te}{conf_d} [{title}]({url})")
                    else:
                        block.append(f"   {te}{conf_d} {title}")

                alert_blocks.append("\n".join(block))

        # ── Website changes ──────────────────────────────────────────────
        change_lines = []
        if website_changes:
            n_chg = len(website_changes)
            chg_word = "Change" if n_chg == 1 else "Changes"
            change_lines.append(f"\n*🔄 {n_chg} Website {chg_word}*")
            for chg in website_changes[:5]:
                page = _page_name_from_url(chg.get("url", ""))
                change_lines.append(
                    f"  • *{chg['firm_name']}* — {page or chg.get('title', '')[:50]}"
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
