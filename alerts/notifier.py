"""
Telegram notifier — v3
=======================
Improvements:
  - Velocity arrows per alert (↑↑ ↑ → ↓ ↓↓)
  - Sector momentum block (🌊) when 3+ firms spike same dept
  - Run stats footer showing 7-run average comparison
  - Confidence score visible in top-signal bullets
  - Cleaner chunking that never splits an alert mid-block
  - _build_message() returns message string for testing / dry-run
  - Helper functions: _clean_title, _fmt_breakdown, _strength_badge, _page_name_from_url
"""

import logging
import os
import re
import requests

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

# Human-readable labels for signal types (no cryptic abbreviations)
_TYPE_LABELS: dict[str, str] = {
    "lateral_hire":       "lateral hire",
    "bar_leadership":     "bar leadership",
    "ranking":            "ranking",
    "office_lease":       "office lease",
    "alumni_hire":        "alumni hire",
    "job_posting":        "job posting",
    "deal_record":        "deal record",
    "court_record":       "court record",
    "press_release":      "press release",
    "thought_leadership": "thought leadership",
    "diversity_signal":   "diversity signal",
    "ip_filing":          "IP filing",
    "bar_speaking":       "bar speaking",
    "recruit_posting":    "recruit posting",
    "publication":        "publication",
    "practice_page":      "practice page",
    "website_snapshot":   "website snapshot",
    "deal_counsel":       "deal counsel",
    "alumni_track":       "alumni track",
    "conference_signal":  "conference signal",
    "podcast_signal":     "podcast signal",
    "award_signal":       "award signal",
    "sedar_filing":       "SEDAR filing",
    "cipo_filing":        "CIPO filing",
    "lobbyist_signal":    "lobbyist filing",
    "event_signal":       "event signal",
    "cross_ref_signal":   "cross-reference signal",
}

_SCORE_THRESHOLDS = [
    (12.0, "🔴 Very Strong"),
    (8.0,  "🟠 Strong"),
    (5.0,  "🟡 Moderate"),
    (0.0,  "🟢 Emerging"),
]

# Prefixes that appear before real content after [Source Tag] stripping
_SOURCE_PREFIXES = re.compile(
    r"^(?:Publication|Press Release|News Release|Article|Blog Post|Firm News|"
    r"Practice Area|Practice Page|Insights?|Update)\s+",
    re.IGNORECASE,
)


def _clean_title(title: str) -> str:
    """
    Strip [Source Tag] prefixes and fix common title artifacts.

    Examples:
      "[Practice Page] Goodmans — Capital Markets"  → "Capital Markets"
      "[Firm News] PublicationThe rise of class actionsCanada"
                                                    → "The rise of class actions Canada"
      "Foundations for settlement: A guide"         → "Foundations for settlement: A guide"
      ""                                            → "(no title)"
    """
    if not title or not title.strip():
        return "(no title)"

    # Strip [Source Tag] prefix (e.g. [Firm News], [Practice Page], [NRF Insights])
    title = re.sub(r"^\[.*?\]\s*", "", title).strip()

    # Insert space before uppercase that follows 3+ lowercase chars (run-on words).
    # e.g. "PublicationThe" → "Publication The",  "actionsCanada" → "actions Canada"
    # Requires 3 preceding lowercase chars so proper-name prefixes like "Mc" in
    # "McCarthy" (only 2 lowercase before "C") are not incorrectly split.
    # This is a pragmatic heuristic suitable for law-firm title text.
    title = re.sub(r"(?<=[a-z]{3})([A-Z])", r" \1", title)

    # Strip known leading source-type word(s) that still prefix the real title
    title = _SOURCE_PREFIXES.sub("", title).strip()

    # Practice-page style "FirmName — Practice Area" → take only "Practice Area"
    if " — " in title:
        after = title.split(" — ", 1)[1].strip()
        if after:
            return after

    return title.strip() or "(no title)"


def _fmt_breakdown(breakdown: dict) -> str:
    """
    Format a signal-type count dict into a human-readable string.
    Never uses abbreviations like 'pg', 'pub', 'rec'.

    Example: {"press_release": 3, "publication": 2} → "3 press releases, 2 publications"
    """
    parts = []
    for sig_type, count in sorted(breakdown.items(), key=lambda x: -x[1]):
        if sig_type == "website_snapshot":
            continue
        label = _TYPE_LABELS.get(sig_type, sig_type.replace("_", " "))
        if count != 1:
            # Simple pluralisation — append 's' unless already ending in 's'
            label = label if label.endswith("s") else label + "s"
        parts.append(f"{count} {label}")
    return ", ".join(parts[:5])


def _strength_badge(score: float) -> str:
    """Return a strength badge emoji + label for a given expansion score."""
    for threshold, label in _SCORE_THRESHOLDS:
        if score >= threshold:
            return label
    return "🟢 Emerging"


def _page_name_from_url(url: str) -> str:
    """
    Extract a human-readable page name from a URL path.

    Example: "https://osler.com/expertise/capital-markets/" → "Capital Markets"
    """
    from urllib.parse import urlparse
    try:
        path = urlparse(url).path
        segments = [s for s in path.split("/") if s]
        if segments:
            name = segments[-1].replace("-", " ").replace("_", " ").title()
            for ext in (".html", ".htm", ".php", ".aspx"):
                name = name.replace(ext.title(), "").replace(ext, "")
            return name.strip() or url
    except Exception:
        pass
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
        run_trends: list[dict] | None = None,
        duration_secs: float = 0,
        error_count: int = 0,
    ) -> str:
        """
        Build and return the complete digest message string without sending it.
        Used internally by send_combined_digest and by the test harness.

        Invariants guaranteed:
          - No raw "Score X.X" pattern (uses _strength_badge instead)
          - No lazy "(s)" plurals  (proper count-based pluralisation)
          - No "website_snapshot" exposed in message body
          - No [Source Tag] prefixes in signal titles (uses _clean_title)
          - No cryptic abbreviations in breakdowns (uses _fmt_breakdown)
          - No verbose "Practice area page content changed at https://..." lines
          - Message length <= MAX_MSG_LEN
        """
        from datetime import datetime, timezone
        from collections import Counter

        today       = datetime.now(timezone.utc).strftime("%B %d, %Y")
        new_signals = new_signals or []
        run_trends  = run_trends  or []

        n_sig   = len(new_signals)
        n_alert = len(alerts)
        sig_word   = "signal"  if n_sig   == 1 else "signals"
        alert_word = "alert"   if n_alert == 1 else "alerts"

        # Count visible signal types (exclude website_snapshot from display)
        type_counts = Counter(
            s.get("signal_type", "other")
            for s in new_signals
            if s.get("signal_type") != "website_snapshot"
        )

        # ── Header ──────────────────────────────────────────────────────
        lines = [
            "📊 *Law Firm Expansion Tracker*",
            f"_{today}_",
        ]
        if self.dash_url:
            lines.append(f"🖥 [Open Live Dashboard]({self.dash_url})")
        lines.append("─" * 34)

        # ── Summary row ─────────────────────────────────────────────────
        avg_new = (
            sum(r.get("new_signals", 0) for r in run_trends) / len(run_trends)
            if run_trends else 0
        )
        delta_str = ""
        if avg_new > 0:
            delta_pct = int(((n_sig - avg_new) / avg_new) * 100)
            delta_str = f" ({'+' if delta_pct >= 0 else ''}{delta_pct}% vs 7-run avg)"

        lines.append(
            f"*{n_sig}* new {sig_word}{delta_str} · *{n_alert}* {alert_word}"
        )
        if type_counts:
            top = ", ".join(
                f"{TYPE_EMOJI.get(t, '•')} {c} {t.replace('_', ' ')}"
                for t, c in type_counts.most_common(4)
            )
            lines.append(f"_{top}_")
        if error_count:
            lines.append(f"⚠️ _{error_count} scraper errors this run_")

        # ── Sector momentum block ────────────────────────────────────────
        momentum_depts = {a["department"] for a in alerts if a.get("sector_momentum")}
        if momentum_depts:
            n_m = len(momentum_depts)
            dept_word = "department" if n_m == 1 else "departments"
            lines.append(
                f"\n🌊 *Sector Momentum* — {n_m} {dept_word} trending across 3+ firms:"
            )
            for dept in sorted(momentum_depts):
                firms_in_dept = [
                    a["firm_name"].split()[0].rstrip(",;.")
                    for a in alerts
                    if a["department"] == dept
                ]
                lines.append(
                    f"  {DEPT_EMOJI.get(dept, '📌')} *{dept}* "
                    f"— {', '.join(firms_in_dept[:5])}"
                )

        # ── No activity case ─────────────────────────────────────────────
        if not alerts and not website_changes:
            lines.append("\n_No new expansion spikes this run._")
            lines.extend(self._footer_lines(duration_secs))
            return "\n".join(lines)[:MAX_MSG_LEN]

        # ── Alert blocks ─────────────────────────────────────────────────
        if alerts:
            lines.append(f"\n*🔔 {n_alert} Expansion {alert_word.title()}*\n")
            for i, alert in enumerate(alerts[:15], 1):
                dept_e = DEPT_EMOJI.get(alert["department"], "📌")
                arrow  = alert.get("velocity_arrow", "→")
                z      = alert.get("z_score", 0)
                fire   = " 🔥" if z >= 2.0 else ""
                sector = " 🌊" if alert.get("sector_momentum") else ""
                z_label = f" ↑{z}σ" if z else ""
                new_lbl = " 🆕" if alert.get("is_new_baseline") else ""
                badge   = _strength_badge(alert["expansion_score"])
                n_s     = alert["signal_count"]
                s_word  = "signal" if n_s == 1 else "signals"

                lines.append(f"{i}. 🏛 *{alert['firm_name']}*{fire}{sector}")
                lines.append(f"   {dept_e} {alert['department']} {arrow}{new_lbl}")
                lines.append(f"   {badge}{z_label}  {n_s} {s_word}")

                # Breakdown (human-readable, no cryptic abbreviations)
                if alert.get("signal_breakdown"):
                    bd_str = _fmt_breakdown(alert["signal_breakdown"])
                    if bd_str:
                        lines.append(f"   _{bd_str}_")

                # Top 2 signal bullets
                for sig in alert.get("top_signals", [])[:2]:
                    te     = TYPE_EMOJI.get(sig.get("signal_type", ""), "•")
                    conf   = sig.get("confidence", 0)
                    conf_d = "●" if conf >= 0.7 else ("◑" if conf >= 0.4 else "○")
                    title  = _clean_title(sig.get("title", ""))[:85]
                    url    = sig.get("url", "")
                    if url:
                        lines.append(f"   {te}{conf_d} [{title}]({url})")
                    else:
                        lines.append(f"   {te}{conf_d} {title}")

        # ── Website changes ──────────────────────────────────────────────
        if website_changes:
            n_w    = len(website_changes)
            chg_wd = "Change" if n_w == 1 else "Changes"
            lines.append(f"\n*🔄 {n_w} Website {chg_wd}*")
            for chg in website_changes[:5]:
                page = _page_name_from_url(chg.get("url", ""))
                lines.append(f"  • *{chg['firm_name']}* — {page}")

        # ── Footer ───────────────────────────────────────────────────────
        lines.extend(self._footer_lines(duration_secs))

        return "\n".join(lines)[:MAX_MSG_LEN]

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

        msg = self._build_message(
            alerts,
            website_changes,
            new_signals=new_signals,
            run_trends=run_trends,
            duration_secs=duration_secs,
            error_count=error_count,
        )
        self._send(msg)

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
