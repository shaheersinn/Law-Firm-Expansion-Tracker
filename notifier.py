"""
alerts/notifier.py  (v4 — single consolidated message)
═══════════════════════════════════════════════════════
BUG FIX (confirmed in March 21 log):
  dispatch_unalerted() was sending 80 individual Telegram messages per run.
  Root cause: the v2/v3 notifier looped over signals and called send_telegram()
  per signal.  v4 marks ALL signals alerted FIRST then sends exactly ONE message.

DESIGN:
  • Collect all unalerted signals
  • Mark every one alerted=1 immediately (prevents re-fire on failure)
  • Build a single structured digest message
  • Call send_telegram() exactly once
  • Return 1 (sent) or 0 (failed)
"""

import logging
import os
import json
import time
from datetime import datetime, timezone

import requests
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from database.db import get_unalerted_signals, mark_alerted
from config_calgary import (
    TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID,
    SENDGRID_API_KEY, ALERT_EMAIL_FROM, ALERT_EMAIL_TO,
    FIRM_BY_ID,
)

log = logging.getLogger(__name__)

# ── Telegram limits ───────────────────────────────────────────────────────────
TELEGRAM_MAX_CHARS = 4000   # hard limit is 4096; leave margin for HTML tags

# ── Signal type display ───────────────────────────────────────────────────────
SIGNAL_META = {
    "canlii_appearance_spike":    ("⚖️",  "LITIGATION SPIKE",       "TODAY"),
    "canlii_new_large_file":      ("📋",  "LARGE FILE",             "This Week"),
    "sedar_major_deal":           ("💰",  "MAJOR DEAL",             "TODAY"),
    "sedar_counsel_named":        ("📄",  "DEAL COUNSEL",           "This Week"),
    "linkedin_turnover_detected": ("🚪",  "EMPTY CHAIR",            "TODAY"),
    "linkedin_new_vacancy":       ("🪑",  "NEW VACANCY",            "TODAY"),
    "lsa_student_not_retained":   ("🎓",  "HIREBACK GAP",           "3 Days"),
    "lsa_retention_gap":          ("📊",  "RETENTION GAP",          "3 Days"),
    "biglaw_spillage_predicted":  ("🌊",  "BIGLAW SPILLAGE",        "TODAY"),
    "job_posting":                ("📌",  "JOB POSTED",             "This Week"),
    "lateral_hire":               ("🔄",  "LATERAL HIRE",           "This Week"),
    "ranking":                    ("🏆",  "RANKING",                "This Month"),
    "macro_ma_wave_incoming":     ("📈",  "M&A WAVE",               "This Month"),
    "macro_demand_surge":         ("⚡",  "DEMAND SURGE",           "This Week"),
    "macro_demand_collapse":      ("📉",  "MACRO CAUTION",          "This Month"),
    "fiscal_pressure_incoming":   ("🗓",  "FISCAL PRESSURE",        "This Week"),
    "sec_edgar_filing":           ("🇺🇸",  "SEC FILING",             "This Week"),
    "ica_review_announced":       ("🏛",  "ICA REVIEW",             "TODAY"),
    "competition_bureau":         ("⚖️",  "COMPETITION BUREAU",     "This Week"),
    "new_court_filing":           ("📜",  "COURT FILING",           "This Week"),
    "counterfactual_conflict_free":("✅", "CONFLICT FREE",          "TODAY"),
    "web_signal":                 ("🌐",  "WEB SIGNAL",             "This Week"),
}

PRACTICE_EMOJI = {
    "corporate":      "⚖️", "energy":   "⚡", "litigation": "🏛",
    "employment":     "👥", "tax":       "💼", "ip":         "💡",
    "securities":     "📈", "real_estate":"🏗", "regulatory": "📋",
    "restructuring":  "🔄", "general":   "📌",
}


def _firm_name(firm_id: str) -> str:
    if firm_id == "market":
        return "🌐 Calgary Energy Market"
    return FIRM_BY_ID.get(firm_id, {}).get("name", firm_id)


def send_telegram(text: str) -> bool:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        log.warning("[Telegram] Not configured — printing:\n%s", text[:200])
        return False
    url  = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    data = {
        "chat_id":    TELEGRAM_CHAT_ID,
        "text":       text[:TELEGRAM_MAX_CHARS],
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    try:
        resp = requests.post(url, json=data, timeout=15)
        resp.raise_for_status()
        return True
    except Exception as e:
        log.error("[Telegram] Send failed: %s", e)
        return False


def send_email(subject: str, html_body: str, to: str = None) -> bool:
    if not SENDGRID_API_KEY:
        return False
    to = to or ALERT_EMAIL_TO
    payload = {
        "personalizations": [{"to": [{"email": to}]}],
        "from": {"email": ALERT_EMAIL_FROM},
        "subject": subject,
        "content": [{"type": "text/html", "value": html_body}],
    }
    try:
        resp = requests.post(
            "https://api.sendgrid.com/v3/mail/send",
            headers={"Authorization": f"Bearer {SENDGRID_API_KEY}",
                     "Content-Type": "application/json"},
            json=payload, timeout=15,
        )
        resp.raise_for_status()
        return True
    except Exception as e:
        log.error("[Email] Send failed: %s", e)
        return False


def _build_single_message(signals: list) -> str:
    """
    Build ONE Telegram-safe HTML message covering all new signals.
    Hard-capped at TELEGRAM_MAX_CHARS.
    """
    now   = datetime.now(timezone.utc).strftime("%b %d %Y  %H:%M UTC")
    total = len(signals)

    # Count unique firms (exclude market-wide)
    firms_hit = {s["firm_id"] for s in signals if s.get("firm_id") != "market"}
    n_firms   = len(firms_hit)

    # Group by signal type for dedup summary
    type_counts: dict = {}
    for s in signals:
        st = s.get("signal_type", "web_signal")
        type_counts[st] = type_counts.get(st, 0) + 1

    # Practice-area breakdown
    pa_counts: dict = {}
    for s in signals:
        pa = s.get("practice_area") or "general"
        pa_counts[pa] = pa_counts.get(pa, 0) + 1
    pa_top = sorted(pa_counts.items(), key=lambda x: -x[1])[:5]

    # Time-sensitive signals (act today)
    urgent_types = {
        "sedar_major_deal", "biglaw_spillage_predicted",
        "linkedin_turnover_detected", "ica_review_announced",
        "counterfactual_conflict_free", "canlii_appearance_spike",
    }
    urgent = [s for s in signals if s.get("signal_type") in urgent_types]

    # Top 8 by weight
    top = signals[:8]

    # ── Build message ─────────────────────────────────────────────────────
    lines = [
        f"🏛 <b>Calgary Law Tracker  ·  {now}</b>",
        f"<b>{total} signal{'s' if total != 1 else ''}</b>  across  "
        f"<b>{n_firms} firm{'s' if n_firms != 1 else ''}</b>",
        "",
    ]

    # Signal type summary (deduplicated)
    type_lines = []
    for st, cnt in sorted(type_counts.items(), key=lambda x: -x[1])[:6]:
        emoji = SIGNAL_META.get(st, ("📣",))[0]
        label = SIGNAL_META.get(st, ("📣", st.upper()))[1]
        type_lines.append(f"  {emoji} {label} ×{cnt}")
    if type_lines:
        lines.append("📊 <b>Signal Mix</b>")
        lines.extend(type_lines)
        lines.append("")

    # Top signals
    lines.append("📌 <b>Top Signals</b>")
    for i, s in enumerate(top, 1):
        fname    = _firm_name(s.get("firm_id", ""))
        st       = s.get("signal_type", "")
        emoji    = SIGNAL_META.get(st, ("📣",))[0]
        label    = SIGNAL_META.get(st, ("📣", st.upper()))[1]
        weight   = s.get("weight", 0)
        title    = (s.get("title") or "")[:65]
        conf_raw = s.get("confidence_score")
        conf_str = f"  ✓{conf_raw:.0%}" if conf_raw is not None else ""
        lines.append(
            f"  {i}. {emoji} <b>{fname}</b>  [{label}]  w={weight:.1f}{conf_str}\n"
            f"      <i>{title}</i>"
        )

    # Urgent callout
    if urgent:
        lines += ["", f"🔴 <b>Act Today</b> — {len(urgent)} time-sensitive"]
        for s in urgent[:3]:
            fname = _firm_name(s.get("firm_id", ""))
            title = (s.get("title") or "")[:55]
            lines.append(f"  • {fname}: {title}")

    # Practice breakdown
    lines += ["", "📂 <b>By Practice</b>"]
    for pa, cnt in pa_top:
        em = PRACTICE_EMOJI.get(pa, "📌")
        bar = "█" * min(cnt, 8)
        lines.append(f"  {em} {pa:<14} {bar} {cnt}")

    # Dashboard link
    dash_url = os.getenv("DASHBOARD_URL", "")
    if dash_url:
        lines += ["", f'🔗 <a href="{dash_url}">Open Dashboard →</a>']

    lines.append(f"\n<i>{total} signals stored · run {os.getenv('GITHUB_RUN_ID','local')}</i>")

    msg = "\n".join(lines)
    if len(msg) > TELEGRAM_MAX_CHARS:
        msg = msg[:TELEGRAM_MAX_CHARS - 30] + "\n…<i>(truncated)</i>"
    return msg


class AlertDispatcher:

    def dispatch_unalerted(self) -> int:
        """
        SINGLE-MESSAGE dispatch. Sends exactly ONE Telegram message per run.

        Steps (order matters for storm-prevention):
          1. Fetch all unalerted signals
          2. Sort by weight DESC
          3. Mark EVERY signal alerted=1 immediately — before any network call
          4. Build one consolidated message
          5. send_telegram() exactly once
          6. Return 1 if sent, 0 if nothing to send or send failed
        """
        signals = get_unalerted_signals()
        if not signals:
            log.info("[Alerts] No new signals.")
            return 0

        signals.sort(key=lambda s: s.get("weight", 0), reverse=True)
        total = len(signals)

        # ── CRITICAL: mark all alerted BEFORE any network call ────────────────
        for sig in signals:
            mark_alerted(sig["id"])
        log.info("[Alerts] Marked %d signals alerted.", total)

        # ── Build and send ONE message ─────────────────────────────────────────
        msg     = _build_single_message(signals)
        success = send_telegram(msg)

        if success:
            log.info("[Alerts] ✅ Consolidated digest sent — %d signals in 1 message", total)
        else:
            log.warning("[Alerts] ⚠️  Telegram failed (signals already marked alerted — no re-fire)")

        return 1 if success else 0

    def send_weekly_digest(self, leaderboard: list, outreach_plan: list):
        """Weekly email digest + short Telegram summary."""
        subject = f"📊 Calgary Law Tracker — {datetime.utcnow().strftime('%B %d, %Y')}"
        html    = self._build_digest_html(leaderboard, outreach_plan)
        send_email(subject, html)

        top5  = leaderboard[:5]
        lines = ["📊 <b>Weekly Calgary Hiring Digest</b>", ""]
        for i, r in enumerate(top5, 1):
            lines.append(
                f"{i}. 🏛 <b>{r['firm_name']}</b>  "
                f"score={r['score']:.1f}  {r.get('urgency','')}"
            )
        send_telegram("\n".join(lines))

    @staticmethod
    def _build_digest_html(leaderboard: list, outreach_plan: list) -> str:
        rows = ""
        for i, r in enumerate(leaderboard[:15], 1):
            corr   = "✅" if r.get("corroborated") else ""
            strats = ", ".join(r.get("strategies", []))
            rows  += f"""<tr>
              <td>{i}</td>
              <td><b>{r['firm_name']}</b> [{r.get('tier','?')}]</td>
              <td>{r['score']:.1f}</td>
              <td>{r.get('urgency','')}</td>
              <td>{strats}</td><td>{corr}</td>
              <td style='font-size:11px'>{r.get('top_signal','')[:65]}</td>
            </tr>"""
        outreach = ""
        for item in outreach_plan[:5]:
            outreach += f"""
            <div style='border:1px solid #ddd;padding:14px;margin:10px 0;border-radius:8px'>
              <b>{item['to_firm']}</b> — <span style='color:#059669'>{item['urgency']}</span><br>
              <em>Subject:</em> {item['subject']}<br><br>
              <pre style='font-size:11px;background:#f0f0f0;padding:8px'>{item['body'][:400]}</pre>
            </div>"""
        return f"""<html><body style='font-family:sans-serif;max-width:860px;margin:auto;padding:24px'>
        <h1 style='color:#0C9182'>🏛 Calgary Law Firm Hiring Tracker</h1>
        <p style='color:#666'>{datetime.utcnow().strftime('%B %d, %Y')}</p>
        <h2>📊 Leaderboard</h2>
        <table border='1' cellpadding='8' style='border-collapse:collapse;width:100%;font-size:13px'>
          <tr style='background:#f5f5f5'>
            <th>#</th><th>Firm</th><th>Score</th><th>Urgency</th>
            <th>Strategies</th><th></th><th>Top Signal</th></tr>
          {rows}
        </table>
        <h2>📧 Outreach Plan</h2>{outreach}
        </body></html>"""


if __name__ == "__main__":
    import logging as _l
    _l.basicConfig(level=_l.INFO)
    AlertDispatcher().dispatch_unalerted()
