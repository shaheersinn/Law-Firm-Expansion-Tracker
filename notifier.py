"""
alerts/notifier.py  (v3)
─────────────────────────
Telegram alerts for genuinely new signals.
Dedup is enforced at DB insert (dedup_hash); this layer processes only
signals with alerted=0.

Every alert includes:
  • Practice area
  • Signal type
  • Firm tier
  • Urgency / suggested action

BUG FIX (v5.2):
  Previously dispatch_unalerted() sent every queued signal unconditionally.
  In a normal run with macro signals firing per-firm this caused 50-80 Telegram
  messages per run.

  Fix: signals are sorted by weight DESC and capped at MAX_ALERTS_PER_RUN (20).
  All signals beyond the cap are still marked alerted=1 so they:
    (a) don't re-fire next run
    (b) are stored in DB and appear on the dashboard
  Only the top-weighted ones send a Telegram message.
"""

import logging
import json
import time
from datetime import datetime

import requests

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from config_calgary import (
    TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID,
    SENDGRID_API_KEY, ALERT_EMAIL_FROM, ALERT_EMAIL_TO,
    FIRM_BY_ID,
)
from database.db import get_unalerted_signals, mark_alerted

log = logging.getLogger(__name__)

TIER1_WEIGHT = 4.0

# Maximum Telegram messages sent per run — prevents storms when many signals
# fire simultaneously (e.g. macro correlator or first-run DB population).
MAX_ALERTS_PER_RUN = 20

# Minimum weight to send immediately; lighter signals are queued for digest.
MIN_WEIGHT_FOR_INSTANT = 2.0

# ── Practice area display names ───────────────────────────────────────────────
PRACTICE_LABELS = {
    "corporate":        "⚖️  Corporate / M&A",
    "securities":       "📈  Securities",
    "litigation":       "🏛  Litigation",
    "energy":           "⚡  Energy & Resources",
    "employment":       "👥  Employment",
    "real_estate":      "🏗  Real Estate",
    "tax":              "💼  Tax",
    "ip":               "💡  Intellectual Property",
    "restructuring":    "🔄  Restructuring / Insolvency",
    "regulatory":       "📋  Regulatory",
    "general":          "📌  General Practice",
    None:               "📌  General Practice",
}

SIGNAL_LABELS = {
    "canlii_appearance_spike":    ("⚖️",  "LITIGATION SPIKE",     "This Week"),
    "canlii_new_large_file":      ("📋",  "LARGE FILE",           "This Week"),
    "sedar_major_deal":           ("💰",  "MAJOR DEAL",           "TODAY"),
    "sedar_counsel_named":        ("📄",  "DEAL COUNSEL",         "This Week"),
    "linkedin_turnover_detected": ("🚪",  "EMPTY CHAIR",          "TODAY"),
    "linkedin_new_vacancy":       ("🪑",  "NEW VACANCY",          "TODAY"),
    "lsa_student_not_retained":   ("🎓",  "HIREBACK GAP",         "3 Days"),
    "lsa_retention_gap":          ("📊",  "RETENTION GAP",        "3 Days"),
    "biglaw_spillage_predicted":  ("🌊",  "BIGLAW SPILLAGE",      "TODAY"),
    "job_posting":                ("📌",  "JOB POSTED",           "This Week"),
    "lateral_hire":               ("🔄",  "LATERAL HIRE",         "This Week"),
    "ranking":                    ("🏆",  "NEW RANKING",          "This Month"),
    "macro_ma_wave_incoming":     ("📈",  "M&A WAVE INCOMING",    "This Month"),
    "macro_demand_surge":         ("⚡",  "DEMAND SURGE",         "This Week"),
    "macro_demand_collapse":      ("📉",  "MACRO CAUTION",        "This Month"),
    "fiscal_pressure_incoming":   ("🗓",  "FISCAL PRESSURE",      "This Week"),
    "sec_edgar_filing":           ("🇺🇸",  "SEC FILING",           "This Week"),
    "competition_bureau":         ("🏛",  "COMPETITION BUREAU",   "This Week"),
    "new_court_filing":           ("⚖️",  "NEW FILING",           "This Week"),
}


def _firm_info(firm_id: str) -> dict:
    if firm_id == "market":
        return {"name": "Calgary Energy Legal Market", "tier": "market", "focus": ["energy"]}
    return FIRM_BY_ID.get(firm_id, {"name": firm_id, "tier": "?", "focus": []})


def format_telegram_alert(signal: dict) -> str:
    firm    = _firm_info(signal["firm_id"])
    sig     = signal.get("signal_type", "")
    emoji, label, urgency = SIGNAL_LABELS.get(sig, ("📣", sig.upper(), "—"))
    weight  = signal.get("weight", 0)
    pa_raw  = signal.get("practice_area") or _infer_practice(firm, sig)
    pa      = PRACTICE_LABELS.get(pa_raw,
                f"📌  {pa_raw.replace('_',' ').title()}" if pa_raw else "📌  General Practice")
    tier    = firm.get("tier", "?").upper()
    focus   = ", ".join(firm.get("focus", []))

    urgency_line = {
        "TODAY":      "🔴 <b>ACT TODAY</b> — email the hiring partner now",
        "This Week":  "🟡 <b>THIS WEEK</b> — reach out before it's posted",
        "3 Days":     "🟠 <b>WITHIN 3 DAYS</b> — time-sensitive opportunity",
        "This Month": "🟢 <b>THIS MONTH</b> — add to outreach queue",
    }.get(urgency, f"⚪ {urgency}")

    lines = [
        f"{emoji} <b>NEW SIGNAL  ·  w={weight:.1f}</b>",
        "",
        f"🏛 <b>{firm.get('name', signal['firm_id'])}</b>",
        f"   [{tier}]  {focus}",
        "",
        f"📂 <b>Practice Area:</b>  {pa}",
        f"🔖 <b>Signal Type:</b>   {label}",
        "",
        f"<b>{signal.get('title', '')}</b>",
        "",
        (signal.get("description") or "")[:380],
        "",
        urgency_line,
    ]
    if signal.get("source_url"):
        lines += ["", f'🔗 <a href="{signal["source_url"]}">Source →</a>']

    return "\n".join(lines)


def _infer_practice(firm: dict, signal_type: str) -> str:
    if "sedar" in signal_type or "edgar" in signal_type:
        return "securities"
    if "canlii" in signal_type or "court" in signal_type:
        return "litigation"
    if "linkedin" in signal_type or "lsa" in signal_type:
        focus = firm.get("focus", [])
        return focus[0] if focus else "general"
    if "spillage" in signal_type or "macro" in signal_type:
        return "energy"
    return "general"


def send_telegram(text: str) -> bool:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        log.warning("[Telegram] Not configured — printing alert:\n%s", text)
        return False
    url  = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    data = {
        "chat_id":    TELEGRAM_CHAT_ID,
        "text":       text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    try:
        resp = requests.post(url, json=data, timeout=10)
        resp.raise_for_status()
        return True
    except Exception as e:
        log.error("[Telegram] Send failed: %s", e)
        return False


def send_email(subject: str, html_body: str, to: str = None) -> bool:
    if not SENDGRID_API_KEY:
        log.info("[Email] No SendGrid key — skipping email")
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


class AlertDispatcher:

    def dispatch_unalerted(self) -> int:
        """
        Sends ONE consolidated Telegram message per run covering all new signals.

        The v3 tracker sent a single digest at the end of the firm-scraper loop
        (see March 2026 log: "Telegram: message delivered"). This replaces the
        old per-signal storm (which sent 50-80 messages) with a single structured
        summary, matching that pattern.

        Message format:
          Header: run stats (N signals across M firms)
          Top signals: up to 10 highest-weight signals with firm/type/title
          Practice breakdown: signal counts by practice area
          Scraper health: any practice areas with zero signals
          Dashboard link

        All signals are marked alerted=1 regardless, so they don't re-fire.
        Returns 1 if the message was sent, 0 otherwise.
        """
        signals = get_unalerted_signals()
        if not signals:
            log.info("[Alerts] No new signals to dispatch.")
            return 0

        # Sort highest-weight first
        signals.sort(key=lambda s: s.get("weight", 0), reverse=True)
        total = len(signals)

        # Mark all as alerted NOW — prevents re-fire regardless of Telegram result
        for sig in signals:
            mark_alerted(sig["id"])

        # ── Build the single consolidated message ──────────────────────────────
        text = self._build_digest_message(signals)
        success = send_telegram(text)

        if success:
            log.info("[Alerts] ✅ Consolidated digest sent — %d signals in 1 message", total)
        else:
            log.warning("[Alerts] ⚠️  Telegram delivery failed (signals still marked alerted)")

        return 1 if success else 0

    @staticmethod
    def _build_digest_message(signals: list) -> str:
        """Build a single Telegram-safe HTML message covering all new signals."""
        from config_calgary import FIRM_BY_ID
        from datetime import datetime, timezone

        total = len(signals)
        now   = datetime.now(timezone.utc).strftime("%b %d, %Y  %H:%M UTC")

        # ── Firm × signal counts ──────────────────────────────────────
        firm_counts: dict = {}
        for s in signals:
            fid = s.get("firm_id", "market")
            firm_counts[fid] = firm_counts.get(fid, 0) + 1
        n_firms = len([f for f in firm_counts if f != "market"])

        # ── Practice-area breakdown ───────────────────────────────────
        pa_counts: dict = {}
        for s in signals:
            pa = s.get("practice_area") or _infer_practice(
                    _firm_info(s["firm_id"]), s.get("signal_type",""))
            pa_counts[pa] = pa_counts.get(pa, 0) + 1
        pa_sorted = sorted(pa_counts.items(), key=lambda x: -x[1])

        # ── Top signals (up to 10 by weight) ──────────────────────────
        top = signals[:10]

        # ── Assemble message ──────────────────────────────────────────
        lines = [
            f"🏛 <b>Calgary Law Tracker  ·  {now}</b>",
            f"<b>{total} new signal{'s' if total != 1 else ''}</b> across "
            f"<b>{n_firms} firm{'s' if n_firms != 1 else ''}</b>",
            "",
        ]

        # Top signals section
        lines.append("📌 <b>Top Signals</b>")
        for i, s in enumerate(top, 1):
            firm     = _firm_info(s["firm_id"])
            fname    = firm.get("name", s["firm_id"])
            if fname == "Calgary Energy Legal Market":
                fname = "🌐 Market-wide"
            sig_type = s.get("signal_type", "")
            emoji, label, urgency = SIGNAL_LABELS.get(sig_type, ("📣", sig_type.upper(), "—"))
            weight   = s.get("weight", 0)
            title    = (s.get("title") or "")[:70]
            lines.append(
                f"  {i}. {emoji} <b>{fname}</b>  [{label}]  w={weight:.1f}\n"
                f"      <i>{title}</i>"
            )

        # Practice-area breakdown
        lines += ["", "📊 <b>By Practice Area</b>"]
        for pa, cnt in pa_sorted[:6]:
            pa_label = PRACTICE_LABELS.get(pa, f"📌 {pa.replace('_',' ').title()}" if pa else "📌 General")
            bar = "█" * min(cnt, 10)
            lines.append(f"  {pa_label:<28} {bar} {cnt}")

        # Urgency breakdown
        today_sigs  = [s for s in signals if s.get("signal_type") in
                       ("sedar_major_deal","linkedin_turnover_detected",
                        "biglaw_spillage_predicted","ica_review_announced",
                        "counterfactual_conflict_free")]
        week_sigs   = [s for s in signals if s.get("weight", 0) >= 3.0
                       and s not in today_sigs]
        if today_sigs:
            lines += ["", f"🔴 <b>ACT TODAY</b> — {len(today_sigs)} time-sensitive signal(s)"]
            for s in today_sigs[:3]:
                firm = _firm_info(s["firm_id"]).get("name", s["firm_id"])
                lines.append(f"  • {firm}: {(s.get('title') or '')[:60]}")
        if week_sigs:
            lines.append(f"🟡 <b>This Week</b> — {len(week_sigs)} high-weight signal(s)")

        # Dashboard link
        import os
        dash_url = os.getenv("DASHBOARD_URL", "")
        if dash_url:
            lines += ["", f'🔗 <a href="{dash_url}">Open Dashboard →</a>']

        lines += ["", f"<i>{total} signals stored in DB · {total - len(top)} below top-10 threshold</i>"]

        # Telegram hard limit is 4096 chars — truncate if needed
        msg = "\n".join(lines)
        if len(msg) > 4000:
            msg = msg[:3950] + "\n…<i>(truncated)</i>"
        return msg

    def send_weekly_digest(self, leaderboard: list, outreach_plan: list):
        subject = f"📊 Calgary Law Tracker — {datetime.utcnow().strftime('%B %d, %Y')}"
        html    = self._build_digest_html(leaderboard, outreach_plan)
        send_email(subject, html)

        top5  = leaderboard[:5]
        lines = ["📊 <b>Weekly Calgary Hiring Digest</b>", ""]
        for i, r in enumerate(top5, 1):
            pa = PRACTICE_LABELS.get(
                (r.get("breakdown") or {}).get("practice_area"),
                "📌 General"
            )
            lines.append(
                f"{i}. 🏛 <b>{r['firm_name']}</b>  score={r['score']:.1f}  {r['urgency']}"
            )
        send_telegram("\n".join(lines))

    @staticmethod
    def _build_digest_html(leaderboard: list, outreach_plan: list) -> str:
        rows = ""
        for i, r in enumerate(leaderboard[:15], 1):
            corr  = "✅" if r["corroborated"] else ""
            strats = ", ".join(r["strategies"])
            rows  += f"""<tr>
              <td>{i}</td>
              <td><b>{r['firm_name']}</b> <span style='color:#888'>[{r['tier']}]</span></td>
              <td>{r['score']:.1f}</td>
              <td>{r['urgency']}</td>
              <td>{strats}</td>
              <td>{corr}</td>
              <td style='font-size:11px;color:#555'>{r['top_signal'][:65]}</td>
            </tr>"""

        outreach = ""
        for item in outreach_plan[:5]:
            outreach += f"""
            <div style='border:1px solid #ddd;padding:14px;margin:10px 0;border-radius:8px;background:#fafafa'>
              <b>{item['to_firm']}</b> &mdash; <span style='color:#059669'>{item['urgency']}</span><br>
              <em>Subject:</em> {item['subject']}<br><br>
              <pre style='font-size:11px;white-space:pre-wrap;background:#f0f0f0;padding:8px;border-radius:4px'>{item['body'][:500]}</pre>
              <small style='color:#999'>{item['strategy']}</small>
            </div>"""

        return f"""<html><body style='font-family:sans-serif;max-width:860px;margin:auto;padding:24px'>
        <h1 style='color:#0C9182'>🏛 Calgary Law Firm Hiring Tracker</h1>
        <p style='color:#666'>Week of {datetime.utcnow().strftime('%B %d, %Y')}</p>
        <h2>📊 Opportunity Leaderboard</h2>
        <table border='1' cellpadding='8' style='border-collapse:collapse;width:100%;font-size:13px'>
          <tr style='background:#f5f5f5'>
            <th>#</th><th>Firm</th><th>Score</th><th>Urgency</th>
            <th>Strategies</th><th></th><th>Top Signal</th></tr>
          {rows}
        </table>
        <h2>📧 Outreach Plan</h2>{outreach}
        <hr style='margin-top:32px'>
        <small style='color:#bbb'>Sources: CanLII API · SEDAR+ RSS · LSA public directory · LinkedIn (Proxycurl) · Google News RSS</small>
        </body></html>"""


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    AlertDispatcher().dispatch_unalerted()
