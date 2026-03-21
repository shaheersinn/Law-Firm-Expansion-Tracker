"""
alerts/notifier.py  (v2)
─────────────────────────
Telegram alerts ONLY for genuinely new signals.
Dedup is enforced at DB insert (dedup_hash); this layer is a final
belt-and-suspenders check: only signals with alerted=0 are processed.

Every alert now includes:
  • Practice area
  • Signal type
  • Firm tier
  • Urgency / suggested action
"""

import logging
import json
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
}


def _firm_info(firm_id: str) -> dict:
    return FIRM_BY_ID.get(firm_id, {"name": firm_id, "tier": "?", "focus": []})


def format_telegram_alert(signal: dict) -> str:
    firm    = _firm_info(signal["firm_id"])
    sig     = signal.get("signal_type", "")
    emoji, label, urgency = SIGNAL_LABELS.get(sig, ("📣", sig.upper(), "—"))
    weight  = signal.get("weight", 0)
    pa_raw  = signal.get("practice_area") or _infer_practice(firm, sig)
    pa      = PRACTICE_LABELS.get(pa_raw, f"📌  {pa_raw.replace('_',' ').title()}" if pa_raw else "📌  General Practice")
    tier    = firm.get("tier", "?").upper()
    focus   = ", ".join(firm.get("focus", []))

    # Urgency line
    urgency_line = {
        "TODAY":     "🔴 <b>ACT TODAY</b> — email the hiring partner now",
        "This Week": "🟡 <b>THIS WEEK</b> — reach out before it's posted",
        "3 Days":    "🟠 <b>WITHIN 3 DAYS</b> — time-sensitive opportunity",
        "This Month":"🟢 <b>THIS MONTH</b> — add to outreach queue",
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
    """Fallback: infer practice area from signal type + firm focus."""
    if "sedar" in signal_type:
        return "securities"
    if "canlii" in signal_type:
        return "litigation"
    if "linkedin" in signal_type or "lsa" in signal_type:
        focus = firm.get("focus", [])
        return focus[0] if focus else "general"
    if "spillage" in signal_type:
        return "corporate"
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
        Sends Telegram for every NEW unalerted signal.
        Dedup is already enforced at DB insert — this only sees truly new rows.
        Returns count of alerts sent.
        """
        signals = get_unalerted_signals()
        if not signals:
            log.info("[Alerts] No new signals to dispatch.")
            return 0

        log.info("[Alerts] %d new signal(s) to dispatch", len(signals))
        sent = 0

        for sig in signals:
            text    = format_telegram_alert(sig)
            success = send_telegram(text)
            mark_alerted(sig["id"])   # mark even if Telegram failed — prevents storm
            if success:
                sent += 1
                log.info("[Alerts] ✅ Telegraphed: [%s] %s",
                         sig.get("signal_type"), sig.get("title", "")[:60])
            else:
                log.warning("[Alerts] ⚠️  Telegram failed for signal id=%s", sig["id"])

        log.info("[Alerts] Done. Sent %d / %d alerts.", sent, len(signals))
        return sent

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
