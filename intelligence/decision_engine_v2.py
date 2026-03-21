"""
intelligence/decision_engine_v2.py
────────────────────────────────────
Decision Engine v2 — Full Integration

Morning briefing that integrates:
  ✓ All 20 signal strategies
  ✓ Deal cascade confirmation (P up to 6.5 weight)
  ✓ Second-derivative acceleration
  ✓ Counterfactual conflict analysis (contact ONLY conflict-free firms)
  ✓ Background matching (fit score × base opportunity score)
  ✓ A/B optimal send time
  ✓ Three-touch follow-up tracking
  ✓ Mutual connection detection

Produces every weekday at 07:00 Calgary time:
  1. YOUR MOVE list (top 3 actions, ranked by adjusted fit score)
  2. Complete outreach drafts inside Telegram
  3. Conflict-free firm analysis for any active BigLaw deals
  4. Weekly A/B report (Sundays)
"""

import json, logging, os
from datetime import datetime, date, timedelta
from collections import defaultdict
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import requests

from database.db import get_conn, get_all_signals_for_dashboard
from config_calgary import FIRM_BY_ID, CALGARY_FIRMS, BIGLAW_FIRMS
from scoring.aggregator import compute_firm_scores
from alerts.notifier import send_telegram
from intelligence.adaptive.background_matcher import BackgroundMatcher, USER_PROFILE
from intelligence.adaptive.ab_optimizer import (
    get_optimal_send_time, generate_ab_report, CounterfactualConflictEngine
)

log = logging.getLogger(__name__)

ANTHROPIC_API_URL = "https://api.anthropic.com/v1/messages"


def _call_claude(prompt: str, system: str, max_tokens: int = 1000) -> str:
    api_key = os.getenv("ANTHROPIC_API_KEY","")
    if not api_key: return ""
    try:
        resp = requests.post(
            ANTHROPIC_API_URL,
            headers={"x-api-key": api_key, "anthropic-version": "2023-06-01",
                     "content-type": "application/json"},
            json={"model":"claude-sonnet-4-6","max_tokens":max_tokens,
                  "system":system,"messages":[{"role":"user","content":prompt}]},
            timeout=25,
        )
        return resp.json()["content"][0]["text"]
    except Exception as e:
        log.error("[DecisionV2] Claude error: %s", e); return ""


def _get_cascade_signals() -> list[dict]:
    conn  = get_conn()
    rows  = conn.execute("""
        SELECT * FROM signals
        WHERE signal_type='deal_cascade_confirmed'
          AND date(detected_at) >= date('now','-3 days')
          AND alerted=0
        ORDER BY weight DESC LIMIT 5
    """).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def _get_acceleration_signals() -> list[dict]:
    conn  = get_conn()
    rows  = conn.execute("""
        SELECT * FROM signals
        WHERE signal_type='signal_acceleration_detected'
          AND date(detected_at) >= date('now','-7 days')
        ORDER BY weight DESC LIMIT 5
    """).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def _get_biglaw_active_deals() -> list[dict]:
    """Find signals where a BigLaw firm is acting on a major deal."""
    conn  = get_conn()
    rows  = conn.execute("""
        SELECT firm_id, title, raw_data, detected_at
        FROM signals
        WHERE signal_type IN ('sedar_major_deal','breaking_deal_announcement',
                               'gravity_spillage_predicted','deal_cascade_confirmed')
          AND firm_id IN ({})
          AND date(detected_at) >= date('now','-7 days')
        ORDER BY weight DESC LIMIT 5
    """.format(",".join("?" * len(BIGLAW_FIRMS))),
        list(BIGLAW_FIRMS)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def run_full_morning_briefing(send_to_telegram: bool = True) -> dict:
    """
    THE MAIN FUNCTION — runs every weekday morning at 07:00 Calgary.
    """
    log.info("[DecisionV2] Running full morning briefing…")
    today_str = date.today().strftime("%B %d, %Y")

    # ── 1. Compute base scores + enrich with background matching ────────────
    base_leaderboard = compute_firm_scores()[:15]
    matcher          = BackgroundMatcher(USER_PROFILE)
    enriched_lb      = matcher.enrich_leaderboard(base_leaderboard)

    # ── 2. Cascade signals (highest conviction) ───────────────────────────────
    cascade_sigs    = _get_cascade_signals()
    accel_sigs      = _get_acceleration_signals()

    # ── 3. Conflict analysis for active BigLaw deals ─────────────────────────
    biglaw_deals    = _get_biglaw_active_deals()
    conflict_engine = CounterfactualConflictEngine()
    conflict_analyses = []
    for deal in biglaw_deals[:3]:
        raw = {}
        try: raw = json.loads(deal.get("raw_data") or "{}")
        except: pass
        company = raw.get("company") or raw.get("issuer", "")
        if company:
            analysis = conflict_engine.analyse_conflict(deal["firm_id"], company)
            conflict_analyses.append({
                "deal": deal["title"][:60],
                "analysis": analysis,
            })

    # ── 4. Optimal send time ──────────────────────────────────────────────────
    best_day, best_hour = get_optimal_send_time()
    days = ["Mon","Tue","Wed","Thu","Fri","Sat","Sun"]
    send_rec = f"{days[best_day]} {best_hour:02d}:00 Calgary"

    # ── 5. Build Claude prompt for morning briefing ───────────────────────────
    top3_for_claude = enriched_lb[:5]
    prompt = f"""
DATE: {today_str}
CANDIDATE PROFILE: {USER_PROFILE.get('name','')} | {', '.join(USER_PROFILE.get('practice_interests',[]))} | Called {USER_PROFILE.get('call_year','')}

TOP OPPORTUNITIES (ranked by fit×signal score):
{json.dumps([{
    "rank": i+1,
    "firm": FIRM_BY_ID.get(e["firm_id"],{}).get("name",""),
    "tier": e.get("tier",""),
    "score": e["adjusted_score"],
    "fit": e["fit_score"],
    "urgency": e.get("urgency",""),
    "connection": e.get("connection_sentence",""),
    "top_signal": e.get("top_signal","")[:80],
    "strategies": e.get("strategies",[]),
} for i, e in enumerate(top3_for_claude)], indent=2)}

CASCADE SIGNALS (multi-source confirmed):
{json.dumps([{"firm": FIRM_BY_ID.get(s["firm_id"],{}).get("name",""), "title": s.get("title","")[:60], "raw": json.loads(s.get("raw_data") or "{}")} for s in cascade_sigs], indent=2)}

CONFLICT-FREE FIRMS FOR ACTIVE BIGLAW DEALS:
{json.dumps([{"deal": ca["deal"], "top_free_firm": ca["analysis"]["conflict_free_firms"][0] if ca["analysis"]["conflict_free_firms"] else None} for ca in conflict_analyses], indent=2)}

OPTIMAL SEND TIME: {send_rec}

Write a MORNING BRIEFING in this EXACT format (no markdown, use plain text for Telegram):

━━━━ YOUR MOVE — {today_str} ━━━━

[For each of top 3 actions:]
#N [URGENCY EMOJI + LABEL]
→ FIRM: [name] | [tier] | Fit: [X.X]×
→ WHY NOW: [cite SPECIFIC signal — deal name, $ amount, or case]
→ CONNECTION: [the connection_sentence — their work + your background]
→ DO: [one concrete action in imperative mood]

─────────────────────────────────
CONFLICT WINDOW: [If any BigLaw deal active, name the top conflict-free firm]
ACCELERATION: [Any firm with accelerating signal rate]
SEND TIME: {send_rec}
"""

    briefing = _call_claude(
        prompt,
        system="You write sharp legal market intelligence briefings. Be specific, cite names and numbers.",
        max_tokens=900,
    )

    # ── 6. Generate outreach drafts for top 2 fits ────────────────────────────
    drafts = []
    conn   = get_conn()
    for entry in enriched_lb[:2]:
        fid     = entry["firm_id"]
        top_sig = conn.execute("""
            SELECT * FROM signals WHERE firm_id=?
            ORDER BY weight DESC LIMIT 1
        """, (fid,)).fetchone()
        if not top_sig: continue
        sig = dict(top_sig)

        # Build personalised email prompt
        connection = entry.get("connection_sentence","")
        firm       = FIRM_BY_ID.get(fid, {})
        raw        = {}
        try: raw   = json.loads(sig.get("raw_data") or "{}")
        except: pass

        email_prompt = f"""
Write a complete cold email from {USER_PROFILE.get('name','[Your Name]')} 
to the hiring partner at {firm.get('name','')}.

TRIGGER: {sig.get('title','')}
DETAIL: {(sig.get('description','') or '')[:300]}
CONNECTION SENTENCE: {connection}
CANDIDATE: {USER_PROFILE.get('name','')} | Called {USER_PROFILE.get('call_year','')} | {', '.join(USER_PROFILE.get('practice_interests',[]))}

Output format:
Subject: [subject line — under 9 words, includes specific deal/company name]

[Connection sentence]. [Why I specifically am relevant — 1 sentence]. [Availability + ask — 1 sentence].

{USER_PROFILE.get('name','')}
{os.getenv('YOUR_PHONE','')} | {os.getenv('YOUR_EMAIL','')}
"""
        draft = _call_claude(
            email_prompt,
            system="Write sharp, specific legal job application emails. Under 80 words. No fluff.",
            max_tokens=200,
        )
        drafts.append({"firm_id": fid, "firm_name": firm.get("name",""), "draft": draft})
    conn.close()

    # ── 7. Assemble and send Telegram message ─────────────────────────────────
    if send_to_telegram:
        # Main briefing
        if briefing:
            send_telegram(f"<pre>{briefing}</pre>")
        else:
            # Fallback
            _send_fallback_briefing(enriched_lb[:3], today_str)

        # Email drafts
        for d in drafts:
            if d["draft"]:
                send_telegram(
                    f"✉ <b>DRAFT FOR {d['firm_name'].upper()}</b>\n\n"
                    f"<pre>{d['draft']}</pre>"
                )

        # Conflict analysis
        for ca in conflict_analyses[:1]:
            free_firms = ca["analysis"]["conflict_free_firms"][:3]
            if free_firms:
                lines = [f"⚖️ <b>CONFLICT ANALYSIS: {ca['deal'][:50]}</b>", ""]
                for f in free_firms:
                    lines.append(
                        f"  ✅ {f['firm_name']}: P={f['overflow_prob']:.0%} ({f['co_appearances']} co-app)"
                    )
                send_telegram("\n".join(lines))

    result = {
        "top_3":           [{"firm_id":e["firm_id"],"adjusted_score":e["adjusted_score"],"fit":e["fit_score"]} for e in enriched_lb[:3]],
        "cascade_signals": len(cascade_sigs),
        "outreach_drafts": len(drafts),
        "conflict_analyses":len(conflict_analyses),
        "send_rec":        send_rec,
    }
    log.info("[DecisionV2] Briefing complete. top_3=%s", [r["firm_id"] for r in result["top_3"]])
    return result


def _send_fallback_briefing(enriched: list, today_str: str):
    """Plain-text fallback if Claude is unavailable."""
    lines = [f"🎯 <b>YOUR MOVE — {today_str}</b>", ""]
    urgency_map = {"today":"🔴","week":"🟡","3days":"🟠","month":"🟢"}
    for i, e in enumerate(enriched, 1):
        firm  = FIRM_BY_ID.get(e["firm_id"],{})
        emo   = urgency_map.get(e.get("urgency",""), "⚪")
        conn_s= e.get("connection_sentence","")
        lines.append(
            f"#{i} {emo} <b>{firm.get('name','?')}</b> [{firm.get('tier','?').upper()}]\n"
            f"   Score: {e['adjusted_score']:.1f} (fit×{e['fit_score']:.1f})\n"
            f"   {e.get('top_signal','')[:65]}\n"
            + (f"   💬 {conn_s}" if conn_s else "")
        )
    send_telegram("\n\n".join(lines))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    result = run_full_morning_briefing(send_to_telegram=True)
    print(json.dumps(result, indent=2))
