"""
intelligence/brief_generator.py
─────────────────────────────────
Intelligence Brief Generator

The "nuclear option" output feature. For any firm, synthesises ALL active
signals into a 2-page intelligence brief using Claude (claude-sonnet-4-6).

The brief contains:
  1. Executive summary (what's happening at this firm right now)
  2. Signal inventory (ranked by weight, grouped by strategy)
  3. Practice area demand analysis
  4. Bespoke outreach strategy (what angle to use, what to reference)
  5. Draft cold email — hyper-personalised, references specific deals/cases
  6. Risk factors (why NOT to reach out, or what to avoid mentioning)
  7. Timing recommendation (send NOW / wait for X event / next Monday)

Also generates:
  • A "Spillage network brief" — for firms in the spillage graph, explains
    exactly which BigLaw firms they most commonly face, which energy companies
    are involved, and what the next trigger event likely is
  • A "Comparative brief" — compares top 3 opportunity firms head-to-head
"""

import json, logging, os
from datetime import datetime, date
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import requests

from database.db import get_conn, get_all_signals_for_dashboard, get_spillage_graph
from config_calgary import FIRM_BY_ID, CALGARY_FIRMS, OPENAI_API_KEY
from scoring.aggregator import compute_firm_scores

log = logging.getLogger(__name__)

ANTHROPIC_API_URL = "https://api.anthropic.com/v1/messages"
ANTHROPIC_MODEL   = "claude-sonnet-4-6"


def _call_claude(system: str, user: str, max_tokens: int = 1500) -> str:
    """Call Claude API for brief generation."""
    api_key = os.getenv("ANTHROPIC_API_KEY", "")
    if not api_key:
        log.warning("[Brief] No ANTHROPIC_API_KEY — returning placeholder")
        return "[Claude API key not configured. Set ANTHROPIC_API_KEY to generate briefs.]"

    headers = {
        "x-api-key":         api_key,
        "anthropic-version": "2023-06-01",
        "content-type":      "application/json",
    }
    payload = {
        "model":      ANTHROPIC_MODEL,
        "max_tokens": max_tokens,
        "system":     system,
        "messages":   [{"role": "user", "content": user}],
    }
    try:
        resp = requests.post(ANTHROPIC_API_URL, headers=headers,
                             json=payload, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        return data["content"][0]["text"]
    except Exception as e:
        log.error("[Brief] Claude API error: %s", e)
        return f"[Error generating brief: {e}]"


SYSTEM_PROMPT = """You are a legal market intelligence analyst specialising in
the Calgary, Alberta legal market. You help law students and junior lawyers
identify and act on hiring opportunities at Calgary law firms.

Your job is to synthesise raw intelligence signals into sharp, actionable
intelligence briefs. Be specific. Reference actual deal names, case citations,
company names, and dollar values from the signals. Be direct about urgency.
Never be vague. Always end with a specific recommended action.

Write in a crisp, intelligence-report style. No fluff. No generic statements.
Every sentence should contain specific, actionable information."""


def generate_firm_brief(firm_id: str,
                        your_background: str = "second-year law student with a background in corporate and energy law seeking a first-year associate position") -> dict:
    """
    Generate a full intelligence brief for a single firm.
    Returns a dict with the brief text and metadata.
    """
    firm    = FIRM_BY_ID.get(firm_id)
    if not firm:
        return {"error": f"Unknown firm_id: {firm_id}"}

    # Pull all signals for this firm
    conn    = get_conn()
    signals = conn.execute("""
        SELECT * FROM signals
        WHERE firm_id = ?
          AND date(detected_at) >= date('now', '-21 days')
        ORDER BY weight DESC
        LIMIT 20
    """, (firm_id,)).fetchall()
    signals = [dict(s) for s in signals]

    # Pull spillage edges
    edges   = conn.execute("""
        SELECT biglaw_id, boutique_id, co_appearances, source
        FROM spillage_edges
        WHERE boutique_id = ? OR biglaw_id = ?
        ORDER BY co_appearances DESC
        LIMIT 10
    """, (firm_id, firm_id)).fetchall()
    edges = [dict(e) for e in edges]
    conn.close()

    if not signals:
        return {
            "firm_id":   firm_id,
            "firm_name": firm["name"],
            "brief":     f"No recent signals for {firm['name']} in the past 21 days.",
            "generated_at": datetime.utcnow().isoformat(),
        }

    # Build context block for Claude
    sigs_text = "\n".join([
        f"• [{s['signal_type']}] w={s['weight']:.1f} | {s['title']} | {s['practice_area'] or 'general'}\n"
        f"  {(s['description'] or '')[:200]}"
        for s in signals
    ])

    edge_text = "\n".join([
        f"• {FIRM_BY_ID.get(e['biglaw_id'],{}).get('name',e['biglaw_id'])} ↔ "
        f"{FIRM_BY_ID.get(e['boutique_id'],{}).get('name',e['boutique_id'])}: "
        f"{e['co_appearances']} co-appearances [{e['source']}]"
        for e in edges
    ]) or "No spillage data yet."

    scores  = compute_firm_scores()
    lb_rank = next((i+1 for i, s in enumerate(scores) if s["firm_id"] == firm_id), "N/A")

    user_prompt = f"""
INTELLIGENCE BRIEF REQUEST
===========================
Target Firm:    {firm['name']}
Tier:           {firm.get('tier','?').upper()}
Practice Focus: {', '.join(firm.get('focus', []))}
Leaderboard:    #{lb_rank} opportunity ranking

ACTIVE SIGNALS ({len(signals)} total):
{sigs_text}

SPILLAGE NETWORK:
{edge_text}

CANDIDATE BACKGROUND:
{your_background}

REQUIRED OUTPUT (follow this structure exactly):

## SITUATION SUMMARY
[2-3 sentences. What is happening at this firm RIGHT NOW? Be specific about deals, cases, dates.]

## SIGNAL INVENTORY
[List top 3 signals with specific data. Why each matters.]

## PRACTICE AREA DEMAND
[Which practice area has highest demand at this firm right now, and why.]

## OUTREACH STRATEGY
[Specific angle to use. Which signal to reference. What NOT to say.]

## DRAFT COLD EMAIL
Subject: [specific subject line]

[Email body — 100-150 words. Reference the specific deal/case/signal. 
Do not use brackets. Write as if sending today.]

## TIMING RECOMMENDATION
[When exactly to send. Why now vs. waiting.]

## RISK FACTORS
[One sentence: what to avoid / what would make this outreach backfire.]
"""

    brief_text = _call_claude(SYSTEM_PROMPT, user_prompt, max_tokens=1800)

    result = {
        "firm_id":       firm_id,
        "firm_name":     firm["name"],
        "lb_rank":       lb_rank,
        "signal_count":  len(signals),
        "brief":         brief_text,
        "generated_at":  datetime.utcnow().isoformat(),
        "top_signal":    signals[0]["title"] if signals else "",
    }

    # Save to DB
    conn = get_conn()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS intelligence_briefs (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            firm_id      TEXT NOT NULL,
            generated_at TEXT DEFAULT (datetime('now')),
            brief_text   TEXT,
            lb_rank      INTEGER
        )""")
    conn.execute(
        "INSERT INTO intelligence_briefs (firm_id, brief_text, lb_rank) VALUES (?,?,?)",
        (firm_id, brief_text, lb_rank if isinstance(lb_rank, int) else 0)
    )
    conn.commit()
    conn.close()

    return result


def generate_top_opportunities_report(top_n: int = 5,
                                       your_background: str = "") -> str:
    """
    Generate a comparative intelligence report across the top N firms.
    Returns a Markdown report string.
    """
    scores = compute_firm_scores()[:top_n]
    if not scores:
        return "No scored firms yet. Run --all first."

    # Pull signal summaries for each firm
    firm_summaries = []
    for s in scores:
        conn = get_conn()
        sigs = conn.execute("""
            SELECT signal_type, weight, title, practice_area
            FROM signals WHERE firm_id=?
            AND date(detected_at) >= date('now','-14 days')
            ORDER BY weight DESC LIMIT 5
        """, (s["firm_id"],)).fetchall()
        conn.close()
        firm_summaries.append({
            "firm":    s,
            "signals": [dict(r) for r in sigs],
        })

    context = json.dumps(firm_summaries, indent=2, default=str)

    user_prompt = f"""
TOP OPPORTUNITIES COMPARATIVE REPORT
=====================================
Background: {your_background or 'recently called Calgary lawyer seeking first-year associate role'}

DATA:
{context}

Write a COMPARATIVE INTELLIGENCE REPORT covering:
1. EXECUTIVE RANKING — 1-2 sentences per firm, ranked by opportunity urgency
2. BEST SINGLE OUTREACH THIS WEEK — pick ONE firm and explain exactly why it's #1
3. PRACTICE AREA HOT SPOTS — which Calgary practice areas are on fire right now
4. STRATEGIC INSIGHT — one non-obvious pattern in the data
5. THIS WEEK'S ACTION PLAN — three specific actions in priority order

Be specific. Reference real firm names, deal types, signal data.
"""
    return _call_claude(SYSTEM_PROMPT, user_prompt, max_tokens=2000)


def print_brief(result: dict):
    print("\n" + "═" * 72)
    print(f"INTELLIGENCE BRIEF — {result.get('firm_name','?').upper()}")
    print(f"Rank #{result.get('lb_rank','?')} · {result.get('signal_count',0)} active signals")
    print(f"Generated: {result.get('generated_at','')[:19]} UTC")
    print("═" * 72)
    print(result.get("brief", ""))
    print("═" * 72 + "\n")


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO)

    if len(sys.argv) > 1:
        firm_id = sys.argv[1]
        bg = " ".join(sys.argv[2:]) if len(sys.argv) > 2 else ""
        result = generate_firm_brief(firm_id, your_background=bg)
        print_brief(result)
    else:
        # Comparative report for top 5
        report = generate_top_opportunities_report(top_n=5)
        print(report)
