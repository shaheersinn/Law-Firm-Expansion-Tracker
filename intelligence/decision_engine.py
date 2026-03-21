"""
intelligence/decision_engine.py
─────────────────────────────────
The Decision Engine — The Brain of the System

Takes ALL signals, models, and predictions and synthesises them into
ONE ranked action list — not 50 signals to parse, but a clear priority queue.

Output: Every morning you wake up to ONE Telegram message:

  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  🎯 YOUR MOVE TODAY — March 21, 2026
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  #1 SEND NOW (P=91%):
     BDP ← Blakes overflow from $2.1B Cenovus deal
     📧 Draft loaded. Subject: "Re: Cenovus-TransAlta transaction"
     38 historical co-appearances. Gravity score: 0.87.

  #2 SEND TODAY (P=84%):
     Field Law — J. Mackenzie left for Cenovus in-house
     📧 Draft loaded. Empty chair signal confirmed.

  #3 RESEARCH FIRST (P=72%, send by Thursday):
     Bennett Jones — $1.2B ARC Resources prospectus
     Confirm practice area: Securities / Energy
     📧 Draft ready when you confirm.

  ─────────────────────────────────
  📊 This week: 7 firms in hot zone
  🎓 Next cohort: Bennett Jones hireback window opens Aug 31
  💰 WTI: $84.20 (+3.2% 3mo) — macro BULLISH — pipeline full

The engine also:
  - Deduplicates overlapping signals about the same event
  - Resolves conflicts (two signals pointing to same firm for different reasons)
  - Applies YOUR preferences (e.g., "prioritise securities over litigation")
  - Prevents outreach fatigue (don't send 5 emails to same firm in 30 days)
"""

import json, logging, os
from datetime import datetime, date, timedelta
from collections import defaultdict
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import requests

from database.db import get_conn, get_all_signals_for_dashboard
from config_calgary import FIRM_BY_ID, CALGARY_FIRMS
from scoring.aggregator import compute_firm_scores
from predictive.demand_model import DemandPredictor
from alerts.notifier import send_telegram

log = logging.getLogger(__name__)

ANTHROPIC_API_URL = "https://api.anthropic.com/v1/messages"

# ── User preferences (override via config or env) ─────────────────────────────
USER_PREFS = {
    "preferred_practice_areas": os.getenv("PREF_PRACTICE", "securities,corporate,energy").split(","),
    "preferred_firm_tiers":     os.getenv("PREF_TIERS",    "mid,boutique").split(","),
    "your_name":                os.getenv("YOUR_NAME",     "[Your Name]"),
    "your_background":          os.getenv("YOUR_BACKGROUND","recently called Calgary lawyer, securities and energy"),
    "max_daily_outreach":       int(os.getenv("MAX_DAILY_OUTREACH", "3")),
    "cooldown_days":            int(os.getenv("OUTREACH_COOLDOWN_DAYS", "21")),
}

# Signal type urgency tier
URGENCY_TIER = {
    "breaking_ccaa_filing":       0,   # IMMEDIATE
    "breaking_deal_announcement": 0,
    "asc_enforcement_emergency":  0,
    "gravity_spillage_predicted": 1,   # TODAY
    "sedar_major_deal":           1,
    "linkedin_turnover_detected": 1,
    "teampage_departure_detected":1,
    "registry_deal_structure":    1,
    "sedi_insider_cluster":       2,   # THIS WEEK
    "partner_appearance_spike":   2,
    "sec_edgar_filing":           2,
    "canlii_appearance_spike":    2,
    "aer_proceeding_upcoming":    2,
    "lsa_retention_gap":          3,   # THIS MONTH
    "macro_ma_wave_incoming":     3,
    "fiscal_pressure_incoming":   3,
    "glassdoor_overwork_spike":   3,
}

TIER_LABELS = {0: "🔴 SEND NOW", 1: "🟠 SEND TODAY", 2: "🟡 SEND THIS WEEK", 3: "🟢 PIPELINE"}


def _is_on_cooldown(firm_id: str) -> bool:
    """Check if we've already sent outreach to this firm recently."""
    conn    = get_conn()
    cutoff  = (date.today() - timedelta(days=USER_PREFS["cooldown_days"])).isoformat()
    try:
        row = conn.execute("""
            SELECT count(*) as c FROM outreach_sent
            WHERE firm_id=? AND date(scheduled_at) >= ?
        """, (firm_id, cutoff)).fetchone()
        conn.close()
        return row and row["c"] > 0
    except Exception:
        conn.close()
        return False


def _get_best_signal_per_firm() -> dict[str, dict]:
    """
    For each firm, return the single highest-priority signal.
    Priority: urgency tier → weight → recency.
    """
    all_sigs = get_all_signals_for_dashboard(days=7)   # fresh signals only for daily action
    firm_best: dict[str, dict] = {}

    for sig in all_sigs:
        fid   = sig["firm_id"]
        tier  = URGENCY_TIER.get(sig["signal_type"], 4)
        w     = sig.get("weight", 0)

        if fid not in firm_best:
            firm_best[fid] = {**sig, "_tier": tier}
        else:
            prev_tier = firm_best[fid]["_tier"]
            prev_w    = firm_best[fid].get("weight", 0)
            if tier < prev_tier or (tier == prev_tier and w > prev_w):
                firm_best[fid] = {**sig, "_tier": tier}

    return firm_best


def _apply_user_preferences(ranked: list[dict]) -> list[dict]:
    """
    Boost firms matching user preferences; demote mismatches.
    Also remove firms on cooldown.
    """
    pref_pa    = set(USER_PREFS["preferred_practice_areas"])
    pref_tiers = set(USER_PREFS["preferred_firm_tiers"])

    result = []
    for item in ranked:
        firm_id = item["firm_id"]
        if _is_on_cooldown(firm_id):
            log.debug("[Decision] Skipping %s — on cooldown", firm_id)
            continue
        firm = FIRM_BY_ID.get(firm_id, {})
        # Preference multiplier
        mult = 1.0
        if item.get("practice_area") in pref_pa:      mult *= 1.3
        if firm.get("tier") in pref_tiers:             mult *= 1.2
        item["_score"] = item.get("weight", 0) * mult * (1.0 / (item["_tier"] + 1))
        result.append(item)

    return sorted(result, key=lambda x: x.get("_score", 0), reverse=True)


def _call_claude_decision(top_actions: list[dict], predictions: list[dict]) -> str:
    """
    Ask Claude to synthesise the top actions into a crisp morning briefing.
    """
    api_key = os.getenv("ANTHROPIC_API_KEY","")
    if not api_key:
        return ""

    actions_json = json.dumps([{
        "rank": i+1,
        "firm": FIRM_BY_ID.get(a["firm_id"],{}).get("name", a["firm_id"]),
        "signal": a.get("title","")[:80],
        "signal_type": a.get("signal_type",""),
        "urgency": TIER_LABELS.get(a.get("_tier",4),""),
        "practice_area": a.get("practice_area",""),
        "description_snippet": (a.get("description","") or "")[:150],
        "p30": next((p["p30"] for p in predictions if p["firm_id"]==a["firm_id"]), None),
    } for i, a in enumerate(top_actions[:5])], indent=2)

    prompt = f"""
You are producing a daily intelligence briefing for a law job seeker targeting Calgary firms.

BACKGROUND: {USER_PREFS['your_background']}

TOP ACTIONS TODAY:
{actions_json}

Write a crisp MORNING BRIEFING in this exact format:

━━━━ YOUR MOVE — {date.today().strftime('%B %d, %Y')} ━━━━

For each action (max 5), write:
#[N] [URGENCY LABEL]
Firm: [firm name]
Why now: [1 sentence — cite the SPECIFIC signal, deal name, or data point]
P(hire in 30d): [from predictions if available]
Action: [exact next step — "Send email now", "Research then send by [day]", etc.]

Then add:
─ STRATEGIC NOTE: [1 insight about the broader market signal pattern you see]

Be brutally specific. No generic advice. Reference actual deal names, dollar values, and signal data.
"""
    try:
        resp = requests.post(
            ANTHROPIC_API_URL,
            headers={"x-api-key": api_key, "anthropic-version": "2023-06-01",
                     "content-type": "application/json"},
            json={"model": "claude-sonnet-4-6", "max_tokens": 900,
                  "system": "You are a sharp legal market intelligence analyst. Be specific. No fluff.",
                  "messages": [{"role":"user","content":prompt}]},
            timeout=25,
        )
        return resp.json()["content"][0]["text"]
    except Exception as e:
        log.error("[Decision] Claude error: %s", e); return ""


def run_daily_decision_engine(
    send_telegram_briefing: bool = True,
    send_outreach_drafts: bool = True,
) -> dict:
    """
    THE MAIN FUNCTION — runs every morning at 07:00.

    1. Collects all fresh signals
    2. Runs predictive model
    3. Applies user preferences + cooldown filter
    4. Selects top N actions
    5. Generates Claude briefing
    6. For tier-0 and tier-1 signals: auto-generates outreach drafts
    7. Sends everything to Telegram
    """
    log.info("[Decision] Running daily decision engine…")

    # ── Step 1: Best signal per firm ──────────────────────────────────────────
    best_per_firm = _get_best_signal_per_firm()
    log.info("[Decision] %d firms with fresh signals", len(best_per_firm))

    # ── Step 2: Predictive scores ─────────────────────────────────────────────
    try:
        predictor   = DemandPredictor()
        predictions = predictor.predict_all()
    except Exception as e:
        log.warning("[Decision] Predictive model error: %s", e)
        predictions = []

    # ── Step 3: Merge prediction scores into signal ranking ───────────────────
    pred_map = {p["firm_id"]: p for p in predictions}
    for fid, sig in best_per_firm.items():
        p30 = pred_map.get(fid, {}).get("p30", 0)
        sig["_p30"]   = p30
        sig["_tier"]  = sig.get("_tier", URGENCY_TIER.get(sig.get("signal_type",""),4))

    # ── Step 4: Apply preferences + sort ─────────────────────────────────────
    ranked  = list(best_per_firm.values())
    ranked  = _apply_user_preferences(ranked)

    # Limit to configured max
    top_actions = ranked[:USER_PREFS["max_daily_outreach"] * 3]

    # ── Step 5: Claude morning briefing ──────────────────────────────────────
    briefing = _call_claude_decision(top_actions, predictions)

    # ── Step 6: Send outreach drafts for hot signals ──────────────────────────
    outreach_count = 0
    if send_outreach_drafts:
        from intelligence.autonomous_outreach import generate_and_deliver_outreach
        for action in top_actions[:USER_PREFS["max_daily_outreach"]]:
            if action.get("_tier", 4) <= 1:   # tier 0 or 1 only = today's actions
                try:
                    generate_and_deliver_outreach(
                        action,
                        your_name=USER_PREFS["your_name"],
                        your_background=USER_PREFS["your_background"],
                    )
                    outreach_count += 1
                except Exception as e:
                    log.error("[Decision] Outreach gen error: %s", e)

    # ── Step 7: Send morning briefing to Telegram ────────────────────────────
    if send_telegram_briefing and briefing:
        send_telegram(f"<pre>{briefing}</pre>")
    elif send_telegram_briefing and not briefing:
        # Fallback: plain text briefing
        lines  = [f"🎯 <b>YOUR MOVE — {date.today().strftime('%B %d, %Y')}</b>\n"]
        for i, action in enumerate(top_actions[:5], 1):
            firm    = FIRM_BY_ID.get(action["firm_id"],{})
            tier_lb = TIER_LABELS.get(action.get("_tier",4),"")
            p30     = action.get("_p30",0)
            lines.append(
                f"#{i} {tier_lb}\n"
                f"  🏛 <b>{firm.get('name','?')}</b>\n"
                f"  📌 {action.get('title','')[:70]}\n"
                f"  P(30d)={p30:.0%}"
            )
        send_telegram("\n\n".join(lines))

    log.info("[Decision] Done. %d actions ranked, %d outreach drafts sent.", 
             len(top_actions), outreach_count)
    return {
        "top_actions":    [{"firm_id": a["firm_id"], "signal": a.get("title",""),
                            "tier": a.get("_tier"), "p30": a.get("_p30")}
                           for a in top_actions[:5]],
        "outreach_count": outreach_count,
        "briefing_sent":  bool(briefing),
    }


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    result = run_daily_decision_engine(
        send_telegram_briefing=True,
        send_outreach_drafts=False,   # set True in production
    )
    print(json.dumps(result, indent=2))
