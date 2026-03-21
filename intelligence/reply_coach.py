"""
intelligence/reply_coach.py
────────────────────────────
Reply Coach — What To Do When They Write Back

Most job-seekers send a cold email and have no plan for when they actually
get a reply. This module handles the ENTIRE reply chain:

1. REPLY CLASSIFICATION
   Given a reply text, classifies it as:
   - POSITIVE_INTEREST   → "Send CV", "Let's set up a call"
   - SOFT_DECLINE        → "Nothing right now but keep in touch"
   - HARD_DECLINE        → "We're not hiring"
   - REFERRAL            → "You should contact [other person]"
   - INFORMATION_REQUEST → "What are your practice areas?"
   - SILENCE_BREAKER     → No reply after 5 days

2. COACHED RESPONSE GENERATION
   For each classification, generates the optimal follow-up:
   - POSITIVE_INTEREST: Cover letter + CV submission strategy,
     what to say in the call, firm-specific research to do first
   - SOFT_DECLINE: The perfect "keep in touch" response + optimal
     re-contact timing (not 3 months, use signal-triggered re-contact)
   - REFERRAL: Extract the referral name + how to warm-enter them
   - INFORMATION_REQUEST: Specific, concise answer template

3. SIGNAL-TRIGGERED RE-CONTACT
   Instead of "follow up in 3 months", the system watches for the
   NEXT signal from that firm and fires a re-contact alert:
   "BDP gave you a soft decline 6 weeks ago. New signal: CCAA filing
   this morning. NOW is the time to re-contact — you have new specific
   context that wasn't true when they declined."

4. THE COVER LETTER ENGINE
   When you get a positive reply and need to send a CV and cover letter,
   generates a firm-specific cover letter that:
   - References the specific signal that triggered the original outreach
   - Includes 2-3 firm-specific points (recent deals, practice areas)
   - Is calibrated to the firm's known hiring culture (BigLaw vs boutique)
   - Includes the specific practice area they're expanding into

5. INTERVIEW PREPARATION BRIEF
   When you get a call scheduled, generates:
   - Key current matters at the firm (from signals)
   - Partner backgrounds (from CanLII appearance data)
   - Recent firm news and rankings
   - Likely questions and suggested answers
   - 3 specific intelligent questions to ask
"""

import json, logging, os
from datetime import datetime, date, timedelta
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import requests

from database.db import get_conn, get_all_signals_for_dashboard
from config_calgary import FIRM_BY_ID
from alerts.notifier import send_telegram
from ml.feedback_loop import record_outcome

log = logging.getLogger(__name__)

ANTHROPIC_API_URL = "https://api.anthropic.com/v1/messages"

# ── Reply classification ───────────────────────────────────────────────────────

REPLY_CLASSES = {
    "positive_interest": [
        "send your cv", "send your resume", "set up a call", "let's chat",
        "tell me more", "interested", "would like to", "come in", "schedule",
        "availability", "when are you available", "forward your",
    ],
    "soft_decline": [
        "not at this time", "no openings", "keep you in mind", "touch base later",
        "not currently", "nothing right now", "future opportunity",
        "check back", "keep in touch", "follow up in", "circling back",
    ],
    "hard_decline": [
        "not hiring", "no positions", "fully staffed", "no vacancies",
        "not looking", "restructuring", "hiring freeze",
    ],
    "referral": [
        "you should speak with", "contact our", "reach out to",
        "speak to [name]", "our recruiting", "hr contact",
    ],
    "information_request": [
        "tell me about", "what is your", "what areas", "your background",
        "practice areas", "call year", "experience in",
    ],
}


def classify_reply(reply_text: str) -> str:
    """Returns the reply classification string."""
    text_lower = reply_text.lower()
    for cls, keywords in REPLY_CLASSES.items():
        if any(kw in text_lower for kw in keywords):
            return cls
    return "unclear"


def _call_claude(system: str, prompt: str, max_tokens: int = 600) -> str:
    api_key = os.getenv("ANTHROPIC_API_KEY", "")
    if not api_key:
        return "[Set ANTHROPIC_API_KEY for AI-powered coaching]"
    try:
        resp = requests.post(
            ANTHROPIC_API_URL,
            headers={"x-api-key": api_key, "anthropic-version": "2023-06-01",
                     "content-type": "application/json"},
            json={"model": "claude-sonnet-4-6", "max_tokens": max_tokens,
                  "system": system, "messages": [{"role":"user","content":prompt}]},
            timeout=25,
        )
        return resp.json()["content"][0]["text"]
    except Exception as e:
        log.error("[ReplyCoach] Claude error: %s", e); return ""


COACH_SYSTEM = """You are a sharp, specific coach for law students and junior lawyers
navigating the Calgary legal job market. You know Canadian legal culture,
how Calgary law firms work, and how to craft perfect professional communications.
Be direct. Be specific. No generic advice. Reference actual firm names and
deal data when provided."""


class ReplyCoach:
    """Handles the entire reply chain after initial outreach."""

    def process_reply(self, firm_id: str, reply_text: str,
                      original_signal: dict | None = None,
                      your_background: str = "",
                      your_name: str = "[Your Name]") -> dict:
        """
        Main entry point. Given a reply, returns coaching + next action.
        """
        firm        = FIRM_BY_ID.get(firm_id, {})
        reply_class = classify_reply(reply_text)

        # Record outcome for learning
        outcome_map = {
            "positive_interest": "reply",
            "soft_decline":      "no_reply",
            "hard_decline":      "rejected",
            "referral":          "reply",
            "information_request":"reply",
            "unclear":           "no_reply",
        }
        record_outcome(
            firm_id=firm_id,
            outcome=outcome_map.get(reply_class, "no_reply"),
            signal_type=original_signal.get("signal_type") if original_signal else None,
        )

        # Generate coached response
        handler = {
            "positive_interest":  self._handle_positive,
            "soft_decline":       self._handle_soft_decline,
            "hard_decline":       self._handle_hard_decline,
            "referral":           self._handle_referral,
            "information_request":self._handle_info_request,
            "unclear":            self._handle_unclear,
        }.get(reply_class, self._handle_unclear)

        result = handler(firm, reply_text, original_signal, your_background, your_name)
        result["reply_class"] = reply_class

        # Send to Telegram
        self._send_coaching_telegram(result, firm)
        return result

    # ── Reply handlers ─────────────────────────────────────────────────────────

    def _handle_positive(self, firm, reply, signal, background, name) -> dict:
        sig_context = (
            f"Original trigger: {signal.get('title','')[:80]}. "
            f"Deal/case: {signal.get('description','')[:200]}."
        ) if signal else ""

        draft = _call_claude(COACH_SYSTEM, f"""
SITUATION: {firm.get('name','')} replied with positive interest to your cold email.
THEIR REPLY: "{reply[:300]}"
FIRM: {firm.get('name','')} [{firm.get('tier','?')}], focus: {', '.join(firm.get('focus',[]))}
{sig_context}
YOUR BACKGROUND: {background}
YOUR NAME: {name}

Generate:
1. IMMEDIATE REPLY (send within 2 hours): Thank them, attach CV note, confirm availability.
   Keep under 4 sentences. Professional but warm.
2. COVER LETTER OPENING PARAGRAPH: Firm-specific, references the original trigger signal.
3. THREE THINGS TO RESEARCH before the call: Specific, verifiable facts about this firm.
4. TWO INTELLIGENT QUESTIONS to ask in the call: Show you know their current matters.

Format clearly with headers.
""", max_tokens=700)

        return {
            "action":           "SEND REPLY WITHIN 2 HOURS + PREPARE FOR CALL",
            "urgency":          "🔴 URGENT",
            "coaching":         draft,
            "schedule_followup": (date.today() + timedelta(days=1)).isoformat(),
        }

    def _handle_soft_decline(self, firm, reply, signal, background, name) -> dict:
        # Set up signal-triggered re-contact
        self._schedule_signal_triggered_recontact(firm["id"] if firm else "", signal)

        draft = _call_claude(COACH_SYSTEM, f"""
SITUATION: {firm.get('name','')} gave a soft decline ("not right now").
THEIR REPLY: "{reply[:200]}"
YOUR NAME: {name}

Write:
1. REPLY (3 sentences max): Keep the door open. Make them remember you positively.
   Do NOT say "I look forward to future opportunities" or any cliché.
   Say something specific that will make them remember this exchange.
2. RE-CONTACT NOTE: What you'll say when you reach back out (don't do this now — 
   wait for the next signal from this firm).
""", max_tokens=400)

        return {
            "action":           "SEND CLOSING REPLY — wait for next signal",
            "urgency":          "🟡",
            "coaching":         draft,
            "recontact_trigger":"next_signal_from_firm",
        }

    def _handle_hard_decline(self, firm, reply, *args) -> dict:
        return {
            "action":           "LOG + MOVE ON — check back in 6 months or on next signal",
            "urgency":          "🟢",
            "coaching":         (
                f"Hard decline from {firm.get('name','')}. "
                f"Do not reply. Set a 6-month calendar reminder, OR watch for a major "
                f"signal trigger (CCAA filing, major deal, mass departure) that "
                f"changes their situation entirely. When that happens, you have new context."
            ),
            "schedule_followup": (date.today() + timedelta(days=180)).isoformat(),
        }

    def _handle_referral(self, firm, reply, signal, background, name) -> dict:
        # Try to extract referral name
        import re
        name_match = re.search(
            r"(?:speak with|contact|reach out to)\s+([A-Z][a-z]+ [A-Z][a-z]+)",
            reply
        )
        referral_name = name_match.group(1) if name_match else "the contact mentioned"

        draft = _call_claude(COACH_SYSTEM, f"""
YOU WERE REFERRED by someone at {firm.get('name','')} to {referral_name}.
ORIGINAL REPLY: "{reply[:200]}"
YOUR NAME: {name}

Write a WARM INTRODUCTION EMAIL to {referral_name}:
- Reference who referred you (by name)
- One sentence on why you're reaching out NOW (signal context if available)
- Keep to 3 sentences max body

Subject line included.
""", max_tokens=300)

        return {
            "action":           f"CONTACT {referral_name} — you have a warm intro",
            "urgency":          "🟠 SEND TODAY",
            "coaching":         draft,
            "referral_name":    referral_name,
        }

    def _handle_info_request(self, firm, reply, signal, background, name) -> dict:
        draft = _call_claude(COACH_SYSTEM, f"""
{firm.get('name','')} is asking for more information: "{reply[:200]}"
YOUR BACKGROUND: {background}
YOUR NAME: {name}
FIRM FOCUS: {', '.join(firm.get('focus',[]))}

Write a SHORT, SPECIFIC response (3-4 sentences) that:
1. Directly answers their question
2. Pivots to their specific practice needs (from the firm's focus areas)  
3. Confirms next step

Do not be generic.
""", max_tokens=300)

        return {
            "action":           "REPLY WITHIN 2 HOURS — they're engaged",
            "urgency":          "🟠 URGENT",
            "coaching":         draft,
        }

    def _handle_unclear(self, firm, reply, *args) -> dict:
        return {
            "action":           "REVIEW MANUALLY — unclear reply type",
            "urgency":          "🟡",
            "coaching":         f"Reply from {firm.get('name','')}: '{reply[:100]}'. "
                                f"Classify manually and use the appropriate strategy.",
        }

    # ── Signal-triggered re-contact ────────────────────────────────────────────

    def _schedule_signal_triggered_recontact(self, firm_id: str, original_signal: dict | None):
        """
        Store a note to re-contact this firm when the NEXT relevant signal fires.
        The decision engine will pick this up and inject it into the morning brief.
        """
        if not firm_id:
            return
        conn = get_conn()
        conn.execute("""
            CREATE TABLE IF NOT EXISTS recontact_queue (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                firm_id         TEXT NOT NULL,
                reason          TEXT,
                original_signal TEXT,
                queued_at       TEXT DEFAULT (date('now')),
                triggered_at    TEXT,
                is_active       INTEGER DEFAULT 1
            )""")
        conn.execute("""
            INSERT INTO recontact_queue (firm_id, reason, original_signal)
            VALUES (?,?,?)
        """, (firm_id, "soft_decline — waiting for next signal",
              json.dumps(original_signal or {})))
        conn.commit()
        conn.close()

    # ── Interview prep brief ───────────────────────────────────────────────────

    def generate_interview_brief(self, firm_id: str, your_background: str = "") -> str:
        """
        Full interview preparation brief for a scheduled call/meeting.
        """
        firm       = FIRM_BY_ID.get(firm_id, {})
        signals    = get_all_signals_for_dashboard(days=30)
        firm_sigs  = [s for s in signals if s["firm_id"] == firm_id][:8]

        # Get partner names from CanLII data
        conn       = get_conn()
        partners   = conn.execute("""
            SELECT DISTINCT partner_name FROM partner_appearances
            WHERE firm_id=? AND date(appearance_date) >= date('now','-90 days')
            ORDER BY rowid DESC LIMIT 6
        """, (firm_id,)).fetchall()
        conn.close()
        partner_names = [r["partner_name"] for r in partners]

        context = {
            "firm":         firm.get("name", firm_id),
            "tier":         firm.get("tier", "?"),
            "focus":        firm.get("focus", []),
            "recent_signals": [{"type": s["signal_type"], "title": s["title"][:80]}
                               for s in firm_sigs],
            "partners_seen_in_court": partner_names,
        }

        return _call_claude(COACH_SYSTEM, f"""
INTERVIEW PREP BRIEF for {firm.get('name','')}

CONTEXT:
{json.dumps(context, indent=2)}

YOUR BACKGROUND: {your_background}

Generate a full interview prep brief:

## CURRENT FIRM SITUATION (2-3 sentences based on signals)

## KEY MATTERS TO KNOW (from signals — be specific about deals/cases)

## PARTNERS TO RESEARCH (from court appearance data)

## 3 INTELLIGENT QUESTIONS TO ASK
(Show you've done research. Reference specific deals or matters.)

## WATCH-OUTS
(What NOT to say. What sensitive topics to avoid.)

## YOUR PITCH (tailored to this firm's focus areas)
(30-second answer to "Tell me about yourself" — firm-specific)
""", max_tokens=900)

    # ── Telegram ───────────────────────────────────────────────────────────────

    def _send_coaching_telegram(self, result: dict, firm: dict):
        msg = (
            f"🎯 <b>REPLY COACH — {firm.get('name','?').upper()}</b>\n\n"
            f"Classification: <b>{result.get('reply_class','?').upper().replace('_',' ')}</b>\n"
            f"Action: <b>{result.get('action','')}</b>\n"
            f"{result.get('urgency','')}\n\n"
            f"━━━━ COACHING ━━━━\n\n"
            f"{result.get('coaching','')[:800]}"
        )
        send_telegram(msg)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    coach = ReplyCoach()
    # Demo
    result = coach.process_reply(
        firm_id="burnet",
        reply_text="Thanks for reaching out. Send your CV and I'll take a look.",
        your_background="recently called, energy and corporate",
        your_name="Jane Smith",
    )
    print(json.dumps(result, indent=2, default=str))
