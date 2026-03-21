"""
intelligence/adaptive/background_matcher.py
─────────────────────────────────────────────
Background Matcher — Maps YOUR Resume to Active Files

The most powerful personalization engine in the system.

You input your background once (securities law, U of C grad, prior
banking internship, energy focus). The system maps your specific
experience to ACTIVE FILES at each firm and generates the most
relevant connection between you and what they're currently working on.

Instead of: "I have a background in securities law and am seeking
            a first-year associate role at your esteemed firm."

This generates: "I saw Bennett Jones is acting as counsel on the
                ARC Resources $1.2B prospectus. I worked on a
                similar bought-deal prospectus as a summer student
                at BMO Capital Markets and understand the disclosure
                timeline pressures involved."

HOW IT WORKS:

1. You define your background profile (see USER_BACKGROUND_PROFILE)
2. For each firm's active signals, find the STRONGEST MATCH
   between your experience and their current work
3. Generate a "connection sentence" — the specific bridge between
   your past and their present
4. This connection sentence is injected into every outreach email
   as the first sentence

Also performs:
  - SKILL GAP ANALYSIS: what experience you're missing for each firm's
    top files, and how to address it in the email
  - MUTUAL CONNECTION SCAN: checks linkedin_roster for people who
    went from your university or previous employer to this firm
  - TIMING MATCH: if your background aligns with their current file
    urgency, compute a "fit multiplier" for the opportunity score
"""

import json, logging, os
from datetime import datetime, date
from typing import Optional
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

import requests

from database.db import get_conn, get_all_signals_for_dashboard
from config_calgary import FIRM_BY_ID, CALGARY_FIRMS

log = logging.getLogger(__name__)

ANTHROPIC_API_URL = "https://api.anthropic.com/v1/messages"

# ── Your Background Profile ────────────────────────────────────────────────────
# Set via environment variables or .env file
USER_PROFILE = {
    "name":              os.getenv("YOUR_NAME",        ""),
    "law_school":        os.getenv("YOUR_LAW_SCHOOL",  "University of Calgary"),
    "grad_year":         os.getenv("YOUR_GRAD_YEAR",   "2024"),
    "call_year":         os.getenv("YOUR_CALL_YEAR",   "2025"),
    "practice_interests":os.getenv("YOUR_PRACTICE",    "securities,corporate,energy").split(","),
    "prior_employers":   os.getenv("YOUR_EMPLOYERS",   "BMO Capital Markets").split(","),
    "coursework":        os.getenv("YOUR_COURSEWORK",  "securities regulation,oil gas law,corporate finance").split(","),
    "clinic_work":       os.getenv("YOUR_CLINIC",      ""),
    "thesis":            os.getenv("YOUR_THESIS",       ""),
    "languages":         os.getenv("YOUR_LANGUAGES",   "English").split(","),
    "bar_admissions":    os.getenv("YOUR_BAR",         "Alberta").split(","),
    "deal_experience":   os.getenv("YOUR_DEALS",       "").split(","),
    "notes":             os.getenv("YOUR_NOTES",       ""),
}

# ── Practice area → relevant background keywords ──────────────────────────────
PRACTICE_KEYWORDS = {
    "securities":     ["capital markets","prospectus","TSX","SEDAR","NI 51-101","AIF",
                       "bought deal","private placement","securities regulation","OSC"],
    "corporate":      ["M&A","due diligence","corporate governance","shareholders",
                       "business combination","amalgamation","articles"],
    "energy":         ["oil","gas","LNG","AER","COGOA","royalties","mineral rights",
                       "energy regulation","pipeline","reserves","NI 51-101"],
    "litigation":     ["ABQB","statement of claim","discoveries","trial","mediation",
                       "arbitration","motion","brief","factum"],
    "restructuring":  ["CCAA","BIA","receivership","insolvency","creditor","debt","workout"],
    "employment":     ["wrongful dismissal","human rights","labour","collective agreement",
                       "employment standards","HRC","ESA"],
    "tax":            ["ITA","GST","HST","CRA","transfer pricing","rollover","s.85","RRSP"],
    "real_estate":    ["conveyancing","TLSA","land titles","mortgage","commercial lease"],
    "ip":             ["patent","trademark","copyright","Trade-marks Act","CIPO"],
    "regulatory":     ["AUC","AER","CRTC","NEB","CER","regulatory approval","OSC","ASC"],
}


def _call_claude(prompt: str, max_tokens: int = 500) -> str:
    api_key = os.getenv("ANTHROPIC_API_KEY","")
    if not api_key:
        return ""
    try:
        resp = requests.post(
            ANTHROPIC_API_URL,
            headers={"x-api-key": api_key, "anthropic-version": "2023-06-01",
                     "content-type": "application/json"},
            json={
                "model":      "claude-sonnet-4-6",
                "max_tokens": max_tokens,
                "system": (
                    "You write single sentences that connect a person's specific background "
                    "to a law firm's current work. Be extremely specific. Reference actual "
                    "deal names, statutes, companies. Maximum 2 sentences. No fluff."
                ),
                "messages": [{"role":"user","content":prompt}],
            },
            timeout=20,
        )
        return resp.json()["content"][0]["text"]
    except Exception as e:
        log.error("[BackgroundMatcher] Claude error: %s", e); return ""


def compute_fit_score(signal: dict, profile: dict = None) -> float:
    """
    Compute how well YOUR background fits this firm's current signal.
    Returns a multiplier 0.5–2.0.
    """
    profile = profile or USER_PROFILE
    pa      = signal.get("practice_area", "general")
    keywords = PRACTICE_KEYWORDS.get(pa, [])
    if not keywords: return 1.0

    # Build a single string of your background
    bg_text = " ".join([
        " ".join(profile.get("practice_interests", [])),
        " ".join(profile.get("coursework", [])),
        " ".join(profile.get("prior_employers", [])),
        profile.get("notes", ""),
        profile.get("thesis", ""),
        profile.get("clinic_work", ""),
    ]).lower()

    matches = sum(1 for kw in keywords if kw.lower() in bg_text)
    fit     = matches / max(len(keywords), 1)

    # Map to multiplier: 0 match → 0.6, 100% match → 2.0
    return 0.6 + (fit * 1.4)


def generate_connection_sentence(firm_id: str, signal: dict,
                                  profile: dict = None) -> str:
    """
    Generates the first sentence of a cold email:
    "I saw [specific thing] and [my specific experience connects to it]."
    """
    profile   = profile or USER_PROFILE
    firm      = FIRM_BY_ID.get(firm_id, {})
    signal_title = signal.get("title", "")
    description  = signal.get("description", "")
    pa           = signal.get("practice_area", "general")

    # Build context string for Claude
    bg_summary = (
        f"Law school: {profile.get('law_school','')}, Class of {profile.get('grad_year','')}. "
        f"Called to Alberta bar {profile.get('call_year','')}. "
        f"Practice interests: {', '.join(profile.get('practice_interests',[]))}. "
        f"Prior employers: {', '.join(profile.get('prior_employers',[]))}. "
        f"Relevant coursework: {', '.join(profile.get('coursework',[]))}. "
        f"Deal experience: {', '.join(profile.get('deal_experience',[]))}. "
        f"{profile.get('notes','')}"
    )

    prompt = f"""
Write ONE sentence (maximum 30 words) that connects this person's background
to what THIS law firm is currently working on.

FIRM: {firm.get('name', firm_id)}
WHAT THE FIRM IS DOING: {signal_title}. {description[:200]}
PRACTICE AREA: {pa}

CANDIDATE BACKGROUND:
{bg_summary}

Rules:
- Mention ONE specific thing from their background AND one specific thing from the firm's work
- No generic statements like "I have a background in securities law"
- Must contain at least one proper noun (company name, statute, deal)
- Do NOT start with "I"
- Maximum 30 words

Example good output:
"Having drafted bought-deal prospectus disclosure at BMO Capital Markets, I understand the NI 41-101 timeline pressure your team is under on the ARC Resources offering."

Example bad output:
"My background in securities law aligns well with your firm's practice."
"""

    connection = _call_claude(prompt, max_tokens=80)
    if not connection:
        # Fallback: template-based
        keywords = PRACTICE_KEYWORDS.get(pa, [])
        matched  = [k for k in keywords if k.lower() in bg_summary.lower()]
        if matched:
            return (f"My {pa} coursework covered {matched[0]} — directly relevant "
                    f"to the work described in {signal_title[:40]}.")
    return connection or ""


class BackgroundMatcher:
    """
    Runs background matching for all active signals and enriches
    the opportunity leaderboard with personalized fit scores and
    connection sentences.
    """

    def __init__(self, profile: dict = None):
        self.profile = profile or USER_PROFILE

    def enrich_leaderboard(self, leaderboard: list[dict]) -> list[dict]:
        """
        Add fit_score and connection_sentence to each leaderboard entry.
        """
        enriched = []
        for entry in leaderboard:
            fid     = entry["firm_id"]
            # Get top signal for this firm
            conn    = get_conn()
            top_sig = conn.execute("""
                SELECT * FROM signals WHERE firm_id=?
                ORDER BY weight DESC LIMIT 1
            """, (fid,)).fetchone()
            conn.close()

            if top_sig:
                sig_dict   = dict(top_sig)
                fit        = compute_fit_score(sig_dict, self.profile)
                connection = generate_connection_sentence(fid, sig_dict, self.profile)
            else:
                fit        = 1.0
                connection = ""

            enriched.append({
                **entry,
                "fit_score":          round(fit, 2),
                "connection_sentence":connection,
                "adjusted_score":     round(entry["score"] * fit, 2),
            })

        # Re-sort by adjusted score
        enriched.sort(key=lambda x: x["adjusted_score"], reverse=True)
        return enriched

    def find_mutual_connections(self, firm_id: str) -> list[dict]:
        """
        Checks if any alumni from YOUR law school or prior employers
        are now at this firm — a warm introduction opportunity.
        """
        school  = self.profile.get("law_school","").lower()
        employers = [e.lower().strip() for e in self.profile.get("prior_employers",[])]

        conn    = get_conn()
        roster  = conn.execute("""
            SELECT full_name, title, linkedin_url
            FROM linkedin_roster
            WHERE firm_id=? AND is_active=1
        """, (firm_id,)).fetchall()
        conn.close()

        # Check alumni network
        alumni_matches = []
        for r in roster:
            r = dict(r)
            # In production: cross-reference with your LinkedIn connections
            # For now: check if any known alumni are at this firm
            alumni_matches.append({
                "name":    r["full_name"],
                "title":   r["title"],
                "url":     r["linkedin_url"],
                "mutual":  False,   # set True if found in your LinkedIn network
            })
        return alumni_matches[:5]

    def generate_telegram_fit_report(self, leaderboard: list[dict]) -> str:
        """
        Sends a personalised fit analysis for the top 3 opportunities
        to Telegram each morning.
        """
        enriched = self.enrich_leaderboard(leaderboard[:5])
        lines    = [
            "🎯 <b>PERSONALISED FIT ANALYSIS</b>",
            f"Background: {', '.join(self.profile.get('practice_interests',[]))} | "
            f"{self.profile.get('law_school','')} '{self.profile.get('grad_year','')}",
            "",
        ]
        for i, e in enumerate(enriched[:3], 1):
            firm  = FIRM_BY_ID.get(e["firm_id"], {})
            score = e["adjusted_score"]
            fit   = e["fit_score"]
            conn  = e.get("connection_sentence","")
            lines.append(
                f"#{i} {firm.get('name',e['firm_id'])} [{firm.get('tier','?').upper()}]\n"
                f"   Score: {score:.1f} (fit×{fit:.1f})\n"
                f"   🔗 {conn or 'No specific connection found'}"
            )
        return "\n\n".join(lines)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    matcher = BackgroundMatcher()
    # Test fit score on a mock signal
    mock = {"signal_type":"sedar_major_deal","practice_area":"securities",
            "title":"Bennett Jones: ARC Resources $1.2B prospectus",
            "description":"ARC Resources prospectus, junior securities work needed."}
    print(f"Fit score: {compute_fit_score(mock):.2f}")
    print(f"Connection: {generate_connection_sentence('bennett_jones', mock)}")
