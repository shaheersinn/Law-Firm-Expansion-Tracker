"""
outreach/generator.py
──────────────────────
Personalized outreach email generator.

Takes a signal dict and generates a bespoke, intelligence-driven cold email
that references the specific trigger — deal, departure, hireback gap, etc.

Uses Claude (via Anthropic API) or a template engine depending on config.
"""

import logging
from datetime import date

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from config_calgary import FIRM_BY_ID, OPENAI_API_KEY, CALGARY_FIRMS
from database.db import get_conn, insert_signal

log = logging.getLogger(__name__)

# ─── Email templates per signal type ────────────────────────────────────────

TEMPLATES = {

    "canlii_appearance_spike": """\
Subject: First-Year Associate — Available Immediately — [{firm_name}] Litigation Volume

Dear Hiring Partner,

I noticed through the Alberta Court of King's Bench docket that {firm_name} has seen a \
significant increase in court appearances over the past 30 days — {appearances} new matters, \
roughly {zscore:.1f} standard deviations above your recent average.

I'm a [{call_year}] call with a litigation background in commercial disputes and document-intensive \
matters. I am available to start immediately and work well under high-volume file pressure.

I'd welcome a brief call at your convenience.

[Your Name]
[Phone] | [Email] | [LinkedIn]
""",

    "sedar_major_deal": """\
Subject: First-Year Associate — Securities / M&A Background — Re: {issuer} Filing

Dear Hiring Partner,

I noticed that {firm_name} was named as counsel on the recent {doc_type} for {issuer} \
on SEDAR+{deal_str}. Transactions of this scale typically require substantial junior \
due diligence and document review support.

My background is in Canadian securities regulation and M&A, and I am available \
to start on short notice. I would be glad to assist on this mandate or similar work.

Please let me know if you have a moment to speak.

[Your Name]
[Phone] | [Email] | [LinkedIn]
""",

    "sedar_counsel_named": """\
Subject: Associate Availability — Re: {issuer} ({doc_type})

Dear Hiring Partner,

I noticed {firm_name} is acting as counsel on the {issuer} {doc_type} recently filed \
on SEDAR+. I have a background in securities regulation and corporate transactions \
and am actively seeking a first-year associate role in Calgary.

I would welcome the chance to discuss whether I might be a fit for your team.

Best regards,
[Your Name]
""",

    "linkedin_turnover_detected": """\
Subject: First-Year Associate — Immediate Availability — Re: Recent Associate Departure

Dear Hiring Partner,

I understand that there may have been a recent change on your junior associate team. \
I'm a recently called lawyer with [{practice_area}] experience, available to start \
immediately, and familiar with the pace of a busy Calgary practice.

I'd be glad to send a CV if that would be helpful.

[Your Name]
[Phone] | [Email]
""",

    "lsa_retention_gap": """\
Subject: First-Year Associate — [{firm_name}] — Immediate Availability

Dear Hiring Partner,

I hope this message finds you well. I'm writing because I understand that following \
the completion of the {year} articling term, {firm_name} may have associate capacity \
available. I am a recently called lawyer in Alberta with a background in \
[{practice_area}] and I am seeking a first-year position in Calgary.

I would be grateful for the opportunity to introduce myself.

[Your Name]
""",

    "biglaw_spillage_predicted": """\
Subject: First-Year Associate — Available for Overflow Mandate Support

Dear Hiring Partner,

I see that {headline_summary} was recently announced. Given {firm_name}'s track record \
of acting on matters of this nature — often opposite major-firm counsel — I imagine \
you may be seeing an uptick in demand.

I'm a [{call_year}] call with [{practice_area}] experience and am available immediately. \
If it would be useful to have an extra set of hands on the file, I'd welcome a conversation.

[Your Name]
""",

    "canlii_new_large_file": """\
Subject: Associate Availability — Commercial Litigation Support

Dear Hiring Partner,

I noticed {firm_name} recently appeared on {case_citation}, a matter that appears to \
involve significant document-review and drafting work. I am a recently called lawyer \
with commercial litigation experience and am available on short notice.

Happy to provide a CV if helpful.

[Your Name]
""",
}


def _get_firm(firm_id: str) -> dict:
    return FIRM_BY_ID.get(firm_id, {"name": firm_id, "focus": [], "tier": "?"})


def generate_outreach(signal: dict, your_call_year: str = "2024",
                      your_practice: str = "corporate and securities") -> dict:
    """
    Given a signal dict, returns:
    {
      "to_firm":    firm name,
      "subject":    email subject line,
      "body":       full email body (plain text),
      "strategy":   one-line strategic rationale,
      "urgency":    "same-day" | "this-week" | "within-3-days",
    }
    """
    firm_id  = signal.get("firm_id", "")
    sig_type = signal.get("signal_type", "")
    raw      = signal.get("raw_data", {}) or {}
    firm     = _get_firm(firm_id)
    firm_name = firm.get("name", firm_id)
    focus    = ", ".join(firm.get("focus", ["law"]))

    template = TEMPLATES.get(sig_type, TEMPLATES["sedar_counsel_named"])

    # Build template vars
    vars_map = {
        "firm_name":       firm_name,
        "call_year":       your_call_year,
        "practice_area":   your_practice or focus,
        "year":            str(raw.get("articling_year", date.today().year - 1)),

        # canlii_appearance_spike
        "appearances":     str(raw.get("recent_30", "?")),
        "zscore":          raw.get("zscore", 0.0),

        # sedar signals
        "issuer":          raw.get("issuer", "the issuer"),
        "doc_type":        raw.get("doc_type", "filing"),
        "deal_str":        (f" (deal value: ~${raw['deal_value_m']:.0f}M)"
                            if raw.get("deal_value_m") else ""),

        # linkedin turnover
        "departed_name":   raw.get("departed_name", "a junior associate"),
        "new_employer":    raw.get("new_employer", "another organization"),

        # spillage graph
        "headline_summary": raw.get("headline", "a major transaction")[:80],

        # canlii large file
        "case_citation":   signal.get("title", "a recent matter")[:60],
    }

    try:
        body = template.format(**vars_map)
    except KeyError as e:
        log.warning("[Outreach] Template key error: %s — using fallback", e)
        body = template

    # Extract subject line from body
    lines   = body.strip().splitlines()
    subject = ""
    body_lines = []
    for line in lines:
        if line.startswith("Subject:"):
            subject = line.replace("Subject:", "").strip()
        else:
            body_lines.append(line)
    body = "\n".join(body_lines).strip()

    # Urgency
    urgency_map = {
        "biglaw_spillage_predicted":  "same-day",
        "sedar_major_deal":           "same-day",
        "linkedin_turnover_detected": "same-day",
        "lsa_retention_gap":          "within-3-days",
        "canlii_appearance_spike":    "this-week",
        "sedar_counsel_named":        "this-week",
        "canlii_new_large_file":      "this-week",
        "lsa_student_not_retained":   "within-3-days",
    }

    strategy_map = {
        "biglaw_spillage_predicted":  f"Mega-deal announced → {firm_name} historically catches overflow. Email TODAY.",
        "sedar_major_deal":           f"{firm_name} named on $M+ SEDAR+ deal. High doc-review hours ahead. Email TODAY.",
        "linkedin_turnover_detected": f"Junior departed {firm_name}. Unadvertised vacancy. Email before word gets out.",
        "lsa_retention_gap":          f"{firm_name} has unfilled post-articling slots = budgeted headcount.",
        "canlii_appearance_spike":    f"{firm_name} litigation volume spiked. They need junior support.",
        "canlii_new_large_file":      f"{firm_name} on large commercial file. Heavy doc-review burden.",
        "sedar_counsel_named":        f"{firm_name} acting on securities transaction. Junior demand elevated.",
    }

    result = {
        "to_firm":  firm_name,
        "subject":  subject,
        "body":     body,
        "strategy": strategy_map.get(sig_type, "Signal-based targeted outreach."),
        "urgency":  urgency_map.get(sig_type, "this-week"),
        "signal_id": signal.get("id"),
        "firm_id":  firm_id,
        "weight":   signal.get("weight", 0),
    }

    # Save draft to DB
    conn = get_conn()
    conn.execute("""
        INSERT INTO outreach_log (firm_id, trigger_type, subject, body, status)
        VALUES (?, ?, ?, ?, 'draft')
    """, (firm_id, sig_type, subject, body))
    conn.commit()
    conn.close()

    return result


def generate_weekly_outreach_plan(top_n: int = 10) -> list[dict]:
    """
    Pulls the top unalerted signals and generates one outreach per firm
    (highest-weight signal wins if a firm has multiple).
    Returns a prioritised list ready to send.
    """
    conn = get_conn()
    rows = conn.execute("""
        SELECT s.*
        FROM signals s
        WHERE s.alerted = 0
        GROUP BY s.firm_id
        HAVING MAX(s.weight)
        ORDER BY s.weight DESC
        LIMIT ?
    """, (top_n,)).fetchall()
    conn.close()

    plan = []
    for row in rows:
        sig   = dict(row)
        email = generate_outreach(sig)
        plan.append(email)

    return plan


def print_outreach_plan(plan: list[dict]):
    print("\n" + "═" * 70)
    print("📧  WEEKLY OUTREACH PLAN")
    print("═" * 70)
    for i, item in enumerate(plan, 1):
        print(f"\n{'─'*60}")
        print(f"#{i}  ⚡ URGENCY: {item['urgency'].upper()}")
        print(f"     Firm:     {item['to_firm']}")
        print(f"     Weight:   {item['weight']:.1f}")
        print(f"     Strategy: {item['strategy']}")
        print(f"\n     Subject: {item['subject']}")
        print(f"\n{item['body']}")
    print("\n" + "═" * 70)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    plan = generate_weekly_outreach_plan(top_n=5)
    print_outreach_plan(plan)
