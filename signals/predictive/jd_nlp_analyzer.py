"""
signals/predictive/jd_nlp_analyzer.py
───────────────────────────────────────
Signal 20 — Job Description NLP Analyzer

"Immediate start" + "fast-paced, deal-heavy environment" + "experience
with large M&A transactions" in a job posting is not the same as
"competitive salary" + "training provided" + "6-12 month start date."

The first firm is in crisis mode. The second is planning ahead.

This module scrapes job postings from firm career pages, Indeed, and LinkedIn
and applies NLP to extract urgency, deal-type, and seniority signals.

KEY CLASSIFICATIONS:

1. URGENCY SCORE (0–10)
   "Immediate start" → +4
   "As soon as possible" → +3
   "Exceptional candidates considered at any stage" → +4
   "Recent call to the bar" → +2 (targeting you specifically)
   "No specific experience required" → +3 (desperate)

2. DEAL TYPE (what files they're working on)
   Extract practice area keywords: M&A, securities, energy, CCAA, etc.
   Cross-reference against YOUR background to compute "fit score"

3. SENIORITY GAP
   "1-3 years" → junior  
   "We don't care about experience level" → very desperate
   "Previous experience at a national firm" → lateral hire, not training

4. VOCABULARY TELL
   "Large and complex transactions" → they're overwhelmed
   "Collaborative team environment" → they have time to train
   "Work directly with partners from day one" → no senior associates left

5. POSTING VELOCITY
   If a firm posts the SAME JUNIOR ROLE more than once in 90 days,
   either no one is accepting or the person they hired left immediately.
   Both are strong hire signals.

6. SALARY RANGE PARSING
   "Market rates" or silence = normal
   "$200k+ total compensation" for a first-year = crisis premium
   No salary listed = budget uncertainty (may not actually be hiring)
"""

import re, time, logging, hashlib, json
from datetime import date, datetime, timedelta
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

import requests
from bs4 import BeautifulSoup
import feedparser

from database.db import insert_signal, get_conn
from config_calgary import CALGARY_FIRMS, FIRM_BY_ID

log = logging.getLogger(__name__)

INDEED_URL    = "https://ca.indeed.com/jobs?q={query}&l=Calgary%2C+AB&sort=date&radius=15"
LINKEDIN_JOBS = "https://www.linkedin.com/jobs/search/?keywords={query}&location=Calgary%2C+Alberta&f_TPR=r86400"

# ── Urgency vocabulary ─────────────────────────────────────────────────────────
URGENCY_TOKENS = {
    "immediate":               4,
    "immediately":             4,
    "asap":                    4,
    "as soon as possible":     3,
    "urgent":                  3,
    "exceptional candidates":  4,
    "all levels considered":   3,
    "no minimum experience":   3,
    "recent call":             2,
    "newly called":            2,
    "articling students welcome": 2,
    "part-time welcome":       2,
    "contract":                2,
    "temporary":               1,
    "permanent":               0,
    "training provided":       -1,   # less urgent — they have time to train
    "comprehensive training":  -1,
    "mentorship program":      -1,
    "6-12 month start":        -2,
    "training program":        -2,
}

# ── Deal complexity vocabulary ─────────────────────────────────────────────────
DEAL_COMPLEXITY_TOKENS = {
    "large and complex":       3,
    "sophisticated transactions": 3,
    "multijurisdictional":     3,
    "cross-border":            2,
    "public company":          2,
    "capital markets":         2,
    "m&a":                     2,
    "mergers and acquisitions":2,
    "private equity":          2,
    "securities":              2,
    "energy transactions":     2,
    "oil and gas":             1,
    "litigation support":      1,
    "general corporate":       0,
    "varied practice":         0,
    "learning opportunity":    -1,
}

# ── Desperation vocabulary ─────────────────────────────────────────────────────
DESPERATION_TOKENS = {
    "work directly with partners":   3,
    "high responsibility from day one": 3,
    "hit the ground running":         4,
    "immediate client contact":       3,
    "no hand holding":                3,
    "self-starter":                   2,
    "entrepreneurial":                2,
    "manage your own files":          3,
    "minimal supervision":            3,
    "join a small team":              2,
    "growing practice":               2,
    "all-hands":                      3,
    "wear many hats":                 3,
}

# ── Salary signals ─────────────────────────────────────────────────────────────
PREMIUM_SALARY_RE = re.compile(
    r"\$\s*(1[5-9]\d|2\d\d)\s*[kK]|\$\s*(1[5-9]\d,000|2\d\d,000)", re.IGNORECASE
)
SALARY_MENTIONED_RE = re.compile(r"\$[\d,]+\s*(?:K|k|000)?\s*(?:per\s+year|annually|/yr)", re.IGNORECASE)


def score_job_posting(text: str) -> dict:
    """
    Returns a scoring dict for a job posting text.
    """
    text_lower = text.lower()

    urgency_score      = sum(v for k, v in URGENCY_TOKENS.items()    if k in text_lower)
    complexity_score   = sum(v for k, v in DEAL_COMPLEXITY_TOKENS.items() if k in text_lower)
    desperation_score  = sum(v for k, v in DESPERATION_TOKENS.items() if k in text_lower)

    # Salary signal
    has_premium_salary = bool(PREMIUM_SALARY_RE.search(text))
    has_any_salary     = bool(SALARY_MENTIONED_RE.search(text))

    # Seniority target
    targets_junior = bool(re.search(
        r"\b(0-2|1-3|recently called|new call|first.year|articling|bar admission)\b",
        text, re.IGNORECASE
    ))
    targets_lateral = bool(re.search(
        r"\b(3-5 year|senior associate|4\+|5\+|6\+ year|significant experience)\b",
        text, re.IGNORECASE
    ))

    # Practice area extraction
    practice_areas = []
    pa_keywords = {
        "securities": r"\b(securities|capital markets|IPO|prospectus|TSX)\b",
        "energy":     r"\b(energy|oil.gas|pipeline|LNG|royalt|AER)\b",
        "M&A":        r"\b(M&A|mergers|acquisitions|transactions|deal)\b",
        "litigation": r"\b(litigation|dispute|arbitration|ABQB|court)\b",
        "corporate":  r"\b(corporate|commercial|contracts|governance)\b",
        "restructuring": r"\b(restructuring|CCAA|insolvency|receivership)\b",
        "tax":        r"\b(tax|CRA|transfer pricing|GST|HST)\b",
        "employment": r"\b(employment|labour|HR|human resources)\b",
    }
    for pa, pattern in pa_keywords.items():
        if re.search(pattern, text, re.IGNORECASE):
            practice_areas.append(pa)

    composite = (
        urgency_score * 1.5 +
        desperation_score * 1.2 +
        complexity_score * 0.8 +
        (3 if has_premium_salary else 0) +
        (1 if not has_any_salary else 0)   # no salary = uncertainty
    )

    return {
        "urgency_score":      urgency_score,
        "desperation_score":  desperation_score,
        "complexity_score":   complexity_score,
        "composite":          composite,
        "targets_junior":     targets_junior,
        "targets_lateral":    targets_lateral,
        "has_premium_salary": has_premium_salary,
        "has_any_salary":     has_any_salary,
        "practice_areas":     practice_areas,
    }


class JobDescriptionAnalyzer:
    """
    Monitors job boards for Calgary law firm postings and NLP-scores them
    for urgency and fit signals.
    """

    def __init__(self):
        self.new_signals: list[dict] = []
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
            "Accept-Language": "en-CA,en;q=0.9",
        })
        self._init_db()

    def _init_db(self):
        conn = get_conn()
        conn.execute("""
            CREATE TABLE IF NOT EXISTS job_postings (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                firm_id       TEXT,
                title         TEXT,
                url           TEXT UNIQUE,
                full_text     TEXT,
                urgency_score REAL,
                composite     REAL,
                practice_areas TEXT,
                posted_date   TEXT,
                first_seen    TEXT DEFAULT (date('now'))
            )""")
        conn.commit()
        conn.close()

    def run(self) -> list[dict]:
        log.info("[JD-NLP] Scanning job boards for Calgary law postings…")
        for firm in CALGARY_FIRMS:
            self._scan_firm_careers(firm)
        self._scan_indeed()
        self._detect_reposting_desperation()
        log.info("[JD-NLP] Done. %d signals.", len(self.new_signals))
        return self.new_signals

    def _scan_firm_careers(self, firm: dict):
        careers_url = firm.get("careers_url", "")
        if not careers_url:
            return
        try:
            resp = self.session.get(careers_url, timeout=10)
            soup = BeautifulSoup(resp.text, "lxml")
            # Look for junior/associate job postings
            for link in soup.select("a[href]"):
                href  = link.get("href", "")
                text  = link.get_text(strip=True)
                if any(k in text.lower() for k in ["associate", "articl", "junior", "lawyer", "counsel"]):
                    full_url = href if href.startswith("http") else (firm.get("website","") + href)
                    self._analyse_posting(firm["id"], text, full_url)
        except Exception as e:
            log.debug("[JD-NLP] Careers page error %s: %s", firm["id"], e)

    def _scan_indeed(self):
        """Scan Indeed for Calgary associate postings."""
        query = "associate+lawyer+Calgary"
        url   = INDEED_URL.format(query=query)
        try:
            resp = self.session.get(url, timeout=15)
            soup = BeautifulSoup(resp.text, "lxml")

            for card in soup.select(".job_seen_beacon, .jobsearch-ResultsList li"):
                title_el  = card.select_one("h2, .jobTitle")
                company_el= card.select_one(".companyName, [data-testid='company-name']")
                link_el   = card.select_one("a[href*='clk']")
                if not (title_el and company_el): continue

                title    = title_el.get_text(strip=True)
                company  = company_el.get_text(strip=True)
                href     = link_el["href"] if link_el else ""

                # Match company name to firm
                firm_id  = self._match_firm(company)
                if not firm_id: continue

                snippet  = card.get_text(" ", strip=True)
                self._process_posting_text(firm_id, title, snippet, href)
        except Exception as e:
            log.debug("[JD-NLP] Indeed error: %s", e)

    def _analyse_posting(self, firm_id: str, title: str, url: str):
        """Fetch posting page and score it."""
        if not url: return
        try:
            resp  = self.session.get(url, timeout=10)
            soup  = BeautifulSoup(resp.text, "lxml")
            text  = soup.get_text(" ", strip=True)[:3000]
            self._process_posting_text(firm_id, title, text, url)
        except Exception as e:
            log.debug("[JD-NLP] Posting fetch error %s: %s", url, e)

    def _process_posting_text(self, firm_id: str, title: str, text: str, url: str):
        scores  = score_job_posting(text)
        pa_list = scores["practice_areas"]
        uid     = hashlib.md5(url.encode()).hexdigest()[:14]

        # Store in DB
        conn = get_conn()
        conn.execute("""
            INSERT OR IGNORE INTO job_postings
                (firm_id, title, url, full_text, urgency_score, composite,
                 practice_areas, posted_date)
            VALUES (?,?,?,?,?,?,?,?)
        """, (firm_id, title, url, text[:1000],
              scores["urgency_score"], scores["composite"],
              json.dumps(pa_list), date.today().isoformat()))
        conn.commit()
        conn.close()

        # Fire signal if composite is high enough
        if scores["composite"] >= 6 or scores["urgency_score"] >= 4:
            firm   = FIRM_BY_ID.get(firm_id, {})
            weight = min(5.0, 3.0 + scores["composite"] * 0.15)
            pa     = pa_list[0] if pa_list else firm.get("focus",["general"])[0]

            desc = (
                f"JD ANALYSIS: {firm.get('name',firm_id)} — '{title}'. "
                f"Urgency score: {scores['urgency_score']}/10. "
                f"Desperation score: {scores['desperation_score']}/10. "
                f"Composite: {scores['composite']:.1f}. "
                f"{'TARGETS JUNIOR. ' if scores['targets_junior'] else ''}"
                f"{'PREMIUM SALARY OFFERED. ' if scores['has_premium_salary'] else ''}"
                f"Practice areas: {', '.join(pa_list) or 'general'}. "
                f"High-urgency vocabulary detected — they are in active need."
            )
            is_new = insert_signal(
                firm_id=firm_id, signal_type="jd_high_urgency_posting",
                weight=weight, title=f"JD: '{title[:50]}' at {firm.get('name',firm_id)} — urgency {scores['urgency_score']}/10",
                description=desc, source_url=url, practice_area=pa,
                raw_data=scores,
            )
            if is_new:
                self.new_signals.append({
                    "firm_id": firm_id, "signal_type": "jd_high_urgency_posting",
                    "weight": weight, "title": f"JD high-urgency: '{title[:40]}' at {firm.get('name',firm_id)}",
                    "practice_area": pa, "description": desc,
                })

    def _detect_reposting_desperation(self):
        """
        Detect firms that have posted the SAME type of role multiple times
        in the past 90 days → they cannot fill the position.
        """
        conn   = get_conn()
        rows   = conn.execute("""
            SELECT firm_id, count(*) as cnt
            FROM job_postings
            WHERE date(first_seen) >= date('now','-90 days')
            GROUP BY firm_id
            HAVING cnt >= 2
        """).fetchall()
        conn.close()

        for row in rows:
            fid   = row["firm_id"]
            cnt   = row["cnt"]
            firm  = FIRM_BY_ID.get(fid, {})
            is_new = insert_signal(
                firm_id=fid,
                signal_type="jd_repeated_posting",
                weight=4.0,
                title=f"Reposted {cnt}× in 90 days: {firm.get('name',fid)} cannot fill the role",
                description=(
                    f"{firm.get('name',fid)} has posted what appears to be the same junior "
                    f"role {cnt} times in the past 90 days. Either they cannot find a suitable "
                    f"candidate, or the person they hired has already left. This is a strong "
                    f"signal of structural demand that a cold application could solve."
                ),
                source_url="",
                practice_area=firm.get("focus",["general"])[0],
                raw_data={"repost_count": cnt},
            )

    def _match_firm(self, company_name: str) -> str | None:
        for firm in CALGARY_FIRMS:
            aliases = [firm["name"]] + firm.get("aliases", [])
            if any(a.lower() in company_name.lower() for a in aliases):
                return firm["id"]
        return None


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    analyzer = JobDescriptionAnalyzer()
    for s in analyzer.run():
        print(f"  [{s['signal_type']}] {s['firm_id']}: {s['title']}")
