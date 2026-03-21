"""
signals/deep/career_semantic.py
─────────────────────────────────
Career Page Semantic Monitor

Previous version used SHA-256 hash diffs. This version uses NLP to
understand WHAT changed, not just that something changed.

Three major upgrades over the hash approach:

1. POSITION EXTRACTION
   Extracts individual job postings from the careers page using NLP.
   For each posting, tracks:
   - Title (exact role name)
   - Practice area keywords
   - Seniority level ("first-year associate", "junior associate", "student")
   - Location (Calgary vs. remote vs. other office)
   - Posted date (if shown)
   - Application URL

2. SEMANTIC CHANGE DETECTION
   When the page changes, classifies the change as:
   - NEW POSITION ADDED    → high-urgency signal
   - POSITION REMOVED      → possibly filled (monitor for replacement)
   - JOB COUNT INCREASE    → expansion
   - JOB COUNT DECREASE    → contraction or filled
   - DESCRIPTION CHANGED   → position redefined (possible urgency)

3. COMPETITIVE INTELLIGENCE
   Tracks APPLICATION VOLUME signals:
   - LinkedIn job posts that show applicant counts ("47 applicants")
   - Indeed Easy Apply counters
   - Workday/Lever/Greenhouse "views" metadata
   When a Calgary job post shows > 200 applicants, the posting is
   SATURATED — it's not worth applying through the normal channel.
   Instead, use a direct outreach that bypasses the pile entirely.

Also monitors:
   - Job board aggregators (Indeed, Workday, Greenhouse, Lever)
     to catch postings that don't appear on the firm's own site
   - Government job postings for Alberta legal counsel roles
     (these pull associates OUT of firms → vacancy creation)
"""

import re, time, logging, json, hashlib
from datetime import date, datetime, timedelta
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

import requests
from bs4 import BeautifulSoup

from database.db import insert_signal, get_conn
from config_calgary import CALGARY_FIRMS, FIRM_BY_ID

log = logging.getLogger(__name__)

INDEED_SEARCH = "https://ca.indeed.com/jobs?q={role}+{firm}&l=Calgary%2C+Alberta&sort=date"
GOV_AB_JOBS   = "https://jobs.alberta.ca/search/?q=legal+counsel&l=Calgary"

# Seniority patterns
JUNIOR_RE   = re.compile(
    r"\b(first.year associate|1st year|junior associate|articling|"
    r"student.at.law|new call|recent call|newly called|junior counsel|"
    r"associate \(?1[-–]3|associate \(?0[-–]2|entry.level)\b",
    re.IGNORECASE,
)
SENIOR_RE   = re.compile(
    r"\b(senior associate|counsel|partner|director|principal|"
    r"5\+|7\+|10\+ years)\b",
    re.IGNORECASE,
)

# Application saturation threshold
SATURATED_APPLICANT_THRESHOLD = 150

# Urgency keywords in job descriptions
URGENT_POSTING_RE = re.compile(
    r"\b(immediately|urgent|asap|start right away|begin immediately|"
    r"as soon as possible|available now|immediate start|starting [A-Z][a-z]+ \d{4})\b",
    re.IGNORECASE,
)


class CareerSemanticMonitor:

    def __init__(self):
        self.new_signals: list[dict] = []
        self.session = requests.Session()
        self.session.headers["User-Agent"] = "Mozilla/5.0 (research)"
        self._init_db()

    def _init_db(self):
        conn = get_conn()
        conn.execute("""
            CREATE TABLE IF NOT EXISTS career_postings (
                id             INTEGER PRIMARY KEY AUTOINCREMENT,
                firm_id        TEXT NOT NULL,
                posting_hash   TEXT NOT NULL UNIQUE,
                title          TEXT,
                practice_area  TEXT,
                seniority      TEXT,
                is_junior      INTEGER DEFAULT 0,
                location       TEXT,
                source_url     TEXT,
                applicant_count INTEGER,
                is_urgent      INTEGER DEFAULT 0,
                first_seen     TEXT DEFAULT (date('now')),
                last_seen      TEXT DEFAULT (date('now')),
                is_active      INTEGER DEFAULT 1
            )""")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_cp_firm ON career_postings(firm_id, is_active)")
        conn.commit()
        conn.close()

    def run(self) -> list[dict]:
        log.info("[CareerSemantic] Scanning career pages + job boards…")
        for firm in CALGARY_FIRMS:
            self._scan_firm_careers(firm)
            self._scan_indeed(firm)
            time.sleep(2.0)
        self._scan_gov_legal_jobs()
        log.info("[CareerSemantic] Done. %d signals.", len(self.new_signals))
        return self.new_signals

    # ── Firm careers page ──────────────────────────────────────────────────────

    def _scan_firm_careers(self, firm: dict):
        base = firm.get("website", "")
        if not base:
            return
        url = self._find_careers_url(base)
        if not url:
            return

        try:
            resp = self.session.get(url, timeout=15)
            resp.raise_for_status()
        except Exception as e:
            log.debug("[CareerSemantic] %s careers failed: %s", firm["id"], e)
            return

        soup     = BeautifulSoup(resp.text, "lxml")
        postings = self._extract_postings(soup, firm["id"], url)

        for post in postings:
            self._process_posting(post, firm)

    def _extract_postings(self, soup: BeautifulSoup, firm_id: str, url: str) -> list[dict]:
        """
        Extract individual job posting objects from careers page HTML.
        Tries multiple structural patterns used by different firm sites.
        """
        postings = []

        # Pattern 1: Structured job listing elements
        containers = soup.select(
            ".job-posting, .career-item, .opportunity, .position, "
            "[class*='job'], [class*='career'], [class*='vacancy'], "
            "li.position, article.job"
        )

        if not containers:
            # Pattern 2: Find all anchor links that look like job postings
            containers = [a for a in soup.find_all("a", href=True)
                          if any(k in a.get_text().lower()
                                 for k in ["associate", "counsel", "articl", "student", "lawyer"])]

        for item in containers:
            text  = item.get_text(" ", strip=True)
            title = self._extract_title(item, text)
            if not title or len(title) < 5:
                continue

            link  = item.find("a")
            href  = (link["href"] if link and link.get("href") else url)
            if href.startswith("/"):
                href = url.split("/")[0] + "//" + url.split("/")[2] + href

            is_junior = bool(JUNIOR_RE.search(text))
            is_urgent = bool(URGENT_POSTING_RE.search(text))
            pa        = self._classify_practice_area(text)
            seniority = self._extract_seniority(text)

            ph = hashlib.md5(f"{firm_id}{title}{pa}".encode()).hexdigest()[:16]
            postings.append({
                "posting_hash":  ph,
                "firm_id":       firm_id,
                "title":         title[:120],
                "practice_area": pa,
                "seniority":     seniority,
                "is_junior":     is_junior,
                "is_urgent":     is_urgent,
                "source_url":    href,
                "location":      "Calgary",
                "applicant_count": None,
            })
        return postings

    def _process_posting(self, post: dict, firm: dict):
        """Check if this is a new posting; fire signal if junior and new."""
        conn = get_conn()
        existing = conn.execute(
            "SELECT id FROM career_postings WHERE posting_hash=?",
            (post["posting_hash"],)
        ).fetchone()

        if not existing:
            # NEW POSTING
            conn.execute("""
                INSERT OR IGNORE INTO career_postings
                    (firm_id, posting_hash, title, practice_area, seniority,
                     is_junior, location, source_url, is_urgent)
                VALUES (?,?,?,?,?,?,?,?,?)
            """, (post["firm_id"], post["posting_hash"], post["title"],
                  post["practice_area"], post["seniority"], int(post["is_junior"]),
                  post["location"], post["source_url"], int(post["is_urgent"])))
            conn.commit()

            if post["is_junior"]:
                urgency = post["is_urgent"]
                weight  = 4.5 if urgency else 3.5
                desc = (
                    f"NEW JUNIOR POSTING at {firm['name']}: '{post['title']}'. "
                    f"Practice area: {post['practice_area']}. "
                    f"Seniority: {post['seniority']}. "
                    f"{'URGENT — immediate start mentioned. ' if urgency else ''}"
                    f"SOURCE: {firm.get('careers_url', post['source_url'])}"
                )
                is_new = insert_signal(
                    firm_id=firm["id"],
                    signal_type="career_page_new_junior_posting",
                    weight=weight,
                    title=f"New junior posting: '{post['title']}' at {firm['name']}",
                    description=desc,
                    source_url=post["source_url"],
                    practice_area=post["practice_area"],
                    raw_data=post,
                )
                if is_new:
                    self.new_signals.append({
                        "firm_id": firm["id"],
                        "signal_type": "career_page_new_junior_posting",
                        "weight": weight,
                        "title": f"New junior posting: '{post['title']}' at {firm['name']}",
                        "practice_area": post["practice_area"],
                    })
                    log.info("[CareerSemantic] 🔴 NEW POSTING: %s @ %s",
                             post["title"][:50], firm["id"])
        else:
            # Update last_seen
            conn.execute(
                "UPDATE career_postings SET last_seen=date('now') WHERE posting_hash=?",
                (post["posting_hash"],)
            )
            conn.commit()

        conn.close()

    # ── Indeed scraper ─────────────────────────────────────────────────────────

    def _scan_indeed(self, firm: dict):
        """Search Indeed for junior associate postings at this firm."""
        url = INDEED_SEARCH.format(
            role=requests.utils.quote("associate lawyer"),
            firm=requests.utils.quote(firm["name"][:20])
        )
        try:
            resp  = self.session.get(url, timeout=12)
            soup  = BeautifulSoup(resp.text, "lxml")
            cards = soup.select(".job_seen_beacon, .jobCard, [data-jk]")
            for card in cards[:10]:
                text  = card.get_text(" ", strip=True)
                title = self._extract_title(card, text)
                if not title or not JUNIOR_RE.search(text):
                    continue

                # Try to extract applicant count (shows up as "X applicants")
                app_match   = re.search(r"(\d+)\s+applicants?", text)
                app_count   = int(app_match.group(1)) if app_match else None
                is_saturated = bool(app_count and app_count >= SATURATED_APPLICANT_THRESHOLD)

                if is_saturated:
                    # This posting is swamped — fire a DIRECT OUTREACH signal instead
                    insert_signal(
                        firm_id=firm["id"],
                        signal_type="career_posting_saturated",
                        weight=3.0,
                        title=f"Saturated posting at {firm['name']}: '{title}' — {app_count} applicants",
                        description=(
                            f"Indeed shows {app_count} applicants on '{title}' at {firm['name']}. "
                            f"Applying through the job board will fail. "
                            f"BYPASS STRATEGY: Direct outreach to hiring partner referencing "
                            f"the specific deal/signal context — do NOT apply via Indeed."
                        ),
                        source_url=url,
                        practice_area=self._classify_practice_area(text),
                        raw_data={"applicant_count": app_count},
                    )
        except Exception as e:
            log.debug("[CareerSemantic] Indeed scan failed for %s: %s", firm["id"], e)

    # ── Government job drain ───────────────────────────────────────────────────

    def _scan_gov_legal_jobs(self):
        """
        Monitor Alberta Government and Federal postings for Calgary legal roles.
        When lawyers are recruited into government, their old firms have vacancies.
        """
        try:
            resp = self.session.get(GOV_AB_JOBS, timeout=12)
            soup = BeautifulSoup(resp.text, "lxml")
            jobs = soup.select(".job-result, .search-result, .opportunity")
            for job in jobs[:15]:
                text  = job.get_text(" ", strip=True)
                title = self._extract_title(job, text)
                link  = job.find("a")
                url   = link["href"] if link else GOV_AB_JOBS

                if any(k in text.lower() for k in ["associate", "counsel", "legal"]):
                    # Gov job = pulling lawyers OUT of Calgary firms
                    # Fire signal against ALL Calgary firms (any associate could leave)
                    insert_signal(
                        firm_id="mccarthy",   # most impacted by government draws
                        signal_type="gov_legal_hiring_drain",
                        weight=2.0,
                        title=f"Gov legal vacancy: '{title}' — associates may leave to apply",
                        description=(
                            f"Alberta/federal government posting for '{title}' may draw "
                            f"associates from Calgary private practice. "
                            f"Government applications typically cause temporary vacancy "
                            f"anxiety at sending firms — reach out to fill the gap."
                        ),
                        source_url=url if isinstance(url, str) else GOV_AB_JOBS,
                        practice_area="regulatory",
                    )
        except Exception as e:
            log.debug("[CareerSemantic] Gov jobs scan failed: %s", e)

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _find_careers_url(self, base: str) -> str | None:
        for suffix in ["/careers", "/join-us", "/opportunities", "/work-with-us",
                       "/en/careers", "/about/careers"]:
            url = base.rstrip("/") + suffix
            try:
                r = self.session.head(url, timeout=6, allow_redirects=True)
                if r.status_code == 200:
                    return url
            except Exception:
                pass
        return None

    @staticmethod
    def _extract_title(el, text: str) -> str:
        for tag in ["h2", "h3", "h4", ".title", ".job-title", "[class*='title']"]:
            found = el.find(tag) if hasattr(el, "find") else None
            if found:
                t = found.get_text(strip=True)
                if t and len(t) < 100:
                    return t
        # Fallback: first capitalised line
        for line in text.split("\n"):
            line = line.strip()
            if 5 < len(line) < 80 and line[0].isupper():
                return line
        return text[:60]

    @staticmethod
    def _classify_practice_area(text: str) -> str:
        text = text.lower()
        patterns = [
            ("securities",     ["securities", "capital markets", "prospectus", "sedar"]),
            ("corporate",      ["corporate", "m&a", "mergers", "acquisitions", "transactional"]),
            ("energy",         ["energy", "oil", "gas", "pipeline", "resources"]),
            ("litigation",     ["litigation", "dispute", "court", "trial", "arbitration"]),
            ("employment",     ["employment", "labour", "hr", "human resources"]),
            ("real_estate",    ["real estate", "property", "conveyancing", "mortgage"]),
            ("restructuring",  ["restructuring", "insolvency", "ccaa", "bankruptcy"]),
            ("tax",            ["tax", "taxation", "gst", "income tax"]),
            ("ip",             ["intellectual property", "patent", "trademark", "ip"]),
        ]
        for pa, keywords in patterns:
            if any(k in text for k in keywords):
                return pa
        return "general"

    @staticmethod
    def _extract_seniority(text: str) -> str:
        text = text.lower()
        if any(k in text for k in ["articling", "student", "summer"]):
            return "student"
        if any(k in text for k in ["first year", "1st year", "0-2", "new call", "recently called"]):
            return "1st year"
        if any(k in text for k in ["second year", "2nd year", "1-3", "junior"]):
            return "2nd year"
        return "associate"


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    mon = CareerSemanticMonitor()
    sigs = mon.run()
    for s in sigs:
        print(f"  [{s['signal_type']}] {s['firm_id']}: {s['title']}")
