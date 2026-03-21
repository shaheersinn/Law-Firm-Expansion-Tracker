"""
signals/deep/dark_pipeline.py
───────────────────────────────
The Dark Pipeline — Hidden Departure Signals

Lawyers who are ABOUT TO LEAVE private practice for public roles, academia,
or governance don't announce it. But they leave a trail of signals in
public records 3-12 months before:

1. ELECTED TO CBA/LAW SOCIETY LEADERSHIP
   Partners elected to CBA section chairs, Law Society committees, or
   advocacy positions typically reduce their private practice files first.
   Many go full-time in-house at their association.
   Source: CBA Alberta announcements, Law Society of Alberta news

2. APPOINTED TO GOVERNMENT ADVISORY ROLES
   Orders in Council (Alberta Gazette) name legal professionals to
   boards, commissions, and regulatory panels. These are often 2-3 year
   commitments that effectively end private practice.
   Source: Alberta Gazette (weekly, free PDF)

3. ACCEPTED ACADEMIC POSITIONS
   Law school faculty appointments (UCalgary, U of A) — announced by
   the universities' comms departments before they appear on LSA.
   Source: UCalgary Law news, U of A Law news

4. CBA CONFERENCE SPEAKER LISTS
   When a partner becomes a frequent conference speaker/panelist on
   topics like "in-house counsel", "leaving private practice", "public service",
   "work-life balance at the bar" — they're telegraphing their intent.
   Source: CBA Alberta CPD calendar

5. PRO BONO LEADERSHIP SURGE
   When an associate takes on a heavy pro bono caseload or chairs a
   legal aid clinic, they're often transitioning. Their billable hours
   are about to crash — and they'll need to leave the firm or get pushed out.
   Source: Pro Bono Law Alberta announcements

Each of these fires a DEPARTURE PRECURSOR signal against the firm,
giving a 3-12 month predictive window.
"""

import re, time, logging, hashlib
from datetime import date, datetime, timedelta
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

import requests, feedparser
from bs4 import BeautifulSoup

from database.db import insert_signal, get_conn
from config_calgary import CALGARY_FIRMS, FIRM_BY_ID

log = logging.getLogger(__name__)

# Source URLs
CBA_ALBERTA_NEWS       = "https://www.cba.org/Alberta-Branch/News"
LSA_NEWS               = "https://www.lawsociety.ab.ca/news/"
ALBERTA_GAZETTE_URL    = "https://www.alberta.ca/gazette"
UCALGARY_LAW_NEWS      = "https://law.ucalgary.ca/news"
UOFA_LAW_NEWS          = "https://law.ualberta.ca/news"
PBLA_NEWS              = "https://pbla.ca/news/"

# Keywords signalling departure intent
CBA_LEADERSHIP_RE = re.compile(
    r"\b(elected chair|appointed chair|elected president|section chair|"
    r"executive committee|board of directors|CBA council|chair of the|"
    r"co.chair|vice.chair|elected to|appointed to the)\b",
    re.IGNORECASE,
)

ACADEMIC_RE = re.compile(
    r"\b(joins faculty|appointed professor|clinical professor|adjunct professor|"
    r"visiting professor|faculty appointment|joins the university|"
    r"joins the faculty of law|secondment to)\b",
    re.IGNORECASE,
)

GOV_APPOINTMENT_RE = re.compile(
    r"\b(Order in Council|Lieutenant Governor|appointed to the|"
    r"commission member|board member|tribunal member|adjudicator|"
    r"regulatory panel|advisory board|public inquiry)\b",
    re.IGNORECASE,
)

SPEAKER_TELLTALE_RE = re.compile(
    r"\b(in.house counsel|leaving private practice|balance at the bar|"
    r"work.life balance|alternative careers|life after practice|"
    r"government career|transition from practice)\b",
    re.IGNORECASE,
)

PROBONO_LEADERSHIP_RE = re.compile(
    r"\b(pro bono coordinator|legal aid|volunteer coordinator|"
    r"access to justice|community legal clinic|chairs the clinic|"
    r"directing solicitor)\b",
    re.IGNORECASE,
)


class DarkPipelineMonitor:
    """
    Monitors dark-pipeline departure precursors.
    All sources are public record.
    """

    def __init__(self):
        self.new_signals: list[dict] = []
        self.session = requests.Session()
        self.session.headers["User-Agent"] = "Mozilla/5.0 (research)"

    def run(self) -> list[dict]:
        log.info("[DarkPipeline] Scanning departure precursor sources…")
        self._scan_cba_news()
        self._scan_lsa_news()
        self._scan_academic_appointments()
        self._scan_alberta_gazette()
        self._scan_probono_appointments()
        log.info("[DarkPipeline] Done. %d signals.", len(self.new_signals))
        return self.new_signals

    # ── CBA Alberta news ───────────────────────────────────────────────────────

    def _scan_cba_news(self):
        self._scan_source(
            CBA_ALBERTA_NEWS,
            "CBA",
            [CBA_LEADERSHIP_RE, SPEAKER_TELLTALE_RE],
            "cba_leadership_departure_precursor",
            weight=3.0,
            horizon="3-12 months",
            desc_template=(
                "CBA LEADERSHIP SIGNAL: {name} at {firm} {detail}. "
                "Partners taking on significant CBA leadership often reduce private "
                "practice files in the 3-12 months following appointment. "
                "Their desk may open up. Pre-emptive outreach window."
            ),
        )

    def _scan_lsa_news(self):
        self._scan_source(
            LSA_NEWS,
            "LSA",
            [CBA_LEADERSHIP_RE],
            "lsa_committee_departure_precursor",
            weight=2.5,
            horizon="6-12 months",
        )

    def _scan_academic_appointments(self):
        for url, source in [(UCALGARY_LAW_NEWS, "UCalgary"), (UOFA_LAW_NEWS, "UofA")]:
            self._scan_source(
                url, source, [ACADEMIC_RE],
                "academic_appointment_departure",
                weight=4.0,
                horizon="0-3 months",
                desc_template=(
                    "ACADEMIC APPOINTMENT: {detail}. "
                    "A lawyer joining law school faculty is leaving private practice "
                    "immediately. Their firm has an unexpected vacancy."
                ),
            )

    def _scan_alberta_gazette(self):
        """
        Scan Alberta Gazette for Orders in Council appointing lawyers
        to government boards and commissions.
        The Gazette is published weekly and is fully public.
        """
        try:
            # Try to fetch the most recent Gazette issue index
            resp = self.session.get(ALBERTA_GAZETTE_URL, timeout=12)
            soup = BeautifulSoup(resp.text, "lxml")
            # Find links to recent PDF issues
            pdf_links = [a["href"] for a in soup.find_all("a", href=True)
                         if a["href"].endswith(".pdf") and "gazette" in a["href"].lower()]

            for pdf_url in pdf_links[:2]:   # Most recent 2 issues
                try:
                    import pdfplumber
                    pdf_resp = self.session.get(pdf_url, timeout=20)
                    import io
                    with pdfplumber.open(io.BytesIO(pdf_resp.content)) as pdf:
                        text = " ".join(
                            page.extract_text() or "" for page in pdf.pages[:15]
                        )
                    self._parse_gazette_text(text, pdf_url)
                except Exception as e:
                    log.debug("[DarkPipeline] Gazette PDF error: %s", e)
        except Exception as e:
            log.debug("[DarkPipeline] Gazette scan error: %s", e)

    def _parse_gazette_text(self, text: str, url: str):
        """Extract OIC appointments from Alberta Gazette text."""
        if not GOV_APPOINTMENT_RE.search(text):
            return

        # Find sentences that mention law/legal professionals
        sentences = re.split(r'[.!?\n]', text)
        for sent in sentences:
            if not GOV_APPOINTMENT_RE.search(sent):
                continue
            if not re.search(r"\b(barrister|solicitor|counsel|lawyer|QC|KC|LLB|JD)\b", sent, re.I):
                continue

            # Try to find a firm association
            firm_matched = self._match_firm_in_text(sent)
            if firm_matched:
                is_new = insert_signal(
                    firm_id=firm_matched,
                    signal_type="gazette_appointment_departure",
                    weight=3.5,
                    title=f"[Alberta Gazette] Government appointment of lawyer from {FIRM_BY_ID.get(firm_matched,{}).get('name',firm_matched)}",
                    description=(
                        f"Order in Council: {sent[:200]}. "
                        f"Government appointment creates immediate vacancy at originating firm."
                    ),
                    source_url=url,
                    practice_area="regulatory",
                )
                if is_new:
                    self.new_signals.append({
                        "firm_id": firm_matched,
                        "signal_type": "gazette_appointment_departure",
                        "weight": 3.5,
                        "title": f"Gazette: government appointment — {FIRM_BY_ID.get(firm_matched,{}).get('name',firm_matched)}",
                        "practice_area": "regulatory",
                    })

    def _scan_probono_appointments(self):
        self._scan_source(
            PBLA_NEWS,
            "PBLA",
            [PROBONO_LEADERSHIP_RE],
            "probono_leadership_precursor",
            weight=2.0,
            horizon="6-18 months",
        )

    # ── Generic source scanner ─────────────────────────────────────────────────

    def _scan_source(self, url: str, source: str, patterns: list,
                     sig_type: str, weight: float, horizon: str,
                     desc_template: str = ""):
        try:
            resp = self.session.get(url, timeout=12)
            soup = BeautifulSoup(resp.text, "lxml")
        except Exception as e:
            log.debug("[DarkPipeline] %s failed: %s", source, e)
            return

        articles = soup.select("article, .news-item, .post, li.news")
        for article in articles[:20]:
            text = article.get_text(" ", strip=True)
            link = article.find("a")
            href = link["href"] if link and link.get("href") else url

            if not any(p.search(text) for p in patterns):
                continue

            # Try to match a Calgary firm
            firm_id = self._match_firm_in_text(text)
            if not firm_id:
                continue

            firm = FIRM_BY_ID.get(firm_id, {})
            desc = desc_template.format(
                name="a lawyer",
                firm=firm.get("name", firm_id),
                detail=text[:150],
            ) if desc_template else (
                f"[{source}] DEPARTURE PRECURSOR: {text[:200]}. "
                f"This pattern typically precedes a private practice departure "
                f"within {horizon}."
            )

            uid = hashlib.md5(f"{firm_id}{sig_type}{text[:60]}".encode()).hexdigest()[:16]
            is_new = insert_signal(
                firm_id=firm_id,
                signal_type=sig_type,
                weight=weight,
                title=f"[{source}] Departure precursor at {firm.get('name',firm_id)}",
                description=desc,
                source_url=href if isinstance(href, str) else url,
                practice_area=firm.get("focus",["general"])[0],
                raw_data={"source": source, "text_snippet": text[:200], "uid": uid},
            )
            if is_new:
                self.new_signals.append({
                    "firm_id": firm_id,
                    "signal_type": sig_type,
                    "weight": weight,
                    "title": f"[{source}] Departure precursor: {firm.get('name',firm_id)}",
                    "practice_area": firm.get("focus",["general"])[0],
                })

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _match_firm_in_text(self, text: str) -> str | None:
        text_lower = text.lower()
        for firm in CALGARY_FIRMS:
            aliases = [firm["name"]] + firm.get("aliases", [])
            if any(a.lower() in text_lower for a in aliases):
                return firm["id"]
        return None


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    mon = DarkPipelineMonitor()
    for s in mon.run():
        print(f"  [{s['signal_type']}] {s['firm_id']}: {s['title']}")
