"""
CanLII Court Records Scraper
=============================
CanLII (Canadian Legal Information Institute) is the authoritative free
database of Canadian court decisions. When a firm starts appearing more
frequently in a new type of case (e.g. privacy disputes, competition
hearings, ESG challenges), it signals expanding litigation practice depth
in that area — even before any public announcement.

What we track:
  - Recent decisions where the firm appears as counsel
  - The court/tribunal type (maps to department)
  - The legal subject area tags CanLII attaches to each decision
  - YoY frequency change by subject area

Signal weight: HIGH for litigation, MEDIUM for advisory departments
(since courts only capture actual filed cases, not transactional work)

API: CanLII has a free REST API (no auth required for search).
Docs: https://api.canlii.org/v1/
"""

import re
import os
import time
from collections import defaultdict
from scrapers.base import BaseScraper
from classifier.department import DepartmentClassifier

classifier = DepartmentClassifier()

CANLII_API_BASE = "https://api.canlii.org/v1"
CANLII_API_KEY  = os.environ.get("CANLII_API_KEY", "").strip()

# CanLII database codes for key Canadian courts and tribunals
# Full list: https://www.canlii.org/en/#databases
COURT_DATABASE_MAP = {
    # Federal courts
    "fca":   ("Federal Court of Appeal",             "Litigation & Disputes"),
    "fc":    ("Federal Court",                        "Litigation & Disputes"),
    "scc":   ("Supreme Court of Canada",              "Litigation & Disputes"),
    # Provincial superior courts
    "onca":  ("Ontario Court of Appeal",              "Litigation & Disputes"),
    "onsc":  ("Ontario Superior Court",               "Litigation & Disputes"),
    "abca":  ("Alberta Court of Appeal",              "Litigation & Disputes"),
    "abkb":  ("Alberta King's Bench",                 "Litigation & Disputes"),
    "bcca":  ("BC Court of Appeal",                   "Litigation & Disputes"),
    "bcsc":  ("BC Supreme Court",                     "Litigation & Disputes"),
    "qcca":  ("Quebec Court of Appeal",               "Litigation & Disputes"),
    "qccs":  ("Quebec Superior Court",                "Litigation & Disputes"),
    # Specialized tribunals — map directly to departments
    "oncpc": ("Ontario Capital Markets Tribunal",     "Capital Markets"),
    "osc":   ("Ontario Securities Commission",        "Capital Markets"),
    "crtc":  ("CRTC",                                 "Financial Services & Regulatory"),
    "cb":    ("Competition Bureau / Tribunal",        "Competition & Antitrust"),
    "ohrt":  ("Ontario Human Rights Tribunal",        "Employment & Labour"),
    "olrb":  ("Ontario Labour Relations Board",       "Employment & Labour"),
    "tax":   ("Tax Court of Canada",                  "Tax"),
    "tat":   ("Tax Appeals Tribunal",                 "Tax"),
    "priv":  ("Privacy Commissioner",                 "Data Privacy & Cybersecurity"),
    "neb":   ("National Energy Board",                "Energy & Natural Resources"),
    "cer":   ("Canada Energy Regulator",              "Energy & Natural Resources"),
    "irb":   ("Immigration and Refugee Board",        "Immigration"),
    "fct":   ("Federal Court (immigration)",          "Immigration"),
}

# CanLII subject area tags → department mappings
SUBJECT_AREA_MAP = {
    "bankruptcy and insolvency":        "Restructuring & Insolvency",
    "companies and corporations":       "Corporate / M&A",
    "securities":                       "Capital Markets",
    "labour law":                       "Employment & Labour",
    "employment":                       "Employment & Labour",
    "intellectual property":            "Intellectual Property",
    "privacy":                          "Data Privacy & Cybersecurity",
    "environmental law":                "ESG & Regulatory",
    "tax":                              "Tax",
    "competition":                      "Competition & Antitrust",
    "real property":                    "Real Estate",
    "health":                           "Healthcare & Life Sciences",
    "immigration":                      "Immigration",
    "energy":                           "Energy & Natural Resources",
    "constitutional":                   "ESG & Regulatory",
    "administrative":                   "ESG & Regulatory",
    "contracts":                        "Corporate / M&A",
    "insurance":                        "Financial Services & Regulatory",
    "banking":                          "Financial Services & Regulatory",
    "construction":                     "Infrastructure & Projects",
    "aboriginal":                       "Energy & Natural Resources",
    "human rights":                     "Employment & Labour",
}


class CanLIIScraper(BaseScraper):
    name = "CanLIIScraper"

    def fetch(self, firm: dict) -> list[dict]:
        # CanLII API requires an API key — skip silently if not configured
        # Get a free key at: https://api.canlii.org/
        if not CANLII_API_KEY:
            self.logger.debug("CanLII skipped — set CANLII_API_KEY env var to enable")
            return []
        signals = []
        signals.extend(self._search_recent_cases(firm))
        return signals

    def _search_recent_cases(self, firm: dict) -> list[dict]:
        """Search CanLII for recent cases mentioning the firm as counsel."""
        signals = []

        # Try multiple name variants — CanLII is inconsistent about firm name format
        name_variants = self._get_name_variants(firm)

        for name_variant in name_variants:
            # CanLII full-text search API
            url = (
                f"{CANLII_API_BASE}/caseBrowse/en/?fullText={name_variant}"
                f"&language=en&resultCount=20"
            )
            response = self._get(url)
            if not response:
                continue

            try:
                data = response.json()
            except Exception:
                continue

            cases = data.get("cases", []) or data.get("results", [])
            if not cases:
                continue

            self.logger.info(f"[{firm['short']}] CanLII: found {len(cases)} case(s) for '{name_variant}'")

            for case in cases[:15]:
                case_id = case.get("caseId", {})
                db_id = case_id.get("en", "") if isinstance(case_id, dict) else str(case_id)
                title = case.get("title", case.get("style", ""))
                citation = case.get("citation", "")
                db_code = case.get("databaseId", "")
                keywords = case.get("keywords", [])
                subjects = case.get("topics", case.get("subjectAreas", []))

                if not title:
                    continue

                # Determine department from court type or subject areas
                department = self._map_to_department(db_code, subjects, title, keywords)
                if not department:
                    continue

                signal_text = f"{title} {' '.join(keywords)} {' '.join(subjects)}"
                classifications = classifier.classify(signal_text, top_n=1)
                score = classifications[0]["score"] if classifications else 3.0
                matched_kws = classifications[0]["matched_keywords"] if classifications else list(keywords)[:5]

                court_label = COURT_DATABASE_MAP.get(db_code.lower(), (db_code, ""))[0] if db_code else "Court"
                case_url = f"https://www.canlii.org/en/{db_code}/{db_id}/" if db_code and db_id else "https://www.canlii.org"

                signals.append(self._make_signal(
                    firm_id=firm["id"],
                    firm_name=firm["name"],
                    signal_type="court_record",
                    title=f"[CanLII] {title} — {citation}",
                    body=f"Court: {court_label} | Subjects: {', '.join(subjects[:5])} | Keywords: {', '.join(keywords[:5])}",
                    url=case_url,
                    department=department,
                    department_score=score,
                    matched_keywords=matched_kws,
                ))

            # Avoid hammering the API
            time.sleep(1.5)
            break  # one successful name variant is enough

        self.logger.info(f"[{firm['short']}] CanLII total: {len(signals)} signal(s)")
        return self._deduplicate(signals)

    def _get_name_variants(self, firm: dict) -> list[str]:
        """Generate search-friendly name variants."""
        name = firm["name"]
        short = firm["short"]
        variants = []

        # Short name (e.g. "Osler")
        if short and len(short) > 3:
            variants.append(short)

        # First two words of full name
        words = name.split()
        if len(words) >= 2:
            variants.append(f"{words[0]}+{words[1]}")

        # Full name URL-encoded
        variants.append(name.replace(" ", "+").replace(",", "").replace(".", ""))

        return variants[:2]  # cap to 2 to avoid rate limits

    def _map_to_department(self, db_code: str, subjects: list, title: str, keywords: list) -> str:
        """Map court + subject area to a practice department."""

        # Direct court-to-department mapping
        db_lower = db_code.lower() if db_code else ""
        if db_lower in COURT_DATABASE_MAP:
            _, dept = COURT_DATABASE_MAP[db_lower]
            if dept != "Litigation & Disputes":  # specialized tribunal = specific dept
                return dept

        # Subject area mapping
        for subj in subjects:
            subj_lower = subj.lower()
            for key, dept in SUBJECT_AREA_MAP.items():
                if key in subj_lower:
                    return dept

        # Keyword-based fallback
        all_text = f"{title} {' '.join(keywords)}".lower()
        for key, dept in SUBJECT_AREA_MAP.items():
            if key in all_text:
                return dept

        # Default: litigation (it's a court record)
        return "Litigation & Disputes"

    def _deduplicate(self, signals: list[dict]) -> list[dict]:
        seen = set()
        result = []
        for s in signals:
            if s["title"] not in seen:
                seen.add(s["title"])
                result.append(s)
        return result
