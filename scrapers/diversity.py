"""
DiversityScraper — diversity, equity & inclusion signals.

Firms that publicly invest in DEI programs, publish diversity reports,
launch affinity groups, or hire Chief Diversity Officers are signaling
cultural investment — which often precedes lateral hiring drives.

These are also standalone ESG practice signals when the firm advises
clients on DEI matters.

Sources:
  Canadian Centre for Diversity and Inclusion (CCDI) — press
  Osgoode DEI initiatives
  OBA Equity initiatives
  CBA Equity and Diversity
  Firm DEI/careers pages (scraped directly)
  Google News: firm + diversity/equity + law
  Law firm DEI reports (annual, scraped)
  Precedent DEI coverage
  Canadian Lawyer DEI section
"""

import re
from urllib.parse import quote_plus, urljoin

from scrapers.base import BaseScraper
from classifier.department import DepartmentClassifier

import feedparser

classifier = DepartmentClassifier()

DEI_KEYWORDS = [
    "diversity", "equity", "inclusion", "dei", "edi",
    "reconciliation", "indigenous", "black lawyer",
    "lgbtq", "women in law", "gender equity",
    "chief diversity", "diversity officer", "affinity group",
    "diversity committee", "equity partner", "racialized",
    "accessibility", "disability", "first generation",
]

DEI_RE = re.compile(
    r"diversity|equity|inclusion|reconciliation|indigenous\s+law"
    r"|black\s+(?:lawyer|counsel|partner)|women\s+(?:in\s+law|lawyers?)"
    r"|lgbtq|gender\s+(?:equity|parity|gap)|affinity\s+group"
    r"|chief\s+diversity|diversity\s+officer|first\s+nations"
    r"|racialized|accessibility\s+plan",
    re.IGNORECASE,
)

RSS_SOURCES = [
    {"name": "CBA Equality",         "url": "https://www.cba.org/Publications-Resources/RSS-Feeds/Equality-Committee", "weight": 3.0},
    {"name": "OBA Equity",           "url": "https://www.oba.org/rss/equity",           "weight": 3.0},
    {"name": "Canadian Lawyer DEI",  "url": "https://www.canadianlawyermag.com/tag/diversity/rss", "weight": 3.5},
    {"name": "Lexpert DEI",          "url": "https://www.lexpert.ca/tag/diversity/rss",  "weight": 3.5},
    {"name": "National CBA",         "url": "https://www.nationalmagazine.ca/en-ca/articles/rss", "weight": 2.5},
    {"name": "Slaw DEI",             "url": "https://www.slaw.ca/feed/",                "weight": 2.0},
]

GOOG = "https://news.google.com/rss/search?q={q}&hl=en-CA&gl=CA&ceid=CA:en"

DEI_PAGE_PATHS = [
    "/en/diversity", "/diversity", "/en/dei", "/dei",
    "/en/about/diversity", "/about/diversity",
    "/en/careers/diversity", "/careers/diversity",
    "/en/inclusion", "/inclusion",
]


class DiversityScraper(BaseScraper):
    name = "DiversityScraper"

    def fetch(self, firm: dict) -> list[dict]:
        signals = []
        seen: set = set()
        firm_tokens = [firm["short"].lower(), firm["name"].split()[0].lower()] + \
                      [a.lower() for a in firm.get("alt_names", [])]

        # 1 — RSS feeds with DEI content mentioning firm
        for src in RSS_SOURCES:
            try:
                feed = feedparser.parse(src["url"], request_headers={
                    "User-Agent": "Mozilla/5.0 (compatible; LegalTracker/2.0)"
                })
            except Exception:
                continue

            for entry in (feed.entries or [])[:30]:
                title   = entry.get("title", "") or ""
                summary = entry.get("summary", "") or ""
                link    = entry.get("link", src["url"]) or src["url"]
                if link in seen:
                    continue
                full  = f"{title} {summary}"
                lower = full.lower()
                if not any(t in lower for t in firm_tokens):
                    continue
                if not DEI_RE.search(full):
                    continue

                cls = classifier.classify(full + " diversity equity inclusion ESG", top_n=1)
                c = cls[0] if cls else {"department": "ESG", "score": 1.0, "matched_keywords": ["dei"]}

                signals.append(self._make_signal(
                    firm_id=firm["id"], firm_name=firm["name"],
                    signal_type="press_release",
                    title=f"[DEI — {src['name']}] {title[:160]}",
                    body=summary[:500],
                    url=link,
                    department=c["department"],
                    department_score=c["score"] * src["weight"],
                    matched_keywords=c["matched_keywords"],
                ))
                seen.add(link)

        # 2 — Firm's own DEI page
        for path in DEI_PAGE_PATHS:
            url = firm["website"].rstrip("/") + path
            soup = self.get_soup(url)
            if not soup:
                continue
            text = soup.get_text(" ", strip=True)
            if not DEI_RE.search(text):
                continue

            # Extract meaningful snippets
            for tag in soup.find_all(["h2", "h3", "p"], limit=30):
                chunk = tag.get_text(" ", strip=True)
                if len(chunk) < 30 or len(chunk) > 400:
                    continue
                if not DEI_RE.search(chunk):
                    continue
                key = chunk[:80]
                if key in seen:
                    continue

                signals.append(self._make_signal(
                    firm_id=firm["id"], firm_name=firm["name"],
                    signal_type="practice_page",
                    title=f"[DEI Page] {firm['short']}: {chunk[:160]}",
                    url=url,
                    department="ESG",
                    department_score=2.5,
                    matched_keywords=DEI_KEYWORDS[:5],
                ))
                seen.add(key)
                break
            break  # one DEI page per firm

        # 3 — Google News DEI
        q   = f'"{firm["short"]}" diversity equity inclusion law firm'
        url = GOOG.format(q=quote_plus(q))
        try:
            feed = feedparser.parse(url, request_headers={
                "User-Agent": "Mozilla/5.0 (compatible; LegalTracker/2.0)"
            })
        except Exception:
            return signals[:8]

        for entry in (feed.entries or [])[:8]:
            title   = entry.get("title", "") or ""
            summary = entry.get("summary", "") or ""
            link    = entry.get("link", url) or url
            if link in seen:
                continue
            full = f"{title} {summary}"
            if not any(t in full.lower() for t in firm_tokens):
                continue
            if not DEI_RE.search(full):
                continue

            signals.append(self._make_signal(
                firm_id=firm["id"], firm_name=firm["name"],
                signal_type="press_release",
                title=f"[DEI News] {title[:160]}",
                body=summary[:400],
                url=link,
                department="ESG",
                department_score=2.5,
                matched_keywords=["diversity", "equity", "inclusion"],
            ))
            seen.add(link)

        return signals[:10]
