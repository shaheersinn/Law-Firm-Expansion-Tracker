"""
EventScraper — conference speaking, sponsorship, and CLEs.

A firm sending partners to speak at CBA, OBA, LSOC, Advocates' Society,
Canadian Club, or major industry conferences is signalling:
  - Which practice areas they want to be known for (topic = department)
  - Investment in client development in that practice

Sources:
  CBA conferences         (cba.org/events)
  OBA events              (oba.org/CLE)
  Advocates' Society      (theadvocate.ca/events)
  CCCA events             (ccca.ca/events)
  LSBC events             (lawsociety.bc.ca)
  LSUC events             (lsuc.on.ca)
  Law Society Alberta     (lawsociety.ab.ca)
  Canadian Club Toronto   (canadianclub.org)
  Empire Club             (empireclubofcanada.com)
  IFLR Events (Canada)    (iflr.com/events)
  Osgoode PD              (osgoode.yorku.ca/programs/professional-development)
  Google News events filter
"""

import re
from urllib.parse import quote_plus, urljoin

from scrapers.base import BaseScraper
from classifier.department import DepartmentClassifier

import feedparser

classifier = DepartmentClassifier()

SPEAKER_RE = re.compile(
    r"speak(?:s|er|ing|ers)?\b|panel(?:list|led)?\b|moderator\b"
    r"|present(?:s|er|ing|ers|ation)?\b|keynote\b|session\s+chair\b"
    r"|featured\s+speaker\b|invited\s+speaker\b|guest\s+speaker\b",
    re.IGNORECASE,
)

SPONSOR_RE = re.compile(
    r"sponsor(?:s|ed|ing|ship)?\b|gold\s+sponsor\b|platinum\s+sponsor\b"
    r"|presenting\s+sponsor\b|title\s+sponsor\b|founding\s+sponsor\b",
    re.IGNORECASE,
)

CLE_RE = re.compile(
    r"cle\b|cpd\b|continuing\s+(?:legal\s+)?education\b|seminar\b|webinar\b"
    r"|workshop\b|conference\b|symposium\b|roundtable\b|forum\b",
    re.IGNORECASE,
)

EVENT_PAGES = [
    {"name": "CBA Events",         "url": "https://www.cba.org/Events-Calendar",          "weight": 3.0},
    {"name": "OBA CLE",            "url": "https://www.oba.org/CLE",                       "weight": 3.0},
    {"name": "Advocates Society",  "url": "https://theadvocate.ca/events",                 "weight": 3.0},
    {"name": "CCCA Events",        "url": "https://www.ccca.ca/events",                    "weight": 3.0},
    {"name": "Osgoode PD",         "url": "https://www.osgoode.yorku.ca/programs/professional-development/", "weight": 3.0},
    {"name": "IFLR Events",        "url": "https://www.iflr.com/events",                   "weight": 3.5},
    {"name": "Empire Club",        "url": "https://empireclubofcanada.com/programs/",      "weight": 2.5},
    {"name": "Canadian Club",      "url": "https://www.canadianclub.org/events",           "weight": 2.5},
]

RSS_SOURCES = [
    {"name": "CBA Pulse",      "url": "https://www.cba.org/Publications-Resources/RSS/CBA-National", "weight": 2.5},
    {"name": "OBA RSS",        "url": "https://www.oba.org/rss/events",                              "weight": 2.5},
    {"name": "CCCA Events",    "url": "https://www.ccca.ca/rss/events",                              "weight": 2.5},
]

GOOG = "https://news.google.com/rss/search?q={q}&hl=en-CA&gl=CA&ceid=CA:en"


class EventScraper(BaseScraper):
    name = "EventScraper"

    def fetch(self, firm: dict) -> list[dict]:
        signals = []
        seen: set = set()
        firm_tokens = [firm["short"].lower(), firm["name"].split()[0].lower()] + \
                      [a.lower() for a in firm.get("alt_names", [])]

        # 1 — Scrape event pages for firm mentions
        for src in EVENT_PAGES:
            soup = self.get_soup(src["url"])
            if not soup:
                continue
            for tag in soup.find_all(["article", "div", "li", "p"], limit=100):
                text = tag.get_text(" ", strip=True)
                if len(text) < 20 or len(text) > 600:
                    continue
                lower = text.lower()
                if not any(t in lower for t in firm_tokens):
                    continue
                if not (SPEAKER_RE.search(text) or SPONSOR_RE.search(text) or CLE_RE.search(text)):
                    continue

                link_tag = tag.find("a", href=True)
                link = ""
                if link_tag:
                    href = link_tag["href"]
                    link = href if href.startswith("http") else urljoin(src["url"], href)
                key = text[:80]
                if key in seen:
                    continue

                weight_mult = 4.0 if SPONSOR_RE.search(text) else 3.0 if SPEAKER_RE.search(text) else 2.0
                cls = classifier.classify(text, top_n=1)
                c = cls[0] if cls else {"department": "Corporate/M&A", "score": 1.0, "matched_keywords": []}

                signals.append(self._make_signal(
                    firm_id=firm["id"], firm_name=firm["name"],
                    signal_type="press_release",
                    title=f"[{src['name']}] {firm['short']} — {text[:160]}",
                    url=link or src["url"],
                    department=c["department"],
                    department_score=c["score"] * src["weight"] * weight_mult,
                    matched_keywords=c["matched_keywords"],
                ))
                seen.add(key)

        # 2 — RSS
        for src in RSS_SOURCES:
            try:
                feed = feedparser.parse(src["url"], request_headers={
                    "User-Agent": "Mozilla/5.0 (compatible; LegalTracker/2.0)"
                })
            except Exception:
                continue
            for entry in (feed.entries or [])[:20]:
                title   = entry.get("title", "") or ""
                summary = entry.get("summary", "") or ""
                link    = entry.get("link", src["url"]) or src["url"]
                if link in seen:
                    continue
                full = f"{title} {summary}"
                if not any(t in full.lower() for t in firm_tokens):
                    continue
                if not (SPEAKER_RE.search(full) or SPONSOR_RE.search(full)):
                    continue
                cls = classifier.classify(full, top_n=1)
                c = cls[0] if cls else {"department": "Corporate/M&A", "score": 1.0, "matched_keywords": []}
                signals.append(self._make_signal(
                    firm_id=firm["id"], firm_name=firm["name"],
                    signal_type="press_release",
                    title=f"[Event — {src['name']}] {title[:160]}",
                    body=summary[:400],
                    url=link,
                    department=c["department"],
                    department_score=c["score"] * src["weight"] * 3.0,
                    matched_keywords=c["matched_keywords"],
                ))
                seen.add(link)

        # 3 — Google News: firm + speaking event
        q   = f'"{firm["short"]}" speaks OR speaker OR panelist conference law Canada'
        url = GOOG.format(q=quote_plus(q))
        try:
            feed = feedparser.parse(url, request_headers={
                "User-Agent": "Mozilla/5.0 (compatible; LegalTracker/2.0)"
            })
        except Exception:
            return signals[:10]

        for entry in (feed.entries or [])[:10]:
            title   = entry.get("title", "") or ""
            summary = entry.get("summary", "") or ""
            link    = entry.get("link", url) or url
            if link in seen:
                continue
            full = f"{title} {summary}"
            if not any(t in full.lower() for t in firm_tokens):
                continue
            cls = classifier.classify(full, top_n=1)
            if not cls:
                continue
            c = cls[0]
            signals.append(self._make_signal(
                firm_id=firm["id"], firm_name=firm["name"],
                signal_type="press_release",
                title=f"[Event GNews] {title[:160]}",
                body=summary[:400],
                url=link,
                department=c["department"],
                department_score=c["score"] * 3.0,
                matched_keywords=c["matched_keywords"],
            ))
            seen.add(link)

        return signals[:12]
