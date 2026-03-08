"""
PodcastScraper — Canadian legal podcasts.

Episode titles/descriptions mentioning a firm by name are high-quality
signals: firms are invited as guests when they're active in a practice area,
or they sponsor episodes to announce new service lines.

Sources:
  Precedent Podcast           (Apple Podcasts RSS)
  Canadian Lawyer Podcast     (canadianlawyermag.com)
  In-House Matters            (CCCA podcast)
  Osgoode Hall Law School     (podcast.osgoode.yorku.ca)
  The Lawyer's Daily Podcast  (thelawyersdaily.ca)
  Law Bytes (Michael Geist)   (michaelgeist.ca/feed) — tech/IP
  Strictly Legal (Globe)      (globeandmail.com podcast)
  Future of Law               (various hosts)
  Canadian Lawyer Top-Ranked  (special edition episodes)
  Advocates' Society podcast  (theadvocate podcast feed)
"""

import re
from datetime import datetime, timezone, timedelta
from email.utils import parsedate_to_datetime
from urllib.parse import quote_plus

from scrapers.base import BaseScraper
from classifier.department import DepartmentClassifier

import feedparser  # shim loaded by base.py

classifier = DepartmentClassifier()
LOOKBACK_DAYS = 60   # podcasts publish irregularly

PODCAST_FEEDS = [
    {"name": "Precedent Podcast",        "url": "https://feeds.buzzsprout.com/1793538.rss",                       "weight": 4.0},
    {"name": "Canadian Lawyer Pod",      "url": "https://www.canadianlawyermag.com/podcasts/rss",                 "weight": 4.0},
    {"name": "In-House Matters (CCCA)",  "url": "https://feeds.simplecast.com/ccca_podcast",                      "weight": 3.5},
    {"name": "Osgoode Hall Law",         "url": "https://podcast.osgoode.yorku.ca/feed",                         "weight": 3.0},
    {"name": "The Lawyer's Daily Pod",   "url": "https://www.thelawyersdaily.ca/podcast/rss",                    "weight": 3.5},
    {"name": "Law Bytes",                "url": "https://feeds.buzzsprout.com/1005957.rss",                       "weight": 3.0},
    {"name": "Strictly Legal",           "url": "https://feeds.simplecast.com/strictly_legal",                   "weight": 3.5},
    {"name": "Advocates Society Pod",    "url": "https://feeds.buzzsprout.com/advocates_society",                 "weight": 3.0},
    {"name": "Future of Law",            "url": "https://anchor.fm/s/futureoflaw/podcast/rss",                   "weight": 3.0},
    {"name": "Dentons Legal Podcast",    "url": "https://www.dentons.com/en/insights/podcasts/rss",               "weight": 2.0},
    {"name": "McCarthy Innovation",      "url": "https://www.mccarthy.ca/en/podcasts/rss",                       "weight": 2.0},
    {"name": "Blakes Business Class",    "url": "https://www.blakes.com/insights/podcasts/rss",                  "weight": 2.0},
]

SPONSOR_RE = re.compile(
    r"sponsor(?:ed|s|ing)\s+by|brought\s+to\s+you\s+by"
    r"|in\s+partnership\s+with|presented\s+by",
    re.IGNORECASE,
)

GUEST_RE = re.compile(
    r"guest(?:s)?\s+(?:from|at|of)|feat(?:ures?|uring)"
    r"|join(?:s|ed)\s+(?:us|me)\s+(?:from|today)"
    r"|interview\s+with|speaks?\s+with|talk(?:s|ed)\s+(?:to|with)",
    re.IGNORECASE,
)


def _is_recent(entry) -> bool:
    for key in ("published", "updated"):
        raw = getattr(entry, key, "") or entry.get(key, "")
        if not raw:
            continue
        try:
            dt = parsedate_to_datetime(raw).astimezone(timezone.utc)
            return dt >= datetime.now(timezone.utc) - timedelta(days=LOOKBACK_DAYS)
        except Exception:
            pass
    return True


class PodcastScraper(BaseScraper):
    name = "PodcastScraper"

    def fetch(self, firm: dict) -> list[dict]:
        signals = []
        seen: set = set()
        firm_tokens = [firm["short"].lower(), firm["name"].split()[0].lower()] + \
                      [a.lower() for a in firm.get("alt_names", [])]

        for src in PODCAST_FEEDS:
            try:
                feed = feedparser.parse(src["url"], request_headers={
                    "User-Agent": "Mozilla/5.0 (compatible; LegalTracker/2.0)"
                })
            except Exception:
                continue

            for entry in (feed.entries or [])[:30]:
                if not _is_recent(entry):
                    continue
                title   = entry.get("title", "") or ""
                summary = entry.get("summary", "") or ""
                link    = entry.get("link", src["url"]) or src["url"]

                if link in seen:
                    continue
                full  = f"{title} {summary}"
                lower = full.lower()

                if not any(t in lower for t in firm_tokens):
                    continue

                # Determine signal subtype
                if SPONSOR_RE.search(full):
                    sig_type, extra_w = "press_release", 3.5
                elif GUEST_RE.search(full):
                    sig_type, extra_w = "press_release", 4.0
                else:
                    sig_type, extra_w = "press_release", 2.5

                cls = classifier.classify(full, top_n=1)
                if not cls:
                    continue
                c = cls[0]

                signals.append(self._make_signal(
                    firm_id=firm["id"], firm_name=firm["name"],
                    signal_type=sig_type,
                    title=f"[{src['name']}] {title[:160]}",
                    body=summary[:500],
                    url=link,
                    department=c["department"],
                    department_score=c["score"] * src["weight"] * extra_w,
                    matched_keywords=c["matched_keywords"],
                ))
                seen.add(link)

        return signals
