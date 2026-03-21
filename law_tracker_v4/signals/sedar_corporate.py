"""
signals/sedar_corporate.py
──────────────────────────
Strategy 2 — "Follow the Money" (Corporate / Securities)

Monitors SEDAR+ daily filings to find which Calgary firms are named as
counsel on major deals (prospectuses, M&A circulars, private placements).
When a target mid-size firm appears on a large deal, fires a high-weight
signal so you can email the lead partner the same day.

Workflow:
  1. Poll the SEDAR+ RSS feed for new filings
  2. Download PDFs / filing metadata
  3. Run regex + fuzzy search for legal counsel names
  4. Identify which Calgary firms are acting as counsel
  5. Score deal size from document text ($ amounts, deal type)
  6. Insert SEDAR signal + update spillage graph
"""

import re
import time
import logging
import hashlib
import json
from datetime import datetime, timedelta
from io import BytesIO
from urllib.parse import urljoin

import requests
import feedparser
import pdfplumber

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from config_calgary import (
    SEDAR_RSS_URL, SEDAR_BASE_URL, SEDAR_COUNSEL_DOC_TYPES,
    CALGARY_FIRMS, FIRM_ALIASES, BIGLAW_FIRMS,
    SIGNAL_WEIGHTS, CANLII_RATE_LIMIT_S,
)
from database.db import insert_sedar_filing, insert_signal, upsert_spillage_edge

log = logging.getLogger(__name__)

# ─── Regex patterns ───────────────────────────────────────────────────────────

# Sections in legal documents that list counsel
_COUNSEL_SECTION_RE = re.compile(
    r"(legal counsel|acting as counsel|counsel to|legal advisors?|"
    r"solicitors? to|as counsel|counsel for|represented by|legal representative)",
    re.IGNORECASE,
)

# Dollar amounts — used to estimate deal size
_DOLLAR_RE = re.compile(
    r"\$\s*(\d[\d,\.]*)\s*(billion|million|B|M)\b",
    re.IGNORECASE,
)

# Firm name patterns (auto-built from config)
_FIRM_RE: dict[str, re.Pattern] = {}
for _firm in CALGARY_FIRMS:
    _tokens = [re.escape(a) for a in [_firm["name"]] + _firm["aliases"]]
    _FIRM_RE[_firm["id"]] = re.compile("|".join(_tokens), re.IGNORECASE)

# Major document type keywords
_MAJOR_DOC_RE = re.compile(
    r"\b(prospectus|take-?over bid|issuer bid|business acquisition|M&A|"
    r"merger|amalgamation|arrangement|private placement)\b",
    re.IGNORECASE,
)


# ─── Deal-size parser ────────────────────────────────────────────────────────

def parse_deal_value_cad(text: str) -> float | None:
    """
    Scan document text for dollar amounts. Returns the largest one found
    (in CAD millions), or None if unparseable.
    """
    values = []
    for m in _DOLLAR_RE.finditer(text):
        num = float(m.group(1).replace(",", ""))
        unit = m.group(2).lower()
        if unit in ("billion", "b"):
            num *= 1000
        values.append(num)
    return max(values) if values else None


# ─── Counsel extractor ───────────────────────────────────────────────────────

def extract_counsel_from_text(text: str) -> list[str]:
    """
    Given full document text, find all Calgary firm names.
    Returns list of firm_ids.
    """
    found = []
    # Prefer the 200-char context around a 'counsel' keyword
    counsel_windows = []
    for m in _COUNSEL_SECTION_RE.finditer(text):
        start = max(0, m.start() - 50)
        end   = min(len(text), m.end() + 300)
        counsel_windows.append(text[start:end])

    # Search windows first, then full doc as fallback
    search_zones = counsel_windows if counsel_windows else [text]

    for zone in search_zones:
        for firm_id, pattern in _FIRM_RE.items():
            if pattern.search(zone):
                found.append(firm_id)

    return list(set(found))


# ─── PDF fetcher ─────────────────────────────────────────────────────────────

class PDFFetcher:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers["User-Agent"] = (
            "LawFirmTracker/2.0 (research tool; contact: admin@example.com)"
        )

    def fetch_text(self, url: str, max_pages: int = 15) -> str:
        """
        Download a PDF and extract the first max_pages of text.
        Falls back to empty string on error.
        """
        try:
            resp = self.session.get(url, timeout=30)
            resp.raise_for_status()
            with pdfplumber.open(BytesIO(resp.content)) as pdf:
                pages = pdf.pages[:max_pages]
                return "\n".join(p.extract_text() or "" for p in pages)
        except Exception as e:
            log.debug("[SEDAR] PDF fetch failed %s: %s", url, e)
            return ""


# ─── RSS Feed Monitor ────────────────────────────────────────────────────────

class SEDARPlusMonitor:
    """
    Polls the SEDAR+ RSS feed and processes new major-deal filings.
    """

    def __init__(self):
        self.fetcher = PDFFetcher()
        self.new_signals: list[dict] = []
        self._seen: set[str] = set()   # dedup within a single run

    def run(self):
        log.info("[SEDAR] Polling RSS feed: %s", SEDAR_RSS_URL)
        feed = feedparser.parse(SEDAR_RSS_URL)

        for entry in feed.entries:
            self._process_entry(entry)
            time.sleep(0.5)   # polite crawl delay

        log.info("[SEDAR] Done. %d signals.", len(self.new_signals))
        return self.new_signals

    def _process_entry(self, entry):
        title     = getattr(entry, "title", "")
        link      = getattr(entry, "link", "")
        summary   = getattr(entry, "summary", "")
        published = getattr(entry, "published", "")

        filing_id = hashlib.md5(link.encode()).hexdigest()
        if filing_id in self._seen:
            return
        self._seen.add(filing_id)

        # Only process document types that carry counsel info
        combined  = f"{title} {summary}"
        is_major  = bool(_MAJOR_DOC_RE.search(combined))
        doc_type  = self._classify_doc(combined)

        if not is_major:
            return   # skip routine filings

        log.info("[SEDAR] Processing: %s", title[:80])

        # Try to get full text from the filing PDF
        pdf_text  = self.fetcher.fetch_text(link) if link.endswith(".pdf") else ""
        full_text = f"{combined}\n{pdf_text}"

        firm_ids   = extract_counsel_from_text(full_text)
        deal_value = parse_deal_value_cad(full_text)
        issuer     = self._extract_issuer(title)

        if not firm_ids:
            return  # no Calgary firm on this deal

        # ── Spillage graph ────────────────────────────────────────────────
        biglaw   = [f for f in firm_ids if f in BIGLAW_FIRMS]
        boutique = [f for f in firm_ids if f not in BIGLAW_FIRMS]
        for bl in biglaw:
            for bt in boutique:
                upsert_spillage_edge(bl, bt, source="sedar")

        # ── Store filing ──────────────────────────────────────────────────
        insert_sedar_filing({
            "filing_id":    filing_id,
            "issuer":       issuer,
            "doc_type":     doc_type,
            "filed_date":   published[:10] if published else "",
            "counsel_firms": firm_ids,
            "deal_value":   deal_value,
            "source_url":   link,
        })

        # ── Fire signals for each named firm ─────────────────────────────
        weight_base = SIGNAL_WEIGHTS["sedar_counsel_named"]
        if deal_value and deal_value >= 500:   # $500M+ = major deal
            weight_base = SIGNAL_WEIGHTS["sedar_major_deal"]
            sig_type = "sedar_major_deal"
        else:
            sig_type = "sedar_counsel_named"

        for firm_id in firm_ids:
            deal_str = f" (deal size: ~${deal_value:.0f}M)" if deal_value else ""
            msg = (f"{doc_type}: {issuer}{deal_str}. "
                   f"Firm named as counsel. "
                   f"High junior doc-review burden expected immediately.")
            self.new_signals.append({
                "firm_id":     firm_id,
                "signal_type": sig_type,
                "weight":      weight_base,
                "title":       f"SEDAR+ deal: {issuer[:50]} — {doc_type}",
                "description": msg,
                "source_url":  link,
                "raw_data":    {
                    "issuer": issuer,
                    "doc_type": doc_type,
                    "deal_value_m": deal_value,
                    "all_counsel_firms": firm_ids,
                },
            })
            insert_signal(
                firm_id=firm_id,
                signal_type=sig_type,
                weight=weight_base,
                title=f"SEDAR+ deal: {issuer[:50]} — {doc_type}",
                description=msg,
                source_url=link,
                raw_data={
                    "issuer": issuer,
                    "doc_type": doc_type,
                    "deal_value_m": deal_value,
                    "other_firms": firm_ids,
                },
            )
            log.info("[SEDAR] Signal → %s | %s | $%s M",
                     firm_id, sig_type, f"{deal_value:.0f}" if deal_value else "?")

    @staticmethod
    def _classify_doc(text: str) -> str:
        for t in SEDAR_COUNSEL_DOC_TYPES:
            if t.lower() in text.lower():
                return t
        return "filing"

    @staticmethod
    def _extract_issuer(title: str) -> str:
        """Best-effort issuer name from filing title."""
        # SEDAR titles often: "Issuer Name - Document Type"
        parts = re.split(r"\s[-–]\s", title, maxsplit=1)
        return parts[0].strip() if parts else title[:60]


# ─── Bonus: SEDAR full-text search endpoint (when available) ─────────────────

class SEDARSearchMonitor:
    """
    Uses SEDAR+'s search API (if/when publicly documented) to search for
    Calgary firm names directly in filed documents, without needing to
    download every PDF.

    Currently implemented as a polling stub; extend with the SEDAR+ API
    once the endpoint is publicly documented.
    """

    BASE_SEARCH = "https://www.sedarplus.ca/csa-party/records/search.html"

    def search_by_counsel(self, firm_name: str, days_back: int = 7) -> list[dict]:
        """
        Placeholder — calls SEDAR+ public search with the firm name.
        Returns a list of result dicts.
        """
        # SEDAR+ does not yet expose a programmatic search API for counsel.
        # This method provides the skeleton; fill in once documented.
        cutoff = (datetime.utcnow() - timedelta(days=days_back)).strftime("%Y-%m-%d")
        params = {
            "keyword":   firm_name,
            "dateFrom":  cutoff,
            "category":  "Prospectuses,Circulars,MaterialChanges",
        }
        try:
            resp = requests.get(self.BASE_SEARCH, params=params, timeout=15)
            # Parse HTML results here once layout is confirmed
            return []
        except Exception as e:
            log.debug("[SEDAR-Search] Error: %s", e)
            return []


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    monitor = SEDARPlusMonitor()
    signals = monitor.run()
    for s in signals:
        print(f"  [{s['signal_type']}] {s['firm_id']}: {s['title']}")
