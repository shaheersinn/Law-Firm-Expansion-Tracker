"""
Website Change Monitor
=======================
Tracks changes to firm practice area pages and attorney profiles.
A new practice area page = firm committed to marketing that department.
A new bio that lists a new practice = lawyer expanding focus.

Also snapshots page content for change detection (hash comparison).
"""

import re
import hashlib
from bs4 import BeautifulSoup
from scrapers.base import BaseScraper
from classifier.department import DepartmentClassifier

classifier = DepartmentClassifier()

PRACTICE_PATH_VARIANTS = [
    "/services", "/practices", "/practice-areas", "/expertise",
    "/en/services", "/en/practices", "/en/expertise",
    "/en-ca/services",
]

ATTORNEY_PATH_VARIANTS = [
    "/lawyers", "/attorneys", "/people", "/our-team",
    "/en/lawyers", "/en/people", "/professionals",
]


class WebsiteScraper(BaseScraper):
    name = "WebsiteScraper"

    def fetch(self, firm: dict) -> list[dict]:
        signals = []
        signals.extend(self._scrape_practice_pages(firm))
        signals.extend(self._scrape_attorney_profiles(firm))
        return signals

    def _scrape_practice_pages(self, firm: dict) -> list[dict]:
        signals = []
        base = firm["website"].rstrip("/")

        for path in PRACTICE_PATH_VARIANTS:
            url = base + path
            resp = self._get(url)
            if not resp:
                continue

            soup = BeautifulSoup(resp.text, "html.parser")

            # Snapshot for change detection
            page_text = soup.get_text(separator=" ", strip=True)
            page_hash = hashlib.md5(page_text.encode()).hexdigest()
            signals.append(self._make_signal(
                firm_id=firm["id"],
                firm_name=firm["name"],
                signal_type="website_snapshot",
                title=f"[Snapshot] {firm['short']} — {url}",
                body=page_hash,
                url=url,
                department="",
                department_score=0,
                matched_keywords=[],
            ))

            # Find individual practice area links
            practice_links = [
                a for a in soup.find_all("a", href=True)
                if any(seg in a.get("href", "").lower()
                       for seg in ["/service/", "/practice/", "/expertise/", "/area/"])
                and len(a.get_text(strip=True)) > 4
            ]

            for link in practice_links[:30]:
                practice_name = link.get_text(strip=True)
                practice_url  = link["href"]
                if practice_url.startswith("/"):
                    practice_url = base + practice_url

                p_resp = self._get(practice_url)
                if not p_resp:
                    continue

                p_soup = BeautifulSoup(p_resp.text, "html.parser")
                p_text = p_soup.get_text(separator=" ", strip=True)[:2000]

                classifications = classifier.classify(f"{practice_name} {p_text}", top_n=1)
                if not classifications:
                    continue

                cls = classifications[0]
                if cls["score"] < 2.0:
                    continue

                signals.append(self._make_signal(
                    firm_id=firm["id"],
                    firm_name=firm["name"],
                    signal_type="practice_page",
                    title=f"[Practice Page] {firm['short']} — {practice_name}",
                    body=p_text[:600],
                    url=practice_url,
                    department=cls["department"],
                    department_score=cls["score"],
                    matched_keywords=cls["matched_keywords"],
                ))

            break  # found working practice page path

        self.logger.info(f"[{firm['short']}] Practice pages: {len([s for s in signals if s['signal_type']=='practice_page'])} signal(s)")
        return signals

    def _scrape_attorney_profiles(self, firm: dict) -> list[dict]:
        signals = []
        base = firm["website"].rstrip("/")

        for path in ATTORNEY_PATH_VARIANTS:
            url = base + path
            resp = self._get(url)
            if not resp:
                continue

            soup = BeautifulSoup(resp.text, "html.parser")
            # Find recently updated bio links (usually sorted by date or show "New")
            new_bio_markers = soup.find_all(
                string=re.compile(r"\bnew\b|\brecent\b|\bjoined\b|\bwelcome\b", re.I)
            )

            for marker in new_bio_markers[:5]:
                parent = marker.parent
                link_tag = parent.find_parent("a") if parent else None
                if not link_tag or not link_tag.get("href"):
                    continue

                bio_url = link_tag["href"]
                if bio_url.startswith("/"):
                    bio_url = base + bio_url

                bio_resp = self._get(bio_url)
                if not bio_resp:
                    continue

                bio_soup = BeautifulSoup(bio_resp.text, "html.parser")
                bio_text = bio_soup.get_text(separator=" ", strip=True)[:2000]

                classifications = classifier.classify(bio_text, top_n=2)
                for cls in classifications[:1]:
                    if cls["score"] < 1.5:
                        continue
                    name_tag = bio_soup.find(["h1", "h2"])
                    name = name_tag.get_text(strip=True) if name_tag else "Attorney"
                    signals.append(self._make_signal(
                        firm_id=firm["id"],
                        firm_name=firm["name"],
                        signal_type="attorney_profile",
                        title=f"[New Bio] {firm['short']} — {name}",
                        body=bio_text[:600],
                        url=bio_url,
                        department=cls["department"],
                        department_score=cls["score"],
                        matched_keywords=cls["matched_keywords"],
                    ))
            break

        self.logger.info(f"[{firm['short']}] Attorney profiles: {len(signals)} signal(s)")
        return signals
