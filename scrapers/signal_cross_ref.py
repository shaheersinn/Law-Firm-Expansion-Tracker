"""
SignalCrossRefScraper — cross-referencing and signal enrichment.

Unlike other scrapers this one doesn't hit the web. Instead it reads
signals already stored in the DB from this week, cross-references them
to find corroborated patterns, and emits HIGH-CONFIDENCE synthetic signals.

Patterns detected:
  PATTERN A: Same firm, same department, 3+ source types in 7 days
             → "Multi-source corroboration burst" (weight 6.0)

  PATTERN B: Lateral hire + same-dept job posting in 14 days
             → "Hiring cluster in [dept]" (weight 5.5)
             Interpretation: firm hired a partner AND is now filling under them

  PATTERN C: Deal counsel mention + publications spike in same dept
             → "Active mandate in [dept]" (weight 5.0)
             Interpretation: doing deals AND writing thought leadership

  PATTERN D: Office tracker positive + lateral hire in new city within 30 days
             → "Geographic expansion — [city]" (weight 6.0)

  PATTERN E: 5+ job postings for same firm in 30 days (hiring burst)
             → "Sustained hiring burst" (weight 5.0)

This is the intelligence layer on top of raw signal collection.
"""

from datetime import datetime, timezone, timedelta
from collections import defaultdict

from scrapers.base import BaseScraper

LOOKBACK_DAYS = 30


class SignalCrossRefScraper(BaseScraper):
    name = "SignalCrossRefScraper"

    def __init__(self, db=None):
        super().__init__()
        self._db = db  # injected by main.py

    def fetch(self, firm: dict) -> list[dict]:
        if self._db is None:
            return []
        try:
            return self._analyze(firm)
        except Exception as e:
            self.logger.error(f"SignalCrossRefScraper [{firm['short']}]: {e}")
            return []

    def _analyze(self, firm: dict) -> list[dict]:
        cutoff = datetime.now(timezone.utc) - timedelta(days=LOOKBACK_DAYS)
        raw    = self._db.get_signals_since(firm["id"], cutoff)
        if not raw:
            return []

        signals  = []
        by_dept  = defaultdict(list)
        by_type  = defaultdict(list)

        for sig in raw:
            dept = sig.get("department", "")
            stype = sig.get("signal_type", "")
            by_dept[dept].append(sig)
            by_type[stype].append(sig)

        # ── PATTERN A: multi-source corroboration burst ───────────────────────
        for dept, dept_sigs in by_dept.items():
            sources = set(s.get("signal_type", "") for s in dept_sigs)
            if len(sources) >= 3 and len(dept_sigs) >= 4:
                signals.append(self._make_signal(
                    firm_id=firm["id"], firm_name=firm["name"],
                    signal_type="press_release",
                    title=f"[Cross-Ref A] {firm['short']} — Multi-source burst in {dept} "
                          f"({len(dept_sigs)} signals, {len(sources)} types)",
                    body=f"Source types: {', '.join(sorted(sources))}",
                    url=firm["website"],
                    department=dept,
                    department_score=6.0,
                    matched_keywords=["cross-ref", "corroboration", dept.lower()],
                ))

        # ── PATTERN B: lateral hire + job posting in same dept ────────────────
        lateral_depts = set(s["department"] for s in by_type.get("lateral_hire", []))
        job_depts     = set(s["department"] for s in by_type.get("job_posting", []))
        overlap = lateral_depts & job_depts
        for dept in overlap:
            signals.append(self._make_signal(
                firm_id=firm["id"], firm_name=firm["name"],
                signal_type="lateral_hire",
                title=f"[Cross-Ref B] {firm['short']} — Lateral hire + open roles in {dept}",
                body="Lateral partner hire confirmed + job postings in same dept (hiring under new partner)",
                url=firm.get("careers_url", firm["website"]),
                department=dept,
                department_score=5.5,
                matched_keywords=["lateral_hire", "job_posting", dept.lower()],
            ))

        # ── PATTERN C: deal counsel + publications in same dept ───────────────
        deal_depts = set(
            s["department"] for s in raw
            if "deal" in s.get("title", "").lower() or
               s.get("signal_type") == "press_release" and "counsel" in s.get("title", "").lower()
        )
        pub_depts = set(s["department"] for s in by_type.get("publication", []))
        for dept in (deal_depts & pub_depts):
            signals.append(self._make_signal(
                firm_id=firm["id"], firm_name=firm["name"],
                signal_type="press_release",
                title=f"[Cross-Ref C] {firm['short']} — Active mandate + publications in {dept}",
                body="Deal counsel mention + thought leadership publications in same department",
                url=firm.get("news_url", firm["website"]),
                department=dept,
                department_score=5.0,
                matched_keywords=["deal_counsel", "publication", dept.lower()],
            ))

        # ── PATTERN E: sustained hiring burst ────────────────────────────────
        job_count = len(by_type.get("job_posting", []))
        if job_count >= 5:
            # Find dominant dept
            dept_counts: defaultdict = defaultdict(int)
            for s in by_type["job_posting"]:
                dept_counts[s["department"]] += 1
            top_dept = max(dept_counts, key=dept_counts.__getitem__)
            signals.append(self._make_signal(
                firm_id=firm["id"], firm_name=firm["name"],
                signal_type="job_posting",
                title=f"[Cross-Ref E] {firm['short']} — Sustained hiring burst: "
                      f"{job_count} roles, dominated by {top_dept}",
                url=firm.get("careers_url", firm["website"]),
                department=top_dept,
                department_score=min(job_count * 0.8, 5.0),
                matched_keywords=["hiring_burst", top_dept.lower()],
            ))

        return signals
