"""
dedup_audit.py  —  Diagnose deduplication suppression.

Log observation:
  • 161 total signals in DB, 45 alerts fired historically.
  • This run: 728 scraper executions, only 1 "new" signal.
  • Pattern: every scraper shows "N signals (0 new)" — meaning signals
    ARE being found but ALL are being classified as duplicates.

Hypotheses:
  A) SIGNAL_LOOKBACK_DAYS=21 window is correct but the DB already has
     these exact signals from a prior run — correct dedup behavior.
  B) The URL/title hash is too coarse — slightly reformatted titles
     from the same article hash to the same key, collapsing new content.
  C) The dedup key doesn't include the scraper name, so the same article
     found by both PressScraper and PublicationsScraper collapses to 1.
  D) The discovered_at timestamp window check is using server time vs UTC,
     causing signals from the last few days to appear "old."

This module:
  1. Reports dedup rate per scraper per firm (suppression ratio).
  2. Shows the distribution of "age" of suppressed signals.
  3. Suggests whether to widen/narrow the dedup window.
  4. Can export a CSV of all signals for manual inspection.
"""

from __future__ import annotations

import csv
import io
import sqlite3
from collections import defaultdict
from datetime import datetime, timedelta, timezone

DB_PATH = "law_firm_tracker.db"


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def get_dedup_stats(lookback_days: int = 21) -> dict:
    """
    Returns per-scraper dedup statistics.
    Compares signals found vs signals marked as new within the lookback window.
    """
    conn = _connect()
    cutoff = (datetime.now(timezone.utc) - timedelta(days=lookback_days)).isoformat()
    try:
        total_by_scraper = defaultdict(int)
        new_by_scraper = defaultdict(int)
        age_buckets = defaultdict(lambda: defaultdict(int))

        rows = conn.execute(
            """
            SELECT scraper_name, firm, discovered_at,
                   CASE WHEN is_new = 1 THEN 1 ELSE 0 END as is_new
            FROM signals
            WHERE discovered_at >= ?
            ORDER BY discovered_at DESC
            """,
            (cutoff,),
        ).fetchall()

        now = datetime.now(timezone.utc)
        for row in rows:
            scraper = row["scraper_name"]
            total_by_scraper[scraper] += 1
            if row["is_new"]:
                new_by_scraper[scraper] += 1

            # Age bucket
            try:
                dt = datetime.fromisoformat(row["discovered_at"].replace("Z", "+00:00"))
                age_days = (now - dt).days
                if age_days <= 1:
                    bucket = "0-1d"
                elif age_days <= 7:
                    bucket = "2-7d"
                elif age_days <= 14:
                    bucket = "8-14d"
                else:
                    bucket = "15-21d"
                age_buckets[scraper][bucket] += 1
            except Exception:
                pass

    finally:
        conn.close()

    result = {}
    for scraper in total_by_scraper:
        total = total_by_scraper[scraper]
        new = new_by_scraper.get(scraper, 0)
        suppressed = total - new
        suppression_rate = suppressed / total if total > 0 else 0.0
        result[scraper] = {
            "total": total,
            "new": new,
            "suppressed": suppressed,
            "suppression_rate": round(suppression_rate, 3),
            "age_distribution": dict(age_buckets[scraper]),
        }

    return result


def print_dedup_report(lookback_days: int = 21) -> None:
    stats = get_dedup_stats(lookback_days)
    if not stats:
        print("No signals in database for the given window.")
        return

    print(f"\n{'='*70}")
    print(f"DEDUPLICATION AUDIT  —  last {lookback_days} days")
    print(f"{'='*70}")
    print(f"{'Scraper':<35} {'Total':>6} {'New':>6} {'Supp%':>7}  Age distribution")
    print(f"{'─'*70}")

    # Sort by suppression rate descending
    for scraper, s in sorted(stats.items(), key=lambda x: -x[1]["suppression_rate"]):
        age_str = "  ".join(
            f"{b}:{c}" for b, c in sorted(s["age_distribution"].items())
        )
        flag = " ⚠️" if s["suppression_rate"] == 1.0 and s["total"] >= 5 else ""
        print(
            f"{scraper:<35} {s['total']:>6} {s['new']:>6} "
            f"{s['suppression_rate']*100:>6.1f}%  {age_str}{flag}"
        )

    print(f"{'─'*70}")
    high_suppress = [s for s in stats.values() if s["suppression_rate"] == 1.0 and s["total"] >= 3]
    if high_suppress:
        print(f"\n⚠️  {len(high_suppress)} scrapers at 100% suppression with ≥ 3 signals.")
        print("   Likely causes:")
        print("   A) Dedup window too long — signals from a prior identical run")
        print("   B) URL/title hash too coarse — new content hashing to old key")
        print("   C) Try: python main.py --dedup-window 7 (narrow window temporarily)")


def find_near_duplicate_urls(similarity_threshold: float = 0.85) -> list[tuple]:
    """
    Finds pairs of signals whose URLs are very similar but not identical.
    This catches cases where a URL change (e.g. ?utm_source=X added) is
    causing the dedup to miss what is actually the same article.
    """
    conn = _connect()
    try:
        rows = conn.execute(
            "SELECT id, firm, scraper_name, url, title FROM signals ORDER BY firm, url"
        ).fetchall()
    finally:
        conn.close()

    # Group by firm, then find URL pairs with edit distance < threshold
    from difflib import SequenceMatcher
    near_dupes = []
    by_firm = defaultdict(list)
    for row in rows:
        by_firm[row["firm"]].append(row)

    for firm_signals in by_firm.values():
        for i, a in enumerate(firm_signals):
            for b in firm_signals[i+1:]:
                if a["scraper_name"] == b["scraper_name"]:
                    url_a = a["url"] or ""
                    url_b = b["url"] or ""
                    if url_a and url_b:
                        ratio = SequenceMatcher(None, url_a, url_b).ratio()
                        if similarity_threshold <= ratio < 1.0:
                            near_dupes.append((a["id"], b["id"], url_a, url_b, round(ratio, 3)))

    return near_dupes


def export_signals_csv(output_path: str = "signals_export.csv", lookback_days: int = 30) -> str:
    """Exports all signals to CSV for manual inspection."""
    conn = _connect()
    cutoff = (datetime.now(timezone.utc) - timedelta(days=lookback_days)).isoformat()
    try:
        rows = conn.execute(
            """
            SELECT id, firm, scraper_name, signal_type, score, title, url,
                   discovered_at, is_new
            FROM signals
            WHERE discovered_at >= ?
            ORDER BY discovered_at DESC
            """,
            (cutoff,),
        ).fetchall()
    finally:
        conn.close()

    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["id", "firm", "scraper", "type", "score", "title", "url", "discovered_at", "is_new"])
    for row in rows:
        writer.writerow(list(row))

    Path(output_path).write_text(buf.getvalue())
    return f"Exported {len(rows)} signals to {output_path}"


if __name__ == "__main__":
    from pathlib import Path
    print_dedup_report()
    near_dupes = find_near_duplicate_urls()
    if near_dupes:
        print(f"\nFound {len(near_dupes)} near-duplicate URL pairs (similarity ≥ 0.85):")
        for id_a, id_b, url_a, url_b, ratio in near_dupes[:10]:
            print(f"  {ratio:.0%}  {url_a[:60]}")
            print(f"       {url_b[:60]}")
