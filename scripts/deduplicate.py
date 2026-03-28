#!/usr/bin/env python3
"""
deduplicate.py — Detect likely duplicate expansion records.

Usage:
    python scripts/deduplicate.py data/firms/2026/*.yaml
    python scripts/deduplicate.py data/firms/          # all YAML files in tree
    python scripts/deduplicate.py --json               # output JSON report

Duplicate Detection Strategy
-----------------------------
Records are flagged as likely duplicates when they share:
  - Same firm_name (case-insensitive, trimmed)
  - Same expansion_type
  - Same country
  - Overlapping practice_areas (at least 1 in common)
  - announced_date within 30 days of each other

Near-duplicate criteria also considers:
  - Same city (if both records have it)
  - Same source URL

Scoring:
  Each matching criterion adds points:
    firm_name match:      +30
    expansion_type match: +20
    country match:        +15
    city match:           +15
    practice_area overlap:+10 per shared area (max +30)
    date proximity:
      same date:          +20
      within 7 days:      +15
      within 30 days:     +10
    same source_url:      +20

  Threshold for flagging:
    >= 80 points: "likely_duplicate" (high concern)
    >= 50 points: "possible_duplicate" (needs review)

Actions:
  - Never auto-deletes records
  - Flags candidates for manual review
  - Outputs reason with score

Exit codes:
    0  — no duplicates found
    1  — duplicates detected
"""

import argparse
import json
import sys
from datetime import date, timedelta
from pathlib import Path

try:
    import yaml
except ImportError:
    print("ERROR: PyYAML is required. Install with: pip install pyyaml", file=sys.stderr)
    sys.exit(2)

REPO_ROOT = Path(__file__).resolve().parent.parent

LIKELY_THRESHOLD = 80
POSSIBLE_THRESHOLD = 50


def load_records(paths: list[str]) -> list[tuple[Path, dict]]:
    """Load all YAML records from files/directories."""
    records = []
    for p_str in paths:
        p = Path(p_str)
        if p.is_dir():
            files = sorted(p.rglob("*.yaml")) + sorted(p.rglob("*.yml"))
        elif p.is_file():
            files = [p]
        else:
            print(f"WARNING: Path not found: {p}", file=sys.stderr)
            continue
        for f in files:
            try:
                with open(f, encoding="utf-8") as fh:
                    record = yaml.safe_load(fh)
                if isinstance(record, dict):
                    records.append((f, record))
            except Exception as exc:
                print(f"WARNING: Could not load {f}: {exc}", file=sys.stderr)
    return records


def score_pair(a: dict, b: dict) -> tuple[int, list[str]]:
    """
    Score similarity between two records.

    Returns (score, reasons).
    """
    score = 0
    reasons = []

    # firm_name
    a_firm = (a.get("firm_name") or "").strip().lower()
    b_firm = (b.get("firm_name") or "").strip().lower()
    if a_firm and b_firm and a_firm == b_firm:
        score += 30
        reasons.append("same firm_name")

    # expansion_type
    if a.get("expansion_type") and a.get("expansion_type") == b.get("expansion_type"):
        score += 20
        reasons.append("same expansion_type")

    # country
    if a.get("country") and a.get("country") == b.get("country"):
        score += 15
        reasons.append("same country")

    # city
    a_city = (a.get("city") or "").strip().lower()
    b_city = (b.get("city") or "").strip().lower()
    if a_city and b_city and a_city == b_city:
        score += 15
        reasons.append("same city")

    # practice_areas overlap
    a_areas = set(a.get("practice_areas") or [])
    b_areas = set(b.get("practice_areas") or [])
    overlap = a_areas & b_areas
    if overlap:
        area_pts = min(len(overlap) * 10, 30)
        score += area_pts
        reasons.append(f"overlapping practice_areas: {sorted(overlap)}")

    # announced_date proximity
    try:
        a_date = date.fromisoformat(a.get("announced_date", ""))
        b_date = date.fromisoformat(b.get("announced_date", ""))
        delta = abs((a_date - b_date).days)
        if delta == 0:
            score += 20
            reasons.append("same announced_date")
        elif delta <= 7:
            score += 15
            reasons.append(f"announced_date within {delta} days")
        elif delta <= 30:
            score += 10
            reasons.append(f"announced_date within {delta} days")
    except (ValueError, TypeError):
        pass

    # same source_url
    a_url = (a.get("source_url") or "").strip()
    b_url = (b.get("source_url") or "").strip()
    if a_url and b_url and a_url == b_url:
        score += 20
        reasons.append("same source_url")

    return score, reasons


def detect_duplicates(records: list[tuple[Path, dict]]) -> list[dict]:
    """
    Compare all record pairs.

    Returns list of duplicate-candidate dicts.
    Skips pairs where one record already lists the other in its related_records
    (these are explicitly modelled as related events, not duplicates).
    """
    candidates = []
    n = len(records)
    for i in range(n):
        for j in range(i + 1, n):
            path_a, rec_a = records[i]
            path_b, rec_b = records[j]

            # Skip if already explicitly linked as related records
            id_a = rec_a.get("record_id") or ""
            id_b = rec_b.get("record_id") or ""
            related_a = set(rec_a.get("related_records") or [])
            related_b = set(rec_b.get("related_records") or [])
            if id_b in related_a or id_a in related_b:
                continue

            score, reasons = score_pair(rec_a, rec_b)

            if score >= POSSIBLE_THRESHOLD:
                level = "likely_duplicate" if score >= LIKELY_THRESHOLD else "possible_duplicate"
                candidates.append({
                    "level": level,
                    "score": score,
                    "record_a": {
                        "file": str(path_a),
                        "record_id": rec_a.get("record_id"),
                    },
                    "record_b": {
                        "file": str(path_b),
                        "record_id": rec_b.get("record_id"),
                    },
                    "reasons": reasons,
                    "action": "FLAG_FOR_REVIEW",
                })
    return candidates


def _print_report(candidates: list[dict], total_records: int) -> None:
    print(f"\n{'=' * 60}")
    print(f"  Law Firm Expansion Tracker — Deduplication Report")
    print(f"{'=' * 60}")
    print(f"  Records scanned: {total_records}")
    print(f"  Candidate pairs: {len(candidates)}")
    likely = sum(1 for c in candidates if c["level"] == "likely_duplicate")
    possible = sum(1 for c in candidates if c["level"] == "possible_duplicate")
    print(f"    Likely duplicates:   {likely}")
    print(f"    Possible duplicates: {possible}")
    print()

    for c in sorted(candidates, key=lambda x: -x["score"]):
        icon = "🔴" if c["level"] == "likely_duplicate" else "🟡"
        print(f"  {icon} [{c['level'].upper()}]  score={c['score']}")
        print(f"     A: {c['record_a']['file']}  [{c['record_a']['record_id']}]")
        print(f"     B: {c['record_b']['file']}  [{c['record_b']['record_id']}]")
        print(f"     Reasons: {'; '.join(c['reasons'])}")
        print(f"     Action:  {c['action']}")
        print()

    if not candidates:
        print("  ✅ No duplicate candidates found.")
    else:
        print("  ⚠  Review all flagged pairs before publishing.")
    print(f"{'=' * 60}\n")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Detect likely duplicate Law Firm Expansion records.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "paths",
        nargs="*",
        default=["data/firms/"],
        help="YAML files or directories (default: data/firms/)",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        dest="output_json",
        help="Output results as JSON",
    )
    args = parser.parse_args()

    resolved = []
    for p in args.paths:
        if Path(p).is_absolute():
            resolved.append(p)
        else:
            resolved.append(str(REPO_ROOT / p))

    records = load_records(resolved)
    candidates = detect_duplicates(records)

    if args.output_json:
        print(json.dumps({
            "total_records": len(records),
            "candidates": candidates,
        }, indent=2, ensure_ascii=False))
    else:
        _print_report(candidates, len(records))

    return 0 if not candidates else 1


if __name__ == "__main__":
    sys.exit(main())
