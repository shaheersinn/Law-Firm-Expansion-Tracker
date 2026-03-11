#!/usr/bin/env python3
"""
summarize.py — Generate summary reports from expansion records.

Usage:
    python scripts/summarize.py                         # summary of data/firms/
    python scripts/summarize.py data/firms/2026/        # summary of a specific year
    python scripts/summarize.py --format csv > out.csv  # CSV export
    python scripts/summarize.py --format json           # JSON export
    python scripts/summarize.py --weekly                # weekly digest (last 7 days)

Outputs:
  - Count by expansion_type
  - Count by practice_area
  - Count by country
  - Count by status
  - Count by confidence
  - Recent entries (last 7 days if --weekly)
"""

import argparse
import csv
import io
import json
import sys
from collections import Counter, defaultdict
from datetime import date, timedelta
from pathlib import Path

try:
    import yaml
except ImportError:
    print("ERROR: PyYAML is required. Install with: pip install pyyaml", file=sys.stderr)
    sys.exit(2)

REPO_ROOT = Path(__file__).resolve().parent.parent


def load_records(paths: list[str]) -> list[dict]:
    """Load all YAML records."""
    records = []
    for p_str in paths:
        p = Path(p_str)
        files = sorted(p.rglob("*.yaml")) + sorted(p.rglob("*.yml")) if p.is_dir() else [p]
        for f in files:
            try:
                with open(f, encoding="utf-8") as fh:
                    record = yaml.safe_load(fh)
                if isinstance(record, dict):
                    records.append(record)
            except Exception:
                pass
    return records


def build_summary(records: list[dict], since: date | None = None) -> dict:
    """Build a summary dict from records."""
    if since:
        records = [
            r for r in records
            if _parse_date(r.get("announced_date")) and _parse_date(r.get("announced_date")) >= since
        ]

    by_type = Counter(r.get("expansion_type", "unknown") for r in records)
    by_country = Counter(r.get("country", "unknown") for r in records)
    by_status = Counter(r.get("status", "unknown") for r in records)
    by_confidence = Counter(r.get("confidence", "unknown") for r in records)

    area_counter: Counter = Counter()
    for r in records:
        for area in (r.get("practice_areas") or []):
            area_counter[area] += 1

    return {
        "total": len(records),
        "by_expansion_type": dict(by_type.most_common()),
        "by_practice_area": dict(area_counter.most_common()),
        "by_country": dict(by_country.most_common()),
        "by_status": dict(by_status.most_common()),
        "by_confidence": dict(by_confidence.most_common()),
        "records": records,
    }


def _parse_date(value: str | None) -> date | None:
    if not value:
        return None
    try:
        return date.fromisoformat(str(value))
    except (ValueError, TypeError):
        return None


def print_text_report(summary: dict, weekly: bool = False) -> None:
    header = "Weekly Digest" if weekly else "Summary Report"
    print(f"\n{'=' * 60}")
    print(f"  Law Firm Expansion Tracker — {header}")
    print(f"{'=' * 60}")
    print(f"  Total records: {summary['total']}")

    def _section(title: str, data: dict) -> None:
        if not data:
            return
        print(f"\n  {title}:")
        for k, v in sorted(data.items(), key=lambda x: -x[1]):
            print(f"    {k:<40} {v:>4}")

    _section("By Expansion Type", summary["by_expansion_type"])
    _section("By Practice Area", summary["by_practice_area"])
    _section("By Country", summary["by_country"])
    _section("By Status", summary["by_status"])
    _section("By Confidence", summary["by_confidence"])

    if weekly and summary.get("records"):
        print(f"\n  Recent Entries:")
        for r in sorted(summary["records"], key=lambda x: x.get("announced_date", ""), reverse=True):
            print(f"    [{r.get('announced_date', '?')}] {r.get('firm_name', '?')} — "
                  f"{r.get('expansion_type', '?')} ({r.get('country', '?')})")

    print(f"\n{'=' * 60}\n")


def export_csv(records: list[dict]) -> str:
    """Export records to CSV string."""
    fields = [
        "record_id", "firm_name", "expansion_type", "country", "city",
        "announced_date", "effective_date", "source_type", "confidence",
        "status", "headcount", "notes",
    ]
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=fields, extrasaction="ignore")
    writer.writeheader()
    for r in records:
        row = {k: r.get(k, "") for k in fields}
        # Flatten practice_areas to comma-separated
        row["expansion_type"] = r.get("expansion_type", "")
        writer.writerow(row)
    return buf.getvalue()


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Generate summary reports from expansion records.",
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
        "--format",
        choices=["text", "json", "csv"],
        default="text",
        help="Output format (default: text)",
    )
    parser.add_argument(
        "--weekly",
        action="store_true",
        help="Only include records from the last 7 days",
    )
    args = parser.parse_args()

    resolved = [
        str(REPO_ROOT / p) if not Path(p).is_absolute() else p
        for p in args.paths
    ]
    records = load_records(resolved)
    since = date.today() - timedelta(days=7) if args.weekly else None
    summary = build_summary(records, since=since)

    if args.format == "json":
        # Exclude raw records from JSON output for size
        output = {k: v for k, v in summary.items() if k != "records"}
        print(json.dumps(output, indent=2, ensure_ascii=False))
    elif args.format == "csv":
        print(export_csv(summary["records"]))
    else:
        print_text_report(summary, weekly=args.weekly)

    return 0


if __name__ == "__main__":
    sys.exit(main())
