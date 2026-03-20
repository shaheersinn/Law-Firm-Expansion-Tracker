#!/usr/bin/env python3
"""
validate.py — Law Firm Expansion Tracker record validator.

Usage:
    python scripts/validate.py data/firms/2026/*.yaml
    python scripts/validate.py data/firms/          # validate all YAML files in tree
    python scripts/validate.py --json               # output JSON report

Exit codes:
    0  — no errors (warnings may exist)
    1  — one or more validation errors found

Validation Rules
----------------
Errors (block publication):
  - required fields missing
  - invalid field types
  - invalid enum values (expansion_type, confidence, status, source_type)
  - unknown practice_area values
  - invalid country code (not ISO 3166-1 alpha-2)
  - malformed dates (announced_date, effective_date)
  - malformed datetimes (created_at, last_modified)
  - malformed source_url (must start with http:// or https://)
  - invalid record_id pattern
  - duplicate record_id across all validated files
  - empty practice_areas array
  - self-reference in related_records
  - unknown additional properties

Warnings (noted but do not block):
  - announced_date older than 2 years while still in draft status
  - effective_date earlier than announced_date
  - confidence=unverified combined with status=published
  - future announced_date
  - missing city when country is known
  - notes longer than 500 characters
  - high-confidence record supported only by a weak source type
"""

import argparse
import json
import os
import re
import sys
from datetime import date, datetime, timezone
from pathlib import Path

try:
    import yaml
except ImportError:
    print("ERROR: PyYAML is required. Install with: pip install pyyaml", file=sys.stderr)
    sys.exit(2)

try:
    import jsonschema
    from jsonschema import validate as jsonschema_validate, ValidationError, Draft7Validator
except ImportError:
    print("ERROR: jsonschema is required. Install with: pip install jsonschema", file=sys.stderr)
    sys.exit(2)

# ── Paths ─────────────────────────────────────────────────────────────────────
REPO_ROOT = Path(__file__).resolve().parent.parent
SCHEMA_PATH = REPO_ROOT / "data" / "schema" / "expansion.schema.json"

# ── Weak source types for confidence-mismatch warning ─────────────────────────
WEAK_SOURCE_TYPES = {"job_posting", "social_media", "other"}


def load_schema() -> dict:
    """Load and return the JSON schema."""
    if not SCHEMA_PATH.exists():
        raise FileNotFoundError(f"Schema not found: {SCHEMA_PATH}")
    with open(SCHEMA_PATH, encoding="utf-8") as f:
        return json.load(f)


def load_record(path: Path) -> tuple[dict | None, str | None]:
    """Load a YAML record. Returns (record, error_message)."""
    try:
        with open(path, encoding="utf-8") as f:
            record = yaml.safe_load(f)
        if not isinstance(record, dict):
            return None, "File does not contain a YAML mapping."
        return record, None
    except yaml.YAMLError as exc:
        return None, f"YAML parse error: {exc}"
    except Exception as exc:
        return None, f"File read error: {exc}"


def validate_record(record: dict, schema: dict, path: Path) -> tuple[list[str], list[str]]:
    """
    Validate a single record against the schema plus custom rules.

    Returns:
        (errors, warnings) — lists of human-readable messages.
    """
    errors: list[str] = []
    warnings: list[str] = []

    # ── JSON Schema validation ─────────────────────────────────────────────
    validator = Draft7Validator(schema)
    for error in sorted(validator.iter_errors(record), key=lambda e: list(e.path)):
        path_str = " > ".join(str(p) for p in error.path) if error.path else "root"
        errors.append(f"[schema] {path_str}: {error.message}")

    # If schema errors are fatal (missing required fields), skip custom checks
    # for those fields to avoid cascading noise.
    record_id = record.get("record_id", "")
    announced_raw = record.get("announced_date", "")
    effective_raw = record.get("effective_date", "")
    status = record.get("status", "")
    confidence = record.get("confidence", "")
    source_type = record.get("source_type", "")
    notes = record.get("notes", "")
    related = record.get("related_records", [])

    today = date.today()

    # ── Custom: dates ──────────────────────────────────────────────────────
    announced_date: date | None = None
    if announced_raw:
        try:
            announced_date = date.fromisoformat(announced_raw)
        except ValueError:
            errors.append(f"[date] announced_date is not a valid ISO 8601 date: {announced_raw!r}")

    effective_date: date | None = None
    if effective_raw:
        try:
            effective_date = date.fromisoformat(effective_raw)
        except ValueError:
            errors.append(f"[date] effective_date is not a valid ISO 8601 date: {effective_raw!r}")

    if announced_date and effective_date:
        if effective_date < announced_date:
            warnings.append(
                f"[warn] effective_date ({effective_raw}) is earlier than "
                f"announced_date ({announced_raw})"
            )

    if announced_date and announced_date > today:
        warnings.append(f"[warn] announced_date ({announced_raw}) is in the future")

    two_years_ago = date(today.year - 2, today.month, today.day)
    if announced_date and announced_date < two_years_ago and status == "draft":
        warnings.append(
            f"[warn] announced_date ({announced_raw}) is older than 2 years "
            f"and record is still in draft status"
        )

    # ── Custom: self-reference in related_records ──────────────────────────
    if record_id and isinstance(related, list):
        if record_id in related:
            errors.append(
                f"[logic] related_records contains the record's own record_id: {record_id!r}"
            )

    # ── Custom: unverified + published ────────────────────────────────────
    if confidence == "unverified" and status == "published":
        warnings.append(
            "[warn] confidence=unverified combined with status=published — "
            "manual review strongly recommended"
        )

    # ── Custom: high-confidence + weak source ─────────────────────────────
    if confidence in ("confirmed", "high") and source_type in WEAK_SOURCE_TYPES:
        warnings.append(
            f"[warn] confidence={confidence!r} but source_type={source_type!r} "
            f"is a weak source — consider upgrading source or downgrading confidence"
        )

    # ── Custom: notes length ──────────────────────────────────────────────
    if notes and len(notes) > 500:
        warnings.append(
            f"[warn] notes is {len(notes)} characters; 500 is recommended maximum"
        )

    # ── Custom: missing city ───────────────────────────────────────────────
    if record.get("country") and not record.get("city"):
        warnings.append(
            "[warn] country is set but city is missing — add city if known"
        )

    return errors, warnings


def collect_yaml_files(paths: list[str]) -> list[Path]:
    """Expand directories and return all .yaml/.yml files."""
    result: list[Path] = []
    for p in paths:
        target = Path(p)
        if target.is_dir():
            result.extend(sorted(target.rglob("*.yaml")))
            result.extend(sorted(target.rglob("*.yml")))
        elif target.is_file():
            result.append(target)
        else:
            print(f"WARNING: Path not found: {p}", file=sys.stderr)
    return result


def run_validation(
    file_paths: list[str],
    output_json: bool = False,
) -> dict:
    """
    Validate all specified files.

    Returns a summary dict:
      {
        "total": int,
        "passed": int,
        "failed": int,
        "results": [
          {"file": str, "record_id": str|None, "errors": [...], "warnings": [...]}
        ]
      }
    """
    schema = load_schema()
    files = collect_yaml_files(file_paths)

    if not files:
        msg = "No YAML files found to validate."
        if output_json:
            print(json.dumps({"total": 0, "passed": 0, "failed": 0, "results": [], "message": msg}))
        else:
            print(msg)
        return {"total": 0, "passed": 0, "failed": 0, "results": []}

    seen_record_ids: dict[str, str] = {}  # record_id -> first-seen file path
    results = []

    for fpath in files:
        record, load_error = load_record(fpath)
        rel_path = str(fpath.relative_to(REPO_ROOT) if (fpath.is_absolute() and fpath.is_relative_to(REPO_ROOT)) else fpath)

        if load_error:
            results.append({
                "file": rel_path,
                "record_id": None,
                "errors": [f"[load] {load_error}"],
                "warnings": [],
            })
            continue

        errors, warnings = validate_record(record, schema, fpath)

        # ── Duplicate record_id check ─────────────────────────────────────
        rid = record.get("record_id")
        if rid:
            if rid in seen_record_ids:
                errors.append(
                    f"[duplicate] record_id {rid!r} already seen in {seen_record_ids[rid]}"
                )
            else:
                seen_record_ids[rid] = rel_path

        results.append({
            "file": rel_path,
            "record_id": rid,
            "errors": errors,
            "warnings": warnings,
        })

    total = len(results)
    failed = sum(1 for r in results if r["errors"])
    passed = total - failed

    summary = {
        "total": total,
        "passed": passed,
        "failed": failed,
        "results": results,
    }

    if output_json:
        print(json.dumps(summary, indent=2, ensure_ascii=False))
    else:
        _print_report(summary)

    return summary


def _print_report(summary: dict) -> None:
    """Print a human-readable validation report."""
    total = summary["total"]
    passed = summary["passed"]
    failed = summary["failed"]

    print(f"\n{'=' * 60}")
    print(f"  Law Firm Expansion Tracker — Validation Report")
    print(f"{'=' * 60}")
    print(f"  Files:   {total}")
    print(f"  Passed:  {passed}")
    print(f"  Failed:  {failed}")
    print()

    for r in summary["results"]:
        status_icon = "✅" if not r["errors"] else "❌"
        rid = r["record_id"] or "(no record_id)"
        print(f"  {status_icon} {r['file']}  [{rid}]")
        for err in r["errors"]:
            print(f"       ERROR: {err}")
        for warn in r["warnings"]:
            print(f"       WARN:  {warn}")

    print()
    if failed == 0:
        print(f"  ✅ All {total} records passed validation.")
    else:
        print(f"  ❌ {failed} of {total} records have errors.")
    print(f"{'=' * 60}\n")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Validate Law Firm Expansion Tracker YAML records.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "paths",
        nargs="*",
        default=["data/firms/"],
        help="YAML files or directories to validate (default: data/firms/)",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        dest="output_json",
        help="Output results as JSON",
    )
    args = parser.parse_args()

    # Resolve relative paths from repo root
    resolved = []
    for p in args.paths:
        if Path(p).is_absolute():
            resolved.append(p)
        else:
            resolved.append(str(REPO_ROOT / p))

    try:
        summary = run_validation(resolved, output_json=args.output_json)
    except FileNotFoundError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2

    return 0 if summary["failed"] == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
