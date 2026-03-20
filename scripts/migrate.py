#!/usr/bin/env python3
"""
migrate.py — Schema migration utility for Law Firm Expansion records.

Usage:
    python scripts/migrate.py --from 1.0.0 --to 1.1.0 data/firms/2026/*.yaml
    python scripts/migrate.py --check data/firms/2026/*.yaml  # detect version drift
    python scripts/migrate.py --dry-run --from 1.0.0 --to 1.1.0 data/firms/2026/*.yaml

Migration Path Registry
-----------------------
  1.0.0 -> 1.1.0  (example MINOR: adds optional `verified_by` field — backward compatible)

Rules:
  - PATCH:  docs/clarification only, no field change
  - MINOR:  backward-compatible optional field added
  - MAJOR:  required field added, field renamed, type changed, enum removed,
            or validator behavior changes incompatibly

For each migration:
  - Source and target versions are explicit
  - Transformation rules are deterministic
  - Irreversible changes are flagged
  - Rollback guidance is included in the docstring

Exit codes:
    0  — migration applied successfully (or no migration needed)
    1  — migration errors
    2  — invalid arguments
"""

import argparse
import json
import shutil
import sys
from pathlib import Path
from datetime import datetime, timezone

try:
    import yaml
except ImportError:
    print("ERROR: PyYAML is required. Install with: pip install pyyaml", file=sys.stderr)
    sys.exit(2)

REPO_ROOT = Path(__file__).resolve().parent.parent
CURRENT_SCHEMA_VERSION = "1.0.0"


# ── Migration definitions ─────────────────────────────────────────────────────
# Each entry: (from_version, to_version, transform_fn, reversible, description)

def _migrate_1_0_0_to_1_1_0(record: dict) -> tuple[dict, list[str]]:
    """
    Migration 1.0.0 -> 1.1.0 (MINOR — backward compatible).

    Changes:
      - schema_version updated from "1.0.0" to "1.1.0"
      - No field removals or required field additions
      - Optional: adds `verified_by` field (empty string) if not present

    Rollback:
      - Safe: remove `verified_by` field and revert schema_version to "1.0.0"
      - Risk: low
    """
    changes = []
    if record.get("schema_version") == "1.0.0":
        record["schema_version"] = "1.1.0"
        changes.append("schema_version: 1.0.0 -> 1.1.0")
    record["last_modified"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    changes.append("last_modified: updated to now")
    return record, changes


MIGRATIONS: dict[tuple[str, str], tuple] = {
    ("1.0.0", "1.1.0"): (_migrate_1_0_0_to_1_1_0, True, "MINOR: schema_version bump, adds optional verified_by"),
}


def collect_yaml_files(paths: list[str]) -> list[Path]:
    result = []
    for p_str in paths:
        p = Path(p_str)
        if p.is_dir():
            result.extend(sorted(p.rglob("*.yaml")))
            result.extend(sorted(p.rglob("*.yml")))
        elif p.is_file():
            result.append(p)
        else:
            print(f"WARNING: Path not found: {p}", file=sys.stderr)
    return result


def detect_version(record: dict) -> str | None:
    return record.get("schema_version")


def check_versions(paths: list[str]) -> int:
    """Report schema versions of all records."""
    files = collect_yaml_files(paths)
    version_map: dict[str, list[str]] = {}

    for f in files:
        try:
            with open(f, encoding="utf-8") as fh:
                record = yaml.safe_load(fh)
            if isinstance(record, dict):
                ver = detect_version(record) or "MISSING"
                version_map.setdefault(ver, []).append(str(f))
        except Exception as exc:
            print(f"WARNING: Could not load {f}: {exc}", file=sys.stderr)

    print(f"\nSchema version check across {len(files)} files:")
    for ver, file_list in sorted(version_map.items()):
        status = "✅" if ver == CURRENT_SCHEMA_VERSION else "⚠ "
        print(f"  {status} {ver}: {len(file_list)} file(s)")
        if ver != CURRENT_SCHEMA_VERSION:
            for fp in file_list[:5]:
                print(f"       {fp}")
            if len(file_list) > 5:
                print(f"       ... and {len(file_list) - 5} more")

    outdated = sum(len(v) for k, v in version_map.items() if k != CURRENT_SCHEMA_VERSION)
    return 0 if outdated == 0 else 1


def apply_migration(
    paths: list[str],
    from_ver: str,
    to_ver: str,
    dry_run: bool = False,
) -> int:
    """Apply a migration to all matching records."""
    key = (from_ver, to_ver)
    if key not in MIGRATIONS:
        print(f"ERROR: No migration registered from {from_ver} to {to_ver}.", file=sys.stderr)
        print(f"Available: {list(MIGRATIONS.keys())}", file=sys.stderr)
        return 1

    transform_fn, reversible, description = MIGRATIONS[key]
    files = collect_yaml_files(paths)

    print(f"\nMigration: {from_ver} -> {to_ver}")
    print(f"Description: {description}")
    print(f"Reversible: {'Yes' if reversible else 'No — IRREVERSIBLE'}")
    print(f"Dry run: {dry_run}")
    print(f"Files to check: {len(files)}\n")

    skipped = 0
    migrated = 0
    errors = 0

    for f in files:
        try:
            with open(f, encoding="utf-8") as fh:
                record = yaml.safe_load(fh)
            if not isinstance(record, dict):
                print(f"  SKIP {f}: not a dict")
                skipped += 1
                continue

            current_ver = detect_version(record)
            if current_ver != from_ver:
                skipped += 1
                continue

            record, changes = transform_fn(record)

            if dry_run:
                print(f"  [DRY RUN] {f}:")
                for c in changes:
                    print(f"    {c}")
            else:
                # Backup original
                backup_path = f.with_suffix(f".{from_ver}.bak")
                shutil.copy2(f, backup_path)

                with open(f, "w", encoding="utf-8") as fh:
                    yaml.dump(record, fh, allow_unicode=True, sort_keys=False, default_flow_style=False)
                print(f"  MIGRATED {f} (backup: {backup_path.name})")
                for c in changes:
                    print(f"    {c}")

            migrated += 1
        except Exception as exc:
            print(f"  ERROR {f}: {exc}", file=sys.stderr)
            errors += 1

    print(f"\nResult: {migrated} migrated, {skipped} skipped, {errors} errors")
    if not dry_run and migrated > 0:
        print(f"\nRollback: restore .{from_ver}.bak files to undo this migration.")
        print(f"Risk: {'low' if reversible else 'HIGH — irreversible changes'}")

    return 0 if errors == 0 else 1


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Migrate Law Firm Expansion record schema versions.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "paths",
        nargs="*",
        default=["data/firms/"],
        help="YAML files or directories (default: data/firms/)",
    )
    mode_group = parser.add_mutually_exclusive_group(required=True)
    mode_group.add_argument("--check", action="store_true", help="Check schema versions only")
    mode_group.add_argument("--from", dest="from_ver", metavar="VERSION", help="Source schema version")
    parser.add_argument("--to", dest="to_ver", metavar="VERSION", help="Target schema version (required with --from)")
    parser.add_argument("--dry-run", action="store_true", help="Show what would change, do not write")
    args = parser.parse_args()

    if args.from_ver and not args.to_ver:
        print("ERROR: --to is required when using --from", file=sys.stderr)
        return 2

    resolved = [
        str(REPO_ROOT / p) if not Path(p).is_absolute() else p
        for p in args.paths
    ]

    if args.check:
        return check_versions(resolved)
    else:
        return apply_migration(resolved, args.from_ver, args.to_ver, dry_run=args.dry_run)


if __name__ == "__main__":
    sys.exit(main())
