# Migration Guide — Law Firm Expansion Tracker

This guide explains how to migrate records between schema versions.

---

## Current Schema Version: 1.0.0

---

## Versioning Policy

| Change Type | Version Bump | Example |
|---|---|---|
| Documentation/clarification only | PATCH (1.0.0 → 1.0.1) | Improving a field description |
| New optional field added | MINOR (1.0.0 → 1.1.0) | Adding `verified_by` optional field |
| Required field added, field renamed, type changed, enum removed | MAJOR (1.0.0 → 2.0.0) | Making `city` required |

---

## Available Migrations

### 1.0.0 → 1.1.0 *(example — not yet live)*

- **Type:** MINOR
- **Change:** Adds optional `verified_by` field
- **Backward compatible:** Yes — old records without `verified_by` still validate
- **Reversible:** Yes — remove `verified_by` field and revert `schema_version`

---

## Running Migrations

### Check current schema versions

```bash
python scripts/migrate.py --check data/firms/
```

Output:
```
Schema version check across 5 files:
  ✅ 1.0.0: 5 file(s)
```

### Dry-run a migration

```bash
python scripts/migrate.py --dry-run --from 1.0.0 --to 1.1.0 data/firms/
```

### Apply a migration

```bash
python scripts/migrate.py --from 1.0.0 --to 1.1.0 data/firms/
```

The script:
1. Creates a `.bak` backup of each file before modifying it
2. Updates `schema_version` and `last_modified` in each record
3. Reports what changed

### Rollback

If a migration needs to be reversed:

1. Restore the `.bak` files:
   ```bash
   for f in data/firms/**/*.1.0.0.bak; do cp "$f" "${f%.1.0.0.bak}.yaml"; done
   ```
2. Remove the backup files:
   ```bash
   find data/firms/ -name "*.bak" -delete
   ```

---

## Adding a New Migration

To add a migration from version X to Y:

1. Define a transformation function in `scripts/migrate.py`:
   ```python
   def _migrate_X_to_Y(record: dict) -> tuple[dict, list[str]]:
       changes = []
       # ... apply changes ...
       record["schema_version"] = "Y"
       return record, changes
   ```

2. Register it in the `MIGRATIONS` dict:
   ```python
   MIGRATIONS[("X", "Y")] = (_migrate_X_to_Y, True, "MINOR: description")
   ```

3. Update `CURRENT_SCHEMA_VERSION = "Y"`.

4. Update `data/schema/expansion.schema.json` with the new fields/rules.

5. Update `data/schema/schema-changelog.md`.

6. Add tests for the new schema behavior.

---

## Risk Levels

| Change | Risk |
|---|---|
| PATCH (docs only) | Low — no data change |
| MINOR (optional field added) | Low — backward compatible |
| MAJOR (required field, type change) | High — all records must be migrated before publishing |
