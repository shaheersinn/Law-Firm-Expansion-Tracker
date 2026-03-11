# Contributor Guide — Law Firm Expansion Tracker

Welcome! This guide explains how to add, edit, and validate expansion records.

---

## Prerequisites

Install dependencies:

```bash
pip install pyyaml jsonschema pytest
```

---

## Adding a New Record

1. **Create a YAML file** in `data/firms/YYYY/` using the naming convention:
   ```
   firm-name-expansion-type-city-YYYY.yaml
   ```
   Examples:
   - `osler-new-office-london-2026.yaml`
   - `blg-merger-toronto-2026.yaml`

2. **Fill in all required fields.** See [Schema Reference](schema-reference.md) for the full list.

3. **Use controlled vocabulary.** All enums must match exactly. See [Taxonomy Reference](taxonomy-reference.md).

4. **Validate your record locally:**
   ```bash
   python scripts/validate.py data/firms/2026/your-record.yaml
   ```

5. **Check for duplicates:**
   ```bash
   python scripts/deduplicate.py data/firms/
   ```

6. **Open a pull request.** CI will validate automatically.

---

## Required Fields

| Field | Type | Example |
|---|---|---|
| `record_id` | slug | `osler-new-office-london-2026` |
| `firm_name` | string | `"Osler, Hoskin & Harcourt LLP"` |
| `expansion_type` | enum | `new_office` |
| `practice_areas` | array | `["Corporate & M&A"]` |
| `country` | ISO alpha-2 | `CA` |
| `announced_date` | YYYY-MM-DD | `2026-01-15` |
| `source_url` | URL | `https://...` |
| `source_type` | enum | `firm_press_release` |
| `confidence` | enum | `confirmed` |
| `status` | enum | `draft` |
| `created_at` | ISO 8601 UTC | `2026-01-15T09:00:00Z` |
| `last_modified` | ISO 8601 UTC | `2026-01-15T09:00:00Z` |
| `schema_version` | semver | `1.0.0` |

---

## Common Mistakes

### Wrong country code
- ❌ `country: UK` → ✅ `country: GB`
- ❌ `country: USA` → ✅ `country: US`

Use the normalize script to check:
```bash
python scripts/normalize.py --country "UK"
```

### Wrong practice area name
- ❌ `IP` → ✅ `Intellectual Property`
- ❌ `M&A` → ✅ `Corporate & M&A`

Use the normalize script to check:
```bash
python scripts/normalize.py --practice-area "IP"
```

### Invalid record_id
Record IDs must be lowercase letters, digits, and hyphens only, with no leading/trailing hyphens.
- ❌ `OSLER_New_Office_2026` → ✅ `osler-new-office-london-2026`

---

## Status Transitions

Records move through these statuses in order:

```
draft → under_review → verified → published → archived
```

**Allowed shortcuts:**
- `draft → archived` (abandoned record, explain in notes)
- `under_review → archived` (with explanation)
- `verified → archived` (with explanation)

**Never allowed:**
- `published → draft`
- `archived → published`
- `draft → published` (must go through review first)

---

## Confidence Levels

| Level | When to use |
|---|---|
| `confirmed` | Official firm announcement or primary filing |
| `high` | Top-tier legal press credibly confirms it |
| `medium` | Multiple indirect but credible sources |
| `low` | Single weak signal (job posting, rumor) |
| `unverified` | Insufficient evidence — needs review |

Do not publish `unverified` records.

---

## Running Scripts

```bash
# Validate all records
python scripts/validate.py data/firms/

# Validate a single record
python scripts/validate.py data/firms/2026/my-record.yaml

# Normalize a country code
python scripts/normalize.py --country "United States"

# Normalize a practice area
python scripts/normalize.py --practice-area "Labor"

# Normalize a full record
python scripts/normalize.py --file data/firms/2026/my-record.yaml

# Find duplicates
python scripts/deduplicate.py data/firms/

# Generate summary
python scripts/summarize.py

# Weekly digest (last 7 days)
python scripts/summarize.py --weekly

# Export to CSV
python scripts/summarize.py --format csv > export.csv

# Run tests
python -m pytest tests/ -v
```

---

## Multi-City or Multi-Jurisdiction Events

If a single press release announces offices in four cities:
- **Prefer one record per city**, linking them via `related_records`.
- This makes each record independently searchable and exportable.
- Exception: if the event is truly indivisible (e.g., a firm-level merger), use a single record with region-level detail.

---

## Non-ASCII Characters

Firm names and cities with accents are valid and should be preserved:
- `"Müller & Partners LLP"` ✅
- `city: "São Paulo"` ✅
- `city: "Zürich"` ✅

---

## Questions?

See [FAQ](faq.md) or open an issue.
