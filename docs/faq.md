# FAQ — Law Firm Expansion Tracker

---

## General

### What does this tracker track?

This tracker records law firm expansion events: new offices, mergers, lateral hire groups,
practice group launches, office relocations, and similar events.

Each event is stored as a structured YAML record under `data/firms/YYYY/`.

---

### What is a "record_id"?

A unique slug that identifies each expansion event. Rules:
- Lowercase letters, digits, and hyphens only
- Cannot start or end with a hyphen
- Must be unique across all records

**Format convention:** `firm-slug-expansion-type-city-year`  
**Example:** `osler-new-office-toronto-2026`

---

### Why are there two BLG records from the same announcement?

When one press release describes multiple distinct events (e.g., a lateral hire group AND an
office expansion), we create one record per event and link them via `related_records`.

This makes each event independently searchable, reportable, and exportable.

---

## Schema & Validation

### What happens if my record fails validation?

The CI workflow will block the pull request. Fix the errors shown in the output of:
```bash
python scripts/validate.py data/firms/2026/your-record.yaml
```

Common causes:
- Missing required field
- Wrong enum value (check [Taxonomy Reference](taxonomy-reference.md))
- Country code not ISO alpha-2 (e.g., `UK` → use `GB`)
- `source_url` doesn't start with `https://`
- `record_id` contains uppercase or special characters

---

### Can I add fields not in the schema?

No. `additionalProperties: false` is set — unknown fields cause validation errors.
If you think a new field is needed, open an issue to propose it.

---

### My record has warnings but no errors. Is it publishable?

Warnings are advisory. They don't block CI, but you should review them before
setting `status: published`. Key warnings to address:

- `confidence=unverified` + `status=published` → **Do not publish unverified records**
- `effective_date` earlier than `announced_date` → check dates
- Notes over 500 characters → trim or split

---

## Practice Areas & Taxonomy

### Can I use abbreviations like "IP" or "M&A"?

Not in the YAML record. Use the canonical form:
- `IP` → `Intellectual Property`
- `M&A` → `Corporate & M&A`

Use the normalize script to check:
```bash
python scripts/normalize.py --practice-area "M&A"
```

---

### What if the practice area is both Data Privacy AND Cybersecurity?

Use two entries:
```yaml
practice_areas:
  - Data Privacy & Protection
  - Technology & Cybersecurity
```

---

### The firm uses a different spelling for a practice area. What should I do?

Normalize to the canonical value and optionally record the original in `notes`.  
Example: Source says "Privacy & Cybersecurity" → use both `Data Privacy & Protection`
and `Technology & Cybersecurity`, note the original.

---

## Countries

### What country code should I use for the UK?

Use `GB` (ISO 3166-1 alpha-2), not `UK`.

Use `python scripts/normalize.py --country "UK"` to confirm.

---

### What if I don't know the city but I know the country?

Set `country` and leave `city` blank. You'll get a warning, which is expected.
Add a note explaining why city is unknown or confidential.

---

## Duplicates

### How does duplicate detection work?

The deduplication script scores each pair of records based on:
- Same firm name
- Same expansion type
- Same country and city
- Overlapping practice areas
- Similar announcement dates
- Same source URL

Pairs scoring ≥ 80 are flagged as "likely_duplicate".
Pairs scoring ≥ 50 are flagged as "possible_duplicate".

**Records that are already linked via `related_records` are skipped** — they are
intentionally related, not duplicates.

---

### Will the deduplication script delete records?

**No.** It only flags candidates for manual review. No records are ever deleted automatically.

---

## Status & Lifecycle

### Can I go straight from draft to published?

No. The required path is: `draft → under_review → verified → published`.

You can go directly to `archived` from any status if the record is abandoned,
but document why in `notes`.

---

## Contributing

### How do I run the tests?

```bash
python -m pytest tests/ -v
```

### How do I validate all records at once?

```bash
python scripts/validate.py data/firms/
```

### How do I generate a summary report?

```bash
python scripts/summarize.py
python scripts/summarize.py --weekly
python scripts/summarize.py --format csv > export.csv
```
