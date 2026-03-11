# Schema Reference — Law Firm Expansion Tracker

Current schema version: **1.0.0**

Schema file: `data/schema/expansion.schema.json`

---

## All Fields

### `record_id` *(required)*
**Type:** string  
**Pattern:** `^[a-z0-9][a-z0-9-]*[a-z0-9]$`  
Unique slug. Lowercase letters, digits, hyphens only. No leading/trailing hyphens.  
**Example:** `osler-new-office-toronto-2026`

---

### `firm_name` *(required)*
**Type:** string  
Full canonical legal name of the firm. Diacritics preserved.  
**Example:** `"Müller & Partners LLP"`

---

### `expansion_type` *(required)*
**Type:** enum  
See [Taxonomy Reference](taxonomy-reference.md#expansion-types).  
**Example:** `new_office`

---

### `practice_areas` *(required)*
**Type:** array of enum (min 1 item, unique)  
See [Taxonomy Reference](taxonomy-reference.md#practice-areas).  
**Example:**
```yaml
practice_areas:
  - Corporate & M&A
  - Capital Markets
```

---

### `country` *(required)*
**Type:** string  
**Pattern:** ISO 3166-1 alpha-2 (two uppercase letters)  
**Example:** `CA`, `GB`, `DE`, `US`

Common normalizations:
- `UK` → `GB`
- `USA` → `US`
- `Germany` → `DE`

---

### `region` *(optional)*
**Type:** string  
Human-readable region or state/province.  
**Example:** `"Ontario"`, `"New York"`, `"Bavaria"`

---

### `city` *(optional)*
**Type:** string  
City name. Diacritics preserved.  
**Example:** `"São Paulo"`, `"Zürich"`, `"Toronto"`

---

### `announced_date` *(required)*
**Type:** ISO 8601 date (YYYY-MM-DD)  
First known public announcement.  
**Example:** `"2026-01-15"`

---

### `effective_date` *(optional)*
**Type:** ISO 8601 date (YYYY-MM-DD)  
When the change takes effect. Must not be earlier than `announced_date`.  
**Example:** `"2026-04-01"`

---

### `source_url` *(required)*
**Type:** URI  
Must begin with `http://` or `https://`.  
**Example:** `"https://www.osler.com/en/news/2026/expansion"`

---

### `source_type` *(required)*
**Type:** enum  
See [Taxonomy Reference](taxonomy-reference.md#source-types).  
**Example:** `firm_press_release`

---

### `confidence` *(required)*
**Type:** enum  
See [Taxonomy Reference](taxonomy-reference.md#confidence-levels).  
**Example:** `confirmed`

---

### `status` *(required)*
**Type:** enum  
Valid values: `draft`, `under_review`, `verified`, `published`, `archived`  
See [Contributor Guide — Status Transitions](contributor-guide.md#status-transitions).  
**Example:** `published`

---

### `headcount` *(optional)*
**Type:** integer (min 1)  
Number of lawyers/personnel involved, if known.  
**Example:** `12`

---

### `related_records` *(optional)*
**Type:** array of record_id strings (unique)  
Links to related expansion events. Must not include the record's own `record_id`.  
**Example:**
```yaml
related_records:
  - blg-office-expansion-calgary-2026
```

---

### `tags` *(optional)*
**Type:** array of string (unique)  
Supplemental filtering tags. Not canonical vocabulary.  
**Example:** `["energy", "calgary", "lateral-group"]`

---

### `notes` *(optional)*
**Type:** string (max 1000 characters; 500 recommended)  
Free-text context. Use for normalization decisions, source quality notes, etc.

---

### `created_at` *(required)*
**Type:** ISO 8601 UTC datetime  
**Pattern:** `YYYY-MM-DDTHH:MM:SSZ`  
**Example:** `"2026-01-15T09:00:00Z"`

---

### `last_modified` *(required)*
**Type:** ISO 8601 UTC datetime  
**Example:** `"2026-01-20T14:30:00Z"`

---

### `schema_version` *(required)*
**Type:** semver string  
**Example:** `"1.0.0"`

---

### `created_by` *(optional)*
**Type:** string  
GitHub username or automation ID.  
**Example:** `"shaheersinn"`

---

## Validation Rules (Summary)

**Errors (block publication):**
- Missing required fields
- Invalid enum values
- Invalid `record_id` pattern
- Invalid ISO alpha-2 country code
- Malformed dates or datetimes
- `source_url` not starting with `http://` or `https://`
- Empty `practice_areas` array
- Self-reference in `related_records`
- Unknown additional properties
- Duplicate `record_id` across files

**Warnings (need review, do not block):**
- `announced_date` older than 2 years while still `draft`
- `effective_date` earlier than `announced_date`
- `confidence=unverified` + `status=published`
- Future `announced_date`
- Missing `city` when `country` is set
- Notes longer than 500 characters
- High-confidence record with a weak `source_type`
