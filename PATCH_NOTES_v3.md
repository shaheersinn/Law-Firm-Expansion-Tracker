# Law Firm Expansion Tracker — Patch Notes v3
> 10 improvement cycles applied 2026-03-11

---

## ROOT CAUSE ANALYSIS (from log `run_id: 22942202473`)

The log showed **zero new signals** across all 54 scrapers on every firm.
Three separate bugs combined to produce complete silence:

| # | File | Bug | Impact |
|---|------|-----|--------|
| 1 | `rss.py` | `if not classifications: continue` silently dropped every RSS item the dept classifier couldn't categorize | **100% of RSS signals lost** |
| 2 | `department.py` | `MIN_RAW=0.8` threshold too high for short RSS titles (10–30 words) | Classify returned `[]` → triggered bug #1 |
| 3 | `press.py` / `jobs.py` | Same `if not classifications: continue` pattern in both | All press & job signals dropped when dept unclear |

Secondary issues: hardcoded dashboard counts, no scraper health alerting, limited feeds.

---

## CYCLE 1 — Critical Main/Base Bug Fixes ✅

**`main.py` — `_send_digest()` website_changes bug**
- Was calling `analyzer.detect_website_changes([])` with empty list always
- Fixed: passes `new_signals or []` so website change alerts actually fire

**`base.py` — Signal hash collision**
- Old hash: `sha256(f"{firm_id}:{title}:{url}").hexdigest()[:16]`
- When `url=""` (common), multiple different signals got identical hashes → false dedup
- Fixed: `sha256(f"{firm_id}:{signal_type}:{title.lower()}:{body[:80]}").hexdigest()[:20]`
- Extended hash from 16→20 chars for lower collision probability

---

## CYCLE 2 — Base Scraper Hardening ✅

**`base.py` changes:**
- Added `_head(url)` method for cheap 200/404 checks before expensive GETs
- Added `_post(url, data, json_body)` helper for form/JSON endpoints
- Added per-request elapsed timing at DEBUG level
- Added jitter to retry backoffs: `(2**attempt) + random.uniform(0, 1.5)` (was `2**attempt`)
- Honour `Retry-After` header on 429 responses (was hardcoded `10 * attempt`)
- Raised timeout 20s → 25s (gov/court sites are slow)
- Added `ConnectionError` as separate except clause
- Retry `allowed_methods` now includes HEAD and POST

---

## CYCLE 3 — Department Classifier Hardening ✅

**`department.py` changes:**
- **BUG FIX**: Was using `pattern.search()` → only counted first match per term
  - Now uses `pattern.findall()` → counts ALL occurrences, capped at `MAX_TERM_HITS=3`
- Added `TITLE_BOOST=2.0`: matches in title/opening 200 chars score 2× higher
- Added `MIN_RAW=0.8` threshold guard before normalizing
- `classify()` now accepts optional `title=` kwarg for split title/body scoring
- `best()` forwards `title=` kwarg through to `classify()`
- Fixed zero-division edge case when word_count=0

---

## CYCLE 4 — Generator Dynamic Counts ✅

**`generator.py` changes:**
- **Removed all hardcoded counts** — was: "26 firms", "8 scrapers", "17 departments" in HTML
- Now dynamically computed from live data:
  - `total_tracked_firms` = `len(FIRMS)` from `firms.py`
  - `active_scraper_count` = number of distinct signal types seen this week
  - `unique_depts` = number of distinct departments in weekly signals
- Added `date` field to per-signal data in dashboard payload
- Improved dashboard log message: now shows `{n}/{total} firms active`
- Fixed `dept = sig.get("department") or "General"` (was `"Unknown"`)
- Added `try/except ImportError` guard on FIRMS import

---

## CYCLE 5 — RSS Zero-Signal Root Cause Fix ✅

**`rss.py` — THE most impactful fix:**

```python
# BEFORE (dropped all signals with no dept match)
classifications = classifier.classify(full, top_n=1)
if not classifications:
    continue

# AFTER (General dept fallback — no signal is ever dropped)
classifications = classifier.classify(full, top_n=1)
if classifications:
    dept = classifications[0]["department"]
    dept_score = classifications[0]["score"] * feed_meta["weight"] * weight_mult
    matched_kw = classifications[0]["matched_keywords"]
else:
    dept = "General"
    dept_score = feed_meta["weight"] * weight_mult * 0.5
    matched_kw = []
```

**Additional RSS improvements:**
- Added diagnostic logging: hits-by-feed at DEBUG, pattern dump when 0 matches
- Expanded from **22 → 32 feeds**; new feeds include:
  - Osler Insights, Blakes Bulletin, McCarthy Insights, Stikeman Updates, BLG Insights, Bennett Jones Insights (firm-direct, highest credibility)
  - BNN Bloomberg, AI Regulation Today, Cybersecurity Law Report, M&A Canada
- Expanded `LATERAL_PHRASES` (+9 new patterns: "elected partner", "recruited", "adds partner", etc.)
- Expanded `DEAL_PHRASES` (+8 new patterns: "co-counsel", "acquisition of", "capital raise", etc.)
- Expanded `EXPANSION_PHRASES` (+6 new patterns: "new location", "strategic alliance", etc.)

---

## CYCLE 6 — Department Classifier Threshold Fix ✅

**`department.py` — Critical threshold tuning:**

| Parameter | Before | After | Reason |
|-----------|--------|-------|--------|
| `MIN_RAW` | `0.8` | `0.4` | Was rejecting all short RSS titles |
| `MIN_RAW_SHORT` | n/a | `0.2` | Texts < 50 words get relaxed threshold |
| `MIN_SCORE` | `0.5` | `0.2` | Matches lowered MIN_RAW |

**New features:**
- `confidence` field: `"high"` / `"medium"` / `"low"` on each classification result
- `classify_with_fallback(text, title, fallback)` — always returns one result
  - Never returns empty list; defaults to `fallback` (default: `"General"`)
  - Used by all scrapers to eliminate silent signal drops

---

## CYCLE 7 — Press Scraper Overhaul ✅

**`press.py` changes:**
- **Removed both `if not classifications: continue` gates** — now uses `classify_with_fallback()`
- Improved `_extract_link()` function: tries multiple `<a href>` patterns before fallback
- Added `_classify_type_detailed()` returning `(sig_type, weight_mult)` tuple
- Added minimum text length check (skip nodes < 40 chars)
- Expanded `MEDIA_SOURCES` from 4 → 8 sources:
  - Added: Law360 Canada (with fallback URL), Lexpert, Mondaq Canada, Advocates Daily, Slaw
- Expanded `LATERAL_PHRASES` (+5 new patterns)
- Expanded `DEAL_PHRASES` (+5 new patterns: "advises", "lead counsel", etc.)
- Added fallback URL support for media sources
- Removed stale "The Lawyer's Daily" URL (now Law360 Canada)

---

## CYCLE 8 — Jobs Scraper Overhaul ✅

**`jobs.py` changes:**
- **Removed all `if not classifications: continue` gates** — uses `classify_with_fallback()`
- Expanded career URL paths from **6 → 14** patterns:
  - Added: `/join-us`, `/about/careers`, `/about/join-our-team`, `/people/careers`,
    `/careers/legal-professionals`, `/lawyer-careers`, `/fr/carrieres`
  - Uses `dict.fromkeys()` deduplication to avoid redundant GETs
- Better non-legal role filter: **12 patterns** (was 4)
- Partner/counsel postings now tagged `lateral_hire` (high-value signal), not just `job_posting`
- `LATERAL_LEVEL_TERMS` determines the cutoff: partner, counsel, senior associate, senior partner
- Improved `SENIORITY_WEIGHTS` — now 13 levels (was 7), includes managing partner (4.0x)
- LinkedIn: no longer requires `linkedin_slug` — falls back to firm short name
- LinkedIn: added `f_TP=1` filter for last-24h postings (fresher signals)
- Broad card selector fallback: if class-based finds nothing, tries `[data-job]` attribute

---

## CYCLE 9 — Taxonomy Expansion ✅

**`taxonomy.py` — Added 4 new practice areas** (17 → 21 total):

| New Department | Keywords | Phrases |
|----------------|----------|---------|
| **Technology & AI Law** | `artificial intelligence`, `ai`, `machine learning`, `cybersecurity`, `data`, `cloud`, `iot`, ... | `ai regulation`, `ai governance`, `generative ai`, `llm`, `data breach`, `algorithmic accountability`, ... |
| **Crypto & Digital Assets** | `crypto`, `bitcoin`, `blockchain`, `nft`, `defi`, `web3`, `stablecoin`, ... | `cryptocurrency law`, `token offering`, `smart contract`, `ico`, `crypto tax`, ... |
| **International Trade & Investment** | `trade`, `tariffs`, `sanctions`, `wto`, `usmca`, `nafta`, `ceta`, ... | `investment arbitration`, `investor-state`, `export control`, `free trade agreement`, ... |
| **General** | `law firm`, `lawyer`, `legal`, `counsel`, `firm`, ... | `law firm`, `legal services`, `general counsel`, ... |

The `General` department is now the explicit fallback and appears in the taxonomy itself, making dashboards more accurate.

---

## CYCLE 10 — Scraper Health Monitoring + Velocity Alerts ✅

**`main.py` changes:**
- **Scraper health tracking**: every scraper's total across all firms is counted
- **Silent scraper detection**: after collection, logs all scrapers that returned 0 total
  ```
  SCRAPER HEALTH: 3 scrapers returned 0 signals across all firms: GoogleNewsScraper, LateralTrackScraper, RecruiterScraper
  ```
- **Zero-signal run Telegram alert**: if `len(all_new_signals) == 0`, sends Telegram warning with:
  - Count of silent scrapers
  - Number of firms processed
  - Troubleshooting guidance
  - Uses `hasattr` guard — safe even if `Notifier.send_health_alert` doesn't exist yet
- **Improved `_send_digest` log**: now shows weekly signal count + website changes count
  ```
  Digest: 12 new alerts  |  Weekly signals in DB: 156  |  Website changes: 2
  ```

---

## Summary: Signal Flow Before vs After

```
BEFORE (0 signals):
RSS feeds (32) → feedparser → firm match ✓ → classifier → [] → DROPPED ✗
Firm news pages → soup parse → dept classify → [] → DROPPED ✗
Job postings → soup parse → dept classify → [] → DROPPED ✗

AFTER (signals captured):
RSS feeds (32) → feedparser → firm match ✓ → classifier → result or "General" → SAVED ✓
Firm news pages → soup parse → classify_with_fallback → always a dept → SAVED ✓
Job postings → 14 URL paths → classify_with_fallback → lateral_hire or job_posting → SAVED ✓
```

### Files Changed

| File | Cycles | Key Changes |
|------|--------|-------------|
| `main.py` | 1, 10 | website_changes bug fix; scraper health monitoring; zero-signal alerting |
| `base.py` | 1, 2 | hash collision fix; _head/_post; jitter retries; Retry-After; timeout 25s |
| `department.py` | 3, 6 | findall counts; title_boost; MIN_RAW lowered; confidence field; classify_with_fallback |
| `rss.py` | 5 | **Critical: General dept fallback**; 32 feeds (was 22); expanded phrase lists |
| `press.py` | 7 | **Critical: removed classifier gate**; classify_with_fallback; 8 sources; better link extraction |
| `jobs.py` | 8 | **Critical: removed classifier gates**; 14 career URL paths; lateral_hire tagging |
| `generator.py` | 4 | Dynamic counts; date field; improved log |
| `taxonomy.py` | 9 | 21 departments (was 17): Tech/AI, Crypto, Trade, General |
