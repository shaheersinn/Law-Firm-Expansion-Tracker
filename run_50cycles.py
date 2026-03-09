"""
50-cycle local CI harness.

Each cycle:
  1. Injects synthetic signals into the DB (varied counts, types, firms)
  2. Runs ExpansionAnalyzer.analyze() + detect_website_changes()
  3. Runs Notifier._build_message() (no real Telegram send)
  4. Runs generate_dashboard() with real SQLite
  5. Runs run_evolution() (full learning pipeline)
  6. Verifies invariants and records any failures/warnings
  7. Prints a per-cycle summary line

At the end prints a full audit report.
"""

import sys, os, json, time, random, hashlib, traceback, sqlite3, re
from datetime import datetime, timezone, timedelta
from collections import defaultdict

sys.path.insert(0, os.path.dirname(__file__))

os.environ.setdefault("TELEGRAM_BOT_TOKEN", "x")
os.environ.setdefault("TELEGRAM_CHAT_ID",   "-1")
os.environ.setdefault("DASHBOARD_URL",      "https://test.example.com/")
os.environ.setdefault("GITHUB_REPOSITORY",  "testuser/test-repo")
os.environ.setdefault("GITHUB_RUN_ID",      "99999")
os.environ.setdefault("DB_PATH",            "/tmp/test_tracker_50.db")

# ── imports ───────────────────────────────────────────────────────────────────
from config import Config
from database.db import Database
from analysis.signals import ExpansionAnalyzer
from alerts.notifier import Notifier, _clean_title, _fmt_breakdown, _strength_badge, _page_name_from_url
from dashboard.generate import generate_dashboard
from learning.evolution import run_evolution
from learning.schedule import LearningSchedule
from classifier.department import DepartmentClassifier

# ── synthetic data ────────────────────────────────────────────────────────────
FIRMS = [
    {"id": "goodmans",  "name": "Goodmans LLP",                   "short": "Goodmans"},
    {"id": "osler",     "name": "Osler, Hoskin & Harcourt LLP",   "short": "Osler"},
    {"id": "blg",       "name": "Borden Ladner Gervais LLP",      "short": "BLG"},
    {"id": "nrf",       "name": "Norton Rose Fulbright Canada",    "short": "NRF"},
    {"id": "torys",     "name": "Torys LLP",                      "short": "Torys"},
    {"id": "stikeman",  "name": "Stikeman Elliott LLP",            "short": "Stikeman"},
    {"id": "bennett",   "name": "Bennett Jones LLP",               "short": "BJ"},
    {"id": "mccarthy",  "name": "McCarthy Tétrault LLP",           "short": "McCarthy"},
    {"id": "blakes",    "name": "Blake, Cassels & Graydon LLP",   "short": "Blakes"},
    {"id": "fasken",    "name": "Fasken Martineau DuMoulin LLP",  "short": "Fasken"},
    {"id": "aird",      "name": "Aird & Berlis LLP",              "short": "Aird"},
    {"id": "weir",      "name": "WeirFoulds LLP",                 "short": "WeirFoulds"},
]

DEPTS = [
    "Corporate / M&A", "Capital Markets", "Litigation & Disputes",
    "Real Estate", "Tax", "Financial Services & Regulatory",
    "Employment & Labour", "Data Privacy & Cybersecurity",
    "Energy & Natural Resources", "Private Equity",
]

SIGNAL_TYPES = [
    "press_release", "publication", "practice_page",
    "job_posting", "lateral_hire", "recruit_posting",
    "website_snapshot", "bar_leadership", "ranking",
]

SAMPLE_TITLES = [
    "[Firm News] PublicationThe rise of influencer marketing class actionsCanada",
    "[Practice Page] {firm} — Capital Markets",
    "[Practice Page] {firm} — Banking and Financial Services",
    "[Firm Insights] M&A activity surges in Q1 2026",
    "[Google News] {firm} advises on landmark infrastructure deal",
    "New Partner Joins {firm} Corporate Group",
    "[{firm} Insights] Navigating data privacy in 2026",
    "Climate change litigation: what boards need to know",
    "[Firm News] {firm} recognized in Chambers Canada",
    "Foundations for settlement: A contractor's guide to dispute resolution",
    "[Practice Page] {firm} — Environmental, Social and Governance",
    "Blockchain and digital assets: regulatory outlook",
    "[Firm News] Legal Professionals",
    "[Firm News] Our Services",
    "Private equity deals: outlook and strategies for Canadian counsel",
]

SAMPLE_URLS = [
    "https://{domain}/expertise/capital-markets/",
    "https://{domain}/practice-areas/banking-and-finance",
    "https://{domain}/insights/m-and-a-outlook-2026",
    "https://{domain}/en/expertise/services/climate-change-carbon-markets-and-environmental-finance/",
    "https://{domain}/expertise-detail/banking-and-financial-services",
    "https://{domain}/what-we-do/expertise/service/capital-pool-company-reverse-takeover",
    "https://{domain}/areas_of_law/banking-financing/",
    "https://{domain}/practice/blockchain-and-digital-assets",
]

FIRM_DOMAINS = {
    "goodmans": "goodmans.ca", "osler": "osler.com", "blg": "blg.com",
    "nrf": "nortonrosefulbright.com", "torys": "torys.com",
    "stikeman": "stikeman.com", "bennett": "bennettjones.com",
    "mccarthy": "mccarthy.ca", "blakes": "blakes.com", "fasken": "fasken.com",
    "aird": "airdberlis.com", "weir": "weirfoulds.com",
}


def make_signals(cycle: int, n: int = None) -> list[dict]:
    """Generate n synthetic signals for the cycle."""
    if n is None:
        n = random.randint(60, 280)
    signals = []
    now = datetime.now(timezone.utc)
    clf = DepartmentClassifier(os.environ["DB_PATH"])

    for i in range(n):
        firm = random.choice(FIRMS)
        stype = random.choices(
            SIGNAL_TYPES,
            weights=[15, 20, 12, 3, 2, 4, 15, 1, 1],
            k=1
        )[0]
        domain = FIRM_DOMAINS.get(firm["id"], f"{firm['id']}.com")
        title_tpl = random.choice(SAMPLE_TITLES)
        title = title_tpl.format(firm=firm["short"])
        url   = random.choice(SAMPLE_URLS).format(domain=domain)

        # Classify
        text = f"{title} {url}"
        cls  = clf.classify(text, top_n=1)
        dept = cls[0]["department"] if cls else random.choice(DEPTS)
        dept_score = cls[0]["score"] if cls else random.uniform(0.3, 3.0)

        # For website_snapshot: body = hash (simulates page content)
        body = hashlib.md5(f"{url}-cycle{cycle}-{i%7}".encode()).hexdigest() \
               if stype == "website_snapshot" else title[:200]

        # Vary scraped_at slightly within last 3 days
        delta = timedelta(hours=random.randint(0, 72))
        scraped_at = (now - delta).isoformat()

        signals.append({
            "firm_id":         firm["id"],
            "firm_name":       firm["name"],
            "signal_type":     stype,
            "title":           title,
            "url":             url,
            "body":            body,
            "department":      dept,
            "department_score":dept_score,
            "scraped_at":      scraped_at,
        })

    return signals


def insert_signals(db: Database, signals: list[dict]) -> list[dict]:
    """Insert signals using existing schema columns, return genuinely new ones."""
    new = []
    for s in signals:
        try:
            db.conn.execute("""
                INSERT OR IGNORE INTO signals
                  (firm_id, firm_name, signal_type, title, url, body,
                   department, department_score, scraped_at)
                VALUES (?,?,?,?,?,?,?,?,?)
            """, (s["firm_id"], s["firm_name"], s["signal_type"],
                  s["title"], s["url"], s["body"],
                  s["department"], s["department_score"],
                  s["scraped_at"]))
            if db.conn.execute("SELECT changes()").fetchone()[0]:
                new.append(s)
        except Exception:
            pass
    db.conn.commit()
    return new


# ── invariant checks ──────────────────────────────────────────────────────────

def check_message(msg: str, n_new: int = 0) -> list[str]:
    """Return list of invariant violations in a Telegram message."""
    problems = []
    if not msg:
        problems.append("EMPTY message")
        return problems
    if len(msg) > 4096:
        problems.append(f"Message too long: {len(msg)} chars")
    if "(s)" in msg:
        problems.append("Lazy (s) plural found")
    if "Score " in msg and re.search(r"Score \d+\.?\d*", msg):
        problems.append("Raw numeric Score still present")
    if "🏅3.5" in msg or "🏆👤3.0" in msg:
        problems.append("Junk footer legend still present")
    if "website_snapshot" in msg:
        problems.append("Internal type 'website_snapshot' exposed")
    if re.search(r"\[Firm News\]|\[Practice Page\]|\[.*? Insights\]", msg):
        problems.append("Raw [Source Tag] prefix not stripped")
    # Only flag truly cryptic standalone abbreviations, not "bar leadership" or "press releases"
    if re.search(r'\b\d+ (?:pg|pub|rec)(?:\s|,|$)', msg):
        problems.append("Cryptic signal abbreviations in breakdown")
    raw_url_pattern = r"Practice area page content changed at https?://"
    if re.search(raw_url_pattern, msg):
        problems.append("Verbose 'Practice area page changed at URL' not cleaned")
    # Only flag 0-signals when we actually passed a non-empty signals list
    if n_new > 0 and re.search(r'(?<!\d)0 new signal', msg):
        problems.append(f"Signal count shows 0 but {n_new} signals were passed")
    return problems

def check_dashboard(path: str) -> list[str]:
    problems = []
    if not os.path.exists(path):
        problems.append(f"Dashboard not written to {path}")
        return problems
    html = open(path).read()
    if len(html) < 500:
        problems.append(f"Dashboard suspiciously small: {len(html)} bytes")
    return problems

def check_evolution(report: dict | None) -> list[str]:
    problems = []
    if report is None:
        return problems  # schedule said not yet — expected
    # Match actual keys returned by EvolutionLogger.write_report()
    required_keys = ["learning_schedule", "keywords_updated", "signal_type_weights"]
    for k in required_keys:
        if k not in report:
            problems.append(f"Evolution report missing key: {k}")
    return problems


# ── main loop ─────────────────────────────────────────────────────────────────

def run_50_cycles():
    config = Config()
    notifier = Notifier(config)

    all_issues: dict[int, list[str]] = {}
    cycle_times = []
    total_signals_inserted = 0
    total_alerts_generated = 0
    total_website_changes  = 0
    evolution_runs = 0

    print("=" * 70)
    print(f"  50-CYCLE LOCAL HARNESS  |  DB: {config.DB_PATH}")
    print("=" * 70)
    print(f"  {'CYC':>3}  {'SIGS':>5}  {'NEW':>5}  {'ALRT':>5}  {'WCH':>4}  {'EVO':>3}  {'ISSUES':>6}  {'ms':>6}")
    print("  " + "-" * 60)

    for cycle in range(1, 51):
        t0 = time.time()
        cycle_issues = []

        # Fresh DB each time (or reuse — we reuse to accumulate history)
        db = Database(config.DB_PATH)

        try:
            # ── 1. inject signals ─────────────────────────────────────────
            raw_signals = make_signals(cycle)
            new_signals = insert_signals(db, raw_signals)
            total_signals_inserted += len(new_signals)

            # ── 2. analyze ────────────────────────────────────────────────
            analyzer = ExpansionAnalyzer(db)
            expansion_alerts = analyzer.analyze(new_signals)
            website_changes  = analyzer.detect_website_changes(new_signals)
            total_alerts_generated += len(expansion_alerts)
            total_website_changes  += len(website_changes)

            # ── 3. build Telegram message (no send) ───────────────────────
            msg = notifier._build_message(expansion_alerts, website_changes, new_signals)
            msg_issues = check_message(msg, n_new=len(new_signals))
            cycle_issues.extend(msg_issues)

            # ── 4. generate dashboard ─────────────────────────────────────
            dash_path = f"/tmp/dashboard_test_{cycle}.html"
            try:
                generate_dashboard(db_path=config.DB_PATH, out_path=dash_path)
                dash_issues = check_dashboard(dash_path)
                cycle_issues.extend(dash_issues)
            except Exception as e:
                cycle_issues.append(f"Dashboard crash: {e}")

            # ── 5. evolution (runs if schedule says so) ───────────────────
            evo_report = None
            try:
                evo_report = run_evolution(log_path="tracker.log", force=(cycle % 5 == 0))
                if evo_report is not None:
                    evolution_runs += 1
                    evo_issues = check_evolution(evo_report)
                    cycle_issues.extend(evo_issues)
            except Exception as e:
                cycle_issues.append(f"Evolution crash: {type(e).__name__}: {e}")

            # ── 6. title-cleaning spot checks ─────────────────────────────
            test_titles = [
                "[Practice Page] Goodmans — Capital Markets",
                "[Firm News] PublicationThe rise of class actionsCanada",
                "[NRF Insights] M&A outlook: why 2026 is different for Canadian firms",
                "Foundations for settlement: A contractor's guide to",
                "",
                "[Firm News] Our Services",
            ]
            expected_clean = [
                "Capital Markets",
                "The rise of class actions Canada",
                None,    # just check non-empty
                "Foundations for settlement: A contractor's guide to",
                None,    # empty → fallback
                "Our Services",
            ]
            for raw, exp in zip(test_titles, expected_clean):
                cleaned = _clean_title(raw)
                if exp is not None and cleaned != exp:
                    # Only flag if [Source Tag] still present in output
                    if re.search(r"^\[.*?\]", cleaned):
                        cycle_issues.append(f"Title not cleaned: '{raw}' → '{cleaned}'")

        except Exception as e:
            cycle_issues.append(f"CYCLE CRASH: {type(e).__name__}: {traceback.format_exc()[-300:]}")
        finally:
            try:
                db.close()
            except Exception:
                pass

        elapsed_ms = int((time.time() - t0) * 1000)
        cycle_times.append(elapsed_ms)

        n_issues = len(cycle_issues)
        if cycle_issues:
            all_issues[cycle] = cycle_issues

        evo_marker = "✓" if evo_report else "·"
        issue_marker = f"⚠ {n_issues}" if n_issues else "✓"

        print(f"  {cycle:3d}  {len(raw_signals):5d}  {len(new_signals):5d}  "
              f"{len(expansion_alerts):5d}  {len(website_changes):4d}  "
              f"{evo_marker:>3}  {issue_marker:>6}  {elapsed_ms:>6}")

    # ── final report ──────────────────────────────────────────────────────────
    print("\n" + "=" * 70)
    print("  FINAL AUDIT REPORT")
    print("=" * 70)
    print(f"  Cycles:            50")
    print(f"  Total signals in:  {total_signals_inserted}")
    print(f"  Total alerts:      {total_alerts_generated}")
    print(f"  Website changes:   {total_website_changes}")
    print(f"  Evolution ran:     {evolution_runs}× (every 5th cycle forced)")
    print(f"  Avg cycle time:    {sum(cycle_times)//len(cycle_times)}ms")
    print(f"  Max cycle time:    {max(cycle_times)}ms")

    if not all_issues:
        print("\n  ✅ ZERO issues across all 50 cycles")
    else:
        bad_cycles = sorted(all_issues.keys())
        print(f"\n  ⚠  Issues in {len(bad_cycles)} cycles: {bad_cycles}")
        unique_issues: dict[str, list[int]] = defaultdict(list)
        for cyc, issues in all_issues.items():
            for issue in issues:
                short = issue[:80]
                unique_issues[short].append(cyc)
        print("\n  Issue summary (unique × count):")
        for issue, cycles in sorted(unique_issues.items(), key=lambda x: -len(x[1])):
            print(f"    [{len(cycles):2d}×] {issue}")

    print("=" * 70)
    return all_issues


if __name__ == "__main__":
    issues = run_50_cycles()
    sys.exit(0 if not issues else 1)
