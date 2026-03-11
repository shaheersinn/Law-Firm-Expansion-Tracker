"""30-cycle local test harness — fully offline, mocked HTTP."""
import sys, os, io, json, logging, time, traceback
os.environ.update({
    "SIGNAL_LOOKBACK_DAYS": "21",
    "SCRAPER_MIN_DELAY": "0",
    "SCRAPER_MAX_DELAY": "0",
    "TELEGRAM_BOT_TOKEN": "",
    "TELEGRAM_CHAT_ID": "",
    "DASHBOARD_URL": "http://localhost/dashboard",
    "GITHUB_RUN_ID": "test-run-001",
    "GITHUB_REPOSITORY": "testuser/law-firm-tracker",
})
sys.path.insert(0, "/home/claude/tracker")

from unittest.mock import patch, MagicMock

# ── Mock responses ─────────────────────────────────────────────────────────
def _resp(status=200, text="", ctype="text/html", headers=None):
    r = MagicMock()
    r.status_code = status; r.text = text; r.ok = (status==200)
    h = {"content-type": ctype}
    if headers: h.update(headers)
    r.headers = h; r.content = text.encode()
    return r

FRESH_HTML = """<html><body>
<article class="news-item">
  <h2><a href="/news/lateral-hire">Senior partner joins Corporate M&A group from rival firm</a></h2>
  <p>Davies Ward welcomes a senior lateral hire who joins from Blakes to strengthen the
  mergers acquisitions and private equity advisory practice.</p>
</article>
<article class="news-item">
  <h2><a href="/news/privacy">New Data Privacy &amp; Cybersecurity practice launches</a></h2>
  <p>The firm announces a dedicated Data Privacy practice group effective immediately.</p>
</article>
<div class="job-posting">
  <h3>Associate – Capital Markets (Bay Street)</h3>
  <p>Seeking a qualified associate for Capital Markets regulatory work. 3–5 years PQE.</p>
</div>
<div class="job-posting">
  <h3>Articling Student – Corporate Group</h3>
  <p>Applications open for articling positions in the corporate practice group.</p>
</div>
<a href="/practice-areas/corporate">Corporate / M&A</a>
<a href="/practice-areas/capital-markets">Capital Markets</a>
<a href="/practice-areas/litigation">Litigation &amp; Disputes</a>
</body></html>"""

RSS_XML = """<?xml version="1.0"?><rss version="2.0"><channel>
<item>
  <title>Blakes welcomes Competition partner joining from McCarthy Tetrault</title>
  <description>Blake Cassels announced a senior Competition partner lateral hire joining from McCarthy.</description>
  <link>https://canadianlawyermag.com/news/lateral-1</link>
  <pubDate>Thu, 05 Mar 2026 12:00:00 +0000</pubDate>
</item>
<item>
  <title>Davies expands Real Estate with major lateral hire from Fasken</title>
  <description>Davies Ward has expanded its Real Estate practice with a senior partner specializing in commercial real estate transactions.</description>
  <link>https://canadianlawyermag.com/news/lateral-2</link>
  <pubDate>Fri, 06 Mar 2026 09:00:00 +0000</pubDate>
</item>
<item>
  <title>STALE: Old firm merger 2019 — should be filtered</title>
  <description>This 2019 article must be filtered by the 21-day lookback window.</description>
  <link>https://example.com/old/1</link>
  <pubDate>Mon, 01 Jan 2019 09:00:00 +0000</pubDate>
</item>
</channel></rss>"""

def mock_get(url, **kw):
    url_l = url.lower()
    if "mccarthy" in url_l:                         return _resp(403)  # instant fail, no sleep
    if "blg.com" in url_l or "torys.com" in url_l: return _resp(500)
    if "indeed.com" in url_l:                       return _resp(403)
    if "linkedin.com" in url_l:                     return _resp(403)
    if "ziprecruiter" in url_l:                     return _resp(403)
    if "glassdoor" in url_l:                        return _resp(403)
    if "rss" in url_l or "news.google" in url_l:   return _resp(200, RSS_XML, "application/rss+xml")
    return _resp(200, FRESH_HTML)

def mock_post(url, **kw):
    return _resp(200, '{"ok":true}')

# ── Cycle runner ───────────────────────────────────────────────────────────
TOTAL = 30
all_results = []

print(f"{'='*60}")
print(f"30-CYCLE LOCAL TEST — {time.strftime('%H:%M:%S')}")
print(f"{'='*60}")

for cycle in range(1, TOTAL + 1):
    db_path = f"/tmp/tracker_cycle_{cycle}.db"
    if os.path.exists(db_path): os.remove(db_path)

    # Capture logs
    log_buf = io.StringIO()
    handler = logging.StreamHandler(log_buf)
    handler.setFormatter(logging.Formatter("%(levelname)s %(name)s: %(message)s"))
    logging.root.handlers = [handler]
    logging.root.setLevel(logging.DEBUG)

    errors, n_signals, n_alerts = [], 0, 0
    t0 = time.time()

    try:
        import scrapers.base as base_mod
        base_mod._DOMAIN_RATE_LIMITED.clear()

        from config import Config
        from firms import FIRMS
        from database.db import Database
        from scrapers.jobs import JobsScraper
        from scrapers.press import PressScraper
        from scrapers.publications import PublicationsScraper
        from scrapers.website import WebsiteScraper
        from scrapers.canlii import CanLIIScraper
        from scrapers.chambers import ChambersScraper
        from scrapers.lawschool import LawSchoolScraper
        from scrapers.barassoc import BarAssociationScraper
        from analysis.signals import ExpansionAnalyzer
        from alerts.notifier import Notifier
        import concurrent.futures

        cfg = Config(); cfg.DB_PATH = db_path
        db = Database(db_path)
        analyzer = ExpansionAnalyzer(db)
        notifier = Notifier(cfg)

        scrapers_list = [
            JobsScraper(), PressScraper(), PublicationsScraper(),
            WebsiteScraper(), CanLIIScraper(), ChambersScraper(),
            LawSchoolScraper(), BarAssociationScraper(),
        ]

        all_signals = []
        # Use first 3 firms — representative sample (Davies, Blakes, McCarthy)
        with patch("requests.Session.get", side_effect=mock_get), \
             patch("requests.post", side_effect=mock_post):
            for firm in FIRMS[:3]:
                for scraper in scrapers_list:
                    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as ex:
                        fut = ex.submit(scraper.fetch, firm)
                        try:
                            sigs = fut.result(timeout=30)
                            for s in sigs:
                                if db.is_new_signal(s):
                                    db.save_signal(s)
                                    all_signals.append(s)
                        except concurrent.futures.TimeoutError:
                            errors.append(f"TIMEOUT: {scraper.name}/{firm['short']}")
                        except Exception as e:
                            errors.append(f"{scraper.name}/{firm['short']}: {type(e).__name__}: {str(e)[:120]}")

        # Analysis
        weekly = db.get_signals_this_week()
        alerts = analyzer.analyze(weekly)
        changes = analyzer.detect_website_changes(all_signals)
        for a in alerts:
            db.save_weekly_score(
                firm_id=a["firm_id"], firm_name=a["firm_name"],
                department=a["department"], score=a["expansion_score"],
                signal_count=a["signal_count"], breakdown=a["signal_breakdown"],
            )
        n_signals = len(all_signals)
        n_alerts = len(alerts)

        # Notification
        with patch("requests.post", side_effect=mock_post):
            notifier.send_combined_digest(alerts, changes, new_signals=all_signals)

        db.close()

    except Exception as e:
        errors.append(f"PIPELINE: {type(e).__name__}: {str(e)[:150]}\n{traceback.format_exc()[-300:]}")

    elapsed = round(time.time() - t0, 1)

    # Parse captured logs for ERROR lines not already in errors list
    for line in log_buf.getvalue().splitlines():
        if "ERROR" in line and not any(line[-80:] in e for e in errors):
            errors.append(f"LOG: {line[-120:]}")

    status = "✅" if not errors else "❌"
    err_str = f" | {errors[0][:90]}" if errors else ""
    print(f"  Cycle {cycle:2d}/{TOTAL} {status} | {n_signals:3d} signals | {n_alerts} alerts | {elapsed:5.1f}s{err_str}")

    all_results.append({
        "cycle": cycle, "ok": not errors,
        "errors": errors, "signals": n_signals,
        "alerts": n_alerts, "elapsed": elapsed,
    })

# ── Summary ────────────────────────────────────────────────────────────────
passes = sum(1 for r in all_results if r["ok"])
fails  = TOTAL - passes
print(f"\n{'='*60}")
print(f"RESULT: {passes}/{TOTAL} PASS  {fails} FAIL")
if fails:
    print("\nAll failures:")
    for r in all_results:
        if not r["ok"]:
            print(f"  Cycle {r['cycle']}: {r['errors']}")
avg = sum(r["elapsed"] for r in all_results) / TOTAL
print(f"Avg cycle time: {avg:.1f}s")
json.dump(all_results, open("/tmp/cycle_results.json","w"), indent=2)
