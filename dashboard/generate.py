"""
Dashboard generator — writes docs/index.html for Vercel.

BUG FIX (v5): column is 'dept_score' not 'department_score' — was crashing every run.
NEW (v5):
  • Momentum tracker — firms trending UP vs last week shown with ↑ indicator
  • Velocity chart — signals per day sparkline
  • Top departments by cumulative expansion score
  • Recency weighting in signal table
  • Dashboard self-links to https://law-firm-tracker.vercel.app/
"""

import json
import sqlite3
import logging
from datetime import datetime, timezone, timedelta
from pathlib import Path
from collections import defaultdict

logger = logging.getLogger("dashboard")

# HARDCODED — never changes
VERCEL_URL = "https://law-firm-tracker.vercel.app/"

DEPT_META = {
    "Corporate/M&A":                {"emoji": "🤝", "color": "#6366f1"},
    "Capital Markets":              {"emoji": "📈", "color": "#3b82f6"},
    "Private Equity":               {"emoji": "💰", "color": "#8b5cf6"},
    "Litigation & Disputes":        {"emoji": "⚖️", "color": "#ef4444"},
    "Litigation":                   {"emoji": "⚖️", "color": "#ef4444"},
    "Restructuring & Insolvency":   {"emoji": "🔄", "color": "#f97316"},
    "Restructuring":                {"emoji": "🔄", "color": "#f97316"},
    "Real Estate":                  {"emoji": "🏢", "color": "#10b981"},
    "Tax":                          {"emoji": "🧾", "color": "#f59e0b"},
    "Employment & Labour":          {"emoji": "👷", "color": "#84cc16"},
    "Employment":                   {"emoji": "👷", "color": "#84cc16"},
    "Intellectual Property":        {"emoji": "💡", "color": "#06b6d4"},
    "IP":                           {"emoji": "💡", "color": "#06b6d4"},
    "Data Privacy & Cybersecurity": {"emoji": "🔒", "color": "#ec4899"},
    "Data Privacy":                 {"emoji": "🔒", "color": "#ec4899"},
    "ESG & Regulatory":             {"emoji": "🌿", "color": "#22c55e"},
    "ESG":                          {"emoji": "🌿", "color": "#22c55e"},
    "Energy & Natural Resources":   {"emoji": "⚡", "color": "#eab308"},
    "Energy":                       {"emoji": "⚡", "color": "#eab308"},
    "Financial Services":           {"emoji": "🏦", "color": "#0ea5e9"},
    "Competition & Antitrust":      {"emoji": "🔍", "color": "#a855f7"},
    "Competition":                  {"emoji": "🔍", "color": "#a855f7"},
    "Healthcare & Life Sciences":   {"emoji": "🏥", "color": "#14b8a6"},
    "Healthcare":                   {"emoji": "🏥", "color": "#14b8a6"},
    "Immigration":                  {"emoji": "🛂", "color": "#64748b"},
    "Infrastructure & Projects":    {"emoji": "🏗️", "color": "#78716c"},
    "Infrastructure":               {"emoji": "🏗️", "color": "#78716c"},
}

SIGNAL_META = {
    "lateral_hire":     {"label": "Lateral Hire",      "emoji": "🔀", "color": "#6366f1"},
    "job_posting":      {"label": "Job Posting",       "emoji": "💼", "color": "#3b82f6"},
    "press_release":    {"label": "Press Release",     "emoji": "📰", "color": "#10b981"},
    "publication":      {"label": "Publication",       "emoji": "📄", "color": "#f59e0b"},
    "practice_page":    {"label": "Practice Page",     "emoji": "🌐", "color": "#8b5cf6"},
    "attorney_profile": {"label": "Attorney Profile",  "emoji": "👔", "color": "#ec4899"},
    "bar_leadership":   {"label": "Bar Leadership",    "emoji": "🏅", "color": "#f97316"},
    "ranking":          {"label": "Ranking",           "emoji": "🏆", "color": "#eab308"},
    "court_record":     {"label": "Court Record",      "emoji": "⚖️", "color": "#ef4444"},
    "recruit_posting":  {"label": "Articling Recruit", "emoji": "🎓", "color": "#06b6d4"},
    "deal_counsel":     {"label": "Deal Counsel",      "emoji": "🤝", "color": "#a855f7"},
    "media_mention":    {"label": "Media Mention",     "emoji": "📡", "color": "#84cc16"},
}

SCORE_THRESHOLDS = [
    (12.0, "Very Strong", "#ef4444"),
    (8.0,  "Strong",      "#f97316"),
    (5.0,  "Moderate",    "#f59e0b"),
    (0.0,  "Emerging",    "#6366f1"),
]


def _score_meta(score: float) -> tuple:
    for thresh, label, color in SCORE_THRESHOLDS:
        if score >= thresh:
            return label, color
    return "Emerging", "#6366f1"


def _dept_meta(dept: str) -> dict:
    if not dept:
        return {"emoji": "⚖️", "color": "#64748b"}
    # Exact match first
    if dept in DEPT_META:
        return DEPT_META[dept]
    # Partial match
    dl = dept.lower()
    for key, meta in DEPT_META.items():
        if key.lower() in dl or dl in key.lower():
            return meta
    return {"emoji": "⚖️", "color": "#64748b"}


def _load_data(db_path: str) -> dict:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    now      = datetime.now(timezone.utc)
    cut_30d  = (now - timedelta(days=30)).strftime("%Y-%m-%d")
    cut_14d  = (now - timedelta(days=14)).strftime("%Y-%m-%d")
    cut_7d   = (now - timedelta(days=7)).strftime("%Y-%m-%d")
    cut_prev = (now - timedelta(days=14)).strftime("%Y-%m-%d")  # week 2-3 ago

    # ── Signals (30d) — BUG FIX: column is 'dept_score' not 'department_score' ──
    cur.execute("""
        SELECT firm_id, firm_name, signal_type, title, url,
               department, dept_score, collected_at
        FROM signals
        WHERE collected_at >= ?
        ORDER BY dept_score DESC, collected_at DESC
    """, (cut_30d,))
    signals = [dict(r) for r in cur.fetchall()]

    # ── Weekly scores ──────────────────────────────────────────────────────────
    cur.execute("""
        SELECT firm_id, firm_name, department, score, signal_count, breakdown, week_start
        FROM weekly_scores
        ORDER BY week_start DESC, score DESC
        LIMIT 400
    """)
    scores = [dict(r) for r in cur.fetchall()]

    # ── Top alerts this week ───────────────────────────────────────────────────
    cur.execute("""
        SELECT firm_id, firm_name, department, score, signal_count, breakdown
        FROM weekly_scores WHERE week_start >= ?
        ORDER BY score DESC LIMIT 30
    """, (cut_7d,))
    top_alerts = [dict(r) for r in cur.fetchall()]

    # ── Alert count ────────────────────────────────────────────────────────────
    cur.execute("SELECT COUNT(*) FROM weekly_scores WHERE week_start >= ?", (cut_7d,))
    alert_count_7d = (cur.fetchone() or [0])[0]

    # ── Signal type breakdown (30d) ────────────────────────────────────────────
    cur.execute("""
        SELECT signal_type, COUNT(*) as cnt FROM signals
        WHERE collected_at >= ? GROUP BY signal_type ORDER BY cnt DESC
    """, (cut_30d,))
    by_type = [dict(r) for r in cur.fetchall()]

    # ── Firm activity (30d) ────────────────────────────────────────────────────
    cur.execute("""
        SELECT firm_name, COUNT(*) as cnt FROM signals
        WHERE collected_at >= ? GROUP BY firm_name ORDER BY cnt DESC LIMIT 15
    """, (cut_30d,))
    by_firm = [dict(r) for r in cur.fetchall()]

    # ── Momentum: score this week vs previous week per firm ───────────────────
    this_week: dict  = defaultdict(float)
    prev_week: dict  = defaultdict(float)
    for s in scores:
        if s["week_start"] >= cut_7d:
            this_week[s["firm_name"]] += s["score"]
        elif s["week_start"] >= cut_prev:
            prev_week[s["firm_name"]] += s["score"]

    momentum = []
    for firm, cur_score in sorted(this_week.items(), key=lambda x: -x[1])[:10]:
        prev = prev_week.get(firm, 0)
        delta = cur_score - prev
        pct   = (delta / prev * 100) if prev > 0 else 0
        momentum.append({
            "firm":       firm.split()[0],
            "full_name":  firm,
            "score":      round(cur_score, 1),
            "prev":       round(prev, 1),
            "delta":      round(delta, 1),
            "pct":        round(pct, 0),
        })

    # ── 14-day daily signal volume ─────────────────────────────────────────────
    daily_raw: dict = defaultdict(int)
    for s in signals:
        day = (s.get("collected_at") or "")[:10]
        if day >= cut_14d:
            daily_raw[day] += 1
    daily_series = []
    for i in range(13, -1, -1):
        d = (now - timedelta(days=i)).strftime("%Y-%m-%d")
        daily_series.append({"date": d[-5:], "count": daily_raw.get(d, 0)})

    # ── Dept cumulative scores ─────────────────────────────────────────────────
    dept_totals: dict = defaultdict(float)
    for s in scores:
        if s["week_start"] >= cut_7d:
            dept_totals[s["department"]] += s["score"]

    dept_scores = sorted(
        [{"dept": d, "score": round(v, 1), **_dept_meta(d)}
         for d, v in dept_totals.items()],
        key=lambda x: -x["score"]
    )[:10]

    conn.close()
    return {
        "signals":          signals,
        "scores":           scores,
        "top_alerts":       top_alerts,
        "by_type":          by_type,
        "by_firm":          by_firm,
        "daily_series":     daily_series,
        "momentum":         momentum,
        "dept_scores":      dept_scores,
        "generated_at":     now.strftime("%Y-%m-%d %H:%M UTC"),
        "signal_count_30d": len(signals),
        "signal_count_7d":  sum(1 for s in signals if s.get("collected_at","") >= cut_7d),
        "alert_count_7d":   alert_count_7d,
        "firm_count":       len(set(r["firm_name"] for r in signals)),
        "type_count":       len(by_type),
    }


def generate_dashboard(db_path: str = "law_firm_tracker.db",
                       out_path: str = "docs/index.html") -> str:
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)

    if not Path(db_path).exists() or db_path == ":memory:":
        logger.warning(f"DB not found at {db_path} — generating empty dashboard")
        data = {
            "signals": [], "scores": [], "top_alerts": [],
            "by_type": [], "by_firm": [], "daily_series": [],
            "momentum": [], "dept_scores": [],
            "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
            "signal_count_30d": 0, "signal_count_7d": 0, "alert_count_7d": 0,
            "firm_count": 0, "type_count": 0,
        }
    else:
        data = _load_data(db_path)

    html = _render(data)
    Path(out_path).write_text(html, encoding="utf-8")
    logger.info(f"Dashboard → {out_path} ({data['signal_count_30d']} signals, "
                f"{data['alert_count_7d']} alerts)")
    return out_path


# ─────────────────────────────────────────────────────────────────────────────
# Renderers
# ─────────────────────────────────────────────────────────────────────────────

def _render_alerts(alerts: list) -> str:
    if not alerts:
        return ('<div class="empty-state">'
                '<div class="empty-icon">📭</div>'
                '<p>No alerts yet — run a collect cycle first.</p></div>')
    rows = []
    for a in alerts[:20]:
        score    = round(a["score"], 1)
        dept     = a.get("department", "Unknown")
        meta     = _dept_meta(dept)
        lbl, col = _score_meta(score)
        sc_pct   = min(score / 15 * 100, 100)
        rows.append(
            f'<div class="alert-card">'
            f'<div class="alert-top">'
            f'<div><div class="alert-firm">{a["firm_name"]}</div>'
            f'<div class="alert-dept">{meta["emoji"]} {dept}</div></div>'
            f'<div class="alert-right">'
            f'<div class="alert-score" style="color:{col}">{score}</div>'
            f'<div class="score-bar-wrap">'
            f'<div class="score-bar" style="width:{sc_pct:.0f}%;background:{col}"></div>'
            f'</div></div></div>'
            f'<div class="alert-meta">'
            f'<span class="badge" style="background:{col}22;color:{col};border:1px solid {col}44">{lbl}</span>'
            f'<span class="sig-count">{a["signal_count"]} signals</span>'
            f'</div></div>'
        )
    return "".join(rows)


def _render_momentum(momentum: list) -> str:
    if not momentum:
        return ('<div class="empty-state">'
                '<div class="empty-icon">📊</div>'
                '<p>Momentum data available after 2 weeks of tracking.</p></div>')
    rows = []
    for m in momentum:
        delta   = m["delta"]
        pct     = m["pct"]
        arrow   = "↑" if delta > 0 else ("↓" if delta < 0 else "→")
        col     = "#10b981" if delta > 0 else ("#ef4444" if delta < 0 else "#64748b")
        sign    = "+" if delta >= 0 else ""
        rows.append(
            f'<div class="momentum-row">'
            f'<div class="mom-firm">{m["full_name"]}</div>'
            f'<div class="mom-right">'
            f'<span class="mom-score">{m["score"]}</span>'
            f'<span class="mom-delta" style="color:{col}">'
            f'{arrow} {sign}{delta} ({sign}{pct:.0f}%)</span>'
            f'</div></div>'
        )
    return "".join(rows)


def _render_signals(signals: list) -> str:
    if not signals:
        return ('<div class="empty-state">'
                '<div class="empty-icon">🔍</div><p>No signals yet.</p></div>')
    rows = []
    for s in signals[:80]:
        raw_type = s.get("signal_type", "")
        sm       = SIGNAL_META.get(raw_type, {"label": raw_type.replace("_"," ").title(),
                                              "emoji": "•", "color": "#64748b"})
        title    = s["title"][:90] + ("…" if len(s["title"]) > 90 else "")
        url      = s.get("url", "")
        dept     = s.get("department", "")
        dm       = _dept_meta(dept)
        date     = (s.get("collected_at") or "")[:10]
        score    = round(s.get("dept_score", 0), 1)
        _, sc    = _score_meta(score)
        title_html = (f'<a href="{url}" target="_blank" rel="noopener">{title}</a>'
                      if url else title)
        rows.append(
            f'<tr data-firm="{s["firm_name"].lower()}" '
            f'data-type="{raw_type}" data-dept="{dept.lower()}">'
            f'<td><span class="type-tag" '
            f'style="background:{sm["color"]}22;color:{sm["color"]}">'
            f'{sm["emoji"]} {sm["label"]}</span></td>'
            f'<td class="sig-title">{title_html}</td>'
            f'<td class="firm-col">{s["firm_name"].split()[0]}</td>'
            f'<td><span class="pill-sm" '
            f'style="background:{dm["color"]}22;color:{dm["color"]}">'
            f'{dm["emoji"]} {dept}</span></td>'
            f'<td><span class="score-num" style="color:{sc}">{score}</span></td>'
            f'<td class="date-col">{date}</td>'
            f'</tr>'
        )

    type_options = "".join(
        f'<option value="{k}">{v["emoji"]} {v["label"]}</option>'
        for k, v in SIGNAL_META.items()
    )
    return (
        f'<div class="table-toolbar">'
        f'<input class="search-box" id="sigSearch" placeholder="🔍  Search signals…" '
        f'oninput="filterSigs(this.value)">'
        f'<select class="filter-select" id="typeFilter" '
        f'onchange="filterSigs(document.getElementById(\'sigSearch\').value)">'
        f'<option value="">All types</option>{type_options}</select>'
        f'</div>'
        f'<div class="table-wrap">'
        f'<table id="sigsTable">'
        f'<thead><tr><th>Type</th><th>Signal</th><th>Firm</th>'
        f'<th>Department</th><th>Score</th><th>Date</th></tr></thead>'
        f'<tbody>{"".join(rows)}</tbody>'
        f'</table></div>'
    )


def _render(data: dict) -> str:
    alerts_html   = _render_alerts(data["top_alerts"])
    momentum_html = _render_momentum(data["momentum"])
    signals_html  = _render_signals(data["signals"])

    by_type_json = json.dumps([
        {"type":  SIGNAL_META.get(r["signal_type"], {"label": r["signal_type"]})["label"],
         "count": r["cnt"],
         "color": SIGNAL_META.get(r["signal_type"], {"color": "#64748b"})["color"]}
        for r in data["by_type"]
    ])
    by_firm_json = json.dumps([
        {"firm": r["firm_name"].split()[0], "count": r["cnt"]}
        for r in data["by_firm"]
    ])
    daily_json = json.dumps(data["daily_series"])
    dept_json  = json.dumps([
        {"dept": d["dept"], "score": d["score"], "color": d["color"], "emoji": d.get("emoji","⚖️")}
        for d in data["dept_scores"]
    ])

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Law Firm Expansion Tracker</title>
<link rel="icon" href="data:image/svg+xml,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 100 100'><text y='.9em' font-size='90'>⚖️</text></svg>">
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.min.js"></script>
<style>
*,*::before,*::after{{box-sizing:border-box;margin:0;padding:0}}
:root{{
  --bg:#080b14;--surface:#0d1220;--card:#111827;
  --border:#1e2a3a;--border2:#2a3a52;
  --text:#e2e8f0;--muted:#64748b;--muted2:#94a3b8;
  --accent:#6366f1;--accent2:#818cf8;
  --green:#10b981;--amber:#f59e0b;--red:#ef4444;
  --r:12px;
}}
html{{scroll-behavior:smooth}}
body{{background:var(--bg);color:var(--text);font-family:-apple-system,BlinkMacSystemFont,'Inter','Segoe UI',sans-serif;font-size:14px;line-height:1.5;min-height:100vh}}

/* HEADER */
.hdr{{position:relative;overflow:hidden;background:linear-gradient(135deg,#09101f 0%,#0e1835 50%,#09101f 100%);border-bottom:1px solid var(--border);padding:20px 32px}}
.hdr::before{{content:'';position:absolute;inset:0;background:radial-gradient(ellipse 70% 100% at 5% 50%,rgba(99,102,241,.13) 0%,transparent 65%),radial-gradient(ellipse 40% 70% at 92% 50%,rgba(16,185,129,.09) 0%,transparent 65%);pointer-events:none}}
.hdr-inner{{position:relative;display:flex;align-items:center;justify-content:space-between;gap:16px;flex-wrap:wrap}}
.hdr-brand{{display:flex;align-items:center;gap:14px}}
.hdr-icon{{width:44px;height:44px;border-radius:10px;background:linear-gradient(135deg,#4f52d3,#818cf8);display:flex;align-items:center;justify-content:center;font-size:22px;box-shadow:0 0 28px rgba(99,102,241,.45);flex-shrink:0}}
.hdr-title{{font-size:20px;font-weight:800;letter-spacing:-.4px;background:linear-gradient(90deg,#e2e8f0,#a5b4fc);-webkit-background-clip:text;-webkit-text-fill-color:transparent;background-clip:text}}
.hdr-sub{{font-size:12px;color:var(--muted);margin-top:2px}}
.hdr-right{{display:flex;align-items:center;gap:10px;flex-wrap:wrap}}
.live-badge{{display:flex;align-items:center;gap:6px;padding:5px 10px;border-radius:99px;background:rgba(16,185,129,.1);border:1px solid rgba(16,185,129,.25);font-size:11px;color:var(--green);font-weight:600}}
.dot{{width:7px;height:7px;border-radius:50%;background:var(--green);box-shadow:0 0 8px var(--green);animation:pulse 2s ease-in-out infinite}}
@keyframes pulse{{0%,100%{{opacity:1;transform:scale(1)}}50%{{opacity:.5;transform:scale(.8)}}}}
.hdr-ts{{font-size:11px;color:var(--muted)}}
.vercel-btn{{display:inline-flex;align-items:center;gap:7px;padding:7px 14px;border-radius:8px;background:rgba(99,102,241,.14);border:1px solid rgba(99,102,241,.28);color:#a5b4fc;font-size:12px;font-weight:700;text-decoration:none;transition:all .2s;white-space:nowrap}}
.vercel-btn:hover{{background:rgba(99,102,241,.24);border-color:rgba(99,102,241,.5);color:#c7d2fe}}

/* LAYOUT */
.main{{padding:20px 24px 56px;max-width:1680px;margin:0 auto}}

/* KPIs */
.kpi-row{{display:grid;grid-template-columns:repeat(4,1fr);gap:14px;margin-bottom:20px}}
@media(max-width:900px){{.kpi-row{{grid-template-columns:repeat(2,1fr)}}}}
@media(max-width:480px){{.kpi-row{{grid-template-columns:1fr}}}}
.kpi{{background:var(--card);border:1px solid var(--border);border-radius:var(--r);padding:18px 20px;position:relative;overflow:hidden;transition:border-color .2s,transform .15s;cursor:default}}
.kpi:hover{{border-color:var(--border2);transform:translateY(-2px)}}
.kpi::after{{content:'';position:absolute;top:0;left:0;right:0;height:2px;background:var(--kpi-g,linear-gradient(90deg,#6366f1,#818cf8))}}
.kpi-lbl{{font-size:10px;font-weight:700;color:var(--muted);text-transform:uppercase;letter-spacing:.08em;margin-bottom:8px}}
.kpi-val{{font-size:36px;font-weight:900;line-height:1;letter-spacing:-2px;color:var(--text)}}
.kpi-sub{{font-size:11px;color:var(--muted);margin-top:5px}}
.kpi-ico{{position:absolute;right:16px;top:50%;transform:translateY(-50%);font-size:32px;opacity:.1}}

/* SPARKLINE */
.spark-card{{background:var(--card);border:1px solid var(--border);border-radius:var(--r);padding:16px 20px;margin-bottom:20px}}
.card-lbl{{font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:.08em;color:var(--muted);margin-bottom:10px;display:flex;align-items:center;gap:8px}}
.card-lbl em{{font-style:normal;font-weight:400;color:var(--muted);text-transform:none;letter-spacing:0;font-size:11px}}
.spark-wrap{{height:66px}}

/* GRIDS */
.g3{{display:grid;grid-template-columns:1fr 1fr 1fr;gap:18px;margin-bottom:20px}}
.g2{{display:grid;grid-template-columns:1fr 1fr;gap:18px;margin-bottom:20px;align-items:start}}
.g2-3{{display:grid;grid-template-columns:2fr 3fr;gap:18px;margin-bottom:20px;align-items:start}}
@media(max-width:1100px){{.g3{{grid-template-columns:1fr 1fr}}}}
@media(max-width:720px){{.g3,.g2,.g2-3{{grid-template-columns:1fr}}}}

/* CARD */
.card{{background:var(--card);border:1px solid var(--border);border-radius:var(--r);padding:18px 20px}}
canvas{{max-height:230px!important}}

/* ALERTS */
.alert-card{{padding:11px 0;border-bottom:1px solid var(--border)}}
.alert-card:last-child{{border-bottom:none}}
.alert-top{{display:flex;justify-content:space-between;align-items:flex-start;gap:8px;margin-bottom:7px}}
.alert-firm{{font-weight:700;font-size:13px}}
.alert-dept{{font-size:11px;color:var(--muted2);margin-top:2px}}
.alert-right{{text-align:right;flex-shrink:0}}
.alert-score{{font-size:24px;font-weight:900;letter-spacing:-1px}}
.score-bar-wrap{{height:3px;width:80px;background:var(--border);border-radius:99px;margin-top:5px;margin-left:auto}}
.score-bar{{height:3px;border-radius:99px}}
.alert-meta{{display:flex;align-items:center;gap:8px}}
.sig-count{{font-size:11px;color:var(--muted)}}

/* MOMENTUM */
.momentum-row{{display:flex;justify-content:space-between;align-items:center;padding:10px 0;border-bottom:1px solid var(--border)}}
.momentum-row:last-child{{border-bottom:none}}
.mom-firm{{font-weight:600;font-size:12px}}
.mom-right{{text-align:right}}
.mom-score{{font-weight:800;font-size:15px;display:block}}
.mom-delta{{font-size:11px;font-weight:600}}

/* BADGES / PILLS */
.badge{{display:inline-block;padding:2px 8px;border-radius:99px;font-size:10px;font-weight:700;letter-spacing:.03em}}
.pill-sm{{display:inline-block;padding:2px 7px;border-radius:5px;font-size:10px;font-weight:600;white-space:nowrap}}
.type-tag{{display:inline-block;padding:3px 8px;border-radius:6px;font-size:10px;font-weight:700;white-space:nowrap}}

/* TABLE */
.table-toolbar{{display:flex;gap:10px;margin-bottom:12px;flex-wrap:wrap}}
.search-box,.filter-select{{background:var(--surface);border:1px solid var(--border2);border-radius:8px;color:var(--text);padding:7px 12px;font-size:13px;outline:none;transition:border-color .2s}}
.search-box{{flex:1;min-width:180px}}
.search-box:focus,.filter-select:focus{{border-color:var(--accent)}}
.filter-select{{cursor:pointer}}
.table-wrap{{overflow-x:auto;max-height:520px;overflow-y:auto}}
.table-wrap::-webkit-scrollbar{{width:4px;height:4px}}
.table-wrap::-webkit-scrollbar-track{{background:var(--border)}}
.table-wrap::-webkit-scrollbar-thumb{{background:var(--border2);border-radius:99px}}
table{{width:100%;border-collapse:collapse;font-size:12px}}
thead th{{position:sticky;top:0;background:var(--card);z-index:1}}
th{{color:var(--muted);text-align:left;padding:8px 10px;border-bottom:1px solid var(--border);font-weight:700;text-transform:uppercase;letter-spacing:.05em;font-size:10px;white-space:nowrap}}
td{{padding:8px 10px;border-bottom:1px solid var(--border);vertical-align:middle}}
tr:last-child td{{border-bottom:none}}
tr:hover td{{background:rgba(99,102,241,.04)}}
td a{{color:var(--accent2);text-decoration:none}}
td a:hover{{color:#c7d2fe;text-decoration:underline}}
.sig-title{{max-width:340px}}
.firm-col{{white-space:nowrap;font-weight:600;font-size:11px}}
.score-num{{font-weight:800;font-size:13px}}
.date-col{{color:var(--muted);white-space:nowrap;font-size:11px}}

/* EMPTY */
.empty-state{{display:flex;flex-direction:column;align-items:center;justify-content:center;padding:36px 20px;text-align:center;color:var(--muted)}}
.empty-icon{{font-size:34px;margin-bottom:10px;opacity:.4}}
.empty-state p{{font-size:12px}}

/* FOOTER */
.footer{{border-top:1px solid var(--border);padding:16px 32px;display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:12px;color:var(--muted);font-size:12px}}
.footer a{{color:var(--muted2);text-decoration:none}}
.footer a:hover{{color:var(--text)}}
</style>
</head>
<body>

<header class="hdr">
  <div class="hdr-inner">
    <div class="hdr-brand">
      <div class="hdr-icon">⚖️</div>
      <div>
        <div class="hdr-title">Law Firm Expansion Tracker</div>
        <div class="hdr-sub">Canadian legal market intelligence · 26 firms · 28 scrapers</div>
      </div>
    </div>
    <div class="hdr-right">
      <div class="live-badge"><div class="dot"></div>Live</div>
      <span class="hdr-ts">Updated {data["generated_at"]}</span>
      <a href="{VERCEL_URL}" class="vercel-btn" target="_blank" rel="noopener">
        🌐 law-firm-tracker.vercel.app
      </a>
    </div>
  </div>
</header>

<main class="main">

  <!-- KPIs -->
  <div class="kpi-row">
    <div class="kpi" style="--kpi-g:linear-gradient(90deg,#6366f1,#818cf8)">
      <div class="kpi-lbl">Signals (30d)</div>
      <div class="kpi-val">{data["signal_count_30d"]}</div>
      <div class="kpi-sub">raw intelligence gathered</div>
      <div class="kpi-ico">📡</div>
    </div>
    <div class="kpi" style="--kpi-g:linear-gradient(90deg,#3b82f6,#06b6d4)">
      <div class="kpi-lbl">Signals This Week</div>
      <div class="kpi-val">{data["signal_count_7d"]}</div>
      <div class="kpi-sub">last 7 days</div>
      <div class="kpi-ico">📊</div>
    </div>
    <div class="kpi" style="--kpi-g:linear-gradient(90deg,#ef4444,#f97316)">
      <div class="kpi-lbl">Active Alerts</div>
      <div class="kpi-val">{data["alert_count_7d"]}</div>
      <div class="kpi-sub">expansion signals this week</div>
      <div class="kpi-ico">🚨</div>
    </div>
    <div class="kpi" style="--kpi-g:linear-gradient(90deg,#10b981,#84cc16)">
      <div class="kpi-lbl">Firms Active</div>
      <div class="kpi-val">{data["firm_count"] or "—"}</div>
      <div class="kpi-sub">with signals this month</div>
      <div class="kpi-ico">🏛</div>
    </div>
  </div>

  <!-- Sparkline -->
  <div class="spark-card">
    <div class="card-lbl">📈 Signal Volume <em>— last 14 days</em></div>
    <div class="spark-wrap"><canvas id="sparkChart"></canvas></div>
  </div>

  <!-- Charts row -->
  <div class="g3">
    <div class="card">
      <div class="card-lbl">📊 Signals by Type <em>(30d)</em></div>
      {"<canvas id='typeChart'></canvas>" if data["by_type"] else '<div class="empty-state"><div class="empty-icon">📊</div><p>No data yet</p></div>'}
    </div>
    <div class="card">
      <div class="card-lbl">🏛 Firm Activity <em>(30d)</em></div>
      {"<canvas id='firmChart'></canvas>" if data["by_firm"] else '<div class="empty-state"><div class="empty-icon">🏛</div><p>No data yet</p></div>'}
    </div>
    <div class="card">
      <div class="card-lbl">⚖️ Top Departments <em>(this week)</em></div>
      {"<canvas id='deptChart'></canvas>" if data["dept_scores"] else '<div class="empty-state"><div class="empty-icon">⚖️</div><p>No data yet</p></div>'}
    </div>
  </div>

  <!-- Alerts + Momentum -->
  <div class="g2">
    <div class="card">
      <div class="card-lbl">🚨 Top Expansion Alerts <em>(this week)</em></div>
      {alerts_html}
    </div>
    <div class="card">
      <div class="card-lbl">🚀 Firm Momentum <em>(vs prior week)</em></div>
      {momentum_html}
    </div>
  </div>

  <!-- Signals table (full width) -->
  <div class="card">
    <div class="card-lbl" style="margin-bottom:0">📡 Recent Signals <em>(30d · searchable)</em></div>
    {signals_html}
  </div>

</main>

<footer class="footer">
  <div>⚖️ Law Firm Expansion Tracker · 28 scrapers · 26 Canadian firms ·
    <a href="{VERCEL_URL}" target="_blank">law-firm-tracker.vercel.app</a>
  </div>
  <div>Updated {data["generated_at"]}</div>
</footer>

<script>
const byType  = {by_type_json};
const byFirm  = {by_firm_json};
const daily   = {daily_json};
const depts   = {dept_json};

Chart.defaults.color = '#64748b';
Chart.defaults.font.family = "-apple-system,BlinkMacSystemFont,'Inter','Segoe UI',sans-serif";

// Sparkline
const spEl = document.getElementById('sparkChart');
if (spEl) {{
  const ctx = spEl.getContext('2d');
  const g = ctx.createLinearGradient(0, 0, 0, 66);
  g.addColorStop(0, 'rgba(99,102,241,.42)');
  g.addColorStop(1, 'rgba(99,102,241,.02)');
  new Chart(spEl, {{
    type: 'line',
    data: {{
      labels: daily.map(d => d.date),
      datasets: [{{ data: daily.map(d => d.count), borderColor: '#6366f1', backgroundColor: g,
        borderWidth: 2, fill: true, tension: 0.4,
        pointRadius: 3, pointBackgroundColor: '#6366f1',
        pointBorderColor: '#111827', pointBorderWidth: 2 }}]
    }},
    options: {{
      responsive: true, maintainAspectRatio: false,
      plugins: {{ legend: {{ display: false }} }},
      scales: {{
        x: {{ grid: {{ color: '#1e2a3a' }}, ticks: {{ color: '#64748b', font: {{ size: 10 }} }} }},
        y: {{ grid: {{ color: '#1e2a3a' }}, ticks: {{ color: '#64748b', stepSize: 1 }}, beginAtZero: true }}
      }}
    }}
  }});
}}

// Doughnut — signal types
const tEl = document.getElementById('typeChart');
if (tEl && byType.length) {{
  new Chart(tEl, {{
    type: 'doughnut',
    data: {{
      labels: byType.map(d => d.type),
      datasets: [{{ data: byType.map(d => d.count),
        backgroundColor: byType.map(d => d.color),
        borderColor: '#111827', borderWidth: 3, hoverBorderWidth: 0 }}]
    }},
    options: {{
      responsive: true, cutout: '66%',
      plugins: {{
        legend: {{ position: 'bottom', labels: {{
          color: '#94a3b8', boxWidth: 10, boxHeight: 10, padding: 10, font: {{ size: 10 }}
        }} }},
        tooltip: {{ callbacks: {{ label: c => ` ${{c.label}}: ${{c.raw}}` }} }}
      }}
    }}
  }});
}}

// Bar — firm activity
const fEl = document.getElementById('firmChart');
if (fEl && byFirm.length) {{
  new Chart(fEl, {{
    type: 'bar',
    data: {{
      labels: byFirm.map(d => d.firm),
      datasets: [{{ data: byFirm.map(d => d.count),
        backgroundColor: byFirm.map((_,i) => `hsla(${{190+i*18}},65%,58%,.85)`),
        borderRadius: 5, borderSkipped: false }}]
    }},
    options: {{
      indexAxis: 'y', responsive: true,
      plugins: {{ legend: {{ display: false }} }},
      scales: {{
        x: {{ grid: {{ color: '#1e2a3a' }}, ticks: {{ color: '#64748b' }} }},
        y: {{ grid: {{ display: false }}, ticks: {{ color: '#e2e8f0', font: {{ size: 11 }} }} }}
      }}
    }}
  }});
}}

// Bar — dept scores
const dEl = document.getElementById('deptChart');
if (dEl && depts.length) {{
  new Chart(dEl, {{
    type: 'bar',
    data: {{
      labels: depts.map(d => d.emoji + ' ' + d.dept),
      datasets: [{{ data: depts.map(d => d.score),
        backgroundColor: depts.map(d => d.color + 'cc'),
        borderRadius: 5, borderSkipped: false }}]
    }},
    options: {{
      indexAxis: 'y', responsive: true,
      plugins: {{ legend: {{ display: false }} }},
      scales: {{
        x: {{ grid: {{ color: '#1e2a3a' }}, ticks: {{ color: '#64748b' }} }},
        y: {{ grid: {{ display: false }}, ticks: {{ color: '#e2e8f0', font: {{ size: 11 }} }} }}
      }}
    }}
  }});
}}

// Table filter
function filterSigs(q) {{
  q = (q || '').toLowerCase();
  const type = document.getElementById('typeFilter')?.value || '';
  document.querySelectorAll('#sigsTable tbody tr').forEach(row => {{
    const matchQ = !q || row.textContent.toLowerCase().includes(q);
    const matchT = !type || row.dataset.type === type;
    row.style.display = (matchQ && matchT) ? '' : 'none';
  }});
}}
</script>
</body>
</html>"""
