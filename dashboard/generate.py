"""
Dashboard generator — writes docs/index.html for GitHub Pages.

Called via:  python main.py --dashboard
Or inline:   from dashboard.generate import generate_dashboard

Reads from the SQLite DB and produces a self-contained HTML file that
GitHub Pages serves at https://<owner>.github.io/<repo>/
"""

import os
import json
import sqlite3
import logging
from datetime import datetime, timezone, timedelta
from pathlib import Path

logger = logging.getLogger("dashboard")

DEPT_EMOJI = {
    "Corporate / M&A":                 "🤝",
    "Private Equity":                  "💰",
    "Capital Markets":                 "📈",
    "Litigation & Disputes":           "⚖️",
    "Restructuring & Insolvency":      "🔄",
    "Real Estate":                     "🏢",
    "Tax":                             "🧾",
    "Employment & Labour":             "👷",
    "Intellectual Property":           "💡",
    "Data Privacy & Cybersecurity":    "🔒",
    "ESG & Regulatory":                "🌿",
    "Energy & Natural Resources":      "⚡",
    "Financial Services & Regulatory": "🏦",
    "Competition & Antitrust":         "🔍",
    "Healthcare & Life Sciences":      "🏥",
    "Immigration":                     "🛂",
    "Infrastructure & Projects":       "🏗️",
}

SIGNAL_LABEL = {
    "lateral_hire":     "👤 Lateral hire",
    "job_posting":      "📋 Job posting",
    "press_release":    "📰 Press release",
    "publication":      "✍️ Publication",
    "practice_page":    "🌐 Practice page",
    "attorney_profile": "👔 Attorney profile",
    "bar_leadership":   "🏅 Bar leadership",
    "ranking":          "🏆 Ranking",
    "court_record":     "⚖️ Court record",
    "recruit_posting":  "🎓 Articling recruit",
}


def _load_data(db_path: str) -> dict:
    """Pull all relevant data from the DB for the dashboard."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cutoff_30d = (datetime.now(timezone.utc) - timedelta(days=30)).strftime("%Y-%m-%d")
    cutoff_7d  = (datetime.now(timezone.utc) - timedelta(days=7)).strftime("%Y-%m-%d")

    # Recent signals (30 days)
    cur.execute("""
        SELECT firm_id, firm_name, signal_type, title, url, department,
               department_score, seen_at
        FROM signals
        WHERE seen_at >= ?
        ORDER BY department_score DESC, seen_at DESC
    """, (cutoff_30d,))
    signals = [dict(r) for r in cur.fetchall()]

    # Weekly scores — last 8 weeks
    cur.execute("""
        SELECT firm_id, firm_name, department, score, signal_count,
               breakdown, week_start
        FROM weekly_scores
        ORDER BY week_start DESC, score DESC
        LIMIT 200
    """)
    scores = [dict(r) for r in cur.fetchall()]

    # Total alert count this week (separate from display cap)
    cur.execute("SELECT COUNT(*) FROM weekly_scores WHERE week_start >= ?", (cutoff_7d,))
    total_alert_count = (cur.fetchone() or [0])[0]

    # Top alerts this week — display top 30 sorted by score
    cur.execute("""
        SELECT firm_id, firm_name, department, score, signal_count, breakdown
        FROM weekly_scores
        WHERE week_start >= ?
        ORDER BY score DESC
        LIMIT 30
    """, (cutoff_7d,))
    top_alerts = [dict(r) for r in cur.fetchall()]

    # Signal counts by type (30d)
    cur.execute("""
        SELECT signal_type, COUNT(*) as cnt
        FROM signals WHERE seen_at >= ?
        GROUP BY signal_type ORDER BY cnt DESC
    """, (cutoff_30d,))
    by_type = [dict(r) for r in cur.fetchall()]

    # Signal counts by firm (30d)
    cur.execute("""
        SELECT firm_name, COUNT(*) as cnt
        FROM signals WHERE seen_at >= ?
        GROUP BY firm_name ORDER BY cnt DESC
    """, (cutoff_30d,))
    by_firm = [dict(r) for r in cur.fetchall()]

    conn.close()
    return {
        "signals": signals,
        "scores": scores,
        "top_alerts": top_alerts,
        "by_type": by_type,
        "by_firm": by_firm,
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        "signal_count_30d": len(signals),
        "alert_count_7d": total_alert_count,
    }


def generate_dashboard(db_path: str = "law_firm_tracker.db",
                       out_path: str = "docs/index.html") -> str:
    """Generate the dashboard HTML. Returns the output path."""
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)

    if not Path(db_path).exists():
        logger.warning(f"DB not found at {db_path} — generating empty dashboard")
        data = {
            "signals": [], "scores": [], "top_alerts": [],
            "by_type": [], "by_firm": [],
            "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
            "signal_count_30d": 0, "alert_count_7d": 0,
        }
    else:
        data = _load_data(db_path)

    html = _render_html(data)
    Path(out_path).write_text(html, encoding="utf-8")
    logger.info(f"Dashboard written to {out_path} "
                f"({data['signal_count_30d']} signals, {data['alert_count_7d']} alerts)")
    return out_path


# ── HTML renderer ──────────────────────────────────────────────────────────

def _render_html(data: dict) -> str:
    top_alerts_html = _render_top_alerts(data["top_alerts"])
    signals_html    = _render_signals_table(data["signals"][:50])
    by_type_json    = json.dumps([{"type": r["signal_type"], "count": r["cnt"]} for r in data["by_type"]])
    by_firm_json    = json.dumps([{"firm": r["firm_name"].split()[0], "count": r["cnt"]} for r in data["by_firm"]])
    scores_json     = json.dumps(data["scores"][:60])

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Law Firm Expansion Tracker</title>
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.min.js"></script>
<style>
  :root {{
    --bg: #0f1117; --card: #1a1d27; --border: #2a2d3e;
    --text: #e2e8f0; --muted: #8892a4; --accent: #6366f1;
    --green: #10b981; --amber: #f59e0b; --red: #ef4444;
  }}
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{ background: var(--bg); color: var(--text); font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; font-size: 14px; }}
  header {{ background: var(--card); border-bottom: 1px solid var(--border); padding: 16px 24px; display: flex; justify-content: space-between; align-items: center; }}
  header h1 {{ font-size: 18px; font-weight: 700; color: var(--accent); }}
  header span {{ font-size: 12px; color: var(--muted); }}
  .grid {{ display: grid; gap: 16px; padding: 20px; }}
  .grid-4 {{ grid-template-columns: repeat(4, 1fr); }}
  .grid-2 {{ grid-template-columns: 1fr 1fr; }}
  .grid-3 {{ grid-template-columns: 2fr 1fr; }}
  @media (max-width: 900px) {{ .grid-4, .grid-2, .grid-3 {{ grid-template-columns: 1fr; }} }}
  .card {{ background: var(--card); border: 1px solid var(--border); border-radius: 10px; padding: 16px; }}
  .card h2 {{ font-size: 12px; font-weight: 600; color: var(--muted); text-transform: uppercase; letter-spacing: .05em; margin-bottom: 12px; }}
  .stat-val {{ font-size: 32px; font-weight: 700; color: var(--text); }}
  .stat-sub {{ font-size: 12px; color: var(--muted); margin-top: 4px; }}
  .alert-row {{ padding: 10px 0; border-bottom: 1px solid var(--border); display: flex; justify-content: space-between; align-items: flex-start; gap: 8px; }}
  .alert-row:last-child {{ border-bottom: none; }}
  .alert-firm {{ font-weight: 600; font-size: 13px; }}
  .alert-dept {{ font-size: 12px; color: var(--muted); }}
  .alert-score {{ font-weight: 700; color: var(--accent); font-size: 15px; white-space: nowrap; }}
  .badge {{ display: inline-block; padding: 2px 8px; border-radius: 99px; font-size: 11px; font-weight: 600; background: rgba(99,102,241,.15); color: var(--accent); }}
  table {{ width: 100%; border-collapse: collapse; font-size: 12px; }}
  th {{ color: var(--muted); text-align: left; padding: 6px 8px; border-bottom: 1px solid var(--border); font-weight: 600; text-transform: uppercase; letter-spacing: .04em; font-size: 11px; }}
  td {{ padding: 7px 8px; border-bottom: 1px solid var(--border); vertical-align: top; }}
  tr:last-child td {{ border-bottom: none; }}
  td a {{ color: var(--accent); text-decoration: none; }}
  td a:hover {{ text-decoration: underline; }}
  .sig-type {{ display: inline-block; padding: 2px 6px; border-radius: 4px; font-size: 10px; font-weight: 600; background: rgba(99,102,241,.12); color: var(--accent); white-space: nowrap; }}
  canvas {{ max-height: 220px; }}
  .empty {{ color: var(--muted); font-size: 13px; padding: 20px 0; text-align: center; }}
</style>
</head>
<body>
<header>
  <h1>📊 Law Firm Expansion Tracker</h1>
  <span>Updated {data["generated_at"]}</span>
</header>

<!-- KPI row -->
<div class="grid grid-4" style="padding-bottom:0">
  <div class="card">
    <h2>Signals (30d)</h2>
    <div class="stat-val">{data["signal_count_30d"]}</div>
    <div class="stat-sub">across all firms</div>
  </div>
  <div class="card">
    <h2>Active Alerts</h2>
    <div class="stat-val">{data["alert_count_7d"]}</div>
    <div class="stat-sub">this week</div>
  </div>
  <div class="card">
    <h2>Firms Tracked</h2>
    <div class="stat-val">{len(set(r["firm_name"] for r in data["signals"])) or "—"}</div>
    <div class="stat-sub">with signals this month</div>
  </div>
  <div class="card">
    <h2>Signal Types</h2>
    <div class="stat-val">{len(data["by_type"])}</div>
    <div class="stat-sub">active categories</div>
  </div>
</div>

<!-- Charts row -->
<div class="grid grid-2" style="padding-bottom:0">
  <div class="card">
    <h2>Signals by Type (30d)</h2>
    {"<canvas id='typeChart'></canvas>" if data["by_type"] else '<p class="empty">No data yet</p>'}
  </div>
  <div class="card">
    <h2>Signals by Firm (30d)</h2>
    {"<canvas id='firmChart'></canvas>" if data["by_firm"] else '<p class="empty">No data yet</p>'}
  </div>
</div>

<!-- Alerts + Signals table -->
<div class="grid grid-3">
  <div class="card">
    <h2>Recent Signals</h2>
    {signals_html}
  </div>
  <div class="card">
    <h2>🔥 Top Expansion Alerts</h2>
    {top_alerts_html}
  </div>
</div>

<script>
const byType = {by_type_json};
const byFirm = {by_firm_json};

const COLORS = ['#6366f1','#10b981','#f59e0b','#ef4444','#8b5cf6','#06b6d4','#f97316','#84cc16'];

if (byType.length && document.getElementById('typeChart')) {{
  new Chart(document.getElementById('typeChart'), {{
    type: 'doughnut',
    data: {{
      labels: byType.map(d => d.type.replace(/_/g,' ')),
      datasets: [{{ data: byType.map(d => d.count), backgroundColor: COLORS, borderWidth: 0 }}]
    }},
    options: {{ plugins: {{ legend: {{ position: 'right', labels: {{ color: '#8892a4', boxWidth: 12, font: {{ size: 11 }} }} }} }}, cutout: '60%' }}
  }});
}}

if (byFirm.length && document.getElementById('firmChart')) {{
  new Chart(document.getElementById('firmChart'), {{
    type: 'bar',
    data: {{
      labels: byFirm.map(d => d.firm),
      datasets: [{{ data: byFirm.map(d => d.count), backgroundColor: COLORS, borderRadius: 4 }}]
    }},
    options: {{
      indexAxis: 'y',
      plugins: {{ legend: {{ display: false }} }},
      scales: {{
        x: {{ grid: {{ color: '#2a2d3e' }}, ticks: {{ color: '#8892a4' }} }},
        y: {{ grid: {{ display: false }}, ticks: {{ color: '#e2e8f0', font: {{ size: 11 }} }} }}
      }}
    }}
  }});
}}
</script>
</body>
</html>"""


def _render_top_alerts(alerts: list) -> str:
    if not alerts:
        return '<p class="empty">No alerts yet — run a collect cycle first</p>'
    rows = []
    for a in alerts[:12]:
        emoji = DEPT_EMOJI.get(a["department"], "⚖️")
        dept = a["department"] or "Unknown"
        score = round(a["score"], 1)
        rows.append(f"""
        <div class="alert-row">
          <div>
            <div class="alert-firm">🏛 {a["firm_name"].split()[0]}</div>
            <div class="alert-dept">{emoji} {dept}</div>
          </div>
          <div class="alert-score">{score}</div>
        </div>""")
    return "".join(rows)


def _render_signals_table(signals: list) -> str:
    if not signals:
        return '<p class="empty">No signals yet — run a collect cycle first</p>'
    rows = []
    for s in signals:
        sig_type = SIGNAL_LABEL.get(s["signal_type"], s["signal_type"]).split(" ", 1)[-1]
        title = s["title"][:70] + ("…" if len(s["title"]) > 70 else "")
        url = s.get("url", "")
        title_cell = f'<a href="{url}" target="_blank">{title}</a>' if url else title
        dept = s.get("department") or ""
        rows.append(f"""<tr>
          <td><span class="sig-type">{sig_type}</span></td>
          <td>{title_cell}</td>
          <td style="color:var(--muted);white-space:nowrap">{s.get("seen_at","")[:10]}</td>
        </tr>""")
    return f"""<table>
    <thead><tr><th>Type</th><th>Signal</th><th>Date</th></tr></thead>
    <tbody>{"".join(rows)}</tbody>
    </table>"""
