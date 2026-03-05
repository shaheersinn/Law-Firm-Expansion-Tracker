"""
Dashboard Generator
====================
Generates a self-contained static HTML dashboard from the SQLite database.
Written to docs/index.html and served via GitHub Pages.

Design: Deep navy intelligence platform — dark slate, amber gold, warm off-white.
Fonts: Fraunces (variable optical serif) + Instrument Sans + Azeret Mono.
No firm tiers — pure merit ranking by expansion score.
"""

import json
import logging
import os
from datetime import datetime
from pathlib import Path

logger = logging.getLogger("dashboard.generator")

DEPT_COLOR = {
    "Corporate / M&A":                 "#E8A838",
    "Private Equity":                  "#F07C3E",
    "Capital Markets":                 "#5BA8D4",
    "Litigation & Disputes":           "#D45B5B",
    "Restructuring & Insolvency":      "#E07845",
    "Real Estate":                     "#5BBD8A",
    "Tax":                             "#A87ED4",
    "Employment & Labour":             "#5BA8D4",
    "Intellectual Property":           "#D4A83C",
    "Data Privacy & Cybersecurity":    "#D45B8A",
    "ESG & Regulatory":                "#6BBD6B",
    "Energy & Natural Resources":      "#D4BE3C",
    "Financial Services & Regulatory": "#5B8AD4",
    "Competition & Antitrust":         "#5BBDBD",
    "Healthcare & Life Sciences":      "#5B9BD4",
    "Immigration":                     "#BD8A6B",
    "Infrastructure & Projects":       "#8A9BBD",
}

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
    "lateral_hire":     "Lateral Hire",
    "job_posting":      "Job Posting",
    "press_release":    "Press Release",
    "publication":      "Publication",
    "practice_page":    "New Practice Page",
    "attorney_profile": "Attorney Profile",
    "bar_leadership":   "Bar Leadership",
    "ranking":          "Ranking",
    "court_record":     "Court Record",
    "recruit_posting":  "Student Recruit",
    "bar_speaking":     "Bar Speaking",
    "bar_sponsorship":  "Bar Sponsorship",
    "bar_mention":      "Bar Mention",
}

SIGNAL_WEIGHT = {
    "bar_leadership":  3.5,
    "lateral_hire":    3.0,
    "ranking":         3.0,
    "court_record":    2.5,
    "practice_page":   2.5,
    "job_posting":     2.0,
    "recruit_posting": 2.0,
    "press_release":   1.5,
    "bar_speaking":    1.5,
    "publication":     1.0,
    "bar_sponsorship": 1.0,
    "attorney_profile":1.0,
    "bar_mention":     0.5,
}


def generate(db, output_path: str = "docs/index.html", repo_url: str = "") -> str:
    weekly_signals = db.get_signals_this_week()
    generated_at   = datetime.utcnow()

    firms_data = {}
    for sig in weekly_signals:
        fid = sig["firm_id"]
        if fid not in firms_data:
            firms_data[fid] = {
                "firm_id":      fid,
                "firm_name":    sig["firm_name"],
                "total_score":  0.0,
                "signal_count": 0,
                "departments":  {},
            }
        dept = sig.get("department", "Unknown")
        if dept not in firms_data[fid]["departments"]:
            firms_data[fid]["departments"][dept] = {
                "score":   0.0,
                "count":   0,
                "color":   DEPT_COLOR.get(dept, "#8A9BBD"),
                "emoji":   DEPT_EMOJI.get(dept, "📌"),
                "signals": [],
            }
        w  = SIGNAL_WEIGHT.get(sig["signal_type"], 0.5)
        ds = min(sig.get("department_score", 1.0), 20.0)
        contribution = w * (1 + ds * 0.1)

        firms_data[fid]["departments"][dept]["score"]   += contribution
        firms_data[fid]["departments"][dept]["count"]   += 1
        firms_data[fid]["departments"][dept]["signals"].append({
            "title":      sig["title"][:100],
            "type":       sig["signal_type"],
            "type_label": SIGNAL_LABEL.get(sig["signal_type"], sig["signal_type"]),
            "url":        sig.get("url", ""),
            "weight":     w,
        })
        firms_data[fid]["total_score"]  += contribution
        firms_data[fid]["signal_count"] += 1

    ranked_firms = sorted(firms_data.values(), key=lambda x: x["total_score"], reverse=True)

    for firm in ranked_firms:
        firm["total_score"] = round(firm["total_score"], 1)
        for dept in firm["departments"].values():
            dept["score"]   = round(dept["score"], 1)
            dept["signals"] = sorted(dept["signals"], key=lambda s: s["weight"], reverse=True)[:5]

    max_score = ranked_firms[0]["total_score"] if ranked_firms else 1.0

    type_totals = {}
    for sig in weekly_signals:
        t = sig["signal_type"]
        type_totals[t] = type_totals.get(t, 0) + 1

    dept_totals = {}
    for sig in weekly_signals:
        d = sig.get("department", "")
        if d:
            dept_totals[d] = dept_totals.get(d, 0) + 1
    dept_totals_sorted = sorted(dept_totals.items(), key=lambda x: x[1], reverse=True)[:10]

    payload = {
        "generated_at":  generated_at.strftime("%d %b %Y · %H:%M UTC"),
        "week_label":    generated_at.strftime("%B %d, %Y"),
        "total_signals": len(weekly_signals),
        "total_firms":   len(ranked_firms),
        "firms":         ranked_firms,
        "max_score":     max_score,
        "type_totals":   type_totals,
        "dept_totals":   dept_totals_sorted,
        "repo_url":      repo_url,
    }

    html = _render_html(payload)
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)
    logger.info(f"Dashboard written → {output_path} ({len(html):,} bytes)")

    if repo_url:
        parts = repo_url.rstrip("/").split("/")
        if len(parts) >= 2:
            user, repo = parts[-2], parts[-1]
            return f"https://{user}.github.io/{repo}/"
    return output_path


def _render_html(data: dict) -> str:
    firms_json        = json.dumps(data["firms"],      ensure_ascii=False)
    type_totals_json  = json.dumps(data["type_totals"])
    dept_totals_json  = json.dumps(data["dept_totals"])
    max_score         = data["max_score"]
    total_signals     = data["total_signals"]
    total_firms       = data["total_firms"]
    generated_at      = data["generated_at"]
    week_label        = data["week_label"]

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>Canadian Law Firm Intelligence — {week_label}</title>
<link rel="preconnect" href="https://fonts.googleapis.com"/>
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin/>
<link href="https://fonts.googleapis.com/css2?family=Fraunces:ital,opsz,wght@0,9..144,100..900;1,9..144,100..900&family=Instrument+Sans:ital,wght@0,400;0,500;0,600;0,700;1,400;1,500&family=Azeret+Mono:wght@300;400;500;600&display=swap" rel="stylesheet"/>
<style>
/* ══════════════════════════════════════════════════════
   DESIGN TOKENS
══════════════════════════════════════════════════════ */
:root {{
  /* Palette */
  --navy:      #0B1622;
  --navy-2:    #0F1E2E;
  --navy-3:    #152638;
  --navy-4:    #1C3348;
  --border:    rgba(255,255,255,0.07);
  --border-2:  rgba(255,255,255,0.04);

  --amber:     #E8A838;
  --amber-2:   #F0C060;
  --amber-dim: rgba(232,168,56,0.12);
  --amber-glow:rgba(232,168,56,0.06);

  --text:      #EDE8DF;
  --text-2:    #B8B0A2;
  --text-3:    #7A7264;
  --text-4:    #4A4438;

  /* Typography */
  --font-display: 'Fraunces', Georgia, serif;
  --font-ui:      'Instrument Sans', system-ui, sans-serif;
  --font-mono:    'Azeret Mono', 'Courier New', monospace;

  /* Motion */
  --ease-out-expo: cubic-bezier(0.16, 1, 0.3, 1);
}}

/* ══════════════════════════════════════════════════════
   BASE
══════════════════════════════════════════════════════ */
*, *::before, *::after {{ box-sizing:border-box; margin:0; padding:0; }}
html {{ font-size:16px; scroll-behavior:smooth; }}

body {{
  background: var(--navy);
  color: var(--text);
  font-family: var(--font-ui);
  font-size: 0.875rem;
  line-height: 1.6;
  -webkit-font-smoothing: antialiased;
  min-height: 100vh;
  overflow-x: hidden;
}}

/* Subtle noise texture */
body::after {{
  content: '';
  position: fixed; inset: 0; z-index: 0; pointer-events: none;
  background-image: url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='256' height='256'%3E%3Cfilter id='n'%3E%3CfeTurbulence type='fractalNoise' baseFrequency='0.9' numOctaves='4' stitchTiles='stitch'/%3E%3C/filter%3E%3Crect width='256' height='256' filter='url(%23n)' opacity='0.025'/%3E%3C/svg%3E");
  mix-blend-mode: overlay;
}}

/* Radial glow at top */
body::before {{
  content: '';
  position: fixed; top: -200px; left: 50%;
  transform: translateX(-50%);
  width: 900px; height: 500px;
  background: radial-gradient(ellipse, rgba(232,168,56,0.07) 0%, transparent 70%);
  pointer-events: none; z-index: 0;
}}

/* ══════════════════════════════════════════════════════
   HEADER
══════════════════════════════════════════════════════ */
.site-header {{
  position: relative; z-index: 20;
  border-bottom: 1px solid var(--border);
  background: rgba(11,22,34,0.92);
  backdrop-filter: blur(20px);
  -webkit-backdrop-filter: blur(20px);
}}

.header-inner {{
  max-width: 1400px; margin: 0 auto;
  padding: 0 clamp(20px, 4vw, 60px);
  display: flex; align-items: center;
  justify-content: space-between;
  height: 64px; gap: 24px;
}}

.header-brand {{
  display: flex; align-items: center; gap: 14px;
}}

.brand-mark {{
  width: 34px; height: 34px;
  border: 1.5px solid var(--amber);
  border-radius: 6px;
  display: flex; align-items: center; justify-content: center;
  flex-shrink: 0;
}}

.brand-mark svg {{
  width: 16px; height: 16px;
  fill: none; stroke: var(--amber); stroke-width: 1.5;
}}

.brand-name {{
  font-family: var(--font-display);
  font-size: 1.05rem;
  font-weight: 600;
  font-optical-sizing: auto;
  color: var(--text);
  letter-spacing: -0.01em;
  line-height: 1;
}}

.brand-sub {{
  font-family: var(--font-mono);
  font-size: 0.6rem;
  letter-spacing: 0.12em;
  text-transform: uppercase;
  color: var(--text-3);
  margin-top: 3px;
}}

.header-meta {{
  display: flex; align-items: center; gap: 20px;
}}

.header-pill {{
  font-family: var(--font-mono);
  font-size: 0.62rem;
  letter-spacing: 0.08em;
  text-transform: uppercase;
  padding: 4px 10px;
  border-radius: 100px;
  border: 1px solid var(--border);
  color: var(--text-3);
  white-space: nowrap;
}}

.live-pill {{
  border-color: rgba(110,210,130,0.3);
  color: #6ED282;
  background: rgba(110,210,130,0.06);
  display: flex; align-items: center; gap: 6px;
}}

.live-dot {{
  width: 5px; height: 5px; border-radius: 50%;
  background: #6ED282;
  box-shadow: 0 0 6px #6ED282;
  animation: livePulse 2.4s ease-in-out infinite;
}}

@keyframes livePulse {{
  0%,100% {{ opacity:1; transform:scale(1); }}
  50%      {{ opacity:0.4; transform:scale(1.4); }}
}}

/* ══════════════════════════════════════════════════════
   HERO STRIP
══════════════════════════════════════════════════════ */
.hero-strip {{
  position: relative; z-index: 10;
  border-bottom: 1px solid var(--border);
  background: linear-gradient(180deg, rgba(232,168,56,0.04) 0%, transparent 100%);
}}

.hero-inner {{
  max-width: 1400px; margin: 0 auto;
  padding: clamp(32px, 5vw, 64px) clamp(20px, 4vw, 60px) clamp(28px, 4vw, 52px);
}}

.hero-eyebrow {{
  font-family: var(--font-mono);
  font-size: 0.62rem;
  letter-spacing: 0.2em;
  text-transform: uppercase;
  color: var(--amber);
  margin-bottom: 14px;
  display: flex; align-items: center; gap: 10px;
}}

.hero-eyebrow::before {{
  content: '';
  width: 24px; height: 1px;
  background: var(--amber);
  opacity: 0.6;
}}

.hero-title {{
  font-family: var(--font-display);
  font-size: clamp(2.4rem, 6vw, 5rem);
  font-weight: 800;
  font-optical-sizing: auto;
  line-height: 0.95;
  letter-spacing: -0.03em;
  color: var(--text);
  margin-bottom: 20px;
}}

.hero-title em {{
  font-style: italic;
  color: var(--amber);
}}

.hero-desc {{
  font-family: var(--font-ui);
  font-size: 0.95rem;
  color: var(--text-2);
  max-width: 520px;
  line-height: 1.65;
}}

/* ══════════════════════════════════════════════════════
   STAT CARDS ROW
══════════════════════════════════════════════════════ */
.stats-row {{
  position: relative; z-index: 10;
  border-bottom: 1px solid var(--border);
}}

.stats-inner {{
  max-width: 1400px; margin: 0 auto;
  padding: 0 clamp(20px, 4vw, 60px);
  display: grid;
  grid-template-columns: repeat(4, 1fr);
  border-left: 1px solid var(--border);
}}

.stat-card {{
  padding: 28px 24px;
  border-right: 1px solid var(--border);
  position: relative;
  overflow: hidden;
  transition: background 0.2s;
}}

.stat-card:hover {{ background: rgba(232,168,56,0.03); }}

.stat-card::before {{
  content: '';
  position: absolute; top: 0; left: 0;
  width: 2px; height: 0;
  background: var(--amber);
  transition: height 0.4s var(--ease-out-expo);
}}

.stat-card:hover::before {{ height: 100%; }}

.stat-label {{
  font-family: var(--font-mono);
  font-size: 0.6rem;
  letter-spacing: 0.16em;
  text-transform: uppercase;
  color: var(--text-3);
  margin-bottom: 8px;
}}

.stat-value {{
  font-family: var(--font-display);
  font-size: clamp(2rem, 4vw, 3.2rem);
  font-weight: 800;
  font-optical-sizing: auto;
  color: var(--amber);
  line-height: 1;
  letter-spacing: -0.04em;
}}

.stat-note {{
  font-family: var(--font-ui);
  font-size: 0.78rem;
  color: var(--text-3);
  margin-top: 6px;
}}

/* ══════════════════════════════════════════════════════
   MAIN LAYOUT
══════════════════════════════════════════════════════ */
.main-wrap {{
  position: relative; z-index: 10;
  max-width: 1400px; margin: 0 auto;
  padding: 48px clamp(20px, 4vw, 60px) 100px;
  display: grid;
  grid-template-columns: 1fr 320px;
  gap: 48px;
  align-items: start;
}}

/* ══════════════════════════════════════════════════════
   SECTION LABELS
══════════════════════════════════════════════════════ */
.section-label {{
  font-family: var(--font-mono);
  font-size: 0.6rem;
  letter-spacing: 0.2em;
  text-transform: uppercase;
  color: var(--text-3);
  display: flex; align-items: center; gap: 12px;
  margin-bottom: 24px;
}}

.section-label::after {{
  content: '';
  flex: 1; height: 1px;
  background: var(--border);
}}

/* ══════════════════════════════════════════════════════
   SEARCH
══════════════════════════════════════════════════════ */
.search-bar {{
  display: flex; align-items: center; gap: 0;
  margin-bottom: 28px;
  background: var(--navy-3);
  border: 1px solid var(--border);
  border-radius: 8px;
  padding: 0 16px;
  transition: border-color 0.2s, box-shadow 0.2s;
}}

.search-bar:focus-within {{
  border-color: rgba(232,168,56,0.4);
  box-shadow: 0 0 0 3px rgba(232,168,56,0.06);
}}

.search-icon {{
  width: 15px; height: 15px;
  flex-shrink: 0; margin-right: 10px;
  opacity: 0.35;
}}

.search-input {{
  flex: 1;
  font-family: var(--font-ui);
  font-size: 0.875rem;
  background: transparent;
  border: none; outline: none;
  color: var(--text);
  padding: 12px 0;
}}

.search-input::placeholder {{ color: var(--text-3); }}

.search-count {{
  font-family: var(--font-mono);
  font-size: 0.6rem;
  letter-spacing: 0.08em;
  color: var(--text-3);
  white-space: nowrap;
  border-left: 1px solid var(--border);
  padding-left: 12px;
  margin-left: 8px;
}}

/* ══════════════════════════════════════════════════════
   FIRM CARDS
══════════════════════════════════════════════════════ */
.firm-list {{ display: flex; flex-direction: column; gap: 0; }}

.firm-card {{
  background: var(--navy-2);
  border: 1px solid var(--border);
  border-radius: 12px;
  overflow: hidden;
  margin-bottom: 12px;
  cursor: pointer;
  transition: border-color 0.2s, box-shadow 0.2s, transform 0.15s;
  animation: cardIn 0.5s var(--ease-out-expo) both;
}}

.firm-card:nth-child(1)  {{ animation-delay: 0.04s; }}
.firm-card:nth-child(2)  {{ animation-delay: 0.08s; }}
.firm-card:nth-child(3)  {{ animation-delay: 0.12s; }}
.firm-card:nth-child(4)  {{ animation-delay: 0.16s; }}
.firm-card:nth-child(5)  {{ animation-delay: 0.20s; }}
.firm-card:nth-child(6)  {{ animation-delay: 0.24s; }}
.firm-card:nth-child(7)  {{ animation-delay: 0.28s; }}
.firm-card:nth-child(8)  {{ animation-delay: 0.32s; }}
.firm-card:nth-child(9)  {{ animation-delay: 0.36s; }}
.firm-card:nth-child(10) {{ animation-delay: 0.40s; }}

@keyframes cardIn {{
  from {{ opacity:0; transform:translateY(20px); }}
  to   {{ opacity:1; transform:translateY(0); }}
}}

.firm-card:hover {{
  border-color: rgba(232,168,56,0.25);
  box-shadow: 0 4px 32px rgba(0,0,0,0.3), 0 0 0 0 transparent;
  transform: translateY(-1px);
}}

.firm-card.expanded {{
  border-color: rgba(232,168,56,0.35);
  box-shadow: 0 8px 48px rgba(0,0,0,0.4), 0 0 24px rgba(232,168,56,0.04);
}}

/* Card header */
.card-header {{
  padding: 22px 24px 0;
  display: grid;
  grid-template-columns: 52px 1fr auto;
  gap: 18px;
  align-items: center;
}}

.rank-badge {{
  font-family: var(--font-display);
  font-size: 2.2rem;
  font-weight: 900;
  font-optical-sizing: auto;
  letter-spacing: -0.05em;
  line-height: 1;
  color: rgba(255,255,255,0.07);
  text-align: right;
  transition: color 0.25s;
  user-select: none;
}}

.firm-card:hover .rank-badge,
.firm-card.expanded .rank-badge {{
  color: var(--amber);
}}

.firm-info {{}}

.firm-name {{
  font-family: var(--font-display);
  font-size: clamp(1.1rem, 2vw, 1.4rem);
  font-weight: 700;
  font-optical-sizing: auto;
  color: var(--text);
  line-height: 1.2;
  letter-spacing: -0.02em;
  margin-bottom: 3px;
}}

.firm-tagline {{
  font-family: var(--font-mono);
  font-size: 0.6rem;
  letter-spacing: 0.1em;
  text-transform: uppercase;
  color: var(--text-3);
}}

.score-block {{
  text-align: right;
}}

.score-num {{
  font-family: var(--font-display);
  font-size: 2.8rem;
  font-weight: 900;
  font-optical-sizing: auto;
  color: var(--amber);
  line-height: 1;
  letter-spacing: -0.05em;
}}

.score-label {{
  font-family: var(--font-mono);
  font-size: 0.56rem;
  letter-spacing: 0.14em;
  text-transform: uppercase;
  color: var(--text-3);
  margin-top: 2px;
  display: block;
}}

/* Score bar */
.score-bar-wrap {{
  padding: 16px 24px 4px;
  display: grid;
  grid-template-columns: 52px 1fr;
  gap: 18px;
  align-items: center;
}}

.score-bar-track {{
  height: 3px;
  background: rgba(255,255,255,0.06);
  border-radius: 2px;
  overflow: visible;
  position: relative;
}}

.score-bar-fill {{
  height: 100%;
  background: linear-gradient(90deg, var(--amber), var(--amber-2));
  border-radius: 2px;
  width: 0%;
  transition: width 1.2s var(--ease-out-expo);
  position: relative;
}}

.score-bar-fill::after {{
  content: '';
  position: absolute; right: -1px; top: -4px;
  width: 11px; height: 11px;
  background: var(--amber-2);
  border-radius: 50%;
  box-shadow: 0 0 8px rgba(232,168,56,0.6);
}}

/* Practice area tags */
.dept-tags {{
  display: flex; flex-wrap: wrap; gap: 6px;
  padding: 14px 24px 20px;
}}

.dept-tag {{
  font-family: var(--font-mono);
  font-size: 0.58rem;
  letter-spacing: 0.08em;
  text-transform: uppercase;
  padding: 4px 10px;
  border-radius: 100px;
  border: 1px solid;
  cursor: pointer;
  transition: opacity 0.15s, transform 0.1s;
  white-space: nowrap;
}}

.dept-tag:hover {{
  opacity: 0.85;
  transform: scale(1.02);
}}

/* Toggle hint */
.toggle-hint {{
  display: flex; align-items: center; justify-content: center;
  padding: 10px 24px 16px;
  gap: 6px;
  font-family: var(--font-mono);
  font-size: 0.58rem;
  letter-spacing: 0.1em;
  text-transform: uppercase;
  color: var(--text-3);
  transition: color 0.2s;
}}

.toggle-hint svg {{
  width: 12px; height: 12px;
  stroke: currentColor; fill: none; stroke-width: 2;
  transition: transform 0.3s var(--ease-out-expo);
}}

.firm-card.expanded .toggle-hint {{ color: var(--amber); }}
.firm-card.expanded .toggle-hint svg {{ transform: rotate(180deg); }}

/* ══════════════════════════════════════════════════════
   EXPANDED DETAIL PANEL
══════════════════════════════════════════════════════ */
.detail-panel {{
  display: none;
  border-top: 1px solid var(--border);
  background: rgba(0,0,0,0.2);
  padding: 28px 24px;
}}

.detail-panel.open {{ display: block; }}

.dept-grid {{
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(280px, 1fr));
  gap: 20px;
}}

.dept-block {{
  border-left: 2px solid;
  padding-left: 16px;
}}

.dept-block-name {{
  font-family: var(--font-display);
  font-size: 1rem;
  font-weight: 600;
  font-optical-sizing: auto;
  margin-bottom: 2px;
  line-height: 1.3;
}}

.dept-block-meta {{
  font-family: var(--font-mono);
  font-size: 0.58rem;
  letter-spacing: 0.08em;
  text-transform: uppercase;
  color: var(--text-3);
  margin-bottom: 14px;
}}

.signal-item {{
  display: flex; align-items: flex-start; gap: 10px;
  padding: 8px 0;
  border-bottom: 1px solid var(--border-2);
  font-size: 0.83rem;
  line-height: 1.45;
}}

.signal-item:last-child {{ border-bottom: none; padding-bottom: 0; }}

.signal-pill {{
  flex-shrink: 0;
  font-family: var(--font-mono);
  font-size: 0.54rem;
  letter-spacing: 0.06em;
  text-transform: uppercase;
  padding: 3px 7px;
  border-radius: 4px;
  background: rgba(255,255,255,0.05);
  color: var(--text-2);
  white-space: nowrap;
  margin-top: 1px;
  border: 1px solid var(--border);
}}

.signal-text {{ flex: 1; color: var(--text-2); }}

.signal-text a {{
  color: inherit;
  text-decoration: underline;
  text-decoration-color: rgba(255,255,255,0.15);
  text-underline-offset: 2px;
  transition: color 0.15s, text-decoration-color 0.15s;
}}

.signal-text a:hover {{
  color: var(--amber-2);
  text-decoration-color: var(--amber);
}}

.signal-wt {{
  flex-shrink: 0;
  font-family: var(--font-mono);
  font-size: 0.6rem;
  color: var(--amber);
  opacity: 0.7;
  margin-top: 1px;
  white-space: nowrap;
}}

/* ══════════════════════════════════════════════════════
   SIDEBAR
══════════════════════════════════════════════════════ */
.sidebar {{ position: sticky; top: 80px; display: flex; flex-direction: column; gap: 16px; }}

.widget {{
  background: var(--navy-2);
  border: 1px solid var(--border);
  border-radius: 12px;
  padding: 22px;
  overflow: hidden;
}}

.widget-title {{
  font-family: var(--font-mono);
  font-size: 0.6rem;
  letter-spacing: 0.18em;
  text-transform: uppercase;
  color: var(--text-3);
  border-bottom: 1px solid var(--border);
  padding-bottom: 12px;
  margin-bottom: 18px;
}}

/* Dept bars */
.dept-bar-row {{
  display: flex; align-items: center; gap: 10px;
  margin-bottom: 10px;
  font-size: 0.8rem;
}}

.dept-bar-label {{
  width: 130px; flex-shrink: 0;
  overflow: hidden; text-overflow: ellipsis;
  white-space: nowrap;
  color: var(--text-2);
  font-family: var(--font-ui);
}}

.dept-bar-track {{
  flex: 1; height: 4px;
  background: rgba(255,255,255,0.05);
  border-radius: 2px; overflow: hidden;
}}

.dept-bar-fill {{
  height: 100%; border-radius: 2px;
  width: 0%; transition: width 1.1s var(--ease-out-expo);
}}

.dept-bar-n {{
  width: 22px; text-align: right;
  font-family: var(--font-mono);
  font-size: 0.62rem; color: var(--text-3);
}}

/* Signal type rows */
.stype-item {{ margin-bottom: 10px; }}

.stype-top {{
  display: flex; justify-content: space-between; align-items: baseline;
  margin-bottom: 4px;
}}

.stype-name {{
  font-family: var(--font-ui);
  font-size: 0.8rem;
  color: var(--text-2);
}}

.stype-n {{
  font-family: var(--font-mono);
  font-size: 0.64rem;
  color: var(--amber);
  font-weight: 500;
}}

.stype-track {{
  height: 2px;
  background: rgba(255,255,255,0.05);
  border-radius: 1px; overflow: hidden;
}}

.stype-fill {{
  height: 100%;
  background: linear-gradient(90deg, var(--amber), var(--amber-2));
  width: 0%; border-radius: 1px;
  transition: width 1.1s var(--ease-out-expo);
}}

/* Weight reference */
.weight-table {{
  display: grid;
  grid-template-columns: 1fr auto;
  gap: 7px 20px;
  font-size: 0.82rem;
}}

.wt-key {{ color: var(--text-2); }}

.wt-val {{
  font-family: var(--font-mono);
  font-size: 0.65rem;
  color: var(--amber);
  text-align: right;
  letter-spacing: 0.04em;
}}

/* ══════════════════════════════════════════════════════
   FOOTER
══════════════════════════════════════════════════════ */
footer {{
  position: relative; z-index: 10;
  border-top: 1px solid var(--border);
  padding: 24px clamp(20px, 4vw, 60px);
  display: flex; align-items: center; justify-content: space-between;
  flex-wrap: wrap; gap: 12px;
  font-family: var(--font-mono);
  font-size: 0.6rem;
  letter-spacing: 0.1em;
  text-transform: uppercase;
  color: var(--text-3);
}}

footer a {{ color: var(--amber); text-decoration: none; }}
footer a:hover {{ text-decoration: underline; }}

/* ══════════════════════════════════════════════════════
   RESPONSIVE
══════════════════════════════════════════════════════ */
.hidden {{ display: none !important; }}

@media (max-width: 1100px) {{
  .main-wrap {{ grid-template-columns: 1fr; gap: 32px; }}
  .sidebar {{
    position: static;
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(260px, 1fr));
    gap: 16px;
  }}
}}

@media (max-width: 720px) {{
  .stats-inner {{ grid-template-columns: repeat(2, 1fr); }}
  .header-meta .header-pill:not(.live-pill) {{ display: none; }}
  .hero-title {{ line-height: 1.0; }}
  .card-header {{ grid-template-columns: 36px 1fr auto; gap: 12px; }}
  .score-num {{ font-size: 2.2rem; }}
  .rank-badge {{ font-size: 1.8rem; }}
  .dept-grid {{ grid-template-columns: 1fr; }}
}}

@media (max-width: 480px) {{
  .stats-inner {{ grid-template-columns: 1fr 1fr; }}
  .score-bar-wrap {{ grid-template-columns: 36px 1fr; gap:12px; }}
  .dept-tags {{ padding: 12px 16px 16px; }}
  .card-header {{ padding: 18px 16px 0; }}
}}
</style>
</head>
<body>

<!-- ══ HEADER ═══════════════════════════════════════ -->
<header class="site-header">
  <div class="header-inner">
    <div class="header-brand">
      <div class="brand-mark" aria-hidden="true">
        <svg viewBox="0 0 16 16"><polyline points="2,12 6,6 10,9 14,3"/></svg>
      </div>
      <div>
        <div class="brand-name">Canadian Law Firm Intelligence</div>
        <div class="brand-sub">Expansion Signal Tracker</div>
      </div>
    </div>
    <div class="header-meta">
      <span class="header-pill">{generated_at}</span>
      <span class="header-pill">26 firms</span>
      <span class="header-pill live-pill">
        <span class="live-dot"></span>Live
      </span>
    </div>
  </div>
</header>

<!-- ══ HERO ═════════════════════════════════════════ -->
<section class="hero-strip">
  <div class="hero-inner">
    <div class="hero-eyebrow">Week of {week_label}</div>
    <h1 class="hero-title">
      Firm Expansion<br><em>Intelligence</em>
    </h1>
    <p class="hero-desc">
      Real-time signals from 8 scrapers — bar association leadership,
      Chambers rankings, CanLII court records, lateral hires, job postings,
      and more. Ranked by weighted expansion score.
    </p>
  </div>
</section>

<!-- ══ STATS ════════════════════════════════════════ -->
<section class="stats-row">
  <div class="stats-inner">
    <div class="stat-card">
      <div class="stat-label">Signals this week</div>
      <div class="stat-value" id="aSig">0</div>
      <div class="stat-note">across all scrapers</div>
    </div>
    <div class="stat-card">
      <div class="stat-label">Firms active</div>
      <div class="stat-value" id="aFirms">0</div>
      <div class="stat-note">of 26 tracked</div>
    </div>
    <div class="stat-card">
      <div class="stat-label">Top score</div>
      <div class="stat-value" id="aScore">0</div>
      <div class="stat-note">highest expansion score</div>
    </div>
    <div class="stat-card">
      <div class="stat-label">Last updated</div>
      <div class="stat-value" style="font-size:1.1rem;padding-top:6px">{generated_at}</div>
      <div class="stat-note">daily at 07:00 UTC</div>
    </div>
  </div>
</section>

<!-- ══ MAIN ══════════════════════════════════════════ -->
<div class="main-wrap">

  <!-- Firm list -->
  <div>
    <div class="section-label">Firms ranked by expansion score</div>

    <div class="search-bar">
      <svg class="search-icon" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5">
        <circle cx="6.5" cy="6.5" r="4.5"/><path d="M10.5 10.5L14 14"/>
      </svg>
      <input class="search-input" id="searchBox" type="text"
             placeholder="Search firm name or practice area…"
             oninput="applySearch()" autocomplete="off"/>
      <span class="search-count" id="searchCount"></span>
    </div>

    <div class="firm-list" id="firmList"></div>
  </div>

  <!-- Sidebar -->
  <aside class="sidebar">

    <div class="widget">
      <div class="widget-title">Top practice areas</div>
      <div id="deptChart"></div>
    </div>

    <div class="widget">
      <div class="widget-title">Signal types</div>
      <div id="signalTypeChart"></div>
    </div>

    <div class="widget">
      <div class="widget-title">Signal weight reference</div>
      <div class="weight-table">
        <span class="wt-key">🏅 Bar Leadership</span><span class="wt-val">3.5×</span>
        <span class="wt-key">🏆 Ranking</span><span class="wt-val">3.0×</span>
        <span class="wt-key">👤 Lateral Hire</span><span class="wt-val">3.0×</span>
        <span class="wt-key">⚖️ Court Record</span><span class="wt-val">2.5×</span>
        <span class="wt-key">🌐 Practice Page</span><span class="wt-val">2.5×</span>
        <span class="wt-key">📋 Job Posting</span><span class="wt-val">2.0×</span>
        <span class="wt-key">🎓 Student Recruit</span><span class="wt-val">2.0×</span>
        <span class="wt-key">📰 Press Release</span><span class="wt-val">1.5×</span>
        <span class="wt-key">✍️ Publication</span><span class="wt-val">1.0×</span>
      </div>
    </div>

  </aside>

</div><!-- /main-wrap -->

<footer>
  <span>Canadian Law Firm Intelligence Report</span>
  <span>8 scrapers · 26 firms · 17 departments</span>
  <span>Generated {generated_at}</span>
</footer>

<!-- ══ DATA + JS ═════════════════════════════════════ -->
<script>
const FIRMS       = {firms_json};
const TYPE_TOTALS = {type_totals_json};
const DEPT_TOTALS = {dept_totals_json};
const MAX_SCORE   = {max_score};

const DEPT_COLOR = {{
  "Corporate / M&A":"#E8A838","Private Equity":"#F07C3E",
  "Capital Markets":"#5BA8D4","Litigation & Disputes":"#D45B5B",
  "Restructuring & Insolvency":"#E07845","Real Estate":"#5BBD8A",
  "Tax":"#A87ED4","Employment & Labour":"#5BA8D4",
  "Intellectual Property":"#D4A83C","Data Privacy & Cybersecurity":"#D45B8A",
  "ESG & Regulatory":"#6BBD6B","Energy & Natural Resources":"#D4BE3C",
  "Financial Services & Regulatory":"#5B8AD4","Competition & Antitrust":"#5BBDBD",
  "Healthcare & Life Sciences":"#5B9BD4","Immigration":"#BD8A6B",
  "Infrastructure & Projects":"#8A9BBD",
}};

/* ── animated counter ─────────────────────────── */
function animCount(el, target, isFloat) {{
  const dur = 950, t0 = performance.now();
  const fn = now => {{
    const p = Math.min((now - t0) / dur, 1);
    const e = 1 - Math.pow(1 - p, 4);
    el.textContent = isFloat ? (target * e).toFixed(1) : Math.round(target * e);
    if (p < 1) requestAnimationFrame(fn);
    else el.textContent = isFloat ? target.toFixed(1) : target;
  }};
  requestAnimationFrame(fn);
}}

window.addEventListener('load', () => {{
  animCount(document.getElementById('aSig'),   {total_signals}, false);
  animCount(document.getElementById('aFirms'), {total_firms},   false);
  animCount(document.getElementById('aScore'), {max_score},     true);
}});

/* ── build firm cards ─────────────────────────── */
function renderFirms(firms) {{
  const c = document.getElementById('firmList');
  c.innerHTML = '';
  firms.forEach((f, i) => c.appendChild(buildCard(f, i + 1)));
  updateCount();
  requestAnimationFrame(() =>
    document.querySelectorAll('.score-bar-fill[data-w]')
      .forEach(el => el.style.width = el.dataset.w)
  );
}}

function buildCard(firm, rank) {{
  const pct = MAX_SCORE > 0 ? (firm.total_score / MAX_SCORE * 100).toFixed(1) : 0;
  const depts = Object.entries(firm.departments).sort((a,b) => b[1].score - a[1].score);

  /* dept tags */
  const tagsHtml = depts.map(([dn, dd]) => {{
    const c = DEPT_COLOR[dn] || '#8A9BBD';
    return `<span class="dept-tag"
      style="color:${{c}};border-color:${{c}}44;background:${{c}}14"
      onclick="event.stopPropagation();searchDept('${{dn}}')"
    >${{dd.emoji}} ${{dn}}</span>`;
  }}).join('');

  /* dept detail blocks */
  const deptBlocksHtml = depts.map(([dn, dd]) => {{
    const c = DEPT_COLOR[dn] || '#8A9BBD';
    const sigsHtml = dd.signals.map(s => {{
      const t = s.url
        ? `<a href="${{s.url}}" target="_blank" rel="noopener">${{s.title}}</a>`
        : s.title;
      return `<div class="signal-item">
        <span class="signal-pill">${{s.type_label}}</span>
        <span class="signal-text">${{t}}</span>
        <span class="signal-wt">×${{s.weight.toFixed(1)}}</span>
      </div>`;
    }}).join('');
    return `<div class="dept-block" style="border-left-color:${{c}}">
      <div class="dept-block-name" style="color:${{c}}">${{dd.emoji}} ${{dn}}</div>
      <div class="dept-block-meta">${{dd.score}} pts · ${{dd.count}} signal${{dd.count !== 1 ? 's' : ''}}</div>
      ${{sigsHtml}}
    </div>`;
  }}).join('');

  const el = document.createElement('div');
  el.className = 'firm-card';
  el.dataset.name  = firm.firm_name.toLowerCase();
  el.dataset.depts = Object.keys(firm.departments).join('|').toLowerCase();

  el.innerHTML = `
    <div onclick="toggleCard(this)">
      <div class="card-header">
        <div class="rank-badge">${{String(rank).padStart(2,'0')}}</div>
        <div class="firm-info">
          <div class="firm-name">${{firm.firm_name}}</div>
          <div class="firm-tagline">${{firm.signal_count}} signal${{firm.signal_count !== 1 ? 's' : ''}} this week</div>
        </div>
        <div class="score-block">
          <div class="score-num">${{firm.total_score}}</div>
          <span class="score-label">Exp. Score</span>
        </div>
      </div>
      <div class="score-bar-wrap">
        <div></div>
        <div class="score-bar-track">
          <div class="score-bar-fill" style="width:0%" data-w="${{pct}}%"></div>
        </div>
      </div>
      <div class="dept-tags">${{tagsHtml}}</div>
      <div class="toggle-hint">
        <span>View signals</span>
        <svg viewBox="0 0 12 12"><path d="M2 4l4 4 4-4"/></svg>
      </div>
    </div>
    <div class="detail-panel">
      <div class="dept-grid">${{deptBlocksHtml}}</div>
    </div>`;

  return el;
}}

function toggleCard(el) {{
  const card   = el.closest('.firm-card');
  const panel  = card.querySelector('.detail-panel');
  const wasOpen = panel.classList.contains('open');
  document.querySelectorAll('.detail-panel.open').forEach(p => p.classList.remove('open'));
  document.querySelectorAll('.firm-card.expanded').forEach(c => c.classList.remove('expanded'));
  if (!wasOpen) {{
    panel.classList.add('open');
    card.classList.add('expanded');
  }}
}}

/* ── search ───────────────────────────────────── */
function applySearch() {{
  const q = document.getElementById('searchBox').value.toLowerCase().trim();
  document.querySelectorAll('.firm-card').forEach(el => {{
    const hit = !q || el.dataset.name.includes(q) || el.dataset.depts.includes(q);
    el.classList.toggle('hidden', !hit);
  }});
  updateCount();
}}

function searchDept(dept) {{
  document.getElementById('searchBox').value = dept;
  applySearch();
}}

function updateCount() {{
  const tot = document.querySelectorAll('.firm-card').length;
  const vis = document.querySelectorAll('.firm-card:not(.hidden)').length;
  const el  = document.getElementById('searchCount');
  if (el) el.textContent = vis < tot ? `${{vis}} / ${{tot}}` : `${{tot}} firms`;
}}

/* ── dept chart ───────────────────────────────── */
function renderDeptChart() {{
  const c = document.getElementById('deptChart');
  if (!DEPT_TOTALS.length) {{ c.innerHTML = '<span style="color:var(--text-3)">No data</span>'; return; }}
  const max = DEPT_TOTALS[0][1];
  c.innerHTML = DEPT_TOTALS.map(([dept, n]) => {{
    const col = DEPT_COLOR[dept] || '#8A9BBD';
    const pct = (n / max * 100).toFixed(1);
    return `<div class="dept-bar-row">
      <div class="dept-bar-label" title="${{dept}}">${{dept}}</div>
      <div class="dept-bar-track">
        <div class="dept-bar-fill" style="background:${{col}};width:0%" data-w="${{pct}}%"></div>
      </div>
      <div class="dept-bar-n">${{n}}</div>
    </div>`;
  }}).join('');
  requestAnimationFrame(() =>
    c.querySelectorAll('.dept-bar-fill').forEach(el => el.style.width = el.dataset.w)
  );
}}

/* ── signal type chart ────────────────────────── */
function renderSignalTypes() {{
  const c = document.getElementById('signalTypeChart');
  const entries = Object.entries(TYPE_TOTALS).sort((a,b) => b[1] - a[1]);
  if (!entries.length) {{ c.innerHTML = '<span style="color:var(--text-3)">No data</span>'; return; }}
  const max = entries[0][1];
  const labels = {{
    lateral_hire:'Lateral Hire', job_posting:'Job Posting',
    bar_leadership:'Bar Leadership', ranking:'Ranking',
    court_record:'Court Record', recruit_posting:'Student Recruit',
    press_release:'Press Release', publication:'Publication',
    practice_page:'Practice Page', bar_speaking:'Bar Speaking',
    bar_sponsorship:'Bar Sponsorship', bar_mention:'Bar Mention',
    attorney_profile:'Attorney Profile',
  }};
  c.innerHTML = entries.map(([type, n]) => {{
    const pct = (n / max * 100).toFixed(1);
    return `<div class="stype-item">
      <div class="stype-top">
        <span class="stype-name">${{labels[type] || type}}</span>
        <span class="stype-n">${{n}}</span>
      </div>
      <div class="stype-track">
        <div class="stype-fill" style="width:0%" data-w="${{pct}}%"></div>
      </div>
    </div>`;
  }}).join('');
  requestAnimationFrame(() =>
    c.querySelectorAll('.stype-fill').forEach(el => el.style.width = el.dataset.w)
  );
}}

/* ── init ─────────────────────────────────────── */
document.addEventListener('DOMContentLoaded', () => {{
  renderFirms(FIRMS);
  renderDeptChart();
  renderSignalTypes();
  updateCount();
}});
</script>
</body>
</html>"""
