"""
intelligence/custom_agents.py
────────────────────────────
Deterministic multi-agent intelligence layer for the tracker.

These agents do not replace the existing signal scrapers; they sit on top of
the collected data and answer four higher-level questions:

1. What changed most recently?
2. Which firms are accelerating?
3. Which opportunities deserve action first?
4. Which recommendations should be tempered by risk or outreach fatigue?
"""

from __future__ import annotations

import json
import logging
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

from config_calgary import FIRM_BY_ID
from database.db import get_all_signals_for_dashboard, get_conn, save_agent_run
from predictive.demand_model import DemandPredictor
from scoring.aggregator import compute_firm_scores, recency_decay

log = logging.getLogger(__name__)


@dataclass
class AgentFinding:
    agent: str
    firm_id: str | None
    headline: str
    details: str
    score: float = 0.0
    severity: str = "info"


class BaseTrackerAgent:
    name = "base"

    def analyze(self, context: dict) -> list[AgentFinding]:
        raise NotImplementedError


class SignalScoutAgent(BaseTrackerAgent):
    name = "signal_scout"

    def analyze(self, context: dict) -> list[AgentFinding]:
        findings: list[AgentFinding] = []
        practice_counter: Counter[str] = Counter()

        for firm_id, signals in context["signals_by_firm"].items():
            strategy_count = len({sig["signal_type"] for sig in signals})
            weighted_freshness = sum(sig["_fresh_weight"] for sig in signals)

            for sig in signals:
                if sig.get("practice_area"):
                    practice_counter[sig["practice_area"]] += 1

            if strategy_count >= 2 and weighted_freshness >= 6:
                top_signal = max(signals, key=lambda sig: sig["_fresh_weight"])
                findings.append(
                    AgentFinding(
                        agent=self.name,
                        firm_id=firm_id,
                        headline=f"Corroborated activity at {context['firm_names'][firm_id]}",
                        details=(
                            f"{len(signals)} fresh signals across {strategy_count} signal types. "
                            f"Best proof point: {top_signal['title']}"
                        ),
                        score=weighted_freshness,
                        severity="high" if weighted_freshness >= 10 else "medium",
                    )
                )

        for practice_area, count in practice_counter.most_common(3):
            findings.append(
                AgentFinding(
                    agent=self.name,
                    firm_id=None,
                    headline=f"Practice hotspot: {practice_area}",
                    details=f"{count} recent signals mention {practice_area}.",
                    score=float(count),
                    severity="info",
                )
            )

        return sorted(findings, key=lambda item: item.score, reverse=True)


class MomentumAgent(BaseTrackerAgent):
    name = "momentum_analyst"

    def analyze(self, context: dict) -> list[AgentFinding]:
        findings: list[AgentFinding] = []
        recent_cutoff = context["now"] - timedelta(days=context["window_days"])
        prior_cutoff = recent_cutoff - timedelta(days=context["window_days"])

        for firm_id, signals in context["signals_by_firm"].items():
            recent = [
                sig for sig in signals
                if sig["_detected_dt"] >= recent_cutoff
            ]
            prior = [
                sig for sig in context["all_signals_by_firm"].get(firm_id, [])
                if prior_cutoff <= sig["_detected_dt"] < recent_cutoff
            ]

            recent_score = sum(sig["weight"] for sig in recent)
            prior_score = sum(sig["weight"] for sig in prior)
            delta = recent_score - prior_score

            if recent and (delta >= 2 or (recent_score >= 6 and not prior)):
                findings.append(
                    AgentFinding(
                        agent=self.name,
                        firm_id=firm_id,
                        headline=f"Momentum building at {context['firm_names'][firm_id]}",
                        details=(
                            f"Recent window score {recent_score:.1f} versus {prior_score:.1f} in the "
                            f"previous window ({len(recent)} vs {len(prior)} signals)."
                        ),
                        score=delta if prior else recent_score,
                        severity="high" if recent_score >= 8 else "medium",
                    )
                )

        return sorted(findings, key=lambda item: item.score, reverse=True)


class OpportunityAgent(BaseTrackerAgent):
    name = "opportunity_strategist"

    def analyze(self, context: dict) -> list[AgentFinding]:
        findings: list[AgentFinding] = []
        predictions = context["predictions"]

        for row in context["leaderboard"][: context["top_n"]]:
            p30 = predictions.get(row["firm_id"], {}).get("p30", 0)
            action = "Send now" if row["urgency"] == "🚨 Same-Day" else "Research then send"
            findings.append(
                AgentFinding(
                    agent=self.name,
                    firm_id=row["firm_id"],
                    headline=f"{action}: {row['firm_name']}",
                    details=(
                        f"Leaderboard score {row['score']:.1f}, {row['signal_count']} signals, "
                        f"P30={p30:.0%}. Top signal: {row['top_signal']}"
                    ),
                    score=row["score"] + (p30 * 10),
                    severity="high" if row["urgency"] == "🚨 Same-Day" else "medium",
                )
            )

        return findings


class RiskGuardAgent(BaseTrackerAgent):
    name = "risk_guard"

    def analyze(self, context: dict) -> list[AgentFinding]:
        findings: list[AgentFinding] = []
        outreach_counts = context["recent_outreach_counts"]

        for row in context["leaderboard"][: context["top_n"]]:
            firm_id = row["firm_id"]
            signals = context["signals_by_firm"].get(firm_id, [])
            if not signals:
                continue

            latest_signal = max(signals, key=lambda sig: sig["_detected_dt"])
            age_days = (context["now"] - latest_signal["_detected_dt"]).days
            outreach_count = outreach_counts.get(firm_id, 0)
            risk_flags: list[str] = []

            if outreach_count >= 2:
                risk_flags.append(f"{outreach_count} recent outreach attempts already logged")
            if row["signal_count"] == 1:
                risk_flags.append("only one signal on record")
            if age_days >= 5:
                risk_flags.append(f"latest signal is {age_days} days old")

            if risk_flags:
                findings.append(
                    AgentFinding(
                        agent=self.name,
                        firm_id=firm_id,
                        headline=f"Watchouts for {row['firm_name']}",
                        details="; ".join(risk_flags).capitalize() + ".",
                        score=float(len(risk_flags)),
                        severity="medium" if len(risk_flags) == 1 else "high",
                    )
                )

        return sorted(findings, key=lambda item: item.score, reverse=True)


class CustomAgentSwarm:
    def __init__(self):
        self.agents = [
            SignalScoutAgent(),
            MomentumAgent(),
            OpportunityAgent(),
            RiskGuardAgent(),
        ]

    def run(self, top_n: int = 5, days: int = 14, persist: bool = True) -> dict:
        context = _build_context(top_n=top_n, days=days)
        findings_by_agent: dict[str, list[AgentFinding]] = {}

        for agent in self.agents:
            try:
                findings_by_agent[agent.name] = agent.analyze(context)
            except Exception as exc:
                log.exception("[Agents] %s failed", agent.name)
                findings_by_agent[agent.name] = [
                    AgentFinding(
                        agent=agent.name,
                        firm_id=None,
                        headline=f"{agent.name} failed",
                        details=str(exc),
                        severity="high",
                    )
                ]

        consensus = _build_consensus(context, findings_by_agent)
        markdown = _render_markdown_report(context, findings_by_agent, consensus)
        payload = {
            "generated_at": context["now"].isoformat(),
            "window_days": days,
            "top_n": top_n,
            "consensus": consensus,
            "agents": {
                name: [finding.__dict__ for finding in findings]
                for name, findings in findings_by_agent.items()
            },
        }

        report_path = Path("reports/agent_swarm_report.json")
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(json.dumps(payload, indent=2, default=str))

        run_id = None
        if persist:
            run_id = save_agent_run(
                agent_name="custom_agent_swarm",
                summary=consensus["summary"],
                report_markdown=markdown,
                report_json=payload,
            )

        return {
            "run_id": run_id,
            "markdown": markdown,
            "json": payload,
            "report_path": str(report_path),
        }


def _build_context(top_n: int, days: int) -> dict:
    now = datetime.utcnow()
    fresh_signals = _decorate_signals(get_all_signals_for_dashboard(days=days))
    all_signals = _decorate_signals(get_all_signals_for_dashboard(days=days * 2))
    leaderboard = compute_firm_scores()
    predictions = _safe_predictions()

    signals_by_firm = _group_signals(fresh_signals)
    all_signals_by_firm = _group_signals(all_signals)
    firm_names = {
        firm_id: FIRM_BY_ID.get(firm_id, {}).get("name", firm_id)
        for firm_id in set(signals_by_firm) | {row["firm_id"] for row in leaderboard}
    }

    return {
        "now": now,
        "window_days": days,
        "top_n": top_n,
        "leaderboard": leaderboard,
        "predictions": predictions,
        "signals_by_firm": signals_by_firm,
        "all_signals_by_firm": all_signals_by_firm,
        "recent_outreach_counts": _recent_outreach_counts(days=30),
        "firm_names": firm_names,
    }


def _decorate_signals(signals: list[dict]) -> list[dict]:
    decorated = []
    for sig in signals:
        detected_raw = sig.get("detected_at") or datetime.utcnow().isoformat()
        try:
            detected_dt = datetime.fromisoformat(detected_raw)
        except ValueError:
            detected_dt = datetime.utcnow()
        sig = dict(sig)
        sig["_detected_dt"] = detected_dt
        sig["_fresh_weight"] = sig.get("weight", 0) * recency_decay(detected_raw)
        decorated.append(sig)
    return decorated


def _group_signals(signals: list[dict]) -> dict[str, list[dict]]:
    grouped: dict[str, list[dict]] = defaultdict(list)
    for sig in signals:
        grouped[sig["firm_id"]].append(sig)
    return grouped


def _safe_predictions() -> dict[str, dict]:
    try:
        predictor = DemandPredictor()
        return {row["firm_id"]: row for row in predictor.predict_all()}
    except Exception as exc:
        if "no such table" in str(exc).lower():
            log.info("[Agents] Predictions unavailable until the forecast tables exist: %s", exc)
        else:
            log.warning("[Agents] Could not compute predictions: %s", exc)
        return {}


def _recent_outreach_counts(days: int = 30) -> dict[str, int]:
    conn = get_conn()
    try:
        rows = conn.execute("""
            SELECT firm_id, COUNT(*) AS cnt
            FROM outreach_sent
            WHERE date(COALESCE(sent_at, scheduled_at)) >= date('now', ? || ' days')
            GROUP BY firm_id
        """, (f"-{days}",)).fetchall()
        return {row["firm_id"]: row["cnt"] for row in rows}
    except Exception:
        return {}
    finally:
        conn.close()


def _build_consensus(context: dict, findings_by_agent: dict[str, list[AgentFinding]]) -> dict:
    firm_votes: dict[str, dict] = defaultdict(lambda: {"score": 0.0, "reasons": []})
    for findings in findings_by_agent.values():
        for finding in findings:
            if not finding.firm_id:
                continue
            firm_votes[finding.firm_id]["score"] += finding.score
            firm_votes[finding.firm_id]["reasons"].append(finding.headline)

    ranked = sorted(
        (
            {
                "firm_id": firm_id,
                "firm_name": context["firm_names"].get(firm_id, firm_id),
                "score": round(data["score"], 2),
                "reasons": data["reasons"][:3],
            }
            for firm_id, data in firm_votes.items()
        ),
        key=lambda item: item["score"],
        reverse=True,
    )

    if ranked:
        summary = (
            f"Best current target: {ranked[0]['firm_name']} "
            f"(multi-agent score {ranked[0]['score']:.1f})."
        )
    else:
        summary = "No agent consensus yet; ingest more signals to unlock recommendations."

    return {"summary": summary, "top_targets": ranked[: context["top_n"]]}


def _render_markdown_report(
    context: dict,
    findings_by_agent: dict[str, list[AgentFinding]],
    consensus: dict,
) -> str:
    lines = [
        "# Custom Agent Swarm Report",
        "",
        f"Generated: {context['now'].strftime('%Y-%m-%d %H:%M UTC')}",
        f"Window: last {context['window_days']} days",
        "",
        "## Consensus",
        f"- {consensus['summary']}",
    ]

    if consensus["top_targets"]:
        for idx, row in enumerate(consensus["top_targets"], start=1):
            reason_text = "; ".join(row["reasons"]) if row["reasons"] else "No reasons recorded"
            lines.append(
                f"- #{idx} {row['firm_name']} — score {row['score']:.1f} — {reason_text}"
            )
    else:
        lines.append("- No firms qualified for consensus ranking.")

    for agent_name, findings in findings_by_agent.items():
        lines.extend(["", f"## {agent_name}"])
        if not findings:
            lines.append("- No findings.")
            continue

        for finding in findings[: context["top_n"]]:
            prefix = f"**{finding.headline}**"
            if finding.firm_id:
                prefix += f" ({context['firm_names'].get(finding.firm_id, finding.firm_id)})"
            lines.append(f"- {prefix}: {finding.details}")

    return "\n".join(lines) + "\n"


def run_custom_agent_swarm(top_n: int = 5, days: int = 14, persist: bool = True) -> dict:
    return CustomAgentSwarm().run(top_n=top_n, days=days, persist=persist)
