from __future__ import annotations

from typing import Any

from .cluster_analysis import analyze_cluster, describe_cluster_specs
from .log_analysis import analyze_logs
from .plan_analysis import analyze_plan, plan_id, summarize_operator_metrics
from .recommendations import category_of, recommendation_for
from .stage_metrics import aggregate_totals, detect_stage_issues, group_by_stage, summarize_stage, task_duration

SEVERITY_ORDER = {"low": 1, "medium": 2, "high": 3, "critical": 4}
MAX_BAD_STAGES = 25
MAX_PLANS = 6
MAX_AFFECTED = 20
MAX_EVIDENCE = 5


def _severity_rank(severity: str) -> int:
    return SEVERITY_ORDER.get(str(severity).lower(), 0)


def _max_severity(findings: list[dict[str, Any]]) -> str:
    if not findings:
        return "low"
    return max((str(f.get("severity", "low")).lower() for f in findings), key=_severity_rank)


def _total_cores(executors: list[dict[str, Any]] | None, specs: list[dict[str, Any]]) -> int | None:
    cores = sum(int(e.get("cores") or 0) for e in (executors or []) if str(e.get("id")) != "driver")
    if cores:
        return cores
    return max((s["provisioned_cores_max"] or 0 for s in specs), default=0) or None


def _stage_info_map(stages: list[dict[str, Any]] | None) -> dict[str, dict[str, Any]]:
    info: dict[str, dict[str, Any]] = {}
    for stage in stages or []:
        if stage.get("stage_id") is not None:
            info[str(stage["stage_id"])] = stage
    return info


def _plan_registry(
    plan: str | None,
    stage_plans: dict[str, str] | None,
    sql_queries: list[dict[str, Any]] | None,
) -> tuple[dict[str, dict[str, Any]], dict[str, str]]:
    """Analyze each distinct plan once. Returns (plan_id -> analysis, stage_id -> plan_id)."""
    operator_metrics: dict[str, list[dict[str, Any]]] = {}
    for query in sql_queries or []:
        if isinstance(query, dict) and query.get("plan") and query.get("nodes"):
            operator_metrics[plan_id(query["plan"])] = summarize_operator_metrics(query["nodes"])

    registry: dict[str, dict[str, Any]] = {}
    stage_to_plan: dict[str, str] = {}

    def register(text: str | None, stage_id: str | None) -> None:
        if not isinstance(text, str) or not text.strip():
            return
        pid = plan_id(text)
        if pid not in registry:
            analysis = analyze_plan(text)
            if analysis is None:
                return
            analysis["stage_ids"] = []
            analysis["operator_metrics"] = operator_metrics.get(pid, [])
            registry[pid] = analysis
        if stage_id is not None:
            registry[pid]["stage_ids"].append(stage_id)
            stage_to_plan[stage_id] = pid

    register(plan, None)
    for stage_id, text in (stage_plans or {}).items():
        register(text, str(stage_id))
    return registry, stage_to_plan


def _compact_finding(finding: dict[str, Any]) -> dict[str, Any]:
    return {k: finding[k] for k in ("type", "severity", "message") if k in finding}


def _aggregate_findings(raw: list[tuple[dict[str, Any], str | None]]) -> list[dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = {}
    for finding, stage_id in raw:
        kind = finding["type"]
        entry = grouped.setdefault(
            kind,
            {
                "type": kind,
                "category": category_of(kind),
                "severity": finding["severity"],
                "message": finding["message"],
                "occurrences": 0,
                "affected_stages": [],
                "evidence": [],
                "recommendation": recommendation_for(kind),
            },
        )
        entry["occurrences"] += 1
        if _severity_rank(finding["severity"]) > _severity_rank(entry["severity"]):
            entry["severity"] = finding["severity"]
        if stage_id is not None and stage_id not in entry["affected_stages"]:
            entry["affected_stages"].append(stage_id)
        evidence = finding.get("evidence")
        if evidence and evidence not in entry["evidence"]:
            entry["evidence"].append(evidence)
    result = []
    for entry in grouped.values():
        if entry["occurrences"] > 1 or entry["message"].startswith("Stage "):
            entry["message"] = _generic_message(entry["type"], entry["message"])
        entry["affected_stage_count"] = len(entry["affected_stages"])
        entry["affected_stages"] = entry["affected_stages"][:MAX_AFFECTED]
        entry["evidence"] = entry["evidence"][:MAX_EVIDENCE]
        result.append(entry)
    return sorted(result, key=lambda e: (-_severity_rank(e["severity"]), -e["occurrences"]))


_GENERIC = {
    "task_skew": "One or more tasks are significantly slower than the rest of their stage (partition skew or hotspot data).",
    "data_skew": "Shuffle data is unevenly distributed across tasks.",
    "disk_spill": "Tasks spilled to disk because they did not fit in memory.",
    "memory_spill": "Tasks show significant in-memory spill.",
    "gc_pressure": "JVM garbage collection takes a large share of task time.",
    "shuffle_heavy": "Large or numerous shuffles.",
    "small_tasks": "Stages with very many tiny tasks.",
    "large_partitions": "Stages with very large partitions per task.",
    "data_growth": "Stages whose output rows greatly exceed input rows.",
    "low_cpu_efficiency": "Tasks use CPU for under half of their run time.",
    "shuffle_fetch_wait": "Tasks wait a long time on shuffle fetches.",
    "under_parallelism": "Stages have fewer tasks than available cores.",
}


def _generic_message(kind: str, fallback: str) -> str:
    return _GENERIC.get(kind, fallback)


def _prioritized_recommendations(findings: list[dict[str, Any]], limit: int = 10) -> list[dict[str, Any]]:
    out = []
    for finding in findings:
        if not finding.get("recommendation") or finding["severity"] == "low" and len(out) >= 5:
            continue
        out.append(
            {
                "priority": len(out) + 1,
                "issue": finding["type"],
                "severity": finding["severity"],
                "action": finding["recommendation"],
                "affected_stages": finding["affected_stages"][:10],
            }
        )
        if len(out) >= limit:
            break
    return out


def _build_text_summary(
    status: str,
    run_id: Any,
    job_id: Any,
    bad_stages: list[dict[str, Any]],
    findings: list[dict[str, Any]],
    totals: dict[str, Any] | None,
    total_bad: int,
) -> str:
    head = f"Run {run_id} for job {job_id} ended with status {status}."
    if totals and totals["tasks"]:
        r = totals["readable"]
        head += (
            f" {totals['tasks']:,} tasks in {totals['stages']} stages, {r['total_task_time']} total task time; "
            f"input {r['input']}, shuffle write {r['shuffle_write']}, shuffle read {r['shuffle_read']}, "
            f"disk spill {r['spill_disk']}, GC {r['gc_ratio']} of task time."
        )
    if not bad_stages and not findings:
        return head + " No severe stage-level performance issues were detected from the provided metrics and plan hints."

    lines = [head]
    if bad_stages:
        worst = bad_stages[0]
        lines.append(
            f"The most likely problem area is Stage {worst['stage_id']} ({worst['severity']} severity), with issues: {', '.join(worst['issue_types'])}."
        )
        for stage in bad_stages[:3]:
            lines.append(
                f"Stage {stage['stage_id']}: {stage['severity']}; {stage['task_count']} tasks, "
                f"task time {stage['metrics']['readable']['total_task_time']}, max task {stage['metrics']['readable']['task_duration_max']}, "
                f"shuffle read {stage['metrics']['readable']['shuffle_read']}, spill {stage['metrics']['readable']['spill_disk']}; "
                f"issues: {', '.join(stage['issue_types'])}."
            )
        if total_bad > len(bad_stages):
            lines.append(f"{total_bad} stages flagged in total.")
    notable = [f for f in findings if f["severity"] in {"high", "critical"}] or findings
    if notable:
        lines.append("Top anti-patterns: " + ", ".join(f"{f['type']} ({f['occurrences']}x)" for f in notable[:6]) + ".")
    return " ".join(lines)


def analyze_job_run(
    run: dict[str, Any],
    tasks: list[dict[str, Any]] | None = None,
    plan: str | None = None,
    stage_plans: dict[str, str] | None = None,
    executors: list[dict[str, Any]] | None = None,
    stages: list[dict[str, Any]] | None = None,
    spark_conf: dict[str, Any] | None = None,
    cluster_specs: list[dict[str, Any]] | None = None,
    sql_queries: list[dict[str, Any]] | None = None,
    logs: dict[str, Any] | None = None,
) -> dict[str, Any]:
    tasks = tasks or []
    state = run.get("state", {}) if isinstance(run, dict) else {}
    status = state.get("result_state") or state.get("life_cycle_state") or "UNKNOWN"
    run_id = run.get("run_id", "unknown")
    job_id = run.get("job_id", "unknown")

    specs = describe_cluster_specs(cluster_specs)
    total_cores = _total_cores(executors, specs)
    stage_info = _stage_info_map(stages)

    stage_metrics: list[dict[str, Any]] = []
    for stage_id, stage_tasks in group_by_stage(tasks).items():
        stage_metrics.append(summarize_stage(stage_id, stage_tasks, stage_info.get(str(stage_id))))
    stage_metrics.sort(key=lambda m: m["total_task_time_ms"], reverse=True)

    registry, stage_to_plan = _plan_registry(plan, stage_plans, sql_queries)

    raw_findings: list[tuple[dict[str, Any], str | None]] = []
    stage_summaries: list[dict[str, Any]] = []
    for metrics in stage_metrics:
        sid = str(metrics["stage_id"])
        issues = detect_stage_issues(metrics, total_cores)
        pid = stage_to_plan.get(sid)
        plan_findings = registry[pid]["findings"] if pid else []
        metric_issues = issues
        if not metrics["has_byte_metrics"]:
            issues = issues + plan_findings
        if not issues:
            continue
        severity = _max_severity(issues)
        types = list(dict.fromkeys(i["type"] for i in issues))
        for issue in metric_issues:
            raw_findings.append((issue, sid))
        stage_summaries.append(
            {
                "stage_id": metrics["stage_id"],
                "stage_name": metrics["stage_name"],
                "severity": severity,
                "task_count": metrics["task_count"],
                "avg_duration_ms": metrics["avg_duration_ms"],
                "max_duration_ms": metrics["max_duration_ms"],
                "records_read": metrics["records_read"],
                "records_written": metrics["records_written"],
                "issue_types": types,
                "issues": [i["message"] for i in issues],
                "issue_details": [i for i in issues if i not in plan_findings],
                "query_findings": [_compact_finding(f) for f in plan_findings],
                "plan_id": pid,
                "metrics": metrics,
            }
        )

    stage_summaries.sort(key=lambda s: (-_severity_rank(s["severity"]), -s["metrics"]["total_task_time_ms"]))

    for analysis in registry.values():
        for finding in analysis["findings"]:
            raw_findings.append((finding, None))

    log_result = analyze_logs(
        logs, stages, set(stage_to_plan), {str(m["stage_id"]) for m in stage_metrics}, total_cores
    )
    log_affected: dict[str, list[str]] = {}
    if log_result:
        for finding, stage_ids in log_result["findings"]:
            raw_findings.append((finding, None))
            log_affected.setdefault(finding["type"], [])
            log_affected[finding["type"]].extend(s for s in stage_ids if s not in log_affected[finding["type"]])

    cluster = analyze_cluster(run, tasks, stage_metrics, executors, spark_conf, cluster_specs, stages, log_result["summary"] if log_result else None)
    for finding in cluster["findings"]:
        raw_findings.append((finding, None))

    findings = _aggregate_findings(raw_findings)
    for finding in findings:
        extra = log_affected.get(finding["type"])
        if extra:
            merged_stages = list(dict.fromkeys(finding["affected_stages"] + extra))
            finding["affected_stages"] = merged_stages[:MAX_AFFECTED]
            finding["affected_stage_count"] = len(merged_stages)
    plan_stage_lookup = {pid: a["stage_ids"] for pid, a in registry.items()}
    for finding in findings:
        plan_stages = [sid for pid, p in registry.items() if any(f["type"] == finding["type"] for f in p["findings"]) for sid in plan_stage_lookup[pid]]
        if plan_stages:
            merged = list(dict.fromkeys(finding["affected_stages"] + plan_stages))
            finding["affected_stage_count"] = len(merged)
            finding["affected_stages"] = merged[:MAX_AFFECTED]

    if status == "FAILED" and not findings:
        findings.append(
            {
                "type": "unknown_failure",
                "category": "other",
                "severity": "medium",
                "message": "The job failed without obvious plan or task evidence; inspect logs and cluster events for root cause.",
                "occurrences": 1,
                "affected_stages": [],
                "affected_stage_count": 0,
                "evidence": [state.get("state_message") or "No clear skew or shuffle issue was detected from supplied data."],
                "recommendation": None,
            }
        )

    totals = aggregate_totals(stage_metrics) if stage_metrics else None
    total_bad = len(stage_summaries)
    bad_stages = stage_summaries[:MAX_BAD_STAGES]

    summary = {
        "status": status,
        "severity": _max_severity(findings),
        "findings_count": len(findings),
        "bad_stages_count": total_bad,
        "worst_stage_id": bad_stages[0]["stage_id"] if bad_stages else None,
        "run_id": run_id,
        "job_id": job_id,
    }
    text_summary = _build_text_summary(status, run_id, job_id, bad_stages, findings, totals, total_bad)

    bad_ids = {str(s["stage_id"]) for s in stage_summaries}
    stage_cost = {str(m["stage_id"]): m["total_task_time_ms"] for m in stage_metrics}
    query_analysis = []
    for pid, analysis in registry.items():
        ids = analysis["stage_ids"]
        entry = {k: v for k, v in analysis.items() if k != "stage_ids"}
        entry["stage_count"] = len(ids)
        entry["stage_ids"] = ids[:MAX_AFFECTED]
        entry["bad_stage_ids"] = [s for s in ids if s in bad_ids][:MAX_AFFECTED]
        entry["task_time_ms"] = round(sum(stage_cost.get(s, 0.0) for s in ids), 2)
        entry["findings"] = [f | {"recommendation": recommendation_for(f["type"])} for f in analysis["findings"]]
        query_analysis.append(entry)
    query_analysis.sort(key=lambda q: (-len(q["bad_stage_ids"]), -q["task_time_ms"]))
    query_analysis = query_analysis[:MAX_PLANS]

    stage_plans_payload = stage_plans or {}
    sql_out = []
    if plan:
        sql_out.append({"source": "run_plan", "sql": plan})
    for stage_id, stage_plan in stage_plans_payload.items():
        if stage_plan:
            sql_out.append({"stage_id": stage_id, "sql": stage_plan})

    return {
        "run_id": run_id,
        "job_id": job_id,
        "summary": summary,
        "text_summary": text_summary,
        "narrative_summary": text_summary,
        "anti_patterns": [
            {"type": f["type"], "category": f["category"], "severity": f["severity"], "occurrences": f["occurrences"], "affected_stage_count": f.get("affected_stage_count", 0)}
            for f in findings
        ],
        "findings": findings,
        "recommendations": _prioritized_recommendations(findings),
        "run_metrics": totals,
        "bad_stages": bad_stages,
        "stage_details": bad_stages,
        "query_analysis": query_analysis,
        "cluster_analysis": cluster,
        "cluster_logs": log_result["summary"] if log_result else None,
        "spark_plan": plan,
        "stage_plans": stage_plans_payload,
        "sql_queries": sql_out,
        "raw_context": {
            "run": run,
            "tasks": tasks,
            "stage_details": bad_stages,
            "spark_plan": plan,
            "stage_plans": stage_plans_payload,
            "sql_queries": sql_out,
        },
    }


def analyze_stage_detail(
    stage_id: Any,
    tasks: list[dict[str, Any]],
    stage_plans: dict[str, str] | None = None,
    sql_queries: list[dict[str, Any]] | None = None,
    stages: list[dict[str, Any]] | None = None,
    executors: list[dict[str, Any]] | None = None,
    cluster_specs: list[dict[str, Any]] | None = None,
    top_tasks: int = 10,
) -> dict[str, Any]:
    """Deep dive for a single stage: full metrics, anti-patterns, slowest tasks and the full plan analysis."""
    sid = str(stage_id)
    stage_tasks = group_by_stage(tasks).get(stage_id) or group_by_stage(tasks).get(int(sid) if sid.isdigit() else sid) or []
    if not stage_tasks:
        return {"stage_id": stage_id, "found": False, "message": f"No task metrics found for stage {sid}."}
    specs = describe_cluster_specs(cluster_specs)
    metrics = summarize_stage(stage_id, stage_tasks, _stage_info_map(stages).get(sid))
    issues = detect_stage_issues(metrics, _total_cores(executors, specs))
    registry, stage_to_plan = _plan_registry(None, {k: v for k, v in (stage_plans or {}).items() if str(k) == sid}, sql_queries)
    pid = stage_to_plan.get(sid)
    slowest = sorted(stage_tasks, key=task_duration, reverse=True)[:top_tasks]
    return {
        "stage_id": stage_id,
        "found": True,
        "metrics": metrics,
        "issues": [i | {"recommendation": recommendation_for(i["type"])} for i in issues],
        "slowest_tasks": [
            {
                "task_id": t.get("task_id"),
                "executor_id": t.get("executor_id"),
                "duration_ms": task_duration(t),
                "shuffle_read_bytes": t.get("shuffle_read_bytes"),
                "input_bytes": t.get("input_bytes"),
                "disk_bytes_spilled": t.get("disk_bytes_spilled"),
                "gc_time_ms": t.get("gc_time"),
                "records_read": t.get("records_read"),
            }
            for t in slowest
        ],
        "plan_analysis": registry.get(pid) if pid else None,
    }
