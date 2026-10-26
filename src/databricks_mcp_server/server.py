from __future__ import annotations

import os
from typing import Any

from mcp.server.mcpserver import MCPServer

from .analysis import analyze_job_run, analyze_stage_detail
from .client import DatabricksAnalyzerClient

server = MCPServer(
    "databricks-job-analyzer",
    title="Databricks Job Analyzer",
    description="Analyze Databricks job runs, Spark plans, and execution metadata for anti-patterns and performance issues.",
)


def _collect(workspace_url: str, token: str, run_id: int, extra_stage_plans: dict[str, str] | None):
    client = DatabricksAnalyzerClient(workspace_url=workspace_url, token=token)
    run = client.get_run(run_id)
    run_tasks = client.get_run_tasks(run_id)
    evidence = client.get_spark_evidence(run)
    # Spark task metrics (with stage ids) drive stage analysis; fall back to run-level task info.
    tasks = evidence["tasks"] or run_tasks
    stage_plans = client.build_stage_plans(run_tasks)
    stage_plans.update(evidence["stage_plans"])
    if extra_stage_plans:
        stage_plans.update(extra_stage_plans)
    sql_queries = client.build_sql_queries(run_tasks) + evidence["sql_queries"]
    try:
        cluster_specs = client.get_cluster_specs(run)
    except Exception:
        cluster_specs = []
    logs, log_sources = None, []
    existing = (run.get("cluster_spec") or {}).get("existing_cluster_id")
    window = (run["start_time"] - 300_000, run["end_time"] + 300_000) if existing and run.get("start_time") and run.get("end_time") else None
    for cluster_id, log_dest in client._cluster_ids_and_log_dests(run):
        if not log_dest:
            log_sources.append({"cluster_id": cluster_id, "component": "cluster_logs", "status": "skipped", "detail": "No cluster_log_conf destination configured."})
            continue
        try:
            logs = client.get_cluster_logs(cluster_id, log_dest, window)
            log_sources.extend({"cluster_id": cluster_id, **s} for s in logs["sources"])
            break
        except Exception as exc:
            log_sources.append({"cluster_id": cluster_id, "component": "cluster_logs", "status": "unavailable", "detail": str(exc)[:300]})
    if not evidence["tasks"] and logs and (logs.get("driver", {}).get("log") or {}).get("tasks"):
        tasks = logs["driver"]["log"]["tasks"]
        evidence["sources"].append({"source": "driver_log", "status": "ok", "task_count": len(tasks), "detail": "Task durations rebuilt from driver TaskSetManager lines."})
    if logs:
        logs["sources"] = log_sources
    return {
        "run": run,
        "tasks": tasks,
        "stage_plans": stage_plans,
        "sql_queries": sql_queries,
        "sources": evidence["sources"],
        "executors": evidence["executors"],
        "stages": evidence["stages"],
        "spark_conf": evidence["spark_conf"],
        "cluster_specs": cluster_specs,
        "logs": logs,
        "log_sources": log_sources,
    }


def _analyze_collected(ctx: dict[str, Any]) -> dict[str, Any]:
    run = ctx["run"]
    plan = run.get("spark_plan") or run.get("plan") or run.get("query_plan")
    result = analyze_job_run(
        run=run,
        tasks=ctx["tasks"],
        plan=plan,
        stage_plans=ctx["stage_plans"],
        executors=ctx["executors"],
        stages=ctx["stages"],
        spark_conf=ctx["spark_conf"],
        cluster_specs=ctx["cluster_specs"],
        sql_queries=ctx["sql_queries"],
        logs=ctx["logs"],
    )
    result["success"] = True
    result["cluster_log_sources"] = ctx["log_sources"]
    result["spark_evidence_sources"] = ctx["sources"]
    result["sql_queries"] = [{k: v for k, v in q.items() if k != "nodes"} for q in ctx["sql_queries"] if isinstance(q, dict)]
    return result


MAX_TEXT = 1000
MAX_ITEMS = 25


def _error_result(run_id: int | None = None, error: Exception | None = None, context: str = "") -> dict[str, Any]:
    message = str(error) if error else "Unknown error"
    return {
        "success": False,
        "run_id": run_id,
        "error": {
            "type": type(error).__name__ if error else "ToolError",
            "message": message,
            "context": context,
        },
        "summary": {
            "status": "error",
            "severity": "low",
            "message": message,
        },
        "findings": [],
        "bad_stages": [],
        "stage_details": [],
        "stage_plans": {},
        "sql_queries": [],
        "spark_plan": None,
        "raw_context": {},
    }


def _truncate(text: Any) -> Any:
    if isinstance(text, str) and len(text) > MAX_TEXT:
        return text[:MAX_TEXT] + f"... [truncated {len(text) - MAX_TEXT} chars]"
    return text


def _compact_context(result: dict[str, Any]) -> dict[str, Any]:
    """Bound response size so MCP clients do not fail on multi-megabyte payloads."""
    bad_ids = {str(stage.get("stage_id")) for stage in result.get("bad_stages", [])}
    plans = result.get("stage_plans") or {}
    selected = {k: v for k, v in plans.items() if k in bad_ids} or dict(list(plans.items())[:MAX_ITEMS])
    result["stage_plans"] = {k: _truncate(v) for k, v in selected.items()}
    result["stage_plans_omitted"] = len(plans) - len(selected)
    queries = result.get("sql_queries") or []
    result["sql_queries"] = [
        {k: _truncate(v) for k, v in q.items()} if isinstance(q, dict) else _truncate(q) for q in queries[:MAX_ITEMS]
    ]
    result["sql_queries_omitted"] = max(len(queries) - MAX_ITEMS, 0)
    raw = result.pop("raw_context", None)
    if raw is not None:
        tasks = raw.get("tasks") or []
        result["raw_context"] = {
            "run": raw.get("run"),
            "task_count": len(tasks),
            "tasks": tasks[:MAX_ITEMS],
            "spark_evidence_sources": raw.get("spark_evidence_sources"),
            "executors": (raw.get("executors") or [])[:MAX_ITEMS],
            "spark_conf": raw.get("spark_conf"),
            "cluster_specs": raw.get("cluster_specs"),
        }
    return result


@server.tool()
def analyze_run(run: dict[str, Any], tasks: list[dict[str, Any]] | None = None, plan: str | None = None, stage_plans: dict[str, str] | None = None, executors: list[dict[str, Any]] | None = None, stages: list[dict[str, Any]] | None = None, spark_conf: dict[str, Any] | None = None, cluster_specs: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    """Analyze a Databricks job run from supplied task metrics, Spark plans, executors and cluster specs: anti-patterns, numeric shuffle/skew/spill metrics, plan analysis and cluster sizing."""
    try:
        result = analyze_job_run(run=run, tasks=tasks or [], plan=plan, stage_plans=stage_plans, executors=executors, stages=stages, spark_conf=spark_conf, cluster_specs=cluster_specs)
        result["success"] = True
        return result
    except Exception as exc:  # pragma: no cover - defensive MCP fallback
        return _error_result(run.get("run_id") if isinstance(run, dict) else None, exc, "analyze_run")


@server.tool()
def analyze_run_by_id(run_id: int, job_id: int | None = None, stage_plans: dict[str, str] | None = None) -> dict[str, Any]:
    """Fetch one Databricks job run (Spark UI or event logs plus cluster config) and return a complete analysis: anti-patterns, shuffle/skew/spill/GC numbers per stage, detailed query-plan analysis for bad stages, and cluster sizing versus data volume, CPU and memory."""
    workspace_url = os.getenv("DATABRICKS_HOST")
    token = os.getenv("DATABRICKS_TOKEN")
    if not workspace_url or not token:
        return _error_result(
            run_id,
            ValueError("Set DATABRICKS_HOST and DATABRICKS_TOKEN before using analyze_run_by_id."),
            "environment",
        )

    try:
        ctx = _collect(workspace_url, token, run_id, stage_plans)
        result = _analyze_collected(ctx)
        result.pop("raw_context", None)
        return _compact_context(result)
    except Exception as exc:  # pragma: no cover - defensive MCP fallback
        return _error_result(run_id, exc, "analyze_run_by_id")


@server.tool()
def fetch_job_run_context(run_id: int, job_id: int | None = None) -> dict[str, Any]:
    """Return the full Databricks run context: run metadata, task details, stage-level metrics, SQL text, and Spark plan evidence ready for AI analysis."""
    workspace_url = os.getenv("DATABRICKS_HOST")
    token = os.getenv("DATABRICKS_TOKEN")
    if not workspace_url or not token:
        return _error_result(
            run_id,
            ValueError("Set DATABRICKS_HOST and DATABRICKS_TOKEN before using fetch_job_run_context."),
            "environment",
        )

    try:
        ctx = _collect(workspace_url, token, run_id, None)
        analyzed = _analyze_collected(ctx)
        plan = ctx["run"].get("spark_plan") or ctx["run"].get("plan") or ctx["run"].get("query_plan")
        analyzed["raw_context"] = {
            "run": ctx["run"],
            "tasks": ctx["tasks"],
            "stage_metrics": analyzed.get("bad_stages", []),
            "stage_plans": ctx["stage_plans"],
            "sql_queries": analyzed["sql_queries"],
            "spark_plan": plan,
            "executors": ctx["executors"],
            "spark_conf": ctx["spark_conf"],
            "cluster_specs": ctx["cluster_specs"],
            "spark_evidence_sources": ctx["sources"],
        }
        return _compact_context(analyzed)
    except Exception as exc:  # pragma: no cover - defensive MCP fallback
        return _error_result(run_id, exc, "fetch_job_run_context")


@server.tool()
def analyze_stage(run_id: int, stage_id: int, job_id: int | None = None) -> dict[str, Any]:
    """Deep-dive one Spark stage of a run: shuffle/skew/spill/GC numbers, slowest tasks, anti-patterns and a detailed analysis of the stage's query plan."""
    workspace_url = os.getenv("DATABRICKS_HOST")
    token = os.getenv("DATABRICKS_TOKEN")
    if not workspace_url or not token:
        return _error_result(run_id, ValueError("Set DATABRICKS_HOST and DATABRICKS_TOKEN before using analyze_stage."), "environment")
    try:
        ctx = _collect(workspace_url, token, run_id, None)
        detail = analyze_stage_detail(
            stage_id,
            ctx["tasks"],
            stage_plans=ctx["stage_plans"],
            sql_queries=ctx["sql_queries"],
            stages=ctx["stages"],
            executors=ctx["executors"],
            cluster_specs=ctx["cluster_specs"],
        )
        detail["success"] = True
        detail["run_id"] = run_id
        return detail
    except Exception as exc:  # pragma: no cover - defensive MCP fallback
        return _error_result(run_id, exc, "analyze_stage")


@server.tool()
def diagnose_job(run: dict[str, Any], tasks: list[dict[str, Any]] | None = None, plan: str | None = None, stage_plans: dict[str, str] | None = None, executors: list[dict[str, Any]] | None = None, stages: list[dict[str, Any]] | None = None, spark_conf: dict[str, Any] | None = None, cluster_specs: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    """Alias for analyze_run intended for natural-language agent workflows."""
    try:
        result = analyze_job_run(run=run, tasks=tasks or [], plan=plan, stage_plans=stage_plans, executors=executors, stages=stages, spark_conf=spark_conf, cluster_specs=cluster_specs)
        result["success"] = True
        return result
    except Exception as exc:  # pragma: no cover - defensive MCP fallback
        return _error_result(run.get("run_id") if isinstance(run, dict) else None, exc, "diagnose_job")


def main() -> None:
    """Run the MCP server over stdio."""
    server.run(transport="stdio")


if __name__ == "__main__":
    main()
