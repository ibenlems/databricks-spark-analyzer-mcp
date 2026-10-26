from __future__ import annotations

from typing import Any

from .units import GIB, MIB, fmt_bytes, fmt_ms, fmt_ratio, percentile

DURATION_KEYS = ("executor_run_time", "duration", "task_duration_ms")


def _num(task: dict[str, Any], key: str) -> float:
    value = task.get(key)
    return float(value) if isinstance(value, (int, float)) else 0.0


def task_duration(task: dict[str, Any]) -> float:
    for key in DURATION_KEYS:
        value = task.get(key)
        if isinstance(value, (int, float)):
            return float(value)
    return 0.0


def task_stage_id(task: dict[str, Any]) -> Any:
    return task.get("stage_id") or task.get("stage") or task.get("stageId") or "unknown"


def group_by_stage(tasks: list[dict[str, Any]]) -> dict[Any, list[dict[str, Any]]]:
    stages: dict[Any, list[dict[str, Any]]] = {}
    for task in tasks:
        stages.setdefault(task_stage_id(task), []).append(task)
    return stages


def _dist(values: list[float]) -> dict[str, float]:
    return {
        "min": round(min(values), 2) if values else 0.0,
        "p50": round(percentile(values, 50), 2),
        "p75": round(percentile(values, 75), 2),
        "p95": round(percentile(values, 95), 2),
        "max": round(max(values), 2) if values else 0.0,
    }


def summarize_stage(stage_id: Any, tasks: list[dict[str, Any]], stage_info: dict[str, Any] | None = None) -> dict[str, Any]:
    """Compute numeric shuffle / skew / spill / GC / CPU metrics for one stage."""
    n = len(tasks)
    durations = [task_duration(t) for t in tasks]
    total_run = sum(durations)
    duration = _dist(durations)
    p50 = duration["p50"]
    avg = total_run / n if n else 0.0
    baseline = p50 if p50 > 0 else avg
    skew_ratio = round(duration["max"] / baseline, 2) if baseline > 0 else 0.0

    shuffle_read = [_num(t, "shuffle_read_bytes") for t in tasks]
    input_bytes_per_task = [_num(t, "input_bytes") for t in tasks]
    sr_dist = _dist(shuffle_read)
    in_dist = _dist(input_bytes_per_task)

    cpu_ms = sum(_num(t, "executor_cpu_time_ns") for t in tasks) / 1e6
    gc_ms = sum(_num(t, "gc_time") for t in tasks)
    mem_spill = sum(_num(t, "memory_bytes_spilled") for t in tasks)
    disk_spill = sum(_num(t, "disk_bytes_spilled") for t in tasks)
    spilled_tasks = sum(1 for t in tasks if _num(t, "disk_bytes_spilled") > 0 or _num(t, "memory_bytes_spilled") > 0)

    metrics: dict[str, Any] = {
        "stage_id": stage_id,
        "stage_name": (stage_info or {}).get("name"),
        "task_count": n,
        "wall_time_ms": None,
        "total_task_time_ms": round(total_run, 2),
        "avg_duration_ms": round(avg, 2),
        "max_duration_ms": duration["max"],
        "duration_ms": duration,
        "skew_ratio": skew_ratio,
        "straggler_overhead_ms": round(max(duration["max"] - baseline, 0.0), 2),
        "records_read": int(sum(_num(t, "records_read") for t in tasks)),
        "records_written": int(sum(_num(t, "records_written") for t in tasks)),
        "input_bytes": int(sum(input_bytes_per_task)),
        "output_bytes": int(sum(_num(t, "output_bytes") for t in tasks)),
        "shuffle_read_bytes": int(sum(shuffle_read)),
        "shuffle_write_bytes": int(sum(_num(t, "shuffle_write_bytes") for t in tasks)),
        "shuffle_read_per_task_bytes": sr_dist,
        "shuffle_read_skew_ratio": round(sr_dist["max"] / sr_dist["p50"], 2) if sr_dist["p50"] > 0 else 0.0,
        "input_per_task_bytes": in_dist,
        "spill": {
            "memory_bytes": int(mem_spill),
            "disk_bytes": int(disk_spill),
            "tasks_spilled": spilled_tasks,
        },
        "gc_time_ms": round(gc_ms, 2),
        "gc_ratio": round(gc_ms / total_run, 4) if total_run > 0 else None,
        "cpu_time_ms": round(cpu_ms, 2),
        "cpu_efficiency": round(cpu_ms / total_run, 4) if total_run > 0 and cpu_ms > 0 else None,
        "shuffle_fetch_wait_ms": round(sum(_num(t, "shuffle_fetch_wait_ms") for t in tasks), 2),
        "peak_execution_memory_bytes": int(max((_num(t, "peak_execution_memory") for t in tasks), default=0.0)),
    }

    if stage_info:
        start, end = stage_info.get("submission_time"), stage_info.get("completion_time")
        if isinstance(start, (int, float)) and isinstance(end, (int, float)) and end >= start:
            metrics["wall_time_ms"] = int(end - start)

    metrics["has_byte_metrics"] = any(
        metrics[k] for k in ("input_bytes", "output_bytes", "shuffle_read_bytes", "shuffle_write_bytes", "gc_time_ms")
    ) or disk_spill > 0 or mem_spill > 0
    metrics["readable"] = {
        "total_task_time": fmt_ms(total_run),
        "wall_time": fmt_ms(metrics["wall_time_ms"]) if metrics["wall_time_ms"] is not None else "n/a",
        "task_duration_p50": fmt_ms(p50),
        "task_duration_max": fmt_ms(duration["max"]),
        "input": fmt_bytes(metrics["input_bytes"]),
        "output": fmt_bytes(metrics["output_bytes"]),
        "shuffle_read": fmt_bytes(metrics["shuffle_read_bytes"]),
        "shuffle_write": fmt_bytes(metrics["shuffle_write_bytes"]),
        "shuffle_read_per_task_p50": fmt_bytes(sr_dist["p50"]),
        "shuffle_read_per_task_max": fmt_bytes(sr_dist["max"]),
        "spill_memory": fmt_bytes(mem_spill),
        "spill_disk": fmt_bytes(disk_spill),
        "gc_time": fmt_ms(gc_ms),
        "gc_ratio": fmt_ratio(metrics["gc_ratio"]),
        "cpu_efficiency": fmt_ratio(metrics["cpu_efficiency"]),
    }
    return metrics


def _issue(kind: str, severity: str, message: str, evidence: str) -> dict[str, Any]:
    return {"type": kind, "severity": severity, "message": message, "evidence": evidence}


def detect_stage_issues(m: dict[str, Any], total_cores: int | None = None) -> list[dict[str, Any]]:
    """Threshold-based anti-pattern detection on stage metrics. Evidence always carries the numbers."""
    issues: list[dict[str, Any]] = []
    sid = m["stage_id"]
    r = m["readable"]
    n = m["task_count"]
    p50 = m["duration_ms"]["p50"]
    mx = m["duration_ms"]["max"]
    overhead = m["straggler_overhead_ms"]

    if n >= 3 and m["skew_ratio"] >= 3 and overhead >= 2000:
        severity = "high" if (m["skew_ratio"] >= 5 and overhead >= 10_000) or overhead >= 60_000 else "medium"
        issues.append(
            _issue(
                "task_skew",
                severity,
                f"Stage {sid}: slowest task is {m['skew_ratio']}x the median task.",
                f"{n} tasks; median {fmt_ms(p50)}, p95 {fmt_ms(m['duration_ms']['p95'])}, max {fmt_ms(mx)}; "
                f"the straggler adds ~{fmt_ms(overhead)} to the stage.",
            )
        )

    sr = m["shuffle_read_per_task_bytes"]
    if n >= 3 and m["shuffle_read_skew_ratio"] >= 4 and sr["max"] >= 64 * MIB:
        issues.append(
            _issue(
                "data_skew",
                "high" if m["shuffle_read_skew_ratio"] >= 10 else "medium",
                f"Stage {sid}: shuffle data is unevenly distributed across tasks.",
                f"Largest task reads {r['shuffle_read_per_task_max']} vs median {r['shuffle_read_per_task_p50']} "
                f"({m['shuffle_read_skew_ratio']}x).",
            )
        )

    inp = m["input_per_task_bytes"]
    if n >= 3 and inp["p50"] > 0 and inp["max"] / inp["p50"] >= 10 and inp["max"] >= 64 * MIB:
        issues.append(
            _issue(
                "data_skew",
                "high" if inp["max"] / inp["p50"] >= 50 else "medium",
                f"Stage {sid}: input data is unevenly split across tasks.",
                f"Largest task reads {fmt_bytes(inp['max'])} of input vs median {fmt_bytes(inp['p50'])} ({inp['max'] / inp['p50']:.0f}x); "
                "likely one oversized file or partition.",
            )
        )

    spill = m["spill"]
    if spill["disk_bytes"] >= 100 * MIB:
        issues.append(
            _issue(
                "disk_spill",
                "high" if spill["disk_bytes"] >= 5 * GIB else "medium",
                f"Stage {sid}: tasks spilled to disk.",
                f"{r['spill_disk']} spilled to disk ({r['spill_memory']} in-memory size) across "
                f"{spill['tasks_spilled']} of {n} tasks.",
            )
        )
    elif spill["memory_bytes"] >= GIB:
        issues.append(
            _issue(
                "memory_spill",
                "low",
                f"Stage {sid}: significant in-memory spill.",
                f"{r['spill_memory']} memory spilled across {spill['tasks_spilled']} of {n} tasks.",
            )
        )

    gc_ratio = m["gc_ratio"]
    if gc_ratio is not None and gc_ratio >= 0.1 and m["gc_time_ms"] >= 5000:
        issues.append(
            _issue(
                "gc_pressure",
                "high" if gc_ratio >= 0.2 else "medium",
                f"Stage {sid}: JVM garbage collection takes a large share of task time.",
                f"GC {r['gc_time']} = {r['gc_ratio']} of {r['total_task_time']} task time.",
            )
        )

    shuffled = max(m["shuffle_read_bytes"], m["shuffle_write_bytes"])
    if shuffled >= 5 * GIB:
        issues.append(
            _issue(
                "shuffle_heavy",
                "high" if shuffled >= 100 * GIB else "medium",
                f"Stage {sid}: large shuffle volume.",
                f"shuffle read {r['shuffle_read']}, shuffle write {r['shuffle_write']}.",
            )
        )

    per_task_load = max(sr["p50"], m["input_per_task_bytes"]["p50"])
    if n >= 200 and p50 < 150 and m["total_task_time_ms"] > 0:
        issues.append(
            _issue(
                "small_tasks",
                "medium" if n >= 1000 else "low",
                f"Stage {sid}: many tiny tasks.",
                f"{n} tasks, median duration {fmt_ms(p50)}, median data per task {fmt_bytes(per_task_load)}.",
            )
        )
    if per_task_load >= GIB:
        issues.append(
            _issue(
                "large_partitions",
                "high" if per_task_load >= 2 * GIB else "medium",
                f"Stage {sid}: very large partitions per task.",
                f"Median task handles {fmt_bytes(per_task_load)} ({n} tasks).",
            )
        )

    read, written = m["records_read"], m["records_written"]
    if read > 0 and written >= 1_000_000 and written >= 2 * read:
        issues.append(
            _issue(
                "data_growth",
                "high" if written >= 10 * read else "medium",
                f"Stage {sid}: output rows greatly exceed input rows.",
                f"{read:,} rows in -> {written:,} rows out ({written / read:.1f}x).",
            )
        )

    eff = m["cpu_efficiency"]
    if eff is not None and m["total_task_time_ms"] >= 30_000 and eff < 0.5:
        issues.append(
            _issue(
                "low_cpu_efficiency",
                "medium",
                f"Stage {sid}: tasks use CPU for under half of their run time.",
                f"CPU time {fmt_ms(m['cpu_time_ms'])} of {r['total_task_time']} run time ({r['cpu_efficiency']}).",
            )
        )

    fetch = m["shuffle_fetch_wait_ms"]
    if m["total_task_time_ms"] > 0 and fetch >= 5000 and fetch / m["total_task_time_ms"] >= 0.2:
        issues.append(
            _issue(
                "shuffle_fetch_wait",
                "medium",
                f"Stage {sid}: tasks wait on shuffle fetches.",
                f"Fetch wait {fmt_ms(fetch)} = {fmt_ratio(fetch / m['total_task_time_ms'])} of task time.",
            )
        )

    if total_cores and n and n < total_cores * 0.5 and m["total_task_time_ms"] >= 10_000:
        issues.append(
            _issue(
                "under_parallelism",
                "medium",
                f"Stage {sid}: fewer tasks than cores.",
                f"{n} tasks for {total_cores} cores ({n / total_cores:.0%} of cores usable).",
            )
        )
    return issues


def aggregate_totals(stage_metrics: list[dict[str, Any]]) -> dict[str, Any]:
    total_run = sum(m["total_task_time_ms"] for m in stage_metrics)
    gc = sum(m["gc_time_ms"] for m in stage_metrics)
    cpu = sum(m["cpu_time_ms"] for m in stage_metrics)
    mem_spill = sum(m["spill"]["memory_bytes"] for m in stage_metrics)
    disk_spill = sum(m["spill"]["disk_bytes"] for m in stage_metrics)
    totals = {
        "stages": len(stage_metrics),
        "tasks": sum(m["task_count"] for m in stage_metrics),
        "total_task_time_ms": round(total_run, 2),
        "input_bytes": sum(m["input_bytes"] for m in stage_metrics),
        "output_bytes": sum(m["output_bytes"] for m in stage_metrics),
        "shuffle_read_bytes": sum(m["shuffle_read_bytes"] for m in stage_metrics),
        "shuffle_write_bytes": sum(m["shuffle_write_bytes"] for m in stage_metrics),
        "spill_memory_bytes": mem_spill,
        "spill_disk_bytes": disk_spill,
        "records_read": sum(m["records_read"] for m in stage_metrics),
        "records_written": sum(m["records_written"] for m in stage_metrics),
        "gc_time_ms": round(gc, 2),
        "gc_ratio": round(gc / total_run, 4) if total_run else None,
        "cpu_time_ms": round(cpu, 2),
        "cpu_efficiency": round(cpu / total_run, 4) if total_run and cpu else None,
        "stages_with_spill": sum(1 for m in stage_metrics if m["spill"]["disk_bytes"] or m["spill"]["memory_bytes"]),
        "max_skew_ratio": max((m["skew_ratio"] for m in stage_metrics), default=0.0),
    }
    totals["readable"] = {
        "total_task_time": fmt_ms(total_run),
        "input": fmt_bytes(totals["input_bytes"]),
        "output": fmt_bytes(totals["output_bytes"]),
        "shuffle_read": fmt_bytes(totals["shuffle_read_bytes"]),
        "shuffle_write": fmt_bytes(totals["shuffle_write_bytes"]),
        "spill_memory": fmt_bytes(mem_spill),
        "spill_disk": fmt_bytes(disk_spill),
        "gc_ratio": fmt_ratio(totals["gc_ratio"]),
        "cpu_efficiency": fmt_ratio(totals["cpu_efficiency"]),
    }
    return totals
